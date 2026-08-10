package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Intent states.
//
//	reserved  -- funds moved out of `available` into `pending_withdraw`,
//	             outbox row written, nothing submitted yet
//	submitted -- handed to the chain at least once; the chain may or may not
//	             have it. This is a first-class state, not an error state.
//	confirmed -- finalised on chain AND recognised in the ledger
//	expired   -- the chain provably never processed it; funds refunded
//	reverted  -- was confirmed, then a reorg removed it; compensated and
//	             pushed back to `submitted` for the resolver to re-decide
const (
	StateReserved  = "reserved"
	StateSubmitted = "submitted"
	StateConfirmed = "confirmed"
	StateExpired   = "expired"
	StateReverted  = "reverted"
)

var ErrConflict = errors.New("state conflict")

type Intent struct {
	ID        string
	Kind      string
	UserID    string
	MarketID  string
	Amount    int64
	State     string
	Attempts  int
	Deadline  time.Time
	ChainTx   string
	ExpiryHt  int64
	Height    sql.NullInt64
	BlockHash string
	LastError string
}

// WithdrawTTL is how long we will wait for a submitted withdrawal before we
// ask the chain whether it can still land. It must be comfortably longer than
// finality, or we will expire intents that are about to confirm.
const WithdrawTTL = 30 * time.Second

// RequestWithdraw reserves funds and enqueues the on-chain call in a single
// Postgres transaction: ledger move, intent row and outbox row commit or fail
// together. There is no window in which money is reserved but nothing will
// ever submit it, and no window in which we submit something the ledger has
// not accounted for.
func (s *Store) RequestWithdraw(ctx context.Context, user string, amount int64, idemKey string, expiryHeight int64) (string, bool, error) {
	if amount <= 0 {
		return "", false, errors.New("withdraw amount must be positive")
	}
	var id string
	var replayed bool
	err := tx(ctx, s.db, func(x *sql.Tx) error {
		id = NewID()
		var key any
		if idemKey != "" {
			key = idemKey
		}
		var got string
		err := x.QueryRowContext(ctx, `
            insert into intents (id, idempotency_key, kind, user_id, amount, state, deadline, expiry_height)
            values ($1, $2, 'withdraw', $3, $4, $5, now() + $6::interval, $7)
            on conflict (idempotency_key) do nothing
            returning id`,
			id, key, user, amount, StateReserved, WithdrawTTL.String(), expiryHeight).Scan(&got)
		if errors.Is(err, sql.ErrNoRows) {
			replayed = true
			var oldUser string
			var oldAmount int64
			if err := x.QueryRowContext(ctx,
				`select id, coalesce(user_id, ''), amount from intents where idempotency_key = $1`, idemKey).
				Scan(&id, &oldUser, &oldAmount); err != nil {
				return err
			}
			if oldUser != user || oldAmount != amount {
				return fmt.Errorf("%w: idempotency key reused with different withdrawal parameters", ErrConflict)
			}
			return nil
		}
		if err != nil {
			return err
		}
		// A replay must return the original result even if the account was
		// frozen afterwards. Only a brand-new reservation is blocked here.
		if halted, reason, err := s.Frozen(ctx, user); err != nil {
			return err
		} else if halted {
			return fmt.Errorf("withdrawals halted: %s", reason)
		}

		tid, _, err := PostTx(ctx, x, Transfer{
			Kind:           "withdraw.reserve",
			IdempotencyKey: "intent:" + id + ":reserve",
			Meta:           map[string]any{"intent": id, "user": user},
			Legs: []Leg{
				{Account: AvailableAcct(user), Amount: -amount},
				{Account: PendingAcct(user), Amount: amount},
			},
		})
		if err != nil {
			return err
		}
		if _, err := x.ExecContext(ctx,
			`update intents set reserve_transfer_id = $2 where id = $1`, id, tid); err != nil {
			return err
		}

		payload, _ := json.Marshal(map[string]any{
			"nonce": id, "kind": "withdraw", "user": user, "amount": amount,
			"expiry_height": expiryHeight})
		_, err = x.ExecContext(ctx, `
            insert into outbox (intent_id, op, payload) values ($1, 'withdraw', $2)
            on conflict (intent_id, op) do nothing`, id, payload)
		return err
	})
	return id, replayed, err
}

// RequestSettlement creates the on-chain settlement intent for a market. No
// funds move yet; the payout happens when the chain confirms the outcome.
func (s *Store) RequestSettlement(ctx context.Context, marketID string, outcome int64) (string, bool, error) {
	if outcome != 0 && outcome != One {
		return "", false, errors.New("settlement outcome must be 0 or 1")
	}
	var id string
	var replayed bool
	err := tx(ctx, s.db, func(x *sql.Tx) error {
		id = NewID()
		key := "settle:" + marketID
		var got string
		err := x.QueryRowContext(ctx, `
            insert into intents (id, idempotency_key, kind, market_id, amount, state, deadline)
            values ($1, $2, 'settle_market', $3, 0, $4, now() + interval '60 seconds')
            on conflict (idempotency_key) do nothing
            returning id`, id, key, marketID, StateReserved).Scan(&got)
		if errors.Is(err, sql.ErrNoRows) {
			replayed = true
			var existingOutcome sql.NullInt64
			if err := x.QueryRowContext(ctx, `
                select i.id, m.outcome
                  from intents i join markets m on m.id = i.market_id
                 where i.idempotency_key = $1`, key).Scan(&id, &existingOutcome); err != nil {
				return err
			}
			if !existingOutcome.Valid || existingOutcome.Int64 != outcome {
				return fmt.Errorf("%w: settlement key reused with a different outcome", ErrConflict)
			}
			return nil
		}
		if err != nil {
			return err
		}
		res, err := x.ExecContext(ctx,
			`update markets set state = 'settling', outcome = $2, version = version + 1
			  where id = $1 and state = 'open'`, marketID, outcome)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("%w: market %s is not open", ErrConflict, marketID)
		}
		payload, _ := json.Marshal(map[string]any{
			"nonce": id, "kind": "settle_market", "market_id": marketID, "outcome": outcome})
		_, err = x.ExecContext(ctx, `
            insert into outbox (intent_id, op, payload) values ($1, 'settle', $2)
            on conflict (intent_id, op) do nothing`, id, payload)
		return err
	})
	return id, replayed, err
}

func (s *Store) MarkSubmitted(ctx context.Context, id, chainTx, errMsg string) error {
	_, err := s.db.ExecContext(ctx, `
        update intents
           set state = case when state = 'reserved' then 'submitted' else state end,
               attempts = attempts + 1,
               chain_tx = coalesce(nullif($2, ''), chain_tx),
               last_error = nullif($3, ''),
               updated_at = now()
         where id = $1 and state in ('reserved', 'submitted')`, id, chainTx, errMsg)
	return err
}

// Confirm recognises a finalised on-chain effect in the ledger. It is
// idempotent twice over: the state guard in the UPDATE, and the transfer's
// idempotency key.
func (s *Store) Confirm(ctx context.Context, id string, height int64, blockHash string) error {
	return tx(ctx, s.db, func(x *sql.Tx) error {
		var kind, user, marketID, state string
		var amount int64
		var outcome sql.NullInt64
		if err := x.QueryRowContext(ctx, `
            select kind, coalesce(user_id, ''), coalesce(market_id, ''), state, amount
              from intents where id = $1 for update`, id).
			Scan(&kind, &user, &marketID, &state, &amount); err != nil {
			return err
		}
		if state == StateConfirmed {
			return nil
		}
		if state != StateSubmitted && state != StateReserved && state != StateReverted {
			return fmt.Errorf("%w: cannot confirm intent in state %s", ErrConflict, state)
		}

		var tid string
		switch kind {
		case "withdraw":
			var err error
			tid, _, err = PostTx(ctx, x, Transfer{
				Kind:           "withdraw.settle",
				IdempotencyKey: "intent:" + id + ":final",
				Meta:           map[string]any{"intent": id, "block": blockHash},
				Legs: []Leg{
					{Account: PendingAcct(user), Amount: -amount},
					{Account: AcctExternal, Amount: amount},
				},
			})
			if err != nil {
				return err
			}
		case "settle_market":
			if err := x.QueryRowContext(ctx,
				`select outcome from markets where id = $1 for update`, marketID).Scan(&outcome); err != nil {
				return err
			}
			if !outcome.Valid {
				return fmt.Errorf("%w: market %s has no outcome", ErrConflict, marketID)
			}
			if err := settleMarketTx(ctx, x, marketID, outcome.Int64); err != nil {
				return err
			}
		}

		_, err := x.ExecContext(ctx, `
            update intents set state = $2, block_height = $3, block_hash = $4,
                   final_transfer_id = nullif($5, '')::uuid, updated_at = now()
             where id = $1`, id, StateConfirmed, height, blockHash, tid)
		return err
	})
}

// Expire refunds a reserved withdrawal only after the caller has observed the
// chain beyond the contract-enforced expiry height. Passing that observation
// into the store makes an early refund impossible even if a worker regresses.
func (s *Store) Expire(ctx context.Context, id string, observedHeight int64) error {
	return tx(ctx, s.db, func(x *sql.Tx) error {
		var kind, user, state string
		var amount, expiryHeight int64
		if err := x.QueryRowContext(ctx, `
			select kind, coalesce(user_id, ''), state, amount, expiry_height
		  from intents where id = $1 for update`, id).
			Scan(&kind, &user, &state, &amount, &expiryHeight); err != nil {
			return err
		}
		if state == StateExpired {
			return nil
		}
		if state != StateSubmitted && state != StateReserved {
			return fmt.Errorf("%w: cannot expire intent in state %s", ErrConflict, state)
		}
		if kind == "withdraw" {
			if expiryHeight <= 0 || observedHeight <= expiryHeight {
				return fmt.Errorf("%w: withdrawal %s has not reached expiry height", ErrConflict, id)
			}
			if _, _, err := PostTx(ctx, x, Transfer{
				Kind:           "withdraw.refund",
				IdempotencyKey: "intent:" + id + ":refund",
				Meta:           map[string]any{"intent": id},
				Legs: []Leg{
					{Account: PendingAcct(user), Amount: -amount},
					{Account: AvailableAcct(user), Amount: amount},
				},
			}); err != nil {
				return err
			}
		}
		if kind == "settle_market" {
			if _, err := x.ExecContext(ctx,
				`update outbox set state = 'pending', lease_until = null, next_attempt_at = now() where intent_id = $1`, id); err != nil {
				return err
			}
			// A settlement must eventually happen, so it remains in `settling`
			// while the outbox retries. Re-opening the market here would allow a
			// new position to be opened against an already-decided outcome.
			_, err := x.ExecContext(ctx,
				`update intents set deadline = now() + interval '60 seconds', updated_at = now() where id = $1`, id)
			return err
		}
		_, err := x.ExecContext(ctx,
			`update intents set state = $2, updated_at = now() where id = $1`, id, StateExpired)
		return err
	})
}

// Revert undoes a confirmation whose block was reorged away. It writes a
// compensating transfer -- the ledger is append-only, so nothing is deleted --
// and returns the intent to `submitted` so the resolver decides again.
func (s *Store) Revert(ctx context.Context, id, orphanHash string) error {
	return tx(ctx, s.db, func(x *sql.Tx) error {
		var kind, user, state string
		var amount int64
		if err := x.QueryRowContext(ctx, `
            select kind, coalesce(user_id, ''), state, amount
              from intents where id = $1 for update`, id).Scan(&kind, &user, &state, &amount); err != nil {
			return err
		}
		if state != StateConfirmed {
			return nil
		}
		if kind == "withdraw" {
			if _, _, err := PostTx(ctx, x, Transfer{
				Kind:           "withdraw.settle.reverted",
				IdempotencyKey: "intent:" + id + ":revert:" + orphanHash,
				Meta:           map[string]any{"intent": id, "orphaned_block": orphanHash},
				Legs: []Leg{
					{Account: AcctExternal, Amount: -amount},
					{Account: PendingAcct(user), Amount: amount},
				},
			}); err != nil {
				return err
			}
		}
		if _, err := x.ExecContext(ctx, `
            update intents set state = $2, block_height = null, block_hash = null,
                   deadline = now() + $3::interval, updated_at = now()
             where id = $1`, id, StateSubmitted, WithdrawTTL.String()); err != nil {
			return err
		}
		_, err := x.ExecContext(ctx,
			`update outbox set state = 'pending', lease_until = null, next_attempt_at = now() where intent_id = $1`, id)
		return err
	})
}

// InFlight returns every intent that is not in a terminal state. Anything in
// here is a claim on money that the chain may or may not agree with, and it is
// exactly the set the reconciler must be able to explain.
func (s *Store) InFlight(ctx context.Context) ([]Intent, error) {
	rows, err := s.db.QueryContext(ctx, `
        select id, kind, coalesce(user_id, ''), coalesce(market_id, ''), amount, state,
               attempts, deadline, expiry_height, coalesce(chain_tx, ''), coalesce(last_error, '')
          from intents where state in ('reserved', 'submitted') order by created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Intent
	for rows.Next() {
		var i Intent
		if err := rows.Scan(&i.ID, &i.Kind, &i.UserID, &i.MarketID, &i.Amount, &i.State,
			&i.Attempts, &i.Deadline, &i.ExpiryHt, &i.ChainTx, &i.LastError); err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

func (s *Store) ConfirmedIntentsInBlock(ctx context.Context, blockHash string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`select id from intents where state = 'confirmed' and block_hash = $1`, blockHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
