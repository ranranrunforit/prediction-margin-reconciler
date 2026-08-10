package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// StoredEvent is a chain event as it lives in our inbox.
type StoredEvent struct {
	EventID   string
	FactID    string
	Seq       int64
	Kind      string
	User      string
	Amount    int64
	MarketID  string
	Height    int64
	BlockHash string
	Status    string
}

func (s *Store) Cursor(ctx context.Context) (int64, error) {
	var v int64
	err := s.db.QueryRowContext(ctx, `select last_seq from chain_cursor where id = 1`).Scan(&v)
	return v, err
}

// AdvanceCursor only moves forward, and the inbox worker will never call it
// past a gap in the sequence -- a withheld event must be waited for, not
// skipped, or its effect would be lost forever.
func (s *Store) AdvanceCursor(ctx context.Context, seq int64) error {
	_, err := s.db.ExecContext(ctx,
		`update chain_cursor set last_seq = greatest(last_seq, $1) where id = 1`, seq)
	return err
}

// RecordEvent inserts an observed event. The primary key on event_id is the
// dedupe: the simulator delivers duplicates freely and they land here once.
func (s *Store) RecordEvent(ctx context.Context, e StoredEvent) (bool, error) {
	payload, _ := json.Marshal(map[string]any{
		"fact_id": e.FactID, "user": e.User, "amount": e.Amount, "market_id": e.MarketID})
	res, err := s.db.ExecContext(ctx, `
        insert into chain_events (event_id, chain_seq, kind, payload, block_height, block_hash)
        values ($1, $2, $3, $4, $5, $6)
        on conflict (event_id) do nothing`,
		e.EventID, e.Seq, e.Kind, payload, e.Height, e.BlockHash)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) PendingEvents(ctx context.Context, limit int) ([]StoredEvent, error) {
	rows, err := s.db.QueryContext(ctx, `
        select event_id, chain_seq, kind, payload, block_height, block_hash, status
          from chain_events where status = 'received' order by chain_seq limit $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StoredEvent
	for rows.Next() {
		var e StoredEvent
		var raw []byte
		if err := rows.Scan(&e.EventID, &e.Seq, &e.Kind, &raw, &e.Height, &e.BlockHash, &e.Status); err != nil {
			return nil, err
		}
		var p struct {
			FactID   string `json:"fact_id"`
			User     string `json:"user"`
			Amount   int64  `json:"amount"`
			MarketID string `json:"market_id"`
		}
		_ = json.Unmarshal(raw, &p)
		e.FactID, e.User, e.Amount, e.MarketID = p.FactID, p.User, p.Amount, p.MarketID
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) SetEventStatus(ctx context.Context, eventID, status string) error {
	_, err := s.db.ExecContext(ctx,
		`update chain_events set status = $2, applied_at = now() where event_id = $1`, eventID, status)
	return err
}

// ApplyDeposit credits a deposit optimistically, at one confirmation rather
// than at finality, so a depositor can trade immediately. That is a deliberate
// product tradeoff and it is the reason this system has to handle reorgs at
// all: see RevertBlock below.
func (s *Store) ApplyDeposit(ctx context.Context, e StoredEvent) error {
	return tx(ctx, s.db, func(x *sql.Tx) error {
		tid, replayed, err := PostTx(ctx, x, Transfer{
			Kind: "deposit.provisional",
			// Keyed on the *event*, not the fact: if this deposit is reorged
			// out and re-included in another block, that is a genuinely new
			// effect, because we will have compensated the first one.
			IdempotencyKey: "deposit:" + e.EventID,
			Meta: map[string]any{"fact": e.FactID, "block": e.BlockHash,
				"height": e.Height, "provisional": true},
			Legs: []Leg{
				{Account: AcctExternal, Amount: -e.Amount},
				{Account: AvailableAcct(e.User), Amount: e.Amount},
			},
		})
		if err != nil {
			return err
		}
		if _, err := x.ExecContext(ctx, `
            insert into chain_facts (event_id, fact_id, kind, amount, transfer_id, block_height, block_hash)
            values ($1, $2, $3, $4, $5, $6, $7) on conflict (event_id) do nothing`,
			e.EventID, e.FactID, e.Kind, e.Amount, tid, e.Height, e.BlockHash); err != nil {
			return err
		}
		_, err = x.ExecContext(ctx,
			`update chain_events set status = $2, applied_at = now() where event_id = $1`,
			e.EventID, map[bool]string{true: "applied", false: "applied"}[replayed])
		return err
	})
}

// FactRef identifies a ledger effect and the block we derived it from.
type FactRef struct {
	EventID   string
	FactID    string
	Height    int64
	BlockHash string
	Amount    int64
}

// ProvisionalFacts are credits granted on blocks that are not final yet. Every
// one of them is re-checked against the chain on each inbox pass: promoted if
// its block is still canonical and now final, compensated if it is gone.
func (s *Store) ProvisionalFacts(ctx context.Context) ([]FactRef, error) {
	rows, err := s.db.QueryContext(ctx, `
        select event_id, fact_id, block_height, block_hash, amount from chain_facts
         where finalized = false and reverted = false order by block_height`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FactRef
	for rows.Next() {
		var f FactRef
		if err := rows.Scan(&f.EventID, &f.FactID, &f.Height, &f.BlockHash, &f.Amount); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// MarkFactFinalized promotes one fact. Deliberately per-fact rather than a bulk
// update by height: a bulk update would happily finalise a fact whose block was
// reorged away, and the reorg detector would then never look at it again. That
// bug shows up as a double credit, and it is exactly the kind of thing the
// chaos harness is for.
func (s *Store) MarkFactFinalized(ctx context.Context, eventID string) error {
	_, err := s.db.ExecContext(ctx,
		`update chain_facts set finalized = true where event_id = $1 and reverted = false`, eventID)
	return err
}

// UnfinalizedApplied is the set of ledger credits we have granted on blocks
// that are not final yet. It is one of the three terms that explain a
// legitimate gap between the ledger and the chain.
//
// The cut-off is the chain's own finalised height, passed in fresh, rather than
// our stored `finalized` flag. Deriving an explanation from a cached flag makes
// the explanation only as current as the last worker pass, and a reconciler
// whose arithmetic lags reality raises false alarms -- which is worse than
// useless, because people mute it.
func (s *Store) UnfinalizedApplied(ctx context.Context, finalizedHeight int64) (int64, error) {
	var v int64
	err := s.db.QueryRowContext(ctx, `
        select coalesce(sum(amount), 0) from chain_facts
         where reverted = false and kind = 'deposit' and block_height > $1`, finalizedHeight).Scan(&v)
	return v, err
}

// AppliedFactIDs is used by the reconciler to spot finalised chain facts that
// never reached us as events (a permanently dropped event) and heal from
// chain state alone.
func (s *Store) AppliedFactIDs(ctx context.Context) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, `select fact_id from chain_facts where reverted = false`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

// ConfirmedBlocks lists blocks we recognised a withdrawal in. Under a chain
// with real finality these can never be orphaned, because we only confirm at
// finality -- the check exists for chains where finality is probabilistic.
func (s *Store) ConfirmedBlocks(ctx context.Context) (map[int64]string, error) {
	out := map[int64]string{}
	rows, err := s.db.QueryContext(ctx, `
        select distinct block_height, block_hash from intents
         where state = 'confirmed' and block_height is not null`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var h int64
		var hash string
		if err := rows.Scan(&h, &hash); err != nil {
			return nil, err
		}
		out[h] = hash
	}
	return out, rows.Err()
}

// RevertBlock compensates every ledger effect derived from an orphaned block.
// Nothing is deleted: a reversal is a new transfer in the opposite direction,
// so the audit trail keeps both the mistake and the correction.
//
// The interesting case is a user who already spent a provisional credit. We
// cannot claw back money that is not there, so the insurance fund absorbs the
// shortfall and the account is frozen for manual review. Silently letting a
// balance go negative would be the actual bug.
func (s *Store) RevertBlock(ctx context.Context, blockHash string) (int, error) {
	rows, err := s.db.QueryContext(ctx, `
        select cf.event_id, cf.fact_id, cf.kind, cf.amount, ce.payload
          from chain_facts cf join chain_events ce on ce.event_id = cf.event_id
         where cf.block_hash = $1 and cf.reverted = false`, blockHash)
	if err != nil {
		return 0, err
	}
	type item struct {
		eventID, factID, kind, user string
		amount                      int64
	}
	var items []item
	for rows.Next() {
		var it item
		var raw []byte
		if err := rows.Scan(&it.eventID, &it.factID, &it.kind, &it.amount, &raw); err != nil {
			rows.Close()
			return 0, err
		}
		var p struct {
			User string `json:"user"`
		}
		_ = json.Unmarshal(raw, &p)
		it.user = p.User
		items = append(items, it)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	n := 0
	for _, it := range items {
		if it.kind != "deposit" {
			continue
		}
		err := tx(ctx, s.db, func(x *sql.Tx) error {
			var avail int64
			if err := x.QueryRowContext(ctx,
				`select coalesce((select balance from account_balances
                    where account_code = $1), 0) for update`, AvailableAcct(it.user)).Scan(&avail); err != nil {
				return err
			}
			clawback := it.amount
			if avail < clawback {
				clawback = avail
			}
			if clawback < 0 {
				clawback = 0
			}
			shortfall := it.amount - clawback
			if _, _, err := PostTx(ctx, x, Transfer{
				Kind:           "deposit.reverted",
				IdempotencyKey: "deposit:" + it.eventID + ":revert",
				Meta: map[string]any{"fact": it.factID, "orphaned_block": blockHash,
					"shortfall": shortfall},
				Legs: []Leg{
					{Account: AvailableAcct(it.user), Amount: -clawback},
					{Account: AcctInsurance, Amount: -shortfall},
					{Account: AcctExternal, Amount: it.amount},
				},
			}); err != nil {
				return err
			}
			if _, err := x.ExecContext(ctx,
				`update chain_facts set reverted = true where event_id = $1`, it.eventID); err != nil {
				return err
			}
			if _, err := x.ExecContext(ctx,
				`update chain_events set status = 'orphaned' where event_id = $1`, it.eventID); err != nil {
				return err
			}
			if shortfall > 0 {
				if _, err := x.ExecContext(ctx, `
                    insert into freezes (subject, reason) values ($1, $2)
                    on conflict (subject) do nothing`, it.user,
					fmt.Sprintf("spent %s of a provisional deposit orphaned in %s", USD(shortfall), blockHash)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// ---------------------------------------------------------------- ops

func (s *Store) Freeze(ctx context.Context, subject, reason string) error {
	_, err := s.db.ExecContext(ctx,
		`insert into freezes (subject, reason) values ($1, $2) on conflict (subject) do nothing`,
		subject, reason)
	return err
}

func (s *Store) Unfreeze(ctx context.Context, subject string) error {
	_, err := s.db.ExecContext(ctx, `delete from freezes where subject = $1`, subject)
	return err
}

// Frozen reports whether a specific user, or the whole system, is halted.
func (s *Store) Frozen(ctx context.Context, subject string) (bool, string, error) {
	var reason string
	err := s.db.QueryRowContext(ctx,
		`select reason from freezes where subject in ('*', $1) order by subject limit 1`, subject).Scan(&reason)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	return true, reason, nil
}

func (s *Store) Freezes(ctx context.Context) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `select subject, reason from freezes order by subject`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}
