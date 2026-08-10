package core

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Risk parameters. A prediction-market share is worth 0 or 1 at settlement, so
// the worst case for a long is `size * mark` and for a short `size * (1-mark)`.
// Margin is a fraction of that worst case.
const (
	InitialMarginRate     = 250_000 // 0.25 -> 4x max leverage
	MaintenanceMarginRate = 150_000 // 0.15
	LiquidationPenalty    = 20_000  // 0.02 of collateral, to the insurance fund
)

type Position struct {
	ID         string
	UserID     string
	MarketID   string
	Side       string
	Size       int64
	EntryPrice int64
	Collateral int64
	State      string
	Version    int64
}

type Market struct {
	ID       string
	Question string
	Mark     int64
	State    string
	Outcome  sql.NullInt64
}

func mul(a, b int64) int64 { return a * b / One }

// worstCase is the notional the position can lose if the market resolves
// against it.
func worstCase(side string, size, price int64) int64 {
	if side == "long" {
		return mul(size, price)
	}
	return mul(size, One-price)
}

// pnl is mark-to-market profit in micro-USD.
func pnl(side string, size, entry, mark int64) int64 {
	if side == "long" {
		return mul(size, mark-entry)
	}
	return mul(size, entry-mark)
}

func RequiredMargin(side string, size, mark int64) int64 {
	return mul(worstCase(side, size, mark), MaintenanceMarginRate)
}

func (s *Store) SeedMarket(ctx context.Context, id, question string, mark int64) error {
	_, err := s.db.ExecContext(ctx, `
        insert into markets (id, question, mark_price, state) values ($1, $2, $3, 'open')
        on conflict (id) do nothing`, id, question, mark)
	return err
}

func (s *Store) SetMark(ctx context.Context, id string, mark int64) error {
	if mark < 1000 {
		mark = 1000
	}
	if mark > One-1000 {
		mark = One - 1000
	}
	_, err := s.db.ExecContext(ctx,
		`update markets set mark_price = $2, version = version + 1 where id = $1 and state = 'open'`, id, mark)
	return err
}

// OpenMarketIDs lists markets that can currently be traded.
func (s *Store) OpenMarketIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `select id from markets where state = 'open' order by id`)
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

func (s *Store) Markets(ctx context.Context) ([]Market, error) {
	rows, err := s.db.QueryContext(ctx,
		`select id, question, mark_price, state, outcome from markets order by id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Market
	for rows.Next() {
		var m Market
		if err := rows.Scan(&m.ID, &m.Question, &m.Mark, &m.State, &m.Outcome); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// OpenPosition locks initial margin out of `available` into `margin`. Money
// never leaves the vault, so this cannot affect the on-chain reconciliation --
// which is exactly why trading can be fast while withdrawals are careful.
func (s *Store) OpenPosition(ctx context.Context, user, marketID, side string, size int64, idemKey string) (string, error) {
	if side != "long" && side != "short" {
		return "", errors.New("side must be long or short")
	}
	if size <= 0 {
		return "", errors.New("size must be positive")
	}
	var pid string
	err := tx(ctx, s.db, func(x *sql.Tx) error {
		// The request id belongs to the position, not merely to its ledger
		// transfer.  Replaying an accepted request must return the original
		// position instead of trying to open a second one.
		if idemKey != "" {
			var oldUser, oldMarket, oldSide string
			var oldSize int64
			err := x.QueryRowContext(ctx, `
                select id, user_id, market_id, side, size from positions
                 where idempotency_key = $1 for update`, idemKey).
				Scan(&pid, &oldUser, &oldMarket, &oldSide, &oldSize)
			if err == nil {
				if oldUser != user || oldMarket != marketID || oldSide != side || oldSize != size {
					return fmt.Errorf("%w: idempotency key reused with different position parameters", ErrConflict)
				}
				return nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}

		var mark int64
		var state string
		if err := x.QueryRowContext(ctx,
			`select mark_price, state from markets where id = $1 for update`, marketID).Scan(&mark, &state); err != nil {
			return err
		}
		if state != "open" {
			return fmt.Errorf("%w: market %s is %s", ErrConflict, marketID, state)
		}
		collateral := mul(worstCase(side, size, mark), InitialMarginRate)
		if collateral <= 0 {
			return errors.New("position too small")
		}
		pid = NewID()
		if _, err := x.ExecContext(ctx, `
            insert into positions (id, user_id, market_id, side, size, entry_price, collateral, idempotency_key)
            values ($1, $2, $3, $4, $5, $6, $7, nullif($8, ''))`,
			pid, user, marketID, side, size, mark, collateral, idemKey); err != nil {
			return err
		}
		_, _, err := PostTx(ctx, x, Transfer{
			Kind:           "position.open",
			IdempotencyKey: "pos:" + pid + ":open",
			Meta:           map[string]any{"position": pid, "market": marketID},
			Legs: []Leg{
				{Account: AvailableAcct(user), Amount: -collateral},
				{Account: MarginAcct(user), Amount: collateral},
			},
		})
		return err
	})
	return pid, err
}

// ClosePosition settles a position at `mark`. The optimistic version check is
// what makes it safe for a user-initiated close and a liquidation to race:
// exactly one of them updates the row, the other gets ErrConflict and stops.
func (s *Store) ClosePosition(ctx context.Context, positionID string, mark int64, reason string) error {
	return tx(ctx, s.db, func(x *sql.Tx) error {
		var p Position
		err := x.QueryRowContext(ctx, `
            select id, user_id, market_id, side, size, entry_price, collateral, state, version
              from positions where id = $1`, positionID).
			Scan(&p.ID, &p.UserID, &p.MarketID, &p.Side, &p.Size, &p.EntryPrice,
				&p.Collateral, &p.State, &p.Version)
		if err != nil {
			return err
		}
		if p.State != "open" {
			return fmt.Errorf("%w: position %s already %s", ErrConflict, positionID, p.State)
		}
		newState := "closed"
		penalty := int64(0)
		if reason == "liquidation" {
			newState = "liquidated"
			penalty = mul(p.Collateral, LiquidationPenalty)
		}

		res, err := x.ExecContext(ctx, `
            update positions set state = $2, version = version + 1, updated_at = now()
             where id = $1 and version = $3 and state = 'open'`, positionID, newState, p.Version)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("%w: lost the race to close %s", ErrConflict, positionID)
		}

		profit := pnl(p.Side, p.Size, p.EntryPrice, mark)
		payout := p.Collateral + profit - penalty
		if payout < 0 {
			// Loss is capped at posted collateral; the insurance fund keeps
			// the difference rather than chasing the user for it.
			payout = 0
		}
		houseLeg := p.Collateral - payout // may be negative when the user won

		_, _, err = PostTx(ctx, x, Transfer{
			Kind:           "position.close." + newState,
			IdempotencyKey: "pos:" + positionID + ":close",
			Meta: map[string]any{"position": positionID, "mark": mark,
				"pnl": profit, "reason": reason},
			Legs: []Leg{
				{Account: MarginAcct(p.UserID), Amount: -p.Collateral},
				{Account: AvailableAcct(p.UserID), Amount: payout},
				{Account: AcctInsurance, Amount: houseLeg},
			},
		})
		return err
	})
}

// AtRisk returns open positions whose equity has fallen below maintenance
// margin at the supplied mark prices.
func (s *Store) AtRisk(ctx context.Context, marks map[string]int64) ([]Position, error) {
	rows, err := s.db.QueryContext(ctx, `
        select id, user_id, market_id, side, size, entry_price, collateral, version
          from positions where state = 'open'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Position
	for rows.Next() {
		var p Position
		if err := rows.Scan(&p.ID, &p.UserID, &p.MarketID, &p.Side, &p.Size,
			&p.EntryPrice, &p.Collateral, &p.Version); err != nil {
			return nil, err
		}
		mark, ok := marks[p.MarketID]
		if !ok {
			continue
		}
		equity := p.Collateral + pnl(p.Side, p.Size, p.EntryPrice, mark)
		if equity < RequiredMargin(p.Side, p.Size, mark) {
			out = append(out, p)
		}
	}
	return out, rows.Err()
}

// settleMarketTx closes every open position at the resolved outcome. It runs
// inside the intent-confirmation transaction, so a market flips to `settled`
// and its positions pay out atomically -- or not at all.
func settleMarketTx(ctx context.Context, x *sql.Tx, marketID string, outcome int64) error {
	rows, err := x.QueryContext(ctx, `
		select id, user_id, side, size, entry_price, collateral
          from positions where market_id = $1 and state = 'open' order by id for update`, marketID)
	if err != nil {
		return err
	}
	type pos struct {
		id, user, side          string
		size, entry, collateral int64
	}
	var ps []pos
	for rows.Next() {
		var p pos
		if err := rows.Scan(&p.id, &p.user, &p.side, &p.size, &p.entry, &p.collateral); err != nil {
			rows.Close()
			return err
		}
		ps = append(ps, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, p := range ps {
		res, err := x.ExecContext(ctx, `
            update positions set state = 'closed', version = version + 1, updated_at = now()
			 where id = $1 and state = 'open'`, p.id)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("%w: position %s changed during settlement", ErrConflict, p.id)
		}
		profit := pnl(p.side, p.size, p.entry, outcome)
		payout := p.collateral + profit
		if payout < 0 {
			payout = 0
		}
		if _, _, err := PostTx(ctx, x, Transfer{
			Kind:           "market.settle",
			IdempotencyKey: "pos:" + p.id + ":settle",
			Meta:           map[string]any{"market": marketID, "outcome": outcome, "pnl": profit},
			Legs: []Leg{
				{Account: MarginAcct(p.user), Amount: -p.collateral},
				{Account: AvailableAcct(p.user), Amount: payout},
				{Account: AcctInsurance, Amount: p.collateral - payout},
			},
		}); err != nil {
			return err
		}
	}
	_, err = x.ExecContext(ctx,
		`update markets set state = 'settled', version = version + 1 where id = $1`, marketID)
	return err
}

// MarginTotal is the sum of every user margin account.
func (s *Store) MarginTotal(ctx context.Context) (int64, error) {
	var v int64
	err := s.db.QueryRowContext(ctx,
		`select coalesce(sum(balance), 0) from account_balances
          where account_code like 'user:%:margin'`).Scan(&v)
	return v, err
}

// OpenCollateral is the sum of collateral recorded on open positions. It must
// equal MarginTotal -- invariant I4.
func (s *Store) OpenCollateral(ctx context.Context) (int64, error) {
	var v int64
	err := s.db.QueryRowContext(ctx,
		`select coalesce(sum(collateral), 0) from positions where state = 'open'`).Scan(&v)
	return v, err
}
