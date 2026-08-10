package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Account code conventions. The prefix is the kind, so a code is enough to
// classify an account without a join.
func AvailableAcct(user string) string { return "user:" + user + ":available" }
func PendingAcct(user string) string   { return "user:" + user + ":pending_withdraw" }
func MarginAcct(user string) string    { return "user:" + user + ":margin" }

const (
	AcctExternal  = "external"
	AcctInsurance = "insurance"
	AcctFees      = "fees"
)

func kindOf(code string) string {
	switch {
	case code == AcctExternal:
		return "external"
	case code == AcctInsurance:
		return "insurance"
	case code == AcctFees:
		return "fees"
	case strings.HasSuffix(code, ":available"):
		return "available"
	case strings.HasSuffix(code, ":pending_withdraw"):
		return "pending_withdraw"
	case strings.HasSuffix(code, ":margin"):
		return "margin"
	}
	return "other"
}

// mustStayNonNegative is true for the account kinds that represent user
// custody. `external` is the counterparty and is expected to go deeply
// negative; `insurance` is the simulator's market-maker counterparty and can
// go negative when a user wins.
func mustStayNonNegative(code string) bool {
	switch kindOf(code) {
	case "available", "pending_withdraw", "margin":
		return true
	}
	return false
}

var ErrInsufficientFunds = errors.New("insufficient funds")

// Leg is one side of a transfer. Positive credits the account.
type Leg struct {
	Account string
	Amount  int64
}

// Transfer is an atomic, balanced set of legs.
type Transfer struct {
	Kind           string
	IdempotencyKey string // optional; when set, replaying it is a guaranteed no-op
	Meta           map[string]any
	Legs           []Leg
}

type Store struct{ db *sql.DB }

func NewStore(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) DB() *sql.DB { return s.db }

// Post writes a transfer in its own transaction.
func (s *Store) Post(ctx context.Context, t Transfer) (id string, replayed bool, err error) {
	err = tx(ctx, s.db, func(x *sql.Tx) error {
		id, replayed, err = PostTx(ctx, x, t)
		return err
	})
	return
}

// PostTx writes a transfer inside a caller-supplied transaction, so a ledger
// move and an outbox row (or an intent state change) commit together. This is
// the whole reason we never dual-write to Postgres and the chain.
func PostTx(ctx context.Context, x *sql.Tx, t Transfer) (string, bool, error) {
	if len(t.Legs) == 0 {
		return "", false, errors.New("transfer has no legs")
	}
	var sum int64
	for _, lg := range t.Legs {
		sum += lg.Amount
	}
	if sum != 0 {
		// I1 is enforced at write time, not just checked afterwards.
		return "", false, fmt.Errorf("unbalanced transfer %q: legs sum to %d", t.Kind, sum)
	}

	meta, _ := json.Marshal(orEmpty(t.Meta))
	id := NewID()

	var key any
	if t.IdempotencyKey != "" {
		key = t.IdempotencyKey
	}

	// The unique index on transfers.idempotency_key is the entire idempotency
	// mechanism. A concurrent duplicate blocks here until the winner commits,
	// then falls into the conflict branch. No application-level locking.
	var got string
	err := x.QueryRowContext(ctx, `
        insert into transfers (id, kind, idempotency_key, meta)
        values ($1, $2, $3, $4)
        on conflict (idempotency_key) do nothing
        returning id`, id, t.Kind, key, meta).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		var existing string
		if err := x.QueryRowContext(ctx,
			`select id from transfers where idempotency_key = $1`, t.IdempotencyKey).Scan(&existing); err != nil {
			return "", false, fmt.Errorf("idempotent replay lookup: %w", err)
		}
		return existing, true, nil // no entries written: replay is a true no-op
	}
	if err != nil {
		return "", false, err
	}

	// Canonical lock order (sorted account code) so concurrent transfers
	// touching the same pair of accounts can never deadlock.
	codes := make([]string, 0, len(t.Legs))
	seen := map[string]bool{}
	for _, lg := range t.Legs {
		if !seen[lg.Account] {
			seen[lg.Account] = true
			codes = append(codes, lg.Account)
		}
	}
	sort.Strings(codes)
	for _, c := range codes {
		if err := ensureAccount(ctx, x, c); err != nil {
			return "", false, err
		}
	}
	for _, c := range codes {
		var b int64
		if err := x.QueryRowContext(ctx,
			`select balance from account_balances where account_code = $1 for update`, c).Scan(&b); err != nil {
			return "", false, fmt.Errorf("lock %s: %w", c, err)
		}
	}

	delta := map[string]int64{}
	for _, lg := range t.Legs {
		if lg.Amount == 0 {
			continue
		}
		if _, err := x.ExecContext(ctx,
			`insert into entries (transfer_id, account_code, amount) values ($1, $2, $3)`,
			id, lg.Account, lg.Amount); err != nil {
			return "", false, err
		}
		delta[lg.Account] += lg.Amount
	}
	for _, c := range codes {
		d := delta[c]
		if d == 0 {
			continue
		}
		var nb int64
		if err := x.QueryRowContext(ctx, `
            update account_balances
               set balance = balance + $2, version = version + 1
             where account_code = $1
            returning balance`, c, d).Scan(&nb); err != nil {
			return "", false, err
		}
		if nb < 0 && mustStayNonNegative(c) {
			return "", false, fmt.Errorf("%w: %s would go to %s", ErrInsufficientFunds, c, USD(nb))
		}
	}
	return id, false, nil
}

func ensureAccount(ctx context.Context, x *sql.Tx, code string) error {
	owner := any(nil)
	if parts := strings.Split(code, ":"); len(parts) == 3 {
		owner = parts[1]
	}
	if _, err := x.ExecContext(ctx,
		`insert into accounts (code, kind, owner) values ($1, $2, $3) on conflict (code) do nothing`,
		code, kindOf(code), owner); err != nil {
		return err
	}
	_, err := x.ExecContext(ctx,
		`insert into account_balances (account_code, balance) values ($1, 0) on conflict do nothing`, code)
	return err
}

func (s *Store) Balance(ctx context.Context, code string) (int64, error) {
	var b int64
	err := s.db.QueryRowContext(ctx,
		`select coalesce((select balance from account_balances where account_code = $1), 0)`, code).Scan(&b)
	return b, err
}

// InternalTotal is the sum of every account that represents money we are
// custodying: everything except the `external` counterparty. This is the
// number that must line up with the on-chain vault balance.
func (s *Store) InternalTotal(ctx context.Context) (int64, error) {
	var v int64
	err := s.db.QueryRowContext(ctx,
		`select coalesce(sum(balance), 0) from account_balances where account_code <> 'external'`).Scan(&v)
	return v, err
}

func orEmpty(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}
