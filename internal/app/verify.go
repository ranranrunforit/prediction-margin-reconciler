package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/ranranrunforit/prediction-margin-reconciler/internal/core"
)

// Violation is one broken invariant.
type Violation struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Detail string `json:"detail"`
}

func (v Violation) String() string { return fmt.Sprintf("%s (%s): %s", v.ID, v.Name, v.Detail) }

// Invariants is the contract this system claims to keep. Everything else in
// the repo is machinery for making these true; the chaos harness is machinery
// for trying to break them.
var Invariants = []struct{ ID, Name string }{
	{"I0", "cached balance equals the sum of its entries"},
	{"I1", "every transfer's entries sum to zero"},
	{"I2", "ledger - chain_finalized equals the explainable in-flight delta"},
	{"I3", "no user available, pending-withdrawal, or margin account is ever negative"},
	{"I4", "margin balances equal the collateral on open positions"},
	{"I5", "each idempotency key maps to exactly one transfer"},
	{"I6", "no in-flight intent is orphaned; funds are never left dangling"},
	{"I7", "per-market escrow converges on ledger collateral and never stalls"},
}

// Verify checks every invariant. In strict mode it additionally requires the
// system to be fully settled: no in-flight intents, no unapplied events, and
// the ledger exactly equal to the on-chain vault. Strict mode is what runs at
// the end of a chaos run.
func (a *App) Verify(ctx context.Context, strict bool) ([]Violation, error) {
	var vs []Violation
	add := func(id, name, format string, args ...any) {
		vs = append(vs, Violation{ID: id, Name: name, Detail: fmt.Sprintf(format, args...)})
	}
	nameOf := func(id string) string {
		for _, inv := range Invariants {
			if inv.ID == id {
				return inv.Name
			}
		}
		return ""
	}

	// I0 -- the denormalised balance cannot drift from the append-only log.
	rows, err := a.db.QueryContext(ctx, `
        select b.account_code, b.balance, coalesce(e.total, 0)
          from account_balances b
          left join (select account_code, sum(amount) total from entries group by account_code) e
            on e.account_code = b.account_code
         where b.balance <> coalesce(e.total, 0)`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var code string
		var bal, total int64
		if err := rows.Scan(&code, &bal, &total); err != nil {
			rows.Close()
			return nil, err
		}
		add("I0", nameOf("I0"), "%s: cached %s, entries sum to %s", code, core.USD(bal), core.USD(total))
	}
	rows.Close()

	// I1 -- double entry. Enforced at write time; verified anyway, because an
	// invariant you only enforce is an invariant you cannot prove.
	rows, err = a.db.QueryContext(ctx, `
        select transfer_id, sum(amount) from entries group by transfer_id having sum(amount) <> 0`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id string
		var sum int64
		if err := rows.Scan(&id, &sum); err != nil {
			rows.Close()
			return nil, err
		}
		add("I1", nameOf("I1"), "transfer %s is unbalanced by %s", id, core.USD(sum))
	}
	rows.Close()

	// I2 -- the reconciliation invariant.
	rep, err := a.Reconcile(ctx)
	if err != nil {
		return nil, err
	}
	if rep.Unexplained != 0 {
		add("I2", nameOf("I2"),
			"ledger %s, chain %s, explained %s, unexplained %s (%s)",
			core.USD(rep.LedgerInternal), core.USD(rep.ChainFinalized),
			core.USD(rep.Explained), core.USD(rep.Unexplained), rep.Status)
	}

	// I3 -- customer balances. `external` is the counterparty and is expected
	// to be deeply negative. The simulator treats insurance as the market-maker
	// counterparty, so it can be negative after a winning position; it is not a
	// user-custody balance.
	rows, err = a.db.QueryContext(ctx, `
        select account_code, balance from account_balances
         where balance < 0 and account_code <> 'external' order by account_code`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var code string
		var bal int64
		if err := rows.Scan(&code, &bal); err != nil {
			rows.Close()
			return nil, err
		}
		if code == core.AcctInsurance {
			continue
		}
		add("I3", nameOf("I3"), "%s is %s", code, core.USD(bal))
	}
	rows.Close()

	// I4 -- margin accounting matches position records.
	margin, err := a.Store.MarginTotal(ctx)
	if err != nil {
		return nil, err
	}
	collateral, err := a.Store.OpenCollateral(ctx)
	if err != nil {
		return nil, err
	}
	if margin != collateral {
		add("I4", nameOf("I4"), "margin accounts hold %s, open positions claim %s",
			core.USD(margin), core.USD(collateral))
	}

	// I5 -- idempotency. The unique index makes this structurally impossible;
	// the check exists so a future migration that drops it fails loudly.
	var dupes int
	if err := a.db.QueryRowContext(ctx, `
        select count(*) from (
            select idempotency_key from transfers
             where idempotency_key is not null
             group by idempotency_key having count(*) > 1) d`).Scan(&dupes); err != nil {
		return nil, err
	}
	if dupes > 0 {
		add("I5", nameOf("I5"), "%d idempotency key(s) map to more than one transfer", dupes)
	}

	// I6 -- no dangling money. Every non-terminal intent must still have a
	// live outbox row driving it, or it is money reserved forever.
	rows, err = a.db.QueryContext(ctx, `
        select i.id, i.state from intents i
          left join outbox o on o.intent_id = i.id
         where i.state in ('reserved', 'submitted') and o.id is null`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, st string
		if err := rows.Scan(&id, &st); err != nil {
			rows.Close()
			return nil, err
		}
		add("I6", nameOf("I6"), "intent %s is %s with no outbox row", id, st)
	}
	rows.Close()

	// Reserved funds must be backed by a pending_withdraw balance of at least
	// that size, per user.
	rows, err = a.db.QueryContext(ctx, `
        select i.user_id, sum(i.amount), coalesce(b.balance, 0)
          from intents i
          left join account_balances b
            on b.account_code = 'user:' || i.user_id || ':pending_withdraw'
         where i.kind = 'withdraw' and i.state in ('reserved', 'submitted')
         group by i.user_id, b.balance
        having sum(i.amount) <> coalesce(b.balance, 0)`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var user string
		var reserved, pending int64
		if err := rows.Scan(&user, &reserved, &pending); err != nil {
			rows.Close()
			return nil, err
		}
		add("I6", nameOf("I6"), "%s has %s of in-flight withdrawals but %s reserved",
			user, core.USD(reserved), core.USD(pending))
	}
	rows.Close()

	// I7 -- the second reconciliation dimension. I2 proves we hold the right
	// *total*; it says nothing about whether that total is earmarked against the
	// right markets, because moving funds into escrow does not change the vault
	// balance at all.
	esc, err := a.ReconcileEscrow(ctx, false)
	if err != nil {
		return nil, err
	}
	// Drift itself is normal -- trading is off-chain and the chain converges
	// behind it. Only a gap that has stopped moving across passes is a bug.
	stuck, err := a.StuckEscrow(ctx)
	if err != nil {
		return nil, err
	}
	for _, d := range stuck {
		add("I7", nameOf("I7"), "%s: ledger holds %s, vault escrows %s, unchanged for %d passes",
			d.Market, core.USD(d.Collateral), core.USD(d.OnChain), d.Misses)
	}

	if strict {
		if esc.Unexplained != 0 {
			add("I7", nameOf("I7"), "strict mode: %d market(s) out of sync with no instruction in flight",
				esc.Unexplained)
		}
		if esc.Explained != 0 {
			add("I7", nameOf("I7"), "strict mode: %d escrow instruction(s) still in flight", esc.Explained)
		}
		if rep.InFlight != 0 {
			add("I6", nameOf("I6"), "strict mode: %d intent(s) still in flight", rep.InFlight)
		}
		if rep.Terms != (Terms{}) {
			add("I2", nameOf("I2"), "strict mode: expected zero in-flight delta, got %+v", rep.Terms)
		}
		if rep.LedgerInternal != rep.ChainFinalized {
			add("I2", nameOf("I2"), "strict mode: ledger %s != chain %s",
				core.USD(rep.LedgerInternal), core.USD(rep.ChainFinalized))
		}
		var unapplied int
		if err := a.db.QueryRowContext(ctx,
			`select count(*) from chain_events where status = 'received'`).Scan(&unapplied); err != nil {
			return nil, err
		}
		if unapplied != 0 {
			add("I2", nameOf("I2"), "strict mode: %d event(s) still unapplied", unapplied)
		}
	}
	return vs, nil
}

// Format renders a verifier result for a terminal.
func Format(vs []Violation) string {
	if len(vs) == 0 {
		return "all invariants hold"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d violation(s):\n", len(vs))
	for _, v := range vs {
		fmt.Fprintf(&b, "  - %s\n", v)
	}
	return b.String()
}
