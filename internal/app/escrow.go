package app

import (
	"context"
	"fmt"
	"sort"

	"github.com/ranranrunforit/prediction-margin-reconciler/internal/core"
)

// EscrowExpiryBlocks is how long an escrow instruction stays valid on chain.
// Shorter than a withdrawal's, because no user funds are reserved behind it: if
// it expires, the next sync pass simply issues a fresh one.
const EscrowExpiryBlocks = 6

// EscrowDrift is one market's disagreement between the ledger and the vault.
type EscrowDrift struct {
	Market      string `json:"market"`
	Collateral  int64  `json:"ledger_collateral"`
	OnChain     int64  `json:"chain_escrow"`
	InFlight    int64  `json:"in_flight_target"`
	Misses      int    `json:"misses"`
	HasInFlight bool   `json:"has_in_flight"`
	Unexplained int64  `json:"unexplained"`
}

// EscrowReport is the second reconciliation dimension.
//
// The vault balance check (I2) answers "do we hold as much money as we think we
// do". It cannot answer "is the money we hold earmarked against the right
// positions", because moving funds into escrow does not change the balance at
// all. A single-number reconciliation would be perfectly happy with collateral
// attributed to entirely the wrong markets, which is exactly the failure that
// bankrupts a margin system: the total looks right, and then one market resolves
// and there is nothing behind it.
type EscrowReport struct {
	Markets     int           `json:"markets"`
	Synced      int           `json:"synced"`
	Explained   int           `json:"explained"`
	Unexplained int           `json:"unexplained"`
	TotalLedger int64         `json:"total_ledger_collateral"`
	TotalChain  int64         `json:"total_chain_escrow"`
	Drifts      []EscrowDrift `json:"drifts,omitempty"`
	Issued      int           `json:"instructions_issued"`
}

// EscrowTick reconciles per-market escrow and issues instructions to close any
// gap.
//
// Deliberately a batched background job rather than an on-chain call per trade.
// Putting the chain in the path of opening a position would make the hot trading
// path as slow as block time, and would tie a user's fill to a transaction that
// might not land. Instead trading is purely off-chain and fast, and the chain's
// view of escrow converges behind it. The cost of that choice is exactly this
// worker plus a reconciliation term, which is a good trade.
func (a *App) EscrowTick(ctx context.Context) (int, error) {
	rep, err := a.ReconcileEscrow(ctx, true)
	if err != nil {
		return 0, err
	}
	return rep.Issued, nil
}

// ReconcileEscrow compares ledger collateral against on-chain escrow for every
// live market. With issue=true it also submits the instructions to converge.
func (a *App) ReconcileEscrow(ctx context.Context, issue bool) (*EscrowReport, error) {
	collateral, err := a.Store.MarketCollateral(ctx)
	if err != nil {
		return nil, err
	}
	targets, err := a.Store.InFlightEscrowTargets(ctx)
	if err != nil {
		return nil, err
	}
	vs := a.Chain.VaultState()

	rep := &EscrowReport{Markets: len(collateral)}
	markets := make([]string, 0, len(collateral))
	for m := range collateral {
		markets = append(markets, m)
	}
	sort.Strings(markets)

	for _, m := range markets {
		want := collateral[m]
		got := vs.EscrowOf(m)
		target, inflight := targets[m]

		rep.TotalLedger += want
		rep.TotalChain += got

		if want == got {
			rep.Synced++
			if issue {
				if _, err := a.db.ExecContext(ctx,
					`delete from escrow_drift where market = $1`, m); err != nil {
					return rep, err
				}
			}
			continue
		}

		d := EscrowDrift{Market: m, Collateral: want, OnChain: got,
			InFlight: target, HasInFlight: inflight}

		// A gap is explained when an instruction is already in flight that would
		// install exactly what the ledger currently believes. Anything else is a
		// gap nobody is working on, and it needs an instruction issued.
		if inflight && target == want {
			// An instruction is already installing exactly what the ledger
			// believes. That is convergence in progress, not a stall, so the
			// stall counter resets.
			d.Unexplained = 0
			rep.Explained++
			if issue {
				if _, err := a.db.ExecContext(ctx, `
                    update escrow_drift set misses = 0, want = $2, got = $3, updated_at = now()
                     where market = $1`, m, want, got); err != nil {
					return rep, err
				}
			}
		} else {
			d.Unexplained = want - got
			rep.Unexplained++
			if issue && !inflight && !a.Chain.SettledOnChain(m) {
				if _, _, err := a.Store.RequestEscrowSet(ctx, m, want, vs.Head+EscrowExpiryBlocks); err != nil {
					return rep, fmt.Errorf("escrow instruction for %s: %w", m, err)
				}
				rep.Issued++
			}
			if issue {
				// A gap with nobody working on it. The counter only advances
				// while the gap stands completely still: any movement means the
				// system is converging, however slowly.
				if _, err := a.db.ExecContext(ctx, `
                    insert into escrow_drift (market, want, got, misses)
                    values ($1, $2, $3, 0)
                    on conflict (market) do update
                       set misses = case when escrow_drift.want = excluded.want
                                          and escrow_drift.got = excluded.got
                                         then escrow_drift.misses + 1 else 0 end,
                           want = excluded.want, got = excluded.got, updated_at = now()`,
					m, want, got); err != nil {
					return rep, err
				}
			}
		}
		if st, err := a.escrowMisses(ctx, m); err != nil {
			return rep, err
		} else {
			d.Misses = st
		}
		rep.Drifts = append(rep.Drifts, d)
	}

	// The contract refuses to earmark more than it holds, so this can only fail
	// if our own arithmetic has drifted. Checking it here means we find out from
	// the reconciler rather than from a rejected transaction.
	if rep.TotalChain > vs.FinalizedBalance {
		return rep, fmt.Errorf("escrow total %s exceeds vault custody %s",
			core.USD(rep.TotalChain), core.USD(vs.FinalizedBalance))
	}
	return rep, nil
}

// escrowMisses is how many reconciliation passes this market has been out of
// sync without the gap changing.
func (a *App) escrowMisses(ctx context.Context, market string) (int, error) {
	var n int
	err := a.db.QueryRowContext(ctx,
		`select coalesce((select misses from escrow_drift where market = $1), 0)`, market).Scan(&n)
	return n, err
}

// EscrowStuckThreshold is how many passes a gap may stand still before it counts
// as stuck rather than converging.
const EscrowStuckThreshold = 8

// StuckEscrow lists markets whose escrow gap has stopped moving. These are the
// only escrow drifts worth alerting on.
func (a *App) StuckEscrow(ctx context.Context) ([]EscrowDrift, error) {
	rows, err := a.db.QueryContext(ctx, `
        select market, want, got, misses from escrow_drift
         where misses >= $1 order by market`, EscrowStuckThreshold)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EscrowDrift
	for rows.Next() {
		var d EscrowDrift
		if err := rows.Scan(&d.Market, &d.Collateral, &d.OnChain, &d.Misses); err != nil {
			return nil, err
		}
		d.Unexplained = d.Collateral - d.OnChain
		out = append(out, d)
	}
	return out, rows.Err()
}
