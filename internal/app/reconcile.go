package app

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ranranrunforit/prediction-margin-reconciler/internal/core"
)

// Status values, in increasing order of how much you should care.
const (
	StatusOK        = "ok"               // ledger and chain agree exactly
	StatusExplained = "explained"        // they differ, and every unit of the difference is accounted for
	StatusShortfall = "unsafe_shortfall" // the ledger believes in money the chain does not hold -> HALT
	StatusSurplus   = "unsafe_surplus"   // the chain holds money the ledger cannot attribute -> alert
)

// Terms are the three legitimate reasons the off-chain ledger and the on-chain
// vault can disagree at any instant. Anything left over after subtracting them
// is a bug, and the whole design exists to make that residue observable.
type Terms struct {
	// Withdrawals the chain has finalised but the ledger has not yet
	// recognised. Ledger is high by this much.
	WithdrawnNotRecognised int64 `json:"withdrawn_not_recognised"`
	// Deposits finalised on chain that never reached us as events. Ledger is
	// low by this much. Healed automatically from chain state.
	DepositsNotIngested int64 `json:"deposits_not_ingested"`
	// Credits we granted optimistically on blocks that are not final yet.
	// Ledger is high by this much.
	ProvisionalCredits int64 `json:"provisional_credits"`
}

func (t Terms) Sum() int64 {
	return t.WithdrawnNotRecognised - t.DepositsNotIngested + t.ProvisionalCredits
}

type Report struct {
	At             time.Time `json:"at"`
	Status         string    `json:"status"`
	LedgerInternal int64     `json:"ledger_internal"`
	ChainFinalized int64     `json:"chain_finalized"`
	RawDelta       int64     `json:"raw_delta"`
	Explained      int64     `json:"explained"`
	Unexplained    int64     `json:"unexplained"`
	Terms          Terms     `json:"terms"`
	InFlight       int       `json:"in_flight_intents"`
	Healed         int       `json:"healed_from_chain_state"`
	Note           string    `json:"note,omitempty"`
}

// Reconcile is the invariant that gives this project its name.
//
// The naive version -- "ledger balance must always equal the on-chain vault
// balance" -- is wrong, and a system built on it either alerts constantly or
// gets muted. Money genuinely is in two places at once while a transaction is
// in flight. The useful statement is:
//
//	ledger_internal - chain_finalized_vault == explainable_in_flight_delta
//
// where the right-hand side is computed from a bounded, enumerable set of
// in-flight facts. Pending is a modelled state, not an anomaly. The residue is
// the only thing worth paging someone about.
func (a *App) Reconcile(ctx context.Context) (*Report, error) {
	// Promote or compensate anything provisional before doing the arithmetic,
	// so the report describes the chain as it is now and not as it was at the
	// last inbox pass.
	if err := a.settleFacts(ctx); err != nil {
		return nil, err
	}
	ledger, err := a.Store.InternalTotal(ctx)
	if err != nil {
		return nil, err
	}
	vs := a.Chain.VaultState()

	var t Terms

	inflight, err := a.Store.InFlight(ctx)
	if err != nil {
		return nil, err
	}
	for _, it := range inflight {
		if it.Kind != "withdraw" {
			continue
		}
		if st := a.Chain.TxStatus(it.ID); st.Processed && st.Finalized {
			t.WithdrawnNotRecognised += it.Amount
		}
	}

	applied, err := a.Store.AppliedFactIDs(ctx)
	if err != nil {
		return nil, err
	}
	missing := []core.StoredEvent{}
	for _, ev := range a.Chain.FinalizedFacts("deposit") {
		if applied[ev.FactID] {
			continue
		}
		t.DepositsNotIngested += ev.Amount
		missing = append(missing, core.StoredEvent{
			EventID: ev.ID, FactID: ev.FactID, Seq: ev.Seq, Kind: ev.Kind,
			User: ev.User, Amount: ev.Amount, Height: ev.Height, BlockHash: ev.BlockHash,
		})
	}

	if t.ProvisionalCredits, err = a.Store.UnfinalizedApplied(ctx, vs.FinalizedHeight); err != nil {
		return nil, err
	}

	r := &Report{
		At: time.Now(), LedgerInternal: ledger, ChainFinalized: vs.FinalizedBalance,
		RawDelta: ledger - vs.FinalizedBalance, Explained: t.Sum(), Terms: t,
		InFlight: len(inflight),
	}
	r.Unexplained = r.RawDelta - r.Explained

	switch {
	case r.Unexplained > 0:
		r.Status = StatusShortfall
	case r.Unexplained < 0:
		r.Status = StatusSurplus
	case r.Explained != 0:
		r.Status = StatusExplained
	default:
		r.Status = StatusOK
	}

	// Automatic healing, but only in the direction that cannot lose money:
	// a finalised on-chain deposit we never saw is safe to ingest from chain
	// state directly, bypassing the event stream entirely. This is the escape
	// hatch for a permanently dropped event.
	for _, e := range missing {
		if _, err := a.Store.RecordEvent(ctx, e); err != nil {
			return r, err
		}
		if err := a.Store.ApplyDeposit(ctx, e); err != nil {
			a.Log.Error("heal deposit from chain state", "fact", e.FactID, "err", err)
			continue
		}
		// Read from finalised chain state, so final by construction.
		if err := a.Store.MarkFactFinalized(ctx, e.EventID); err != nil {
			return r, err
		}
		r.Healed++
	}
	if r.Healed > 0 {
		r.Note = fmt.Sprintf("ingested %d finalised deposit(s) from chain state after event loss", r.Healed)
	}

	// A shortfall means we could pay out money the vault does not have. There
	// is no safe automatic repair for that, so we stop the bleeding and hand
	// it to a human.
	if r.Status == StatusShortfall {
		reason := fmt.Sprintf("unexplained shortfall of %s at %s",
			core.USD(r.Unexplained), r.At.Format(time.RFC3339))
		if err := a.Store.Freeze(ctx, "*", reason); err != nil {
			return r, err
		}
		a.Log.Error("HALT: unexplained shortfall", "amount", core.USD(r.Unexplained))
	}

	detail, _ := json.Marshal(r)
	if _, err := a.db.ExecContext(ctx, `
        insert into reconciliations (status, ledger_internal, chain_finalized,
               explained_delta, unexplained, detail)
        values ($1, $2, $3, $4, $5, $6)`,
		r.Status, r.LedgerInternal, r.ChainFinalized, r.Explained, r.Unexplained, detail); err != nil {
		return r, err
	}
	return r, nil
}

// Quiesce drives the system to a standstill: mine until finality, run every
// worker until nothing is in flight. Only in this state can the ledger and the
// chain be required to match exactly, which is how the chaos test gets a
// strong assertion out of a system that is legitimately fuzzy while busy.
func (a *App) Quiesce(ctx context.Context, maxRounds int) error {
	saved := a.Chain.Faults()
	clean := saved
	clean.SubmitError, clean.LostReceipt, clean.Duplicate = 0, 0, 0
	clean.Reorder, clean.Gap, clean.Reorg = 0, 0, 0
	a.Chain.SetFaults(clean)
	defer a.Chain.SetFaults(saved)

	for i := 0; i < maxRounds; i++ {
		a.Chain.Mine()
		if _, err := a.OutboxTick(ctx); err != nil {
			return err
		}
		if _, err := a.InboxTick(ctx); err != nil {
			return err
		}
		if _, err := a.ResolverTick(ctx); err != nil {
			return err
		}

		inflight, err := a.Store.InFlight(ctx)
		if err != nil {
			return err
		}
		pending, err := a.Store.PendingEvents(ctx, 1)
		if err != nil {
			return err
		}
		prov, err := a.Store.UnfinalizedApplied(ctx, a.Chain.VaultState().FinalizedHeight)
		if err != nil {
			return err
		}
		if len(inflight) == 0 && len(pending) == 0 && prov == 0 {
			if _, err := a.Reconcile(ctx); err != nil {
				return err
			}
			return nil
		}
	}
	return fmt.Errorf("system did not quiesce in %d rounds", maxRounds)
}
