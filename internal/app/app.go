// Package app wires the store, the chain and the workers together.
//
// Every worker is exposed as a single-shot `...Tick` method. The serve command
// puts them on timers; the chaos harness calls them directly in a randomised
// order. That is what makes the chaos runs reproducible from a seed: there is
// no hidden concurrency in the control flow, only in the database access.
package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand"
	"os"
	"time"

	_ "github.com/lib/pq"

	"github.com/ranranrunforit/prediction-margin-reconciler/internal/chain"
	"github.com/ranranrunforit/prediction-margin-reconciler/internal/core"
	"github.com/ranranrunforit/prediction-margin-reconciler/internal/pricefeed"
)

type Options struct {
	DSN        string
	RedisURL   string
	ChainState string
	Seed       int64
	Faults     chain.Faults
	Chain      *chain.Sim // reuse an existing chain across a simulated restart
	LogLevel   slog.Level
}

type App struct {
	Store *core.Store
	Chain *chain.Sim
	Feed  pricefeed.Feed
	Log   *slog.Logger

	db  *sql.DB
	rnd *rand.Rand
}

func New(ctx context.Context, o Options) (*App, error) {
	db, err := core.Open(ctx, o.DSN)
	if err != nil {
		return nil, err
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: o.LogLevel}))

	sim := o.Chain
	if sim == nil {
		sim = chain.New(o.Seed, o.Faults, o.ChainState)
	}

	var feed pricefeed.Feed = pricefeed.NewMemory()
	if o.RedisURL != "" {
		if f, err := pricefeed.NewRedis(o.RedisURL); err == nil {
			feed = f
		} else {
			log.Warn("redis unavailable, using in-memory price cache", "err", err)
		}
	}

	return &App{
		Store: core.NewStore(db), Chain: sim, Feed: feed, Log: log,
		db: db, rnd: rand.New(rand.NewSource(o.Seed + 7)),
	}, nil
}

func (a *App) DB() *sql.DB { return a.db }

// WithdrawExpiryBlocks is how many blocks a withdrawal nonce stays valid for on
// chain. It must exceed finality by a comfortable margin, or we would expire
// intents that were about to confirm.
const WithdrawExpiryBlocks = 10

// RequestWithdraw stamps the intent with a chain-enforced expiry height. The
// height comes from the chain rather than from our clock because the chain is
// the thing that has to honour it.
func (a *App) RequestWithdraw(ctx context.Context, user string, amount int64, key string) (string, bool, error) {
	head := a.Chain.VaultState().Head
	return a.Store.RequestWithdraw(ctx, user, amount, key, head+WithdrawExpiryBlocks)
}

func (a *App) Close() error {
	_ = a.Feed.Close()
	return a.db.Close()
}

// Kill drops the process's handles without any graceful shutdown, to model a
// SIGKILL. Nothing is flushed; whatever was not committed is gone.
func (a *App) Kill() {
	_ = a.db.Close()
	_ = a.Feed.Close()
}

// ---------------------------------------------------------------- outbox

// OutboxTick drains due outbox rows to the chain.
//
// The critical property: a submit error is not a failure of the workflow. The
// intent id is the on-chain nonce, so re-submitting is free, and a lost
// receipt (error returned, tx actually included) is indistinguishable from a
// real failure here and does not need to be distinguished -- the resolver
// finds out the truth from the chain later.
func (a *App) OutboxTick(ctx context.Context) (int, error) {
	t, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = t.Rollback() }()
	rows, err := t.QueryContext(ctx, `
        select id, intent_id, payload from outbox
         where (state = 'pending' and next_attempt_at <= now())
            or (state = 'sending' and (lease_until is null or lease_until <= now()))
         order by id limit 32
         for update skip locked`)
	if err != nil {
		return 0, err
	}
	type job struct {
		id       int64
		intentID string
		payload  []byte
	}
	var jobs []job
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.id, &j.intentID, &j.payload); err != nil {
			rows.Close()
			return 0, err
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	for _, j := range jobs {
		if _, err := t.ExecContext(ctx, `
            update outbox set state = 'sending', lease_until = now() + interval '30 seconds'
             where id = $1`, j.id); err != nil {
			return 0, err
		}
	}
	if err := t.Commit(); err != nil {
		return 0, err
	}

	n := 0
	for _, j := range jobs {
		var tx chain.Tx
		if err := json.Unmarshal(j.payload, &tx); err != nil {
			continue
		}
		hash, subErr := a.Chain.Submit(tx)
		msg := ""
		if subErr != nil {
			msg = subErr.Error()
		}
		if err := a.Store.MarkSubmitted(ctx, j.intentID, hash, msg); err != nil {
			return n, err
		}
		if subErr == nil {
			_, err = a.db.ExecContext(ctx,
				`update outbox set state = 'sent', lease_until = null, attempts = attempts + 1, last_error = null
                  where id = $1 and state = 'sending'`, j.id)
		} else {
			// Linear-ish backoff, capped. We never mark it dead: an intent
			// with reserved funds must be resolved one way or the other.
			_, err = a.db.ExecContext(ctx, `
	                update outbox set state = 'pending', lease_until = null, attempts = attempts + 1, last_error = $2,
                       next_attempt_at = now() + least(attempts, 10) * interval '200 milliseconds'
	                 where id = $1 and state = 'sending'`, j.id, msg)
		}
		if err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// ---------------------------------------------------------------- inbox

// InboxTick pulls the event stream, in three phases:
//
//  1. reorg check first, so we never apply an event from a block we are about
//     to orphan;
//  2. promote provisional facts whose block has become final;
//  3. ingest and apply new events, refusing to advance the cursor past a gap.
func (a *App) InboxTick(ctx context.Context) (int, error) {
	if err := a.settleFacts(ctx); err != nil {
		return 0, err
	}
	if err := a.reorgCheck(ctx); err != nil {
		return 0, err
	}

	cursor, err := a.Store.Cursor(ctx)
	if err != nil {
		return 0, err
	}
	batch := a.Chain.PollEvents(cursor, 64)

	// Gap detection. Sequence numbers are contiguous on the chain side, so a
	// missing seq means the stream withheld an event. Advancing past it would
	// lose its effect permanently, so we only advance to the end of the
	// contiguous prefix and pick the rest up on a later poll.
	seen := map[int64]bool{}
	for _, ev := range batch {
		seen[ev.Seq] = true
	}
	advanceTo := cursor
	for seen[advanceTo+1] {
		advanceTo++
	}

	applied := 0
	for _, ev := range batch {
		if ev.Seq > advanceTo {
			continue // beyond the gap: ingest it later, in order
		}
		isNew, err := a.Store.RecordEvent(ctx, core.StoredEvent{
			EventID: ev.ID, FactID: ev.FactID, Seq: ev.Seq, Kind: ev.Kind,
			User: ev.User, Amount: ev.Amount, MarketID: ev.MarketID,
			Height: ev.Height, BlockHash: ev.BlockHash,
		})
		if err != nil {
			return applied, err
		}
		if !isNew {
			continue // duplicate delivery, already in the inbox
		}
		applied++
	}
	if advanceTo > cursor {
		if err := a.Store.AdvanceCursor(ctx, advanceTo); err != nil {
			return applied, err
		}
	}

	pending, err := a.Store.PendingEvents(ctx, 128)
	if err != nil {
		return applied, err
	}
	for _, e := range pending {
		switch e.Kind {
		case "deposit":
			if err := a.Store.ApplyDeposit(ctx, e); err != nil {
				a.Log.Error("apply deposit", "event", e.EventID, "err", err)
				continue
			}
		default:
			// Withdrawals and settlements are driven by the resolver from
			// authoritative chain state, not from the event stream, because
			// paying money out on the strength of an unreliable stream is
			// not a trade we are willing to make.
			if err := a.Store.SetEventStatus(ctx, e.EventID, "ignored"); err != nil {
				return applied, err
			}
		}
	}
	return applied, nil
}

// settleFacts walks every provisional credit and re-asks the chain about the
// exact block it came from. Two outcomes, and only these two:
//
//	block still canonical and now final -> promote it, it is real money
//	block no longer canonical           -> compensate it, it never happened
//
// Checking per fact rather than in bulk by height is the whole point: a block
// can be replaced by a *different* block at the same height, and a height-based
// promotion would quietly bless the orphan.
func (a *App) settleFacts(ctx context.Context) error {
	facts, err := a.Store.ProvisionalFacts(ctx)
	if err != nil {
		return err
	}
	if len(facts) == 0 {
		return nil
	}
	orphaned := map[string]bool{}
	for _, f := range facts {
		// A fact is live only when the chain's authoritative nonce lookup still
		// points at the exact block from which we derived the ledger credit.
		// CanonicalHash alone is an index over the block tree; TxStatus ties that
		// block back to the specific deposit and avoids compensating a valid
		// credit merely because the event cursor is behind.
		st := a.Chain.TxStatus(f.FactID)
		canon, ok := a.Chain.CanonicalHash(f.Height)
		if !st.Processed || st.BlockHash != f.BlockHash || !ok || canon != f.BlockHash {
			orphaned[f.BlockHash] = true
			continue
		}
		if st.Finalized {
			if err := a.Store.MarkFactFinalized(ctx, f.EventID); err != nil {
				return err
			}
		}
	}
	for hash := range orphaned {
		n, err := a.Store.RevertBlock(ctx, hash)
		if err != nil {
			return err
		}
		a.Log.Warn("reorg: compensated orphaned credits", "block", hash, "facts", n)
	}
	return nil
}

// reorgCheck covers the other direction: a withdrawal we already recognised
// whose block turned out not to be canonical after all.
func (a *App) reorgCheck(ctx context.Context) error {
	live, err := a.Store.ConfirmedBlocks(ctx)
	if err != nil {
		return err
	}
	for height, hash := range live {
		got, ok := a.Chain.CanonicalHash(height)
		if ok && got == hash {
			continue
		}
		a.Log.Warn("finality violated", "height", height, "orphaned_block", hash, "canonical", got)
		ids, err := a.Store.ConfirmedIntentsInBlock(ctx, hash)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := a.Store.Revert(ctx, id, hash); err != nil {
				return err
			}
		}
		a.Log.Warn("reorg compensated", "intents_reverted", len(ids))
	}
	return nil
}

// ---------------------------------------------------------------- resolver

// ResolverTick is the recovery loop. For every in-flight intent it asks the
// chain the only question that matters -- "is this nonce processed, and is that
// final?" -- and drives the state machine to a terminal state.
//
// This is what closes the lost-receipt hole: the outbox never knows whether its
// submit landed, and it does not need to.
func (a *App) ResolverTick(ctx context.Context) (int, error) {
	inflight, err := a.Store.InFlight(ctx)
	if err != nil {
		return 0, err
	}
	head := a.Chain.VaultState().Head
	n := 0
	for _, it := range inflight {
		st := a.Chain.TxStatus(it.ID)
		if st.Processed && st.Finalized {
			if err := a.Store.Confirm(ctx, it.ID, st.Height, st.BlockHash); err != nil {
				if errors.Is(err, core.ErrConflict) {
					continue
				}
				return n, err
			}
			n++
			continue
		}
		if st.Processed {
			// In a block but not final yet. Deliberately do nothing: we do
			// not recognise money leaving until it cannot come back.
			continue
		}

		expiredByContract := it.ExpiryHt > 0 && head > it.ExpiryHt
		expiredWithoutContract := it.ExpiryHt == 0 && time.Now().After(it.Deadline)
		if expiredByContract || expiredWithoutContract {
			// We only refund after the chain has advanced past the nonce's
			// contract-enforced expiry height. A separate cached "expired" flag
			// is not enough evidence: it can be stale or disagree with the nonce
			// status during recovery. At this height the contract will refuse a
			// late submission, so the money can only be in one place: with us.
			//
			// A wall-clock deadline on its own would not be enough. A tx can
			// sit in a mempool, or come back from a reorg, and land after we
			// decided it had failed -- and then the chain pays out money we
			// already gave back. The expiry has to be enforced by the same
			// authority that would execute the transfer.
			if err := a.Store.Expire(ctx, it.ID, head); err != nil {
				if errors.Is(err, core.ErrConflict) {
					continue
				}
				return n, err
			}
			n++
		}
	}
	return n, nil
}

// ---------------------------------------------------------------- liquidator

// LiquidatorTick closes positions that have fallen below maintenance margin.
// Two guards, on purpose:
//
//	the Redis lease  -- cheap, avoids wasted work across replicas
//	the row version  -- authoritative, makes a double-liquidation impossible
//
// If Redis is down the lease always grants, and correctness is unaffected.
func (a *App) LiquidatorTick(ctx context.Context) (int, error) {
	marks, err := a.Feed.Marks(ctx)
	if err != nil || len(marks) == 0 {
		ms, mErr := a.Store.Markets(ctx)
		if mErr != nil {
			return 0, mErr
		}
		marks = map[string]int64{}
		for _, m := range ms {
			if m.State == "open" {
				marks[m.ID] = m.Mark
			}
		}
	}
	at, err := a.Store.AtRisk(ctx, marks)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, p := range at {
		if !a.Feed.Lease(ctx, "liq:"+p.ID, 2*time.Second) {
			continue
		}
		if err := a.Store.ClosePosition(ctx, p.ID, marks[p.MarketID], "liquidation"); err != nil {
			if errors.Is(err, core.ErrConflict) {
				continue // someone else got there first; that is the point
			}
			return n, err
		}
		n++
	}
	return n, nil
}

// ---------------------------------------------------------------- prices

// PriceTick walks the mark prices and republishes them to the hot cache.
func (a *App) PriceTick(ctx context.Context, drift int64) error {
	ms, err := a.Store.Markets(ctx)
	if err != nil {
		return err
	}
	for _, m := range ms {
		if m.State != "open" {
			continue
		}
		step := int64(a.rnd.Intn(int(2*drift+1))) - drift
		next := m.Mark + step
		if err := a.Store.SetMark(ctx, m.ID, next); err != nil {
			return err
		}
		if err := a.Feed.Publish(ctx, m.ID, clamp(next)); err != nil {
			a.Log.Debug("price publish failed", "err", err)
		}
	}
	return nil
}

func clamp(v int64) int64 {
	if v < 1000 {
		return 1000
	}
	if v > core.One-1000 {
		return core.One - 1000
	}
	return v
}
