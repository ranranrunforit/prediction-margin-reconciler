package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/ranranrunforit/prediction-margin-reconciler/internal/chain"
	"github.com/ranranrunforit/prediction-margin-reconciler/internal/core"
)

type ChaosOptions struct {
	DSN         string
	RedisURL    string
	ChainState  string
	Seed        int64
	Iterations  int
	Users       int
	Markets     int
	VerifyEvery int
	CrashEvery  int
	Faults      chain.Faults
	LogLevel    slog.Level
	Fresh       bool
}

type ChaosResult struct {
	Seed        int64          `json:"seed"`
	Iterations  int            `json:"iterations"`
	Crashes     int            `json:"crashes"`
	Verifies    int            `json:"verifies"`
	Ops         map[string]int `json:"ops"`
	Faults      chain.Faults   `json:"faults"`
	Reorgs      int            `json:"reorgs_observed"`
	Final       *Report        `json:"final_reconciliation"`
	Violations  []Violation    `json:"violations"`
	Duration    time.Duration  `json:"duration"`
	FailedAtIdx int            `json:"failed_at_iteration,omitempty"`
}

func (r *ChaosResult) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "seed=%d iterations=%d crashes=%d verifications=%d in %s\n",
		r.Seed, r.Iterations, r.Crashes, r.Verifies, r.Duration.Round(time.Millisecond))
	fmt.Fprintf(&b, "faults: submit_err=%.0f%% lost_receipt=%.0f%% duplicate=%.0f%% reorder=%.0f%% gap=%.0f%% reorg=%.0f%% confirms=%d\n",
		r.Faults.SubmitError*100, r.Faults.LostReceipt*100, r.Faults.Duplicate*100,
		r.Faults.Reorder*100, r.Faults.Gap*100, r.Faults.Reorg*100, r.Faults.Confirms)
	keys := make([]string, 0, len(r.Ops))
	for k := range r.Ops {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b.WriteString("workload:")
	for _, k := range keys {
		fmt.Fprintf(&b, " %s=%d", k, r.Ops[k])
	}
	b.WriteString("\n")
	if r.Final != nil {
		fmt.Fprintf(&b, "final: ledger=%s chain=%s explained=%s unexplained=%s status=%s\n",
			core.USD(r.Final.LedgerInternal), core.USD(r.Final.ChainFinalized),
			core.USD(r.Final.Explained), core.USD(r.Final.Unexplained), r.Final.Status)
	}
	if len(r.Violations) == 0 {
		fmt.Fprintf(&b, "RESULT: all %d invariants hold after chaos\n", len(Invariants))
	} else {
		fmt.Fprintf(&b, "RESULT: FAILED at iteration %d\n%s", r.FailedAtIdx, Format(r.Violations))
	}
	return b.String()
}

// RunChaos is the main demo. It hammers the system with a random workload while
// the chain misbehaves and the process dies underneath it, and asserts the
// invariants throughout.
//
// The chain simulator outlives the App across a simulated crash, which is the
// whole point: recovery has to work against an authority that kept moving while
// we were dead.
func RunChaos(ctx context.Context, o ChaosOptions) (*ChaosResult, error) {
	if o.Users <= 0 {
		o.Users = 8
	}
	if o.Markets <= 0 {
		o.Markets = 12
	}
	if o.VerifyEvery <= 0 {
		o.VerifyEvery = 25
	}
	if o.Iterations <= 0 {
		o.Iterations = 500
	}
	if o.Faults.Confirms == 0 {
		o.Faults = chain.DefaultFaults()
	}

	start := time.Now()
	rnd := rand.New(rand.NewSource(o.Seed))
	sim := chain.New(o.Seed, o.Faults, o.ChainState)

	open := func() (*App, error) {
		return New(ctx, Options{DSN: o.DSN, RedisURL: o.RedisURL, Seed: o.Seed,
			Faults: o.Faults, Chain: sim, LogLevel: o.LogLevel})
	}
	a, err := open()
	if err != nil {
		return nil, err
	}
	defer func() { _ = a.Close() }()

	if o.Fresh {
		if err := core.Reset(ctx, a.DB()); err != nil {
			return nil, err
		}
	}

	users := make([]string, o.Users)
	for i := range users {
		users[i] = fmt.Sprintf("u%02d", i)
	}
	nextMarket := o.Markets
	for i := 0; i < o.Markets; i++ {
		id := fmt.Sprintf("m%03d", i)
		if err := a.Store.SeedMarket(ctx, id,
			fmt.Sprintf("Will event %d resolve YES?", i), int64(200_000+rnd.Intn(600_000))); err != nil {
			return nil, err
		}
	}

	res := &ChaosResult{Seed: o.Seed, Iterations: o.Iterations, Ops: map[string]int{}, Faults: o.Faults}
	var idemKeys []string
	lastHead := int64(0)

	for i := 1; i <= o.Iterations; i++ {
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		default:
		}

		user := users[rnd.Intn(len(users))]
		// Trade against markets that are actually open, and list a new one now
		// and then, so settlement does not slowly starve the trading path.
		market := ""
		if open, err := a.Store.OpenMarketIDs(ctx); err != nil {
			return res, err
		} else if len(open) > 0 {
			market = open[rnd.Intn(len(open))]
		}
		if market == "" || rnd.Intn(100) < 3 {
			nextMarket++
			market = fmt.Sprintf("m%03d", nextMarket)
			if err := a.Store.SeedMarket(ctx, market,
				fmt.Sprintf("Will event %d resolve YES?", nextMarket),
				int64(150_000+rnd.Intn(700_000))); err != nil {
				return res, err
			}
			res.Ops["new_market"]++
		}

		switch n := rnd.Intn(100); {
		case n < 22: // a user sends funds to the vault contract
			amt := int64(1+rnd.Intn(500)) * core.One
			sim.Deposit(user, amt)
			res.Ops["deposit"]++

		case n < 42: // a user asks to withdraw
			amt := int64(1+rnd.Intn(200)) * core.One
			key := fmt.Sprintf("wd-%d-%s", i, user)
			if _, _, err := a.RequestWithdraw(ctx, user, amt, key); err != nil {
				if isExpected(err) {
					res.Ops["withdraw_rejected"]++
				} else {
					return res, fmt.Errorf("iteration %d: withdraw: %w", i, err)
				}
			} else {
				idemKeys = append(idemKeys, key+"|"+user+"|"+fmt.Sprint(amt))
				res.Ops["withdraw"]++
			}

		case n < 62: // open a leveraged position
			side := "long"
			if rnd.Intn(2) == 0 {
				side = "short"
			}
			size := int64(1+rnd.Intn(300)) * core.One
			if _, err := a.Store.OpenPosition(ctx, user, market, side, size, ""); err != nil {
				if isExpected(err) {
					res.Ops["open_rejected"]++
				} else {
					return res, fmt.Errorf("iteration %d: open: %w", i, err)
				}
			} else {
				res.Ops["open"]++
			}

		case n < 72: // close a random open position
			if id, mark, ok := randomOpenPosition(ctx, a, rnd); ok {
				if err := a.Store.ClosePosition(ctx, id, mark, "user"); err != nil {
					if isExpected(err) {
						res.Ops["close_rejected"]++
					} else {
						return res, fmt.Errorf("iteration %d: close: %w", i, err)
					}
				} else {
					res.Ops["close"]++
				}
			}

		case n < 78: // resolve a market
			outcome := int64(0)
			if rnd.Intn(2) == 0 {
				outcome = core.One
			}
			if _, _, err := a.Store.RequestSettlement(ctx, market, outcome); err != nil {
				if isExpected(err) {
					res.Ops["settle_rejected"]++
				} else {
					return res, fmt.Errorf("iteration %d: settle: %w", i, err)
				}
			} else {
				res.Ops["settle"]++
			}

		case n < 88: // replay a previous request verbatim -- I5 under load
			if len(idemKeys) > 0 {
				parts := strings.Split(idemKeys[rnd.Intn(len(idemKeys))], "|")
				var amt int64
				fmt.Sscan(parts[2], &amt)
				before, err := entryCount(ctx, a)
				if err != nil {
					return res, err
				}
				_, replayed, err := a.RequestWithdraw(ctx, parts[1], amt, parts[0])
				if err != nil && !isExpected(err) {
					return res, fmt.Errorf("iteration %d: replay: %w", i, err)
				}
				after, err := entryCount(ctx, a)
				if err != nil {
					return res, err
				}
				if err == nil && (!replayed || before != after) {
					res.Violations = append(res.Violations, Violation{ID: "I5",
						Name: "each idempotency key maps to exactly one transfer",
						Detail: fmt.Sprintf("replay of %s wrote %d new entries (replayed=%v)",
							parts[0], after-before, replayed)})
					res.FailedAtIdx = i
					return res, nil
				}
				res.Ops["replay"]++
			}

		default: // drift prices
			if err := a.PriceTick(ctx, 20_000); err != nil {
				return res, err
			}
			res.Ops["price_tick"]++
		}

		// Mine, then run a random subset of the workers in a random order.
		if rnd.Intn(100) < 60 {
			sim.Mine()
			res.Ops["mine"]++
		}
		if h := sim.VaultState().Head; h < lastHead {
			res.Reorgs++
		}
		lastHead = sim.VaultState().Head

		ticks := []func(context.Context) (int, error){a.OutboxTick, a.InboxTick, a.ResolverTick}
		if rnd.Intn(100) < 40 {
			ticks = append(ticks, a.LiquidatorTick)
		}
		rnd.Shuffle(len(ticks), func(x, y int) { ticks[x], ticks[y] = ticks[y], ticks[x] })
		for _, t := range ticks[:1+rnd.Intn(len(ticks))] {
			if _, err := t(ctx); err != nil {
				if isExpected(err) {
					continue
				}
				return res, fmt.Errorf("iteration %d: worker: %w", i, err)
			}
		}

		// Crash and restart, mid-flight, with no cleanup at all.
		if o.CrashEvery > 0 && i%o.CrashEvery == 0 {
			a.Kill()
			res.Crashes++
			if a, err = open(); err != nil {
				return res, fmt.Errorf("iteration %d: restart: %w", i, err)
			}
		}

		if i%o.VerifyEvery == 0 {
			vs, err := a.Verify(ctx, false)
			if err != nil {
				return res, err
			}
			res.Verifies++
			if len(vs) > 0 {
				res.Violations, res.FailedAtIdx = vs, i
				res.Duration = time.Since(start)
				return res, nil
			}
		}
	}

	// Let everything land, then demand exact equality.
	if err := a.Quiesce(ctx, 400); err != nil {
		return res, err
	}
	vs, err := a.Verify(ctx, true)
	if err != nil {
		return res, err
	}
	res.Verifies++
	res.Violations = vs
	if len(vs) > 0 {
		res.FailedAtIdx = o.Iterations
	}
	rep, err := a.Reconcile(ctx)
	if err != nil {
		return res, err
	}
	res.Final = rep
	res.Duration = time.Since(start)
	return res, nil
}

// isExpected separates "the system correctly refused" from "the system is
// broken". Rejections are part of the workload, not failures of it.
func isExpected(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, core.ErrInsufficientFunds) || errors.Is(err, core.ErrConflict) {
		return true
	}
	msg := err.Error()
	for _, s := range []string{"withdrawals halted", "position too small",
		"no rows in result set", "duplicate key value", "market", "sql: database is closed"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

func entryCount(ctx context.Context, a *App) (int64, error) {
	var n int64
	err := a.DB().QueryRowContext(ctx, `select count(*) from entries`).Scan(&n)
	return n, err
}

func randomOpenPosition(ctx context.Context, a *App, rnd *rand.Rand) (string, int64, bool) {
	rows, err := a.DB().QueryContext(ctx, `
        select p.id, m.mark_price from positions p join markets m on m.id = p.market_id
         where p.state = 'open' and m.state = 'open' limit 200`)
	if err != nil {
		return "", 0, false
	}
	defer rows.Close()
	type row struct {
		id   string
		mark int64
	}
	var all []row
	for rows.Next() {
		var r row
		if rows.Scan(&r.id, &r.mark) == nil {
			all = append(all, r)
		}
	}
	if len(all) == 0 {
		return "", 0, false
	}
	r := all[rnd.Intn(len(all))]
	return r.id, r.mark, true
}
