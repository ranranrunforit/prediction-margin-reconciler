// Command pmr runs the prediction-market margin and reconciliation engine.
//
//	pmr chaos    -- the main demo: random workload + injected faults + crashes,
//	                then prove every invariant still holds
//	pmr demo     -- a narrated walkthrough of each failure mode, one at a time
//	pmr serve    -- HTTP API and operator panel
//	pmr verify   -- check the invariants against whatever is in the database
//	pmr reset    -- drop and recreate the schema
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ranranrunforit/prediction-margin-reconciler/internal/app"
	"github.com/ranranrunforit/prediction-margin-reconciler/internal/chain"
	"github.com/ranranrunforit/prediction-margin-reconciler/internal/core"
	"github.com/ranranrunforit/prediction-margin-reconciler/internal/httpapi"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func defaultDSN() string {
	return env("DATABASE_URL", "postgres://postgres:postgres@127.0.0.1:5432/pmr?sslmode=disable")
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "chaos":
		err = cmdChaos(ctx, os.Args[2:])
	case "demo":
		err = cmdDemo(ctx, os.Args[2:])
	case "serve":
		err = cmdServe(ctx, os.Args[2:])
	case "verify":
		err = cmdVerify(ctx, os.Args[2:])
	case "reset", "migrate":
		err = cmdReset(ctx, os.Args[1] == "reset")
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `pmr -- prediction-market margin and reconciliation engine

  pmr chaos   [-iterations N] [-seed N] [-crash-every N] [-verify-every N]
              [-users N] [-markets N] [-clean] [-fresh] [-json]
  pmr demo    [-seed N]
  pmr serve   [-addr :8080] [-fresh]
  pmr verify  [-strict] [-settle] [-chain-state PATH]
  pmr reset

env: DATABASE_URL, REDIS_URL
`)
}

func cmdReset(ctx context.Context, drop bool) error {
	db, err := core.Open(ctx, defaultDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	if drop {
		if err := core.Reset(ctx, db); err != nil {
			return err
		}
		fmt.Println("schema dropped and recreated")
		return nil
	}
	fmt.Println("schema applied")
	return nil
}

func cmdChaos(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("chaos", flag.ExitOnError)
	iters := fs.Int("iterations", 1000, "workload operations to run")
	seed := fs.Int64("seed", 1, "random seed; the whole run is reproducible from it")
	crash := fs.Int("crash-every", 50, "hard-kill and restart the app every N iterations (0 to disable)")
	verify := fs.Int("verify-every", 50, "run the invariant verifier every N iterations")
	users := fs.Int("users", 8, "number of users")
	markets := fs.Int("markets", 12, "number of markets")
	clean := fs.Bool("clean", false, "run with no injected faults (control group)")
	fresh := fs.Bool("fresh", true, "reset the database first")
	asJSON := fs.Bool("json", false, "emit the result as JSON")
	verbose := fs.Bool("v", false, "verbose logging")
	_ = fs.Parse(args)

	faults := chain.DefaultFaults()
	if *clean {
		faults = chain.NoFaults()
	}
	level := slog.LevelError
	if *verbose {
		level = slog.LevelInfo
	}

	statePath := fmt.Sprintf("%s/pmr-chain-%d.json", os.TempDir(), *seed)
	_ = os.Remove(statePath)

	res, err := app.RunChaos(ctx, app.ChaosOptions{
		DSN: defaultDSN(), RedisURL: os.Getenv("REDIS_URL"), ChainState: statePath,
		Seed: *seed, Iterations: *iters, Users: *users, Markets: *markets,
		VerifyEvery: *verify, CrashEvery: *crash, Faults: faults,
		LogLevel: level, Fresh: *fresh,
	})
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(res); err != nil {
			return err
		}
	} else {
		fmt.Print(res.String())
	}
	if len(res.Violations) > 0 {
		return errors.New("invariant violation")
	}
	return nil
}

func cmdVerify(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	strict := fs.Bool("strict", false, "also require the system to be fully settled")
	statePath := fs.String("chain-state", env("CHAIN_STATE", ""),
		"load the chain from this file; required for I2, which compares against it")
	settle := fs.Bool("settle", false, "drive the workers until nothing is in flight, then verify strictly")
	_ = fs.Parse(args)

	a, err := app.New(ctx, app.Options{DSN: defaultDSN(), RedisURL: os.Getenv("REDIS_URL"),
		ChainState: *statePath, Faults: chain.NoFaults(), LogLevel: slog.LevelError})
	if err != nil {
		return err
	}
	defer a.Close()
	if *settle {
		if err := a.Quiesce(ctx, 600); err != nil {
			return err
		}
		*strict = true
	}
	vs, err := a.Verify(ctx, *strict)
	if err != nil {
		return err
	}
	fmt.Println(app.Format(vs))
	if len(vs) > 0 {
		return errors.New("invariant violation")
	}
	return nil
}

func cmdServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", env("ADDR", ":8080"), "listen address")
	fresh := fs.Bool("fresh", false, "reset the database first")
	statePath := fs.String("chain-state", env("CHAIN_STATE", "/tmp/pmr-chain.json"),
		"where the simulated chain persists, so it survives a kill -9 of this process")
	block := fs.Duration("block", 700*time.Millisecond, "block interval")
	_ = fs.Parse(args)

	a, err := app.New(ctx, app.Options{DSN: defaultDSN(), RedisURL: os.Getenv("REDIS_URL"),
		ChainState: *statePath, Seed: time.Now().UnixNano(),
		Faults: chain.DefaultFaults(), LogLevel: slog.LevelInfo})
	if err != nil {
		return err
	}
	defer a.Close()
	if *fresh {
		if err := core.Reset(ctx, a.DB()); err != nil {
			return err
		}
	}
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("m%02d", i)
		if err := a.Store.SeedMarket(ctx, id, fmt.Sprintf("Will event %d resolve YES?", i),
			int64(200_000+i*50_000)); err != nil {
			return err
		}
	}

	// Each worker gets its own cadence. They are all idempotent and none of
	// them assumes it is the only instance running.
	go loop(ctx, *block, func() { a.Chain.Mine() })
	go tickLoop(ctx, 150*time.Millisecond, "outbox", a.OutboxTick)
	go tickLoop(ctx, 250*time.Millisecond, "inbox", a.InboxTick)
	go tickLoop(ctx, 400*time.Millisecond, "resolver", a.ResolverTick)
	go tickLoop(ctx, 500*time.Millisecond, "liquidator", a.LiquidatorTick)
	go loop(ctx, 1*time.Second, func() {
		if err := a.PriceTick(ctx, 15_000); err != nil {
			a.Log.Error("price tick", "err", err)
		}
	})
	go loop(ctx, 2*time.Second, func() {
		if _, err := a.Reconcile(ctx); err != nil {
			a.Log.Error("reconcile", "err", err)
		}
	})

	fmt.Printf("operator panel on http://localhost%s\n", *addr)
	err = httpapi.Serve(ctx, a, *addr)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func loop(ctx context.Context, d time.Duration, f func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f()
		}
	}
}

func tickLoop(ctx context.Context, d time.Duration, name string,
	f func(context.Context) (int, error)) {
	loop(ctx, d, func() {
		if _, err := f(ctx); err != nil {
			slog.Error("worker failed", "worker", name, "err", err)
		}
	})
}

// ---------------------------------------------------------------- demo

// cmdDemo walks the four failure modes one at a time with the faults turned on
// individually, so each mechanism can be watched in isolation.
func cmdDemo(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	seed := fs.Int64("seed", 7, "random seed")
	_ = fs.Parse(args)

	statePath := os.TempDir() + "/pmr-demo-chain.json"
	_ = os.Remove(statePath)

	sim := chain.New(*seed, chain.NoFaults(), statePath)
	open := func() (*app.App, error) {
		return app.New(ctx, app.Options{DSN: defaultDSN(), RedisURL: os.Getenv("REDIS_URL"),
			Seed: *seed, Chain: sim, LogLevel: slog.LevelError})
	}
	a, err := open()
	if err != nil {
		return err
	}
	defer func() { _ = a.Close() }()
	if err := core.Reset(ctx, a.DB()); err != nil {
		return err
	}
	if err := a.Store.SeedMarket(ctx, "m00", "Will the demo pass?", 400_000); err != nil {
		return err
	}

	settle := func(rounds int) {
		for i := 0; i < rounds; i++ {
			sim.Mine()
			_, _ = a.OutboxTick(ctx)
			_, _ = a.InboxTick(ctx)
			_, _ = a.ResolverTick(ctx)
		}
	}
	show := func(label string) {
		rep, err := a.Reconcile(ctx)
		if err != nil {
			fmt.Println("  reconcile failed:", err)
			return
		}
		fmt.Printf("  %-26s ledger=%-10s chain=%-10s explained=%-10s residue=%-8s %s\n",
			label, core.USD(rep.LedgerInternal), core.USD(rep.ChainFinalized),
			core.USD(rep.Explained), core.USD(rep.Unexplained), rep.Status)
	}
	check := func() error {
		vs, err := a.Verify(ctx, false)
		if err != nil {
			return err
		}
		if len(vs) > 0 {
			return fmt.Errorf("invariants broken:\n%s", app.Format(vs))
		}
		fmt.Println("  invariants: all hold")
		return nil
	}
	step := func(n int, title string) {
		fmt.Printf("\n%d. %s\n%s\n", n, title, strings.Repeat("-", 66))
	}

	// ---------------------------------------------------------------
	step(1, "A deposit is credited optimistically, then finalises")
	fmt.Println("  Deposits are credited at one confirmation so the depositor can")
	fmt.Println("  trade immediately. Until the block is final, the ledger is")
	fmt.Println("  legitimately ahead of the chain -- and the residue stays zero.")
	sim.Deposit("alice", 1000*core.One)
	settle(1)
	show("provisional credit")
	settle(3)
	show("after finality")
	if err := check(); err != nil {
		return err
	}

	// ---------------------------------------------------------------
	step(2, "A withdrawal whose receipt is lost")
	fmt.Println("  The submit returns an error but the transaction is included")
	fmt.Println("  anyway. The outbox cannot tell, and does not need to: the intent")
	fmt.Println("  id is the on-chain nonce, so the resolver settles it from")
	fmt.Println("  authoritative chain state.")
	sim.SetFaults(chain.Faults{LostReceipt: 1.0, Confirms: 2})
	id, _, err := a.RequestWithdraw(ctx, "alice", 300*core.One, "demo-wd-1")
	if err != nil {
		return err
	}
	if _, err := a.OutboxTick(ctx); err != nil {
		return err
	}
	fmt.Printf("  submit reported failure for intent %s\n", id[:8])
	show("funds reserved")
	sim.SetFaults(chain.NoFaults())
	settle(4)
	show("resolver settled it")
	if err := check(); err != nil {
		return err
	}

	// ---------------------------------------------------------------
	step(3, "Duplicate, reordered and withheld events")
	fmt.Println("  Every event arrives twice, shuffled, and one in three is held")
	fmt.Println("  back. Duplicates are absorbed by the event primary key; a")
	fmt.Println("  withheld event shows up as a gap in the sequence and the cursor")
	fmt.Println("  refuses to advance past it rather than losing the effect.")
	sim.SetFaults(chain.Faults{Duplicate: 1.0, Reorder: 1.0, Gap: 0.35, Confirms: 2})
	for i := 0; i < 6; i++ {
		sim.Deposit("bob", 50*core.One)
	}
	settle(14)
	sim.SetFaults(chain.NoFaults())
	settle(4)
	show("all six deposits")
	bob, err := a.Store.Balance(ctx, core.AvailableAcct("bob"))
	if err != nil {
		return err
	}
	fmt.Printf("  bob's balance: %s (expected 300, from 6 x 50)\n", core.USD(bob))
	if err := check(); err != nil {
		return err
	}

	// ---------------------------------------------------------------
	step(4, "A crash in the middle of everything")
	fmt.Println("  The process is killed with no cleanup while intents are in")
	fmt.Println("  flight, then restarted. State lives in Postgres and the chain")
	fmt.Println("  kept moving while we were gone, so recovery is just the normal")
	fmt.Println("  workers running again.")
	sim.SetFaults(chain.Faults{SubmitError: 0.4, LostReceipt: 0.3, Confirms: 2})
	for i := 0; i < 5; i++ {
		if _, _, err := a.RequestWithdraw(ctx, "alice",
			20*core.One, fmt.Sprintf("demo-crash-%d", i)); err != nil {
			return err
		}
	}
	if _, err := a.OutboxTick(ctx); err != nil {
		return err
	}
	sim.Mine()
	a.Kill()
	fmt.Println("  killed mid-flight")
	if a, err = open(); err != nil {
		return err
	}
	show("immediately after restart")
	sim.SetFaults(chain.NoFaults())
	settle(10)
	show("recovered")
	if err := check(); err != nil {
		return err
	}

	// ---------------------------------------------------------------
	step(5, "A reorg takes back a provisional deposit")
	fmt.Println("  A deposit is credited, then its block is orphaned. The ledger is")
	fmt.Println("  append-only, so the fix is a compensating transfer, not a delete.")
	sim.SetFaults(chain.Faults{Reorg: 1.0, Confirms: 4})
	sim.Deposit("carol", 500*core.One)
	settle(2)
	show("carol credited")
	for i := 0; i < 6; i++ {
		sim.Mine()
		if _, err := a.InboxTick(ctx); err != nil {
			return err
		}
	}
	sim.SetFaults(chain.NoFaults())
	settle(8)
	carol, err := a.Store.Balance(ctx, core.AvailableAcct("carol"))
	if err != nil {
		return err
	}
	show("after the dust settles")
	fmt.Printf("  carol's balance: %s (the deposit was re-included, so it stands)\n", core.USD(carol))
	if err := check(); err != nil {
		return err
	}

	// ---------------------------------------------------------------
	step(6, "A real bug: money that no chain deposit backs")
	fmt.Println("  A phantom credit, balanced double entry and all. Nothing inside")
	fmt.Println("  the ledger looks wrong. Only reconciliation against the chain")
	fmt.Println("  finds it -- and it halts withdrawals instead of guessing.")
	if _, _, err := a.Store.Post(ctx, core.Transfer{
		Kind: "INJECTED_BUG.phantom_credit", IdempotencyKey: "demo-bug",
		Legs: []core.Leg{
			{Account: core.AcctExternal, Amount: -777 * core.One},
			{Account: core.AvailableAcct("dave"), Amount: 777 * core.One},
		},
	}); err != nil {
		return err
	}
	show("phantom credit")
	vs, err := a.Verify(ctx, false)
	if err != nil {
		return err
	}
	fmt.Printf("  verifier: %s", app.Format(vs))
	frozen, reason, err := a.Store.Frozen(ctx, "dave")
	if err != nil {
		return err
	}
	fmt.Printf("  withdrawals halted: %v (%s)\n", frozen, reason)
	if _, _, err := a.RequestWithdraw(ctx, "dave", core.One, "demo-blocked"); err != nil {
		fmt.Printf("  a withdrawal attempt is now refused: %v\n", err)
	}

	fmt.Printf("\n%s\nrun `pmr chaos` for the same properties under 1000 randomised operations\n",
		strings.Repeat("=", 66))
	return nil
}
