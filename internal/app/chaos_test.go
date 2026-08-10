package app_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/ranranrunforit/prediction-margin-reconciler/internal/app"
	"github.com/ranranrunforit/prediction-margin-reconciler/internal/chain"
)

func dsn(t *testing.T) string {
	t.Helper()
	v := os.Getenv("DATABASE_URL")
	if v == "" {
		t.Skip("DATABASE_URL not set; start Postgres (make db) to run this test")
	}
	return v
}

// TestChaos is the real test in this repo. The subtests share one database and
// each resets it, so they must not run in parallel. Each seed is a full randomised run
// with the chain misbehaving and the process being killed underneath it; a pass
// means every invariant survived.
func TestChaos(t *testing.T) {
	ctx := context.Background()
	for _, seed := range []int64{1, 2, 3, 4, 5} {
		seed := seed
		t.Run(name(seed), func(t *testing.T) {
			res, err := app.RunChaos(ctx, app.ChaosOptions{
				DSN: dsn(t), ChainState: t.TempDir() + "/chain.json",
				Seed: seed, Iterations: 600, Users: 8, Markets: 12,
				VerifyEvery: 100, CrashEvery: 25,
				Faults: chain.DefaultFaults(), LogLevel: slog.LevelError, Fresh: true,
			})
			if err != nil {
				t.Fatalf("chaos run failed: %v", err)
			}
			if len(res.Violations) > 0 {
				t.Fatalf("seed %d broke invariants at iteration %d:\n%s",
					seed, res.FailedAtIdx, app.Format(res.Violations))
			}
			t.Log(res.String())
		})
	}
}

// TestClean is the control group: with no faults injected, the same workload
// must still end with the ledger and the chain exactly equal. If this fails,
// the bug is in the engine and not in the fault injection.
func TestClean(t *testing.T) {
	res, err := app.RunChaos(context.Background(), app.ChaosOptions{
		DSN: dsn(t), ChainState: t.TempDir() + "/chain.json",
		Seed: 42, Iterations: 400, VerifyEvery: 100, CrashEvery: 0,
		Faults: chain.NoFaults(), LogLevel: slog.LevelError, Fresh: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Violations) > 0 {
		t.Fatalf("clean run broke invariants:\n%s", app.Format(res.Violations))
	}
	if res.Final.LedgerInternal != res.Final.ChainFinalized {
		t.Fatalf("clean run did not converge: ledger %d, chain %d",
			res.Final.LedgerInternal, res.Final.ChainFinalized)
	}
}

func name(seed int64) string { return "seed=" + itoa(seed) }

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	return string(b)
}
