package app

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ranranrunforit/prediction-margin-reconciler/internal/core"
)

// BenchOptions configures the load run.
type BenchOptions struct {
	Markets     int
	Ticks       int           // price ticks to publish per market
	Writers     int           // concurrent ledger writers
	Transfers   int           // ledger transfers per writer
	HotAccounts int           // how many accounts the writers fight over
	Duration    time.Duration // cap on the price phase
}

type Latency struct {
	N             int           `json:"n"`
	P50, P99, Max time.Duration `json:"-"`
}

func (l Latency) String() string {
	return fmt.Sprintf("n=%-7d p50=%-9s p99=%-9s max=%s",
		l.N, l.P50.Round(time.Microsecond), l.P99.Round(time.Microsecond), l.Max.Round(time.Microsecond))
}

func summarise(d []time.Duration) Latency {
	if len(d) == 0 {
		return Latency{}
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	at := func(q float64) time.Duration {
		i := int(q * float64(len(d)))
		if i >= len(d) {
			i = len(d) - 1
		}
		return d[i]
	}
	return Latency{N: len(d), P50: at(0.50), P99: at(0.99), Max: d[len(d)-1]}
}

type BenchResult struct {
	Feed           string
	Markets        int
	PriceTicks     int
	PriceElapsed   time.Duration
	PricePublish   Latency
	PriceFanout    Latency // publish -> visible to a reader
	StaleDropped   int64
	LedgerWrites   int
	LedgerElapsed  time.Duration
	LedgerLatency  Latency
	LedgerConflict int64
	Contention     int
	HotAccounts    int
	ReconcileAt    []Latency
}

func (r BenchResult) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "price feed (%s, %d markets)\n", r.Feed, r.Markets)
	fmt.Fprintf(&b, "  publish        %s\n", r.PricePublish)
	fmt.Fprintf(&b, "  fan-out        %s\n", r.PriceFanout)
	fmt.Fprintf(&b, "  throughput     %.0f ticks/s over %s (%d stale ticks dropped)\n",
		float64(r.PriceTicks)/r.PriceElapsed.Seconds(), r.PriceElapsed.Round(time.Millisecond), r.StaleDropped)
	fmt.Fprintf(&b, "\nledger writes (%d writers contending over %d hot accounts)\n",
		r.Contention, r.HotAccounts)
	fmt.Fprintf(&b, "  per transfer   %s\n", r.LedgerLatency)
	fmt.Fprintf(&b, "  throughput     %.0f transfers/s over %s\n",
		float64(r.LedgerWrites)/r.LedgerElapsed.Seconds(), r.LedgerElapsed.Round(time.Millisecond))
	fmt.Fprintf(&b, "  rejected       %d (insufficient funds or lost a row-version race)\n", r.LedgerConflict)
	fmt.Fprintf(&b, "\nreconciliation, full pass\n")
	for i, l := range r.ReconcileAt {
		fmt.Fprintf(&b, "  %-14s %s\n", []string{"cold", "warm"}[min(i, 1)], l)
	}
	return b.String()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Bench measures the three things that would actually bind at scale: how fast a
// mark price reaches whatever is going to liquidate against it, how fast the
// ledger commits money under contention, and how long a full reconciliation pass
// takes as history grows.
//
// It reports p50 and p99 rather than an average. An average latency on a
// liquidation path is close to meaningless: the position that gets liquidated
// late is the one that costs money, and it lives in the tail.
func (a *App) Bench(ctx context.Context, o BenchOptions) (*BenchResult, error) {
	if o.Markets <= 0 {
		o.Markets = 2000
	}
	if o.Ticks <= 0 {
		o.Ticks = 20
	}
	if o.Writers <= 0 {
		o.Writers = 8
	}
	if o.Transfers <= 0 {
		o.Transfers = 250
	}
	if o.HotAccounts <= 0 {
		o.HotAccounts = 4
	}
	if o.Duration <= 0 {
		o.Duration = 20 * time.Second
	}

	res := &BenchResult{Feed: a.Feed.Name(), Markets: o.Markets,
		Contention: o.Writers, HotAccounts: o.HotAccounts}
	rnd := rand.New(rand.NewSource(1))

	// ---------------------------------------------------------- price fan-out
	//
	// Sequence numbers are the whole reason this is safe to run in parallel: a
	// tick that arrives out of order is dropped rather than applied, so a slow
	// publisher can never roll a mark price backwards.
	marks := make([]string, o.Markets)
	for i := range marks {
		marks[i] = fmt.Sprintf("bm%05d", i)
	}
	var publishes, fanouts []time.Duration
	var mu sync.Mutex
	var stale int64
	latest := make([]int64, o.Markets)

	start := time.Now()
	var wg sync.WaitGroup
	workers := 16
	per := (o.Markets + workers - 1) / workers
	for w := 0; w < workers; w++ {
		lo, hi := w*per, min((w+1)*per, o.Markets)
		if lo >= hi {
			continue
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			localPub := make([]time.Duration, 0, (hi-lo)*o.Ticks)
			localFan := make([]time.Duration, 0, hi-lo)
			for t := 0; t < o.Ticks; t++ {
				for i := lo; i < hi; i++ {
					if time.Since(start) > o.Duration {
						break
					}
					seq := int64(t + 1)
					// One tick in twenty is deliberately stale, so the guard is
					// measured rather than assumed. Without this the counter
					// would read zero and prove nothing.
					if t > 0 && rnd.Intn(20) == 0 {
						seq = int64(rnd.Intn(t) + 1)
					}
					// Monotonic guard: a tick older than what we already
					// published for this market is discarded, not applied.
					if prev := atomic.LoadInt64(&latest[i]); seq <= prev {
						atomic.AddInt64(&stale, 1)
						continue
					}
					price := int64(100_000 + rnd.Intn(800_000))
					t0 := time.Now()
					if err := a.Feed.Publish(ctx, marks[i], price); err != nil {
						continue
					}
					localPub = append(localPub, time.Since(t0))
					atomic.StoreInt64(&latest[i], seq)
					if t == o.Ticks-1 {
						// Measure what the liquidator would actually see.
						if all, err := a.Feed.Marks(ctx); err == nil {
							if _, ok := all[marks[i]]; ok {
								localFan = append(localFan, time.Since(t0))
							}
						}
					}
				}
			}
			mu.Lock()
			publishes = append(publishes, localPub...)
			fanouts = append(fanouts, localFan...)
			mu.Unlock()
		}(lo, hi)
	}
	wg.Wait()
	res.PriceElapsed = time.Since(start)
	res.PricePublish = summarise(publishes)
	res.PriceFanout = summarise(fanouts)
	res.PriceTicks = res.PricePublish.N
	res.StaleDropped = stale

	// ---------------------------------------------------------- ledger writes
	//
	// Writers deliberately fight over a small pool of accounts. This is the case
	// that matters: an idle benchmark tells you nothing about whether the lock
	// ordering holds up, and a deadlock here would be a correctness bug rather
	// than a slowdown.
	// Fund through a real on-chain deposit rather than writing straight to the
	// ledger. Minting money off-chain would make the benchmark trip the
	// reconciler on every pass -- correctly, which is a nice demonstration but a
	// useless benchmark.
	hot := make([]string, o.HotAccounts)
	for i := range hot {
		hot[i] = fmt.Sprintf("bench%02d", i)
		a.Chain.Deposit(hot[i], 1_000_000*core.One)
	}
	for i := 0; i < 8; i++ {
		a.Chain.Mine()
		if _, err := a.InboxTick(ctx); err != nil {
			return nil, err
		}
	}
	for _, h := range hot {
		bal, err := a.Store.Balance(ctx, core.AvailableAcct(h))
		if err != nil {
			return nil, err
		}
		if bal == 0 {
			return nil, fmt.Errorf("bench funding for %s never arrived", h)
		}
	}

	var writeLat []time.Duration
	var conflicts int64
	start = time.Now()
	wg = sync.WaitGroup{}
	for w := 0; w < o.Writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			local := make([]time.Duration, 0, o.Transfers)
			for i := 0; i < o.Transfers; i++ {
				from := hot[r.Intn(len(hot))]
				to := hot[r.Intn(len(hot))]
				if from == to {
					to = hot[(r.Intn(len(hot))+1)%len(hot)]
				}
				amt := int64(1+r.Intn(100)) * core.One
				t0 := time.Now()
				_, _, err := a.Store.Post(ctx, core.Transfer{
					Kind:           "bench.transfer",
					IdempotencyKey: fmt.Sprintf("bench-%d-%d", w, i),
					Legs: []core.Leg{
						{Account: core.AvailableAcct(from), Amount: -amt},
						{Account: core.AvailableAcct(to), Amount: amt},
					},
				})
				if err != nil {
					atomic.AddInt64(&conflicts, 1)
					continue
				}
				local = append(local, time.Since(t0))
			}
			mu.Lock()
			writeLat = append(writeLat, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	res.LedgerElapsed = time.Since(start)
	res.LedgerLatency = summarise(writeLat)
	res.LedgerWrites = res.LedgerLatency.N
	res.LedgerConflict = conflicts

	// ---------------------------------------------------------- reconciliation
	for pass := 0; pass < 2; pass++ {
		var d []time.Duration
		for i := 0; i < 5; i++ {
			t0 := time.Now()
			if _, err := a.Reconcile(ctx); err != nil {
				return nil, err
			}
			if _, err := a.ReconcileEscrow(ctx, false); err != nil {
				return nil, err
			}
			d = append(d, time.Since(t0))
		}
		res.ReconcileAt = append(res.ReconcileAt, summarise(d))
	}
	return res, nil
}
