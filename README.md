# prediction-margin-reconciler

A margin and settlement engine for prediction-market derivatives, built to answer
one question: **when the off-chain ledger and the on-chain vault disagree, how do
you know whether that is normal or a disaster?**

The interesting part is not the feature set. It is that the correctness claims are
executable. One command hammers the system with a randomised workload while the
chain duplicates events, withholds them, loses receipts and reorgs, and while the
process is hard-killed every 25 operations — then proves every funding invariant
still holds. A second command replays the same history through the real Rust
contract and fails if the two implementations disagree by a single unit.

```
make db            # postgres + redis
make demo          # narrated walkthrough of each failure mode, one at a time
make chaos         # 2000 random ops, faults on, hard restart every 25
make differential  # replay finalised history through the Rust contract
make crash-test    # a real SIGKILL to a real process, six times, mid-traffic
make bench         # price fan-out, ledger throughput, reconciliation latency
make serve         # operator panel on :8080
```

Go, Postgres, Redis, Rust, TypeScript. One third-party Go dependency (`lib/pq`);
the Redis client is 90 lines of RESP. The Rust contract has zero dependencies. The
TypeScript layer has zero runtime dependencies.

---

## Results

```
$ make chaos-hard        # 20 seeds, 2000 operations each
seed 1   RESULT: all 8 invariants hold after chaos
...
seed 20  RESULT: all 8 invariants hold after chaos
```

40,000 randomised operations across 20 seeds. 1,600 hard restarts. Injected faults
throughout: 10% of submits fail, another 10% return an error *after* the
transaction landed, 20% of events arrive twice, 20% of batches arrive shuffled,
10% of events are withheld, 5% of blocks reorg.

```
seed=1 iterations=2000 crashes=80 verifications=21 in 15.9s
faults: submit_err=10% lost_receipt=10% duplicate=20% reorder=20% gap=10% reorg=5% confirms=3
workload: deposit=447 withdraw=385 open=292 close=131 settle=133 replay=228
          new_market=122 mine=1181 price_tick=240 withdraw_rejected=20
final: ledger=75666 chain=75666 explained=0 unexplained=0 status=ok
RESULT: all 8 invariants hold after chaos
```

```
$ make differential
replayed 806 finalised call(s): 806 applied, 0 refused
  end state: balance=62063000000 escrow_total=40184487 height=810
OK: the Rust contract and the Go simulator agree exactly
```

`make chaos-clean` is the control group: the same workload with no faults injected.
If that ever fails, the bug is in the engine, not in the fault injection. Every run
is reproducible from its seed — each worker is a single-shot `Tick` method and the
harness calls them in a seeded random order, so there is no hidden concurrency in
the control flow.

---

## The reconciliation invariant

The naive version is wrong:

```
ledger_balance == on_chain_vault_balance        # wrong
```

A system built on that either alerts constantly or gets muted, because money
genuinely is in two places at once while a transaction is in flight. Pending is not
an anomaly. It is a state, and it should be modelled as one.

What this engine asserts instead:

```
ledger_internal − chain_finalized_vault  ==  withdrawn_not_recognised
                                           − deposits_not_ingested
                                           + provisional_credits
```

Three terms, each computed from an enumerable set of in-flight facts:

| Term | Meaning | Direction |
|---|---|---|
| `withdrawn_not_recognised` | the chain has finalised a payout the ledger has not booked yet | ledger high |
| `deposits_not_ingested` | a deposit finalised on chain that never reached us as an event | ledger low |
| `provisional_credits` | credits granted on blocks that are not final yet | ledger high |

Whatever is left over is the **residue**, and the residue is the only thing worth
paging someone about:

- `residue == 0`, terms zero → `ok`
- `residue == 0`, terms non-zero → `explained`, and the panel shows which term
- `residue > 0` → `unsafe_shortfall`. The ledger believes in money the vault does
  not hold. **Withdrawals halt immediately.** There is no safe automatic repair, so
  it goes to a human.
- `residue < 0` → `unsafe_surplus`. The chain holds money we cannot attribute.
  Safe, but still a bug; alert only.

Two details that matter more than they look.

**Every term is read live from the chain, never from a cached flag.** An earlier
version computed `provisional_credits` from a stored `finalized` boolean, so the
explanation was only as fresh as the last worker pass. A reconciler whose
arithmetic lags reality raises false alarms, and a reconciler that raises false
alarms gets muted — which is worse than not having one.

**The residue is signed, and the two signs are not equally urgent.** Collapsing
them into `abs(drift) > threshold` throws away the only thing you need to know at
3am.

### The second dimension: per-market escrow

The balance check above proves we hold the right *total*. It cannot prove the total
is earmarked against the right markets, because moving funds into escrow does not
change the vault balance at all. A single-number reconciliation would be perfectly
happy with collateral attributed to entirely the wrong markets — which is exactly
the failure that bankrupts a margin system: the total looks right, and then one
market resolves and there is nothing behind it.

So the vault also tracks escrow per market, and a background worker converges it on
the ledger's collateral. Three design decisions:

**Batched, not per-trade.** Putting a chain call in the path of opening a position
would make the hot trading path as slow as block time and tie a user's fill to a
transaction that might not land. Trading stays purely off-chain and fast; the
chain's view converges behind it.

**The instruction carries an absolute target, not a delta.** Under an unknown
receipt the same instruction may be submitted several times. An assignment
converges however many times it runs; an increment compounds. When you cannot know
whether your last call landed, idempotence has to come from the shape of the
operation rather than from the caller being careful.

**The invariant is convergence, not equality.** Escrow drift is normal and
constant. What is abnormal is drift that stops shrinking, so the engine counts how
many reconciliation passes a market's gap has stood completely still, and only a
stalled gap is a violation. Stating this as `escrow == collateral` produced a false
positive on the very first run.

---

## Authority boundaries

The single most useful thing to be explicit about. Nothing in the codebase crosses
these lines.

| Concern | Authority | Why |
|---|---|---|
| Intent and the ledger | **Postgres** | transactions, unique indexes, a real audit trail |
| Custody, escrow, finality | **the chain** | it holds the money; we cannot roll it back |
| Mark prices, leases | **Redis** | fully reconstructible from Postgres and the chain |
| Escalation policy | **TypeScript watcher** | reads only; decides when drift becomes a page |

Redis holds nothing that could not be rebuilt. Losing it is an availability problem
and never a correctness problem — the liquidation lease is an optimisation, and if
it is unavailable it simply always grants, because the real guard is the version
check on the position row.

There is no dual write. A ledger move and its on-chain intent commit in one
Postgres transaction via a **transactional outbox**, so there is no window where
money is reserved but nothing will ever submit it, and no window where we submit
something the ledger has not accounted for.

---

## The intent state machine

```
                    ┌─────────────┐
    reserve funds   │  reserved   │  ledger move + intent + outbox row,
    ───────────────►│             │  all in one transaction
                    └──────┬──────┘
                           │ outbox submits (retries are free)
                           ▼
                    ┌─────────────┐
                    │  submitted  │  the chain may or may not have it.
                    └──┬───┬───┬──┘  this is a modelled state, not an error
        finalised      │   │   │      contract confirms it can never execute
        ┌──────────────┘   │   └──────────────┐
        ▼                  │                  ▼
  ┌───────────┐            │            ┌───────────┐
  │ confirmed │            │            │  expired  │  funds refunded
  └─────┬─────┘            │            └───────────┘
        │ finality violated│
        ▼                  │
  ┌───────────┐            │
  │ reverted  │────────────┘  compensating transfer, then decide again
  └───────────┘
```

`reserved` and `submitted` are the in-flight set. Everything in there is a claim on
money the chain may not agree with, and it is exactly the set the reconciler
enumerates to build its explanation.

Withdrawals are only recognised at **finality**. Deposits are credited at **one
confirmation** — a deliberate product tradeoff so a depositor can trade
immediately, and the reason this system is exposed to reorgs at all.

Crossing into `expired` is not a timeout. The engine asks the contract to
permanently refuse the nonce and waits for that to be confirmed before releasing
the reservation. Height alone is an inference about the chain; a cancellation is a
fact recorded *by* the chain.

---

## The contract

`contracts/vault/` is the on-chain half, in Rust: a portable state machine with no
dependencies, `#![forbid(unsafe_code)]`, `u128` micro-units and overflow checks on.
It is deliberately not an Anchor program or a CosmWasm entry point — these are the
custody rules, not a deployment target, so keeping them framework-free means they
compile unchanged for any of those and CI runs `cargo test` in seconds without a
chain toolchain.

It is the authority for three rules the off-chain engine depends on completely:

1. **A nonce executes at most once, and dedupe happens at execution** — not at
   submission. This is what makes an unknown receipt survivable: the engine can
   retry blindly and ask later.
2. **Expiry is enforced by height, in the contract.**
3. **Escrow can never exceed custody, and a withdrawal can never touch escrowed
   funds.** Escrow is a claim on money already held, not extra money.

11 tests, including a deterministic fuzz (400 random calls × 64 rounds, asserting
the contract's own invariants after every one). The test worth reading is
`dedupe_wins_over_expiry_for_a_call_that_already_landed`: the check order has to be
dedupe first, because the other order makes a late retry of a *successful*
withdrawal report `Expired`, and the engine would then refund money the vault had
already paid out.

### The differential test

The engine's entire correctness argument rests on the simulator behaving like the
contract. That was an assumption, and an unchecked assumption in this position is
how you get a reconciler that passes its own tests and still loses money. So it
gets checked:

```
pmr trace -out /tmp/trace.txt                    # Go exports finalised history
cargo run --bin replay -- /tmp/trace.txt         # Rust replays it
```

The Go simulator writes every finalised call, in order, with the height it executed
at. The Rust contract replays them and both must agree on the vault balance and on
every market's escrow, to the unit. A typical trace carries ~800 calls including
withdrawals, escrow instructions, settlements and cancellations.

Zero refusals is a hard assertion, not just information: every call in the trace was
accepted on chain, so a refusal on the Rust side would mean the two disagree about
what is permissible — most likely the free-versus-escrowed funds rule — even if the
final balances happened to line up.

---

## Invariants

All eight are checked by `pmr verify` and by the chaos harness every 100
operations. In strict mode (end of a chaos run, after the system is driven to
quiescence) the ledger is additionally required to equal the chain exactly.

| | Invariant |
|---|---|
| **I0** | a cached balance equals the sum of its entries |
| **I1** | every transfer's entries sum to zero |
| **I2** | `ledger − chain_finalized` equals the explainable in-flight delta |
| **I3** | no user available, pending-withdrawal or margin account is ever negative |
| **I4** | margin balances equal the collateral on open positions |
| **I5** | each idempotency key maps to exactly one transfer |
| **I6** | no in-flight intent is orphaned; funds are never left dangling |
| **I7** | per-market escrow converges on ledger collateral and never stalls |

I1 is enforced at write time *and* verified afterwards, because an invariant you
only enforce is an invariant you cannot prove. I5 is structurally impossible to
break (a unique index), and the check exists so a future migration dropping that
index fails loudly — the chaos harness also replays recorded idempotency keys under
load and asserts that zero new entries are written.

---

## Failure modes

| Failure | Detection | Recovery |
|---|---|---|
| Duplicate event delivery | primary key on `chain_events.event_id` | insert is a no-op |
| Reordered events | apply by `chain_seq`, not arrival order | — |
| Withheld / dropped event | gap in the contiguous sequence | cursor refuses to advance past it; if lost permanently, the reconciler ingests the fact from chain state directly |
| Submit fails | error from the node | retry; the nonce makes it free |
| **Submit "fails" but landed** | not detectable, and not worth detecting | the resolver asks the chain about the nonce and settles from that |
| Reorg of a provisional credit | per-fact block-hash re-check | compensating transfer; if the credit was already spent, the insurance fund absorbs the shortfall and the account is frozen |
| Finality violated | confirmed intent's block no longer canonical | compensating transfer, back to `submitted` |
| Process killed mid-flight | — | Postgres holds all state; recovery is the normal workers running again |
| Withdrawal that will never land | contract-confirmed cancellation | refund only after the chain records the refusal |
| Escrow gap that stops closing | stall counter per market | alert; the instruction is re-issued each pass regardless |
| Concurrent close vs liquidation | optimistic version on the position row | exactly one wins, the loser gets `ErrConflict` and stops |
| Ledger corruption with no chain backing | reconciliation residue | withdrawals halt, human paged |

Two design notes on that table.

**A lost receipt does not need to be distinguished from a real failure.** The
outbox cannot tell them apart and does not try. The intent id *is* the on-chain
nonce, the contract records processed nonces, so re-submitting is free and the
resolver learns the truth from authoritative chain state later. This is the
highest-leverage decision in the codebase.

**The event stream and the truthful tier are separated on purpose.** `PollEvents`
is fast and lies. `TxStatus`, `VaultState` and `CanonicalHash` are slower and do
not. Every recovery path falls back to the second tier. Nothing that moves money
depends on the first.

---

## Five bugs the harness found

Worth writing down, because finding these is the entire point of the project.

**1. Height-based finalisation blesses orphans.** Promoting provisional credits
with `where block_height <= finalized_height` looks obviously correct and is not. A
block can be replaced by a *different* block at the same height, so that bulk
update marks the orphan final — and the reorg detector, which only scans
unfinalised facts, never looks at it again. Double credit. Fixed by checking each
fact's block hash against the chain individually. Symptom: an unexplained surplus
that grew over long runs.

**2. A wall-clock timeout is not an expiry.** Refunding a withdrawal after 30
seconds of silence is unsound: the transaction can sit in a mempool, or come back
from a reorg, and land *after* the refund. The expiry has to be enforced by the
same authority that would execute the transfer, so intents carry an `expiry_height`
and the contract refuses the nonce past it. The engine now also waits for a
contract-confirmed cancellation rather than inferring from height alone.

**3. Nonce dedupe has to happen at execution, not at submission.** A nonce sitting
in the mempool is not yet "processed", so a retry appended it a second time and the
same withdrawal executed twice in one block. Surfaced as a 5 USDC unexplained
shortfall. This one was in the simulator, and it is exactly the mistake a real
contract can make.

**4. An idempotency key on the wrong thing deadlocks.** Escrow instructions were
keyed `escrow:<market>:<target>`. Once an instruction for a given target expired,
no new one for the same target could ever be created — `on conflict do nothing`
returned the dead intent and wrote no outbox row — and the market stayed unsynced
forever. Idempotency has to key on what is actually unique, which here is "an
instruction is in flight for this market", not the value being installed. Now a
partial unique index on `(market_id) where kind = 'escrow_set' and state in
('reserved','submitted')`.

**5. A stall detector that penalises progress.** The escrow stall counter advanced
even for markets that already had a matching instruction in flight. In flight *is*
convergence, so the counter has to reset. Before the fix, every busy market
eventually looked wedged — and a detector that fires on healthy behaviour is
strictly worse than no detector, because it teaches you to ignore it.

Two of these were in the observability rather than the money path. That is not a
coincidence: a wrong invariant is as dangerous as a wrong transfer, because it is
the thing you will trust at 3am.

---

## Alerting

`ts/` is the escalation policy in TypeScript: given a rolling window of engine
states, when does a difference become a page? Zero runtime dependencies — Node's
built-in fetch and http server — because a component whose job is to notice
outages should not have a dependency tree of its own.

The policy is a pure function over the window rather than a threshold on the latest
sample, because every interesting property here is about time:

- a residue of zero with large in-flight terms is a healthy busy system
- a residue that appears and clears within a poll or two is a race, not a bug
- a residue that persists is a bug, and its sign decides the urgency
- a shortfall is never "wait and see" — the engine has already halted withdrawals
- in-flight totals that are bit-for-bit identical for ten polls are wedged, even
  though nothing is technically wrong
- an unreachable engine is a page, not healthy silence

11 tests cover the interesting cases, including that recovery clears a streak
immediately and that churning in-flight work is left alone. Stating the policy
once, with tests, is the alternative to spreading it across a dashboard and a
runbook.

---

## Benchmarks

```
$ make bench
price feed (redis, 2000 markets)
  publish        n=38086   p50=14µs      p99=5.556ms   max=43.307ms
  fan-out        n=1886    p50=1.743ms   p99=9.431ms   max=44.23ms
  throughput     13204 ticks/s over 2.884s (1914 stale ticks dropped)

ledger writes (8 writers contending over 4 hot accounts)
  per transfer   n=2000    p50=8.33ms    p99=44.875ms  max=61.535ms
  throughput     636 transfers/s over 3.143s
  rejected       0 (insufficient funds or lost a row-version race)

reconciliation, full pass
  cold           p50=1.264ms   p99=3.404ms
  warm           p50=1.222ms   p99=1.252ms
```

One run on one laptop, against Postgres and Redis in Docker — the absolute
numbers move around by a factor of two between runs and mean little on their own.
What they are for is shape: publish is microseconds, fan-out is milliseconds, a
ledger commit under contention is tens of milliseconds, and a full reconciliation
pass is cheap enough to run every couple of seconds forever.

p50 and p99, not averages. An average latency on a liquidation path is close to
meaningless: the position that gets liquidated late is the one that costs money,
and it lives in the tail.

Ledger writers deliberately fight over four accounts. An uncontended benchmark says
nothing about whether the canonical lock ordering holds up, and a deadlock there
would be a correctness bug rather than a slowdown. Stale ticks are injected
deliberately (one in twenty carries an old sequence number) so the monotonic guard
is measured rather than assumed — it read zero and proved nothing until that was
added.

---

## Concurrency

- **Canonical lock ordering.** Balance rows are locked `FOR UPDATE` in sorted
  account-code order, so two transfers touching the same pair of accounts cannot
  deadlock. Chosen over `SERIALIZABLE` plus a retry loop: under the hot-account
  contention this workload actually has, explicit ordered locks are easier to reason
  about and produce no retry storms.
- **Optimistic versions on positions.** A user close and a liquidation racing the
  same position is a normal event, not an error. Exactly one `UPDATE ... WHERE
  version = $n` wins.
- **`FOR UPDATE SKIP LOCKED`** plus a lease column on the outbox, so replicas do not
  contend or double-submit.
- **A small connection pool on purpose** (16). It makes contention visible under the
  chaos workload instead of hiding it behind spare connections.
- **One open position per user per market**, as a partial unique index, and one
  in-flight escrow instruction per market by the same mechanism.

---

## Layout

```
cmd/pmr/                 chaos | demo | serve | verify | bench | trace | reset
internal/core/           schema.sql, double-entry ledger, intent state machine,
                         markets and positions, chain-event inbox
internal/chain/sim.go    the hostile chain: duplicates, gaps, lost receipts,
                         reorgs, and execution rules mirroring the contract
internal/app/            worker ticks, reconciler, escrow sync, invariant
                         verifier, chaos harness, benchmarks
internal/httpapi/        API and operator panel
internal/pricefeed/      mark-price cache and liquidation lease (Redis or memory)
contracts/vault/         the Rust contract, its tests, and the replay tool
ts/                      the escalation policy and its tests
```

The operator panel renders the reconciliation identity live, term by term, with the
residue boxed in red when it is non-zero. There is a button that injects a phantom
credit — balanced double entry and all — so you can watch the ledger stay
internally perfect while reconciliation catches it and halts withdrawals.

---

## Scope, deliberately

Not built, on purpose:

- **A deployed contract.** The rules are real Rust and are tested against the
  engine; wiring them to a specific chain's SDK would demonstrate a framework, not
  judgment. The `Chain` surface is a narrow interface, so swapping the simulator for
  an RPC client is a contained change.
- **An order book.** Positions open against a mark price. Matching is a different
  problem.
- Auth, multi-tenancy, Kubernetes, a second chain.

## At 100× scale

- Shard the ledger by account, keeping each transfer inside one shard; cross-shard
  moves become intents through the same machinery already built here.
- Replace reconciler polling with logical replication / CDC, so drift is detected in
  the same pass that writes it.
- Batch settlement per market rather than per position — the current per-position
  loop is the first thing that would bind.
- Partition `entries` by month; it is append-only, which makes this easy.
- Move the price fan-out off Postgres entirely: mark prices already live in Redis,
  and the liquidator should read a snapshot rather than scanning open positions.

---

## Running it

Requires Go 1.22+, Postgres 16, Rust (for the contract), Node 20+ (for the
watcher). Redis is optional — it falls back to an in-memory cache and says so.

```bash
make db
make demo          # start here: six failure modes, narrated
make chaos
make differential
make crash-test
make bench
make serve         # http://localhost:8080
make watch         # in another shell: the alerting layer

go run ./cmd/pmr chaos -iterations 5000 -seed 7 -crash-every 10 -v
go run ./cmd/pmr verify -chain-state /tmp/pmr-chain.json -settle
```

`DATABASE_URL` and `REDIS_URL` override the defaults. CI runs gofmt, `go vet`,
`tsc`, the Rust contract tests, the TypeScript policy tests, the Go chaos suite, the
narrated demo, five chaos seeds, the differential test, the SIGKILL test and the
benchmarks on every push.
