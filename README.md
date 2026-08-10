# prediction-margin-reconciler

A small margin and settlement engine for prediction-market derivatives, built to
answer one question: **when the off-chain ledger and the on-chain vault disagree,
how do you know whether that is normal or a disaster?**

The interesting part is not the feature set. It is that the correctness claims
are executable. A single command hammers the system with a randomised workload
while the chain duplicates events, withholds them, loses receipts and reorgs,
and while the process is hard-killed every 25 operations — then proves that every
funding invariant still holds.

```
make db          # postgres + redis
make demo        # narrated walkthrough of each failure mode, one at a time
make chaos       # 2000 random ops, faults on, hard restart every 25
make crash-test  # a real SIGKILL to a real process, six times, mid-traffic
make serve       # operator panel on :8080
```

Go, Postgres, Redis. One third-party dependency (`lib/pq`); the Redis client is
90 lines of RESP rather than a module.

---

## Results

```
$ make chaos-hard        # 20 seeds, 2000 operations each
seed 1   RESULT: all 7 invariants hold after chaos
...
seed 20  RESULT: all 7 invariants hold after chaos
```

40,000 randomised operations across 20 seeds. 1,600 hard restarts. Injected
faults throughout: 10% of submits fail, another 10% return an error *after* the
transaction landed, 20% of events arrive twice, 20% of batches arrive shuffled,
10% of events are withheld, 5% of blocks reorg.

A single run looks like this:

```
seed=1 iterations=2000 crashes=80 verifications=21 in 26.0s
faults: submit_err=10% lost_receipt=10% duplicate=20% reorder=20% gap=10% reorg=5% confirms=3
workload: deposit=690 withdraw=577 open=477 close=222 settle=186 replay=293
          new_market=175 mine=1803 price_tick=379 withdraw_rejected=12
final: ledger=119315 chain=119315 explained=0 unexplained=0 status=ok
RESULT: all 7 invariants hold after chaos
```

`make chaos-clean` is the control group: the same workload with no faults
injected. If that ever fails, the bug is in the engine, not in the fault
injection.

Every run is reproducible from its seed. There is no hidden concurrency in the
control flow — each worker is a single-shot `Tick` method, and the harness calls
them in a seeded random order.

---

## The reconciliation invariant

The naive version is wrong:

```
ledger_balance == on_chain_vault_balance        # wrong
```

A system built on that either alerts constantly or gets muted, because money
genuinely is in two places at once while a transaction is in flight. Pending is
not an anomaly. It is a state, and it should be modelled as one.

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
  not hold. **Withdrawals halt immediately.** There is no safe automatic repair
  for this, so it goes to a human.
- `residue < 0` → `unsafe_surplus`. The chain holds money we cannot attribute.
  Safe, but still a bug; alert only.

Two details that matter more than they look:

**Every term is read live from the chain, never from a cached flag.** An earlier
version computed `provisional_credits` from a stored `finalized` boolean, so the
explanation was only as fresh as the last worker pass. A reconciler whose
arithmetic lags reality raises false alarms, and a reconciler that raises false
alarms gets muted — which is worse than not having one.

**The residue is signed, and the two signs are not equally urgent.** Collapsing
them into `abs(drift) > threshold` throws away the only thing you actually need
to know at 3am.

---

## Authority boundaries

The single most useful thing to be explicit about. Nothing in the codebase
crosses these lines.

| Concern | Authority | Why |
|---|---|---|
| Intent and the ledger | **Postgres** | transactions, unique indexes, a real audit trail |
| Custody and finality | **the chain** | it holds the money; we cannot roll it back |
| Mark prices, leases | **Redis** | fully reconstructible from Postgres and the chain |

Redis holds nothing that could not be rebuilt. Losing it is an availability
problem and never a correctness problem — the liquidation lease is an
optimisation, and if it is unavailable it simply always grants, because the real
guard is the version check on the position row.

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
        finalised      │   │   │      contract refuses the nonce
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

`reserved` and `submitted` are the in-flight set. Everything in there is a claim
on money the chain may not agree with, and it is exactly the set the reconciler
enumerates to build its explanation.

Withdrawals are only recognised at **finality**. Deposits are credited at **one
confirmation** — a deliberate product tradeoff so a depositor can trade
immediately, and the reason this system is exposed to reorgs at all.

---

## Invariants

All seven are checked by `pmr verify` and by the chaos harness every 100
operations. In strict mode (end of a chaos run, after the system is driven to
quiescence) the ledger is additionally required to equal the chain exactly.

| | Invariant |
|---|---|
| **I0** | a cached balance equals the sum of its entries |
| **I1** | every transfer's entries sum to zero |
| **I2** | `ledger − chain_finalized` equals the explainable in-flight delta |
| **I3** | no user available, pending-withdrawal, or margin account is ever negative |
| **I4** | margin balances equal the collateral on open positions |
| **I5** | each idempotency key maps to exactly one transfer |
| **I6** | no in-flight intent is orphaned; funds are never left dangling |

I1 is enforced at write time *and* verified afterwards, because an invariant you
only enforce is an invariant you cannot prove. I5 is structurally impossible to
break (a unique index), and the check exists so that a future migration dropping
that index fails loudly — the chaos harness also replays recorded idempotency
keys under load and asserts that zero new entries are written.

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
| Concurrent close vs liquidation | optimistic version on the position row | exactly one wins, the loser gets `ErrConflict` and stops |
| Ledger corruption with no chain backing | reconciliation residue | withdrawals halt, human paged |

Two design notes on that table.

**A lost receipt does not need to be distinguished from a real failure.** The
outbox cannot tell them apart and does not try. The intent id *is* the on-chain
nonce, the contract records processed nonces, so re-submitting is free and the
resolver learns the truth from authoritative chain state later. This is the
single highest-leverage decision in the codebase.

**The event stream and the truthful tier are separated on purpose.**
`PollEvents` is fast and lies. `TxStatus`, `VaultState` and `CanonicalHash` are
slower and do not. Every recovery path falls back to the second tier. Nothing
that moves money depends on the first.

---

## Two bugs the harness found

Worth writing down, because finding these is the entire point of the project.

**1. Height-based finalisation blesses orphans.** Promoting provisional credits
with `where block_height <= finalized_height` looks obviously correct and is not.
A block can be replaced by a *different* block at the same height, so that bulk
update marks the orphan final — and the reorg detector, which only scans
unfinalised facts, never looks at it again. Double credit. The fix is to check
each fact's block hash against the chain individually
(`app.settleFacts`). Symptom in the harness: an unexplained surplus that grew
over long runs.

**2. A wall-clock timeout is not an expiry.** Refunding a withdrawal after 30
seconds of silence is unsound: the transaction can sit in a mempool, or come back
from a reorg, and land *after* the refund. The chain then pays out money we
already gave back. The expiry has to be enforced by the same authority that would
execute the transfer — so intents carry an `expiry_height` and the contract
refuses the nonce past it. Only then is an off-chain refund safe.

A third bug was in the simulator itself, and it is instructive: nonce dedupe has
to happen at **execution**, not at submission. A nonce sitting in the mempool is
not yet "processed", so a retry appended it a second time and the same withdrawal
executed twice in one block. The harness surfaced it as a 5 USDC unexplained
shortfall. A real contract has to make the same distinction.

---

## Concurrency

- **Canonical lock ordering.** Balance rows are locked `FOR UPDATE` in sorted
  account-code order, so two transfers touching the same pair of accounts cannot
  deadlock. Chosen over `SERIALIZABLE` plus a retry loop: under the hot-account
  contention this workload actually has, explicit ordered locks are easier to
  reason about and produce no retry storms.
- **Optimistic versions on positions.** A user close and a liquidation racing the
  same position is a normal event, not an error. Exactly one `UPDATE ... WHERE
  version = $n` wins.
- **Leased `FOR UPDATE SKIP LOCKED` claims** on the outbox. A worker marks its
  rows `sending` before committing the claim; if it dies, the lease expires and
  another replica safely retries the same on-chain nonce.
- **A small connection pool on purpose** (16). It makes contention visible under
  the chaos workload instead of hiding it behind spare connections.
- **One open position per user per market**, as a partial unique index. It makes
  the hot-row case concrete instead of theoretical.

---

## Layout

```
cmd/pmr/                 chaos | demo | serve | verify | reset
internal/core/           schema.sql, double-entry ledger, intent state machine,
                         markets and positions, chain-event inbox
internal/chain/sim.go    the hostile chain: duplicates, gaps, lost receipts,
                         reorgs, contract-level nonce dedupe and expiry
internal/app/            five worker ticks, reconciler, invariant verifier,
                         chaos harness
internal/httpapi/        API and operator panel
internal/pricefeed/      mark-price cache and liquidation lease (Redis or memory)
```

The operator panel renders the reconciliation identity live, term by term, with
the residue boxed in red when it is non-zero. There is a button that injects a
phantom credit — balanced double entry and all — so you can watch the ledger stay
internally perfect while reconciliation catches it and halts withdrawals.

---

## Scope, deliberately

Built, because it carries the argument:

- append-only double-entry ledger with write-time balance enforcement
- transactional outbox, intent state machine, chain-enforced expiry
- three-term reconciliation with signed residue and automatic halt
- reorg compensation, including the case where a provisional credit was spent
- leveraged positions, maintenance margin, liquidation, market settlement
- chaos harness, invariant verifier, real SIGKILL recovery test

Not built, on purpose:

- **A real Rust contract.** The simulator is more valuable here: it can be made
  to misbehave on demand, and misbehaviour is what the design is for. A real
  contract would demonstrate Anchor, not systems judgment. The `Chain` surface is
  a narrow interface, so swapping it is a contained change.
- **Per-market on-chain escrow.** A second reconciliation dimension multiplies the
  code without adding a new judgment call. One vault balance exercises every
  mechanism.
- **An order book.** Positions open against a mark price. Matching is a different
  problem.
- Auth, multi-tenancy, Kubernetes, a second chain.

## At 100× scale

- Shard the ledger by account, keeping each transfer inside one shard; cross-shard
  moves become intents through the same machinery already built here.
- Replace reconciler polling with logical replication / CDC, so drift is detected
  in the same pass that writes it.
- Batch settlement per market rather than per position — the current
  per-position loop is the first thing that would bind.
- Partition `entries` by month; it is append-only, which makes this easy.
- Move the price fan-out off Postgres entirely: mark prices already live in Redis,
  and the liquidator should read a snapshot rather than scanning open positions.

---

## Running it

Requires Go 1.22+, Postgres 16, Redis (optional — it falls back to an in-memory
cache and says so). The repository module is
`github.com/ranranrunforit/prediction-margin-reconciler`.

```bash
make db
make demo          # start here: six failure modes, narrated
make chaos
make crash-test
make serve         # http://localhost:8080

go run ./cmd/pmr chaos -iterations 5000 -seed 7 -crash-every 10 -v
go run ./cmd/pmr verify -chain-state /tmp/pmr-chain.json -settle
```

`demo` and `chaos` reset the shared `pmr` schema. Run them with no `pmr serve`
process active against that database; a second process connected to a different
simulated-chain state can otherwise try to resolve the test intents.

`DATABASE_URL` and `REDIS_URL` override the defaults. CI runs `go vet`, `gofmt`,
the test suite, the narrated demo, five chaos seeds and the SIGKILL test on every
push.
