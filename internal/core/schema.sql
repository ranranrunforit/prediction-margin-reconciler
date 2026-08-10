-- prediction-margin-reconciler schema.
-- All money is int64 in micro-units (1 USDC = 1_000_000).
-- Probabilities/prices are int32 in micro-units too (0 .. 1_000_000).

create table if not exists accounts (
    code       text primary key,
    kind       text not null,          -- available | pending_withdraw | margin | insurance | fees | external
    owner      text,
    created_at timestamptz not null default now()
);

create table if not exists transfers (
    id              uuid primary key,
    kind            text not null,
    idempotency_key text unique,       -- the ONLY idempotency mechanism: one unique index
    meta            jsonb not null default '{}'::jsonb,
    created_at      timestamptz not null default now()
);

-- Append-only. Never updated, never deleted. Reversals are new transfers.
create table if not exists entries (
    id           bigserial primary key,
    transfer_id  uuid not null references transfers (id),
    account_code text not null references accounts (code),
    amount       bigint not null,      -- signed; every transfer's entries sum to zero
    created_at   timestamptz not null default now()
);
create index if not exists entries_account_idx on entries (account_code);
create index if not exists entries_transfer_idx on entries (transfer_id);

-- Denormalised balance, written inside the same transaction as the entries.
-- Invariant I0 cross-checks it against sum(entries), so a bug here cannot hide.
create table if not exists account_balances (
    account_code text primary key references accounts (code),
    balance      bigint not null,
    version      bigint not null default 0
);

-- ---------------------------------------------------------------- intents

-- An intent is a durable record of "we want the chain to do X exactly once".
-- intents.id doubles as the on-chain nonce, so retrying a submit is a no-op
-- on the chain side. This is what makes a lost receipt survivable.
create table if not exists intents (
    id                  uuid primary key,
    idempotency_key     text unique,     -- makes "create the intent" idempotent
    kind                text not null,   -- withdraw | settle_market
    user_id             text,
    market_id           text,
    amount              bigint not null default 0,
    state               text not null,   -- reserved | submitted | confirmed | expired | reverted
    reserve_transfer_id uuid references transfers (id),
    final_transfer_id   uuid references transfers (id),
    chain_tx            text,
    block_height        bigint,
    block_hash          text,
    attempts            int not null default 0,
    deadline            timestamptz not null,
    expiry_height       bigint not null default 0, -- enforced by the contract, not by us
    last_error          text,
    created_at          timestamptz not null default now(),
    updated_at          timestamptz not null default now()
);
create index if not exists intents_state_idx on intents (state);

-- At most one escrow instruction per market may be in flight. This is the real
-- idempotency key for escrow: not the value being installed, but the fact that
-- somebody is already working on this market. Keying on the value instead looks
-- natural and deadlocks -- once an instruction for target T expires, no new one
-- for the same T can ever be created, and the market stays out of sync forever.
create unique index if not exists intents_one_escrow_inflight_idx
    on intents (market_id)
    where kind = 'escrow_set' and state in ('reserved', 'submitted');

-- Transactional outbox: written in the same DB transaction as the ledger move,
-- so we never dual-write to Postgres and the chain.
create table if not exists outbox (
    id              bigserial primary key,
    intent_id       uuid not null references intents (id),
    op              text not null,
    payload         jsonb not null,
    state           text not null default 'pending', -- pending | sending | sent
    attempts        int not null default 0,
    next_attempt_at timestamptz not null default now(),
    lease_until     timestamptz,
    last_error      text,
    created_at      timestamptz not null default now()
);

-- `create table if not exists` does not migrate an existing local database.
-- Keep this additive migration before any index that uses the new column.
alter table outbox add column if not exists lease_until timestamptz;
create unique index if not exists outbox_intent_op_idx on outbox (intent_id, op);
create index if not exists outbox_due_idx on outbox (state, next_attempt_at);
create index if not exists outbox_lease_idx on outbox (state, lease_until);

-- ---------------------------------------------------------------- chain inbox

-- Inbox for chain events. event_id is the dedupe key: the simulator will
-- happily deliver the same event five times, out of order.
create table if not exists chain_events (
    event_id     text primary key,
    chain_seq    bigint not null,
    kind         text not null,
    payload      jsonb not null,
    block_height bigint not null,
    block_hash   text not null,
    status       text not null default 'received', -- received | applied | ignored | orphaned
    observed_at  timestamptz not null default now(),
    applied_at   timestamptz
);
create index if not exists chain_events_status_idx on chain_events (status, chain_seq);
create index if not exists chain_events_block_idx on chain_events (block_height);

-- Which ledger transfer was derived from which block. On a reorg we look up
-- the orphaned block here and write compensating transfers.
create table if not exists chain_facts (
    event_id     text primary key references chain_events (event_id),
    fact_id      text not null,
    kind         text not null,
    amount       bigint not null default 0,
    transfer_id  uuid not null references transfers (id),
    block_height bigint not null,
    block_hash   text not null,
    finalized    boolean not null default false,
    reverted     boolean not null default false
);
create index if not exists chain_facts_pending_idx on chain_facts (finalized, reverted);
create index if not exists chain_facts_block_idx on chain_facts (block_hash);

create table if not exists chain_cursor (
    id        int primary key default 1,
    last_seq  bigint not null default 0,
    check (id = 1)
);

-- ---------------------------------------------------------------- markets

create table if not exists markets (
    id         text primary key,
    question   text not null,
    mark_price int not null,            -- 0..1_000_000
    state      text not null,           -- open | settling | settled
    outcome    int,                     -- 0 or 1_000_000
    version    bigint not null default 0
);

create table if not exists positions (
    id           uuid primary key,
    user_id      text not null,
    market_id    text not null references markets (id),
    side         text not null,         -- long | short
    size         bigint not null,       -- shares, micro
    entry_price  int not null,
    collateral   bigint not null,
    idempotency_key text,
    state        text not null default 'open', -- open | closed | liquidated
    version      bigint not null default 0,
    created_at   timestamptz not null default now(),
    updated_at   timestamptz not null default now()
);
create unique index if not exists positions_one_open_idx
    on positions (user_id, market_id) where state = 'open';
create index if not exists positions_open_market_idx
    on positions (market_id) where state = 'open';
alter table positions add column if not exists idempotency_key text;
create unique index if not exists positions_idempotency_idx
    on positions (idempotency_key) where idempotency_key is not null;

-- ---------------------------------------------------------------- ops

create table if not exists reconciliations (
    id              bigserial primary key,
    at              timestamptz not null default now(),
    status          text not null,      -- ok | explained | unsafe_shortfall | unsafe_surplus
    ledger_internal bigint not null,
    chain_finalized bigint not null,
    explained_delta bigint not null,
    unexplained     bigint not null,
    detail          jsonb not null default '{}'::jsonb
);

create table if not exists system_state (
    key   text primary key,
    value text not null,
    at    timestamptz not null default now()
);

-- Escrow drift is normal: trading is off-chain and fast, and the chain converges
-- behind it. What is not normal is drift that stops shrinking. This table counts
-- how many reconciliation passes a market has been out of sync without an
-- instruction closing it, so the invariant can be "converging" rather than the
-- useless "never differs".
create table if not exists escrow_drift (
    market     text primary key references markets (id),
    want       bigint not null,
    got        bigint not null,
    misses     int not null default 0,
    first_seen timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists freezes (
    subject text primary key,           -- user id, or '*' for the whole system
    reason  text not null,
    at      timestamptz not null default now()
);

insert into chain_cursor (id, last_seq) values (1, 0) on conflict do nothing;

insert into accounts (code, kind, owner) values
    ('external',  'external',  null),
    ('insurance', 'insurance', null),
    ('fees',      'fees',      null)
on conflict do nothing;

insert into account_balances (account_code, balance) values
    ('external', 0), ('insurance', 0), ('fees', 0)
on conflict do nothing;
