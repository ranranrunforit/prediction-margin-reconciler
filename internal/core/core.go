// Package core holds the ledger, the intent state machine and the market
// bookkeeping. Everything in here is Postgres-backed: Postgres is the
// authority for intent and for the off-chain ledger. Nothing in this package
// talks to the chain.
package core

import (
	"context"
	"crypto/rand"
	"database/sql"
	_ "embed"
	"fmt"
	"strings"
)

// One = 1.0 in micro-units. All money and all prices use this scale.
const One int64 = 1_000_000

// USD renders micro-units for humans.
func USD(v int64) string {
	neg := v < 0
	if neg {
		v = -v
	}
	s := fmt.Sprintf("%d.%06d", v/One, v%One)
	s = strings.TrimRight(s, "0")
	s = strings.TrimSuffix(s, ".")
	if neg {
		return "-" + s
	}
	return s
}

//go:embed schema.sql
var schemaSQL string

// Open connects to Postgres and applies the schema. The schema is idempotent,
// so this is safe to run on every boot — which matters, because the whole
// point of this project is that boot is a routine event, not a special one.
func Open(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	// A small pool on purpose: it makes lock contention visible under the
	// chaos workload instead of hiding it behind spare connections.
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return db, nil
}

// Reset drops every table. Used by tests and `pmr demo`.
func Reset(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
        drop table if exists chain_facts, chain_events, chain_cursor, outbox, intents,
            positions, markets, entries, account_balances, transfers, accounts,
            reconciliations, system_state, freezes cascade;`)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, schemaSQL)
	return err
}

// NewID returns a random UUIDv4 string. Intent ids double as on-chain nonces,
// so they must be unguessable-ish and never reused.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// tx runs fn inside a serializable-enough transaction. We use the default
// READ COMMITTED and rely on explicit row locks with a canonical lock order,
// rather than SERIALIZABLE + retry loops: under the hot-account contention
// this system actually has, explicit ordered locks are easier to reason about
// and produce no retry storms.
func tx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	t, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = t.Rollback() }()
	if err := fn(t); err != nil {
		return err
	}
	return t.Commit()
}
