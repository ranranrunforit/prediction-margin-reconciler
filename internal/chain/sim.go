// Package chain is a deliberately hostile simulation of an L2.
//
// The point is not to imitate a specific chain. The point is to reproduce the
// four things that actually break off-chain accounting:
//
//  1. duplicate event delivery
//  2. gaps and reordering in the event stream
//  3. a submit that returns an error but lands anyway (lost receipt)
//  4. reorgs that un-confirm something we already believed
//
// Everything the simulator exposes falls into one of two tiers:
//
//	fast + unreliable : PollEvents
//	slow + truthful   : TxStatus, VaultState, CanonicalHash
//
// The recovery design of the whole system rests on that split. Events make us
// fast; the truthful tier is what we fall back to when events lie.
package chain

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
)

var (
	ErrSubmitFailed = errors.New("chain: submit failed")
	// ErrExpired is terminal: the contract will never process this nonce, so
	// the off-chain reservation can be released with no risk of a double spend.
	ErrExpired = errors.New("chain: nonce expired")
)

// Expired reports whether the contract has permanently refused a nonce.
func (s *Sim) Expired(nonce string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expired[nonce]
}

// Tx is a call we ask the vault contract to make. Nonce is the caller-supplied
// idempotency key; the contract records processed nonces and treats a repeat
// as a no-op. That property is what makes retry-on-unknown safe.
type Tx struct {
	Nonce    string `json:"nonce"`
	Kind     string `json:"kind"` // withdraw | settle_market
	User     string `json:"user,omitempty"`
	Amount   int64  `json:"amount,omitempty"`
	MarketID string `json:"market_id,omitempty"`
	Outcome  int64  `json:"outcome,omitempty"`
	// ExpiryHeight makes the contract refuse the call after a deadline it can
	// check itself. Without it, an off-chain timeout is only a guess: a tx that
	// sat in the mempool (or came back from a reorg) can still land after we
	// have refunded the user, and we would pay twice. Zero means never expires.
	ExpiryHeight int64 `json:"expiry_height,omitempty"`
}

type Event struct {
	Seq       int64  `json:"seq"`
	ID        string `json:"id"` // dedupe key for the *event*
	Kind      string `json:"kind"`
	FactID    string `json:"fact_id"` // dedupe key for the *effect* (see README)
	User      string `json:"user,omitempty"`
	Amount    int64  `json:"amount,omitempty"`
	MarketID  string `json:"market_id,omitempty"`
	Outcome   int64  `json:"outcome,omitempty"`
	Height    int64  `json:"height"`
	BlockHash string `json:"block_hash"`
}

type TxStatus struct {
	Processed bool   `json:"processed"`
	Finalized bool   `json:"finalized"`
	Height    int64  `json:"height"`
	BlockHash string `json:"block_hash"`
}

type VaultState struct {
	FinalizedBalance int64 `json:"finalized_balance"`
	HeadBalance      int64 `json:"head_balance"`
	Head             int64 `json:"head"`
	FinalizedHeight  int64 `json:"finalized_height"`
}

// Faults are probabilities in [0,1].
type Faults struct {
	SubmitError float64 `json:"submit_error"` // submit fails, tx NOT included
	LostReceipt float64 `json:"lost_receipt"` // submit "fails", tx IS included
	Duplicate   float64 `json:"duplicate"`    // event delivered twice
	Reorder     float64 `json:"reorder"`      // batch shuffled
	Gap         float64 `json:"gap"`          // event withheld from this poll
	Reorg       float64 `json:"reorg"`        // block reorg on mine
	Confirms    int     `json:"confirms"`     // blocks until finality
}

func DefaultFaults() Faults {
	return Faults{SubmitError: 0.10, LostReceipt: 0.10, Duplicate: 0.20,
		Reorder: 0.20, Gap: 0.10, Reorg: 0.05, Confirms: 3}
}

func NoFaults() Faults { return Faults{Confirms: 2} }

type block struct {
	Height int64  `json:"height"`
	Hash   string `json:"hash"`
	Txs    []Tx   `json:"txs"`
	Log    []int  `json:"log"` // indexes into Sim.log produced by this block
}

type persisted struct {
	Head            int64            `json:"head"`
	FinalizedHeight int64            `json:"finalized_height"`
	FinalizedBal    int64            `json:"finalized_balance"`
	Pending         []block          `json:"pending"`
	Finalised       map[string]block `json:"finalised_hashes"` // height -> block (hash only, for CanonicalHash)
	Processed       map[string]TxStatus
	Log             []Event         `json:"log"`
	Seq             int64           `json:"seq"`
	Mempool         []Tx            `json:"mempool"`
	Expired         map[string]bool `json:"expired"`
	Faults          Faults          `json:"faults"`
}

// Sim is the chain. It is intentionally the only mutable state in the system
// that does NOT live in Postgres, because that is the situation we are trying
// to survive: an authority we cannot roll back.
type Sim struct {
	mu   sync.Mutex
	rnd  *rand.Rand
	path string

	head         int64
	finalHeight  int64
	finalBalance int64
	pending      []block
	hashAt       map[int64]string
	processed    map[string]TxStatus
	log          []Event
	seq          int64
	mempool      []Tx
	expired      map[string]bool
	faults       Faults
	nextDeposit  int64
}

func New(seed int64, f Faults, statePath string) *Sim {
	if f.Confirms <= 0 {
		f.Confirms = 2
	}
	s := &Sim{
		rnd:       rand.New(rand.NewSource(seed)),
		path:      statePath,
		hashAt:    map[int64]string{},
		processed: map[string]TxStatus{},
		expired:   map[string]bool{},
		faults:    f,
	}
	if statePath != "" {
		_ = s.load()
	}
	return s
}

func (s *Sim) Faults() Faults {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.faults
}

func (s *Sim) SetFaults(f Faults) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f.Confirms <= 0 {
		f.Confirms = s.faults.Confirms
	}
	s.faults = f
}

func (s *Sim) hit(p float64) bool { return p > 0 && s.rnd.Float64() < p }

// ---------------------------------------------------------------- writes

// Deposit models a user sending funds straight to the vault contract. It is
// chain-originated: we only learn about it through the event stream (fast) or
// by scanning chain state (slow). Returns the fact id.
func (s *Sim) Deposit(user string, amount int64) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextDeposit++
	id := fmt.Sprintf("dep-%d", s.nextDeposit)
	s.mempool = append(s.mempool, Tx{Nonce: id, Kind: "deposit", User: user, Amount: amount})
	return id
}

// Submit sends a tx. Three outcomes, and the caller cannot tell the second
// from the third:
//
//	nil error   -> included
//	error       -> not included
//	error       -> included anyway (lost receipt)
//
// The only safe response to an error is to retry with the same nonce and let
// the resolver find out the truth from TxStatus.
func (s *Sim) Submit(tx Tx) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.processed[tx.Nonce]; ok {
		// Contract-level idempotency: replaying a nonce is a no-op.
		return st.BlockHash, nil
	}
	if s.expired[tx.Nonce] || (tx.ExpiryHeight > 0 && s.head > tx.ExpiryHeight) {
		s.expired[tx.Nonce] = true
		return "", fmt.Errorf("%w: nonce %s is past its expiry height", ErrExpired, tx.Nonce)
	}
	if s.hit(s.faults.SubmitError) {
		return "", fmt.Errorf("%w: rpc timeout", ErrSubmitFailed)
	}
	s.mempool = append(s.mempool, tx)
	lost := s.hit(s.faults.LostReceipt)
	s.persistLocked()
	if lost {
		return "", fmt.Errorf("%w: receipt lost (tx was accepted)", ErrSubmitFailed)
	}
	return "tx-" + tx.Nonce, nil
}

// Mine produces one block from the mempool, possibly reorging first.
func (s *Sim) Mine() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.pending) > 0 && s.hit(s.faults.Reorg) {
		s.reorgLocked()
	}

	s.head++
	b := block{Height: s.head, Hash: fmt.Sprintf("0x%08x", s.rnd.Uint32())}
	// The contract rejects anything past its own expiry height, and refuses a
	// nonce it has already executed. Both checks belong here, at execution.
	//
	// An earlier version of this simulator deduped in Submit instead, which is
	// wrong in a way that is easy to miss: a nonce that is sitting in the
	// mempool is not yet "processed", so a retry appended it a second time and
	// the same withdrawal executed twice in one block. The chaos harness caught
	// it as a 5 USDC unexplained shortfall. A real contract has to make the
	// same distinction.
	pool := s.mempool
	s.mempool = nil
	staged := map[string]bool{}
	for _, tx := range pool {
		if _, done := s.processed[tx.Nonce]; done || staged[tx.Nonce] {
			continue
		}
		if s.expired[tx.Nonce] || (tx.ExpiryHeight > 0 && b.Height > tx.ExpiryHeight) {
			s.expired[tx.Nonce] = true
			continue
		}
		staged[tx.Nonce] = true
		b.Txs = append(b.Txs, tx)
	}
	for _, tx := range b.Txs {
		s.processed[tx.Nonce] = TxStatus{Processed: true, Height: b.Height, BlockHash: b.Hash}
		s.seq++
		ev := Event{Seq: s.seq, Kind: evKind(tx.Kind), FactID: tx.Nonce,
			User: tx.User, Amount: tx.Amount, MarketID: tx.MarketID, Outcome: tx.Outcome,
			Height: b.Height, BlockHash: b.Hash}
		// Event id is per (fact, block). A re-included fact after a reorg
		// therefore arrives as a *new* event with the *same* fact id.
		ev.ID = fmt.Sprintf("%s@%s", ev.FactID, b.Hash)
		b.Log = append(b.Log, len(s.log))
		s.log = append(s.log, ev)
	}
	s.hashAt[b.Height] = b.Hash
	s.pending = append(s.pending, b)

	for len(s.pending) > s.faults.Confirms {
		f := s.pending[0]
		s.pending = s.pending[1:]
		s.finalBalance += balanceDelta(f.Txs)
		s.finalHeight = f.Height
		for _, tx := range f.Txs {
			st := s.processed[tx.Nonce]
			st.Finalized = true
			s.processed[tx.Nonce] = st
		}
	}
	s.persistLocked()
}

// reorgLocked drops the newest unfinalised block(s) and rebuilds them with
// different hashes and a different tx order. Finalised history is untouched —
// that is the entire meaning of finality.
func (s *Sim) reorgLocked() {
	depth := 1 + s.rnd.Intn(len(s.pending))
	orphaned := s.pending[len(s.pending)-depth:]
	s.pending = s.pending[:len(s.pending)-depth]

	var reinclude []Tx
	for _, b := range orphaned {
		delete(s.hashAt, b.Height)
		for _, tx := range b.Txs {
			delete(s.processed, tx.Nonce)
			reinclude = append(reinclude, tx)
		}
		for _, i := range b.Log {
			s.log[i].Kind = "orphaned" // still in the log; a late poll may see it
		}
	}
	s.head -= int64(depth)
	// Shuffle: the same facts, a different order, different block hashes.
	s.rnd.Shuffle(len(reinclude), func(i, j int) { reinclude[i], reinclude[j] = reinclude[j], reinclude[i] })
	s.mempool = append(reinclude, s.mempool...)
}

// ---------------------------------------------------------------- reads

// PollEvents is the fast, unreliable tier. It duplicates, reorders and
// withholds. Because seq numbers are contiguous, a consumer can detect a
// withheld event as a gap and refuse to advance its cursor past it — which is
// exactly what the inbox worker does.
func (s *Sim) PollEvents(since int64, limit int) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Event
	for _, ev := range s.log {
		if ev.Seq <= since || ev.Kind == "orphaned" {
			continue
		}
		if len(out) >= limit {
			break
		}
		if s.hit(s.faults.Gap) {
			continue // withheld this round; the consumer will see the gap
		}
		out = append(out, ev)
		if s.hit(s.faults.Duplicate) {
			out = append(out, ev)
		}
	}
	if s.hit(s.faults.Reorder) && len(out) > 1 {
		s.rnd.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	}
	return out
}

// TxStatus is the truthful tier: the answer to "did my nonce land?".
func (s *Sim) TxStatus(nonce string) TxStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.processed[nonce]
}

// FinalizedFacts lists every finalised fact of a kind. The reconciler uses it
// to heal from permanently-lost events without trusting the stream at all.
func (s *Sim) FinalizedFacts(kind string) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Event
	for _, ev := range s.log {
		if ev.Kind != kind {
			continue
		}
		if st, ok := s.processed[ev.FactID]; ok && st.Finalized && st.BlockHash == ev.BlockHash {
			out = append(out, ev)
		}
	}
	return out
}

func (s *Sim) VaultState() VaultState {
	s.mu.Lock()
	defer s.mu.Unlock()
	head := s.finalBalance
	for _, b := range s.pending {
		head += balanceDelta(b.Txs)
	}
	return VaultState{FinalizedBalance: s.finalBalance, HeadBalance: head,
		Head: s.head, FinalizedHeight: s.finalHeight}
}

// CanonicalHash answers "is the block I derived that ledger entry from still
// part of history?". A false second return means: it was reorged away.
func (s *Sim) CanonicalHash(height int64) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.hashAt[height]
	return h, ok
}

func evKind(txKind string) string {
	switch txKind {
	case "deposit":
		return "deposit"
	case "withdraw":
		return "withdraw_processed"
	case "settle_market":
		return "market_settled"
	}
	return txKind
}

func balanceDelta(txs []Tx) int64 {
	var d int64
	for _, tx := range txs {
		switch tx.Kind {
		case "deposit":
			d += tx.Amount
		case "withdraw":
			d -= tx.Amount
		}
	}
	return d
}

// ---------------------------------------------------------------- persistence

// The chain state is written to disk so that a real `kill -9` of the process
// (see scripts/crash_test.sh) leaves the chain standing while the service
// restarts — which is the only way to test recovery honestly.
func (s *Sim) persistLocked() {
	if s.path == "" {
		return
	}
	hashes := map[string]block{}
	for h, hash := range s.hashAt {
		hashes[fmt.Sprint(h)] = block{Height: h, Hash: hash}
	}
	p := persisted{Head: s.head, FinalizedHeight: s.finalHeight, FinalizedBal: s.finalBalance,
		Pending: s.pending, Finalised: hashes, Processed: s.processed, Log: s.log,
		Seq: s.seq, Mempool: s.mempool, Expired: s.expired, Faults: s.faults}
	b, err := json.Marshal(p)
	if err != nil {
		return
	}
	tmp := s.path + ".tmp"
	if os.WriteFile(tmp, b, 0o644) == nil {
		_ = os.Rename(tmp, s.path)
	}
}

func (s *Sim) load() error {
	b, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	var p persisted
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}
	s.head, s.finalHeight, s.finalBalance = p.Head, p.FinalizedHeight, p.FinalizedBal
	s.pending, s.processed, s.log, s.seq, s.mempool = p.Pending, p.Processed, p.Log, p.Seq, p.Mempool
	if s.processed == nil {
		s.processed = map[string]TxStatus{}
	}
	if s.expired = p.Expired; s.expired == nil {
		s.expired = map[string]bool{}
	}
	for _, blk := range p.Finalised {
		s.hashAt[blk.Height] = blk.Hash
	}
	for _, blk := range p.Pending {
		s.hashAt[blk.Height] = blk.Hash
	}
	return nil
}

// Snapshot is for the dashboard.
func (s *Sim) Snapshot() map[string]any {
	v := s.VaultState()
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{
		"vault": v, "faults": s.faults, "pending_blocks": len(s.pending),
		"mempool": len(s.mempool), "events": len(s.log),
	}
}
