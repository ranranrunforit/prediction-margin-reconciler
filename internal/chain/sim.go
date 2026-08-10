// Package chain is a deliberately hostile simulation of an L2.
//
// The point is not to imitate a specific chain. The point is to reproduce the
// things that actually break off-chain accounting:
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
//
// The execution rules in applyTx are a mirror of the Rust contract in
// contracts/vault. They are not merely similar by intention: `make differential`
// replays this simulator's finalised history through the real Rust
// implementation and fails if the two disagree by a single unit.
package chain

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
)

var (
	ErrSubmitFailed = errors.New("chain: submit failed")
	// ErrExpired is terminal: the contract will never process this nonce, so
	// the off-chain reservation can be released with no risk of a double spend.
	ErrExpired = errors.New("chain: nonce expired")
)

// Tx is a call into the vault contract.
type Tx struct {
	Nonce    string `json:"nonce"`
	Kind     string `json:"kind"` // deposit | withdraw | escrow_set | settle_market | cancel
	User     string `json:"user,omitempty"`
	Amount   int64  `json:"amount,omitempty"`
	MarketID string `json:"market_id,omitempty"`
	// Target is the absolute escrow level for escrow_set. Absolute, not a
	// delta: under an unknown receipt the same intent may be submitted several
	// times, and an assignment converges where an increment compounds.
	Target  int64 `json:"target,omitempty"`
	Outcome int64 `json:"outcome,omitempty"`
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
	FactID    string `json:"fact_id"` // dedupe key for the *effect*
	User      string `json:"user,omitempty"`
	Amount    int64  `json:"amount,omitempty"`
	MarketID  string `json:"market_id,omitempty"`
	Target    int64  `json:"target,omitempty"`
	Outcome   int64  `json:"outcome,omitempty"`
	Height    int64  `json:"height"`
	BlockHash string `json:"block_hash"`
}

type TxStatus struct {
	Processed bool   `json:"processed"`
	Finalized bool   `json:"finalized"`
	Height    int64  `json:"height"`
	BlockHash string `json:"block_hash"`
	// Rejected is set when the contract executed the call and refused it for a
	// reason that cannot change. The engine can act on that immediately instead
	// of waiting out a timeout.
	Rejected string `json:"rejected,omitempty"`
}

type VaultState struct {
	FinalizedBalance int64            `json:"finalized_balance"`
	FinalizedFree    int64            `json:"finalized_free"`
	FinalizedEscrow  map[string]int64 `json:"finalized_escrow"`
	HeadBalance      int64            `json:"head_balance"`
	Head             int64            `json:"head"`
	FinalizedHeight  int64            `json:"finalized_height"`
}

func (v VaultState) EscrowOf(market string) int64 { return v.FinalizedEscrow[market] }

func (v VaultState) EscrowTotal() int64 {
	var t int64
	for _, e := range v.FinalizedEscrow {
		t += e
	}
	return t
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

// ---------------------------------------------------------------- vault state

// vault is the contract's storage. It is a value type that clones cheaply,
// because a reorg is modelled by recomputing head state from finalised state
// plus the surviving pending blocks. Recomputing is far easier to get right than
// unwinding, and the pending window is bounded by finality.
type vault struct {
	Balance   int64            `json:"balance"`
	Escrow    map[string]int64 `json:"escrow"`
	Settled   map[string]bool  `json:"settled"`
	Processed map[string]bool  `json:"processed"`
	Cancelled map[string]bool  `json:"cancelled"`
}

func newVault() vault {
	return vault{Escrow: map[string]int64{}, Settled: map[string]bool{},
		Processed: map[string]bool{}, Cancelled: map[string]bool{}}
}

func (v vault) clone() vault {
	c := vault{Balance: v.Balance,
		Escrow:    make(map[string]int64, len(v.Escrow)),
		Settled:   make(map[string]bool, len(v.Settled)),
		Processed: make(map[string]bool, len(v.Processed)),
		Cancelled: make(map[string]bool, len(v.Cancelled))}
	for k, x := range v.Escrow {
		c.Escrow[k] = x
	}
	for k, x := range v.Settled {
		c.Settled[k] = x
	}
	for k, x := range v.Processed {
		c.Processed[k] = x
	}
	for k, x := range v.Cancelled {
		c.Cancelled[k] = x
	}
	return c
}

func (v vault) escrowTotal() int64 {
	var t int64
	for _, e := range v.Escrow {
		t += e
	}
	return t
}

// free is custody not earmarked for a market: the only funds a withdrawal may
// draw on.
func (v vault) free() int64 { return v.Balance - v.escrowTotal() }

// applyTx is the contract. Mirror of Vault::execute in contracts/vault.
//
// The check order matters. Dedupe comes before expiry, so a late retry of a call
// that already landed reports "already processed" and not "expired". The other
// order would have the engine refund money the vault had already paid out.
//
// A refusal is either permanent (returned as a reason, so the engine can act at
// once) or transient (empty reason, nonce not consumed, outbox free to retry).
func (v *vault) applyTx(tx Tx, height int64) (permanentReject string, ok bool) {
	if v.Processed[tx.Nonce] {
		return "already_processed", false
	}
	if v.Cancelled[tx.Nonce] {
		return "expired", false
	}
	if tx.Kind != "cancel" && tx.ExpiryHeight > 0 && height > tx.ExpiryHeight {
		return "expired", false
	}

	switch tx.Kind {
	case "deposit":
		if tx.Amount <= 0 {
			return "zero_amount", false
		}
		v.Balance += tx.Amount

	case "withdraw":
		if tx.Amount <= 0 {
			return "zero_amount", false
		}
		if v.free() < tx.Amount {
			return "", false // transient: free custody may arrive later
		}
		v.Balance -= tx.Amount

	case "escrow_set":
		if v.Settled[tx.MarketID] {
			return "market_settled", false
		}
		cur := v.Escrow[tx.MarketID]
		if tx.Target > cur && v.free() < tx.Target-cur {
			return "", false // transient: not enough free custody yet
		}
		if tx.Target == 0 {
			delete(v.Escrow, tx.MarketID)
		} else {
			v.Escrow[tx.MarketID] = tx.Target
		}

	case "settle_market":
		if v.Settled[tx.MarketID] {
			return "market_settled", false
		}
		// Releasing escrow does not move money out of the vault, it just stops
		// earmarking it. Who gets paid is the ledger's business.
		delete(v.Escrow, tx.MarketID)
		v.Settled[tx.MarketID] = true

	case "cancel":
		if tx.ExpiryHeight == 0 || height <= tx.ExpiryHeight {
			return "not_yet_expired", false
		}
		v.Cancelled[tx.Nonce] = true
		return "", true // a cancel does not consume its own nonce slot

	default:
		return "unknown_call", false
	}

	v.Processed[tx.Nonce] = true
	return "", true
}

// ---------------------------------------------------------------- sim

type block struct {
	Height int64  `json:"height"`
	Hash   string `json:"hash"`
	Txs    []Tx   `json:"txs"`
	Log    []int  `json:"log"`
}

type persisted struct {
	Head        int64               `json:"head"`
	FinalHeight int64               `json:"finalized_height"`
	Final       vault               `json:"final"`
	Pending     []block             `json:"pending"`
	HashAt      map[string]string   `json:"hash_at"`
	Receipts    map[string]TxStatus `json:"receipts"`
	Log         []Event             `json:"log"`
	Seq         int64               `json:"seq"`
	Mempool     []Tx                `json:"mempool"`
	Faults      Faults              `json:"faults"`
	NextDeposit int64               `json:"next_deposit"`
}

type Sim struct {
	mu   sync.Mutex
	rnd  *rand.Rand
	path string

	head        int64
	finalHeight int64
	final       vault
	pending     []block
	hashAt      map[int64]string
	receipts    map[string]TxStatus
	log         []Event
	seq         int64
	mempool     []Tx
	faults      Faults
	nextDeposit int64

	trace *os.File
}

func New(seed int64, f Faults, statePath string) *Sim {
	if f.Confirms <= 0 {
		f.Confirms = 2
	}
	s := &Sim{
		rnd: rand.New(rand.NewSource(seed)), path: statePath,
		final: newVault(), hashAt: map[int64]string{},
		receipts: map[string]TxStatus{}, faults: f,
	}
	if statePath != "" {
		_ = s.load()
	}
	return s
}

// TraceTo records every finalised call, in order, to a file the Rust contract
// can replay. Finalised history only: it is the part that cannot be taken back,
// which is what makes the comparison meaningful.
func (s *Sim) TraceTo(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	s.trace = f
	fmt.Fprintln(f, "# finalised vault history exported by the Go simulator")
	return nil
}

// CloseTrace writes the expected end state and closes the file.
func (s *Sim) CloseTrace() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.trace == nil {
		return nil
	}
	markets := make([]string, 0, len(s.final.Escrow))
	for m := range s.final.Escrow {
		markets = append(markets, m)
	}
	sort.Strings(markets)
	fmt.Fprintf(s.trace, "expect balance %d\n", s.final.Balance)
	fmt.Fprintf(s.trace, "expect escrow_total %d\n", s.final.escrowTotal())
	for _, m := range markets {
		fmt.Fprintf(s.trace, "expect escrow %s %d\n", m, s.final.Escrow[m])
	}
	err := s.trace.Close()
	s.trace = nil
	return err
}

func (s *Sim) writeTrace(tx Tx, height int64) {
	if s.trace == nil {
		return
	}
	switch tx.Kind {
	case "deposit":
		fmt.Fprintf(s.trace, "deposit %s %s %d %d\n", tx.Nonce, tx.User, tx.Amount, height)
	case "withdraw":
		fmt.Fprintf(s.trace, "withdraw %s %s %d %d %d\n",
			tx.Nonce, tx.User, tx.Amount, tx.ExpiryHeight, height)
	case "escrow_set":
		fmt.Fprintf(s.trace, "escrow_set %s %s %d %d %d\n",
			tx.Nonce, tx.MarketID, tx.Target, tx.ExpiryHeight, height)
	case "settle_market":
		o := 0
		if tx.Outcome > 0 {
			o = 1
		}
		fmt.Fprintf(s.trace, "settle %s %s %d %d %d\n",
			tx.Nonce, tx.MarketID, o, tx.ExpiryHeight, height)
	case "cancel":
		fmt.Fprintf(s.trace, "cancel %s %d %d\n", tx.Nonce, tx.ExpiryHeight, height)
	}
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

// headLocked is the executing state: finalised state plus every surviving
// pending block, replayed in order.
func (s *Sim) headLocked() vault {
	v := s.final.clone()
	for _, b := range s.pending {
		for _, tx := range b.Txs {
			v.applyTx(tx, b.Height)
		}
	}
	return v
}

// ---------------------------------------------------------------- writes

// Deposit models a user sending funds straight to the vault contract. It is
// chain-originated: we learn about it through the event stream (fast) or by
// scanning chain state (slow).
func (s *Sim) Deposit(user string, amount int64) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextDeposit++
	id := fmt.Sprintf("dep-%d", s.nextDeposit)
	s.mempool = append(s.mempool, Tx{Nonce: id, Kind: "deposit", User: user, Amount: amount})
	return id
}

// Submit sends a tx. Three outcomes, and the caller cannot tell the second from
// the third:
//
//	nil error   -> included
//	error       -> not included
//	error       -> included anyway (lost receipt)
//
// The only safe response to an error is to retry with the same nonce and let the
// resolver learn the truth from TxStatus.
func (s *Sim) Submit(tx Tx) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.receipts[tx.Nonce]; ok && st.Processed {
		return st.BlockHash, nil // contract-level idempotency
	}
	if s.headLocked().Cancelled[tx.Nonce] {
		return "", fmt.Errorf("%w: nonce %s was cancelled", ErrExpired, tx.Nonce)
	}
	if tx.Kind != "cancel" && tx.ExpiryHeight > 0 && s.head > tx.ExpiryHeight {
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

// Cancel asks the contract to permanently refuse a nonce. The engine calls it
// before releasing an off-chain reservation, turning "this can never execute"
// from an assumption into a fact recorded by the executing authority.
//
// Returns true only once the cancellation is in force.
func (s *Sim) Cancel(nonce string, expiryHeight int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.receipts[nonce]; ok && st.Processed {
		return false // too late, it landed
	}
	if s.headLocked().Cancelled[nonce] {
		return true
	}
	if expiryHeight == 0 || s.head <= expiryHeight {
		return false // could still legitimately execute
	}
	for _, tx := range s.mempool {
		if tx.Nonce == nonce && tx.Kind == "cancel" {
			return false // already queued
		}
	}
	s.mempool = append(s.mempool, Tx{Nonce: nonce, Kind: "cancel", ExpiryHeight: expiryHeight})
	return false
}

// Mine produces one block, possibly reorging first.
func (s *Sim) Mine() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.pending) > 0 && s.hit(s.faults.Reorg) {
		s.reorgLocked()
	}

	s.head++
	b := block{Height: s.head, Hash: fmt.Sprintf("0x%08x", s.rnd.Uint32())}

	state := s.headLocked()
	pool := s.mempool
	s.mempool = nil
	for _, tx := range pool {
		reason, ok := state.applyTx(tx, b.Height)
		if !ok {
			if reason != "" && reason != "already_processed" {
				s.receipts[tx.Nonce] = TxStatus{Rejected: reason}
			}
			continue
		}
		b.Txs = append(b.Txs, tx)
	}

	for _, tx := range b.Txs {
		if tx.Kind != "cancel" {
			s.receipts[tx.Nonce] = TxStatus{Processed: true, Height: b.Height, BlockHash: b.Hash}
		}
		s.seq++
		ev := Event{Seq: s.seq, Kind: evKind(tx.Kind), FactID: tx.Nonce,
			User: tx.User, Amount: tx.Amount, MarketID: tx.MarketID,
			Target: tx.Target, Outcome: tx.Outcome, Height: b.Height, BlockHash: b.Hash}
		// Event id is per (fact, block). A re-included fact after a reorg
		// arrives as a *new* event carrying the *same* fact id.
		ev.ID = fmt.Sprintf("%s@%s", ev.FactID, b.Hash)
		b.Log = append(b.Log, len(s.log))
		s.log = append(s.log, ev)
	}
	s.hashAt[b.Height] = b.Hash
	s.pending = append(s.pending, b)

	for len(s.pending) > s.faults.Confirms {
		f := s.pending[0]
		s.pending = s.pending[1:]
		for _, tx := range f.Txs {
			s.final.applyTx(tx, f.Height)
			s.writeTrace(tx, f.Height)
			if st, ok := s.receipts[tx.Nonce]; ok && st.Processed {
				st.Finalized = true
				s.receipts[tx.Nonce] = st
			}
		}
		s.finalHeight = f.Height
	}
	s.persistLocked()
}

// reorgLocked drops the newest unfinalised block(s) and rebuilds them with
// different hashes and a different tx order. Finalised history is untouched --
// that is the entire meaning of finality.
func (s *Sim) reorgLocked() {
	depth := 1 + s.rnd.Intn(len(s.pending))
	orphaned := s.pending[len(s.pending)-depth:]
	s.pending = s.pending[:len(s.pending)-depth]

	var reinclude []Tx
	for _, b := range orphaned {
		delete(s.hashAt, b.Height)
		for _, tx := range b.Txs {
			delete(s.receipts, tx.Nonce)
			reinclude = append(reinclude, tx)
		}
		for _, i := range b.Log {
			s.log[i].Kind = "orphaned"
		}
	}
	s.head -= int64(depth)
	s.rnd.Shuffle(len(reinclude), func(i, j int) { reinclude[i], reinclude[j] = reinclude[j], reinclude[i] })
	s.mempool = append(reinclude, s.mempool...)
}

// ---------------------------------------------------------------- reads

// PollEvents is the fast, unreliable tier. It duplicates, reorders and
// withholds. Because seq numbers are contiguous, a consumer can detect a
// withheld event as a gap and refuse to advance its cursor past it.
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
			continue
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
	return s.receipts[nonce]
}

// Expired reports whether the contract has permanently refused a nonce.
func (s *Sim) Expired(nonce string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.receipts[nonce]; ok {
		if st.Processed {
			return false
		}
		if st.Rejected == "expired" {
			return true
		}
	}
	return s.headLocked().Cancelled[nonce]
}

// Rejected returns a permanent rejection reason, if the contract gave one.
func (s *Sim) Rejected(nonce string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.receipts[nonce].Rejected
}

// FinalizedFacts lists every finalised fact of a kind. The reconciler uses it to
// heal from permanently-lost events without trusting the stream at all.
func (s *Sim) FinalizedFacts(kind string) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Event
	for _, ev := range s.log {
		if ev.Kind != kind {
			continue
		}
		if st, ok := s.receipts[ev.FactID]; ok && st.Finalized && st.BlockHash == ev.BlockHash {
			out = append(out, ev)
		}
	}
	return out
}

func (s *Sim) VaultState() VaultState {
	s.mu.Lock()
	defer s.mu.Unlock()
	esc := make(map[string]int64, len(s.final.Escrow))
	for k, v := range s.final.Escrow {
		esc[k] = v
	}
	return VaultState{
		FinalizedBalance: s.final.Balance, FinalizedFree: s.final.free(),
		FinalizedEscrow: esc, HeadBalance: s.headLocked().Balance,
		Head: s.head, FinalizedHeight: s.finalHeight,
	}
}

// SettledOnChain reports whether the contract has recorded a market's outcome.
func (s *Sim) SettledOnChain(market string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.final.Settled[market]
}

// CanonicalHash answers "is the block I derived that ledger entry from still
// part of history?". A false second return means it was reorged away.
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
	case "escrow_set":
		return "escrow_set"
	case "settle_market":
		return "market_settled"
	case "cancel":
		return "cancelled"
	}
	return txKind
}

// ---------------------------------------------------------------- persistence

// The chain state is written to disk so that a real `kill -9` of the process
// leaves the chain standing while the service restarts, which is the only way to
// test recovery honestly.
func (s *Sim) persistLocked() {
	if s.path == "" {
		return
	}
	hashes := make(map[string]string, len(s.hashAt))
	for h, hash := range s.hashAt {
		hashes[fmt.Sprint(h)] = hash
	}
	p := persisted{Head: s.head, FinalHeight: s.finalHeight, Final: s.final,
		Pending: s.pending, HashAt: hashes, Receipts: s.receipts, Log: s.log,
		Seq: s.seq, Mempool: s.mempool, Faults: s.faults, NextDeposit: s.nextDeposit}
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
	s.head, s.finalHeight = p.Head, p.FinalHeight
	s.final, s.pending, s.receipts = p.Final, p.Pending, p.Receipts
	s.log, s.seq, s.mempool, s.nextDeposit = p.Log, p.Seq, p.Mempool, p.NextDeposit
	if s.final.Escrow == nil {
		s.final = newVault()
	}
	if s.receipts == nil {
		s.receipts = map[string]TxStatus{}
	}
	for hs, hash := range p.HashAt {
		var h int64
		if _, err := fmt.Sscan(hs, &h); err == nil {
			s.hashAt[h] = hash
		}
	}
	return nil
}

// Snapshot is for the dashboard.
func (s *Sim) Snapshot() map[string]any {
	v := s.VaultState()
	s.mu.Lock()
	defer s.mu.Unlock()
	settled := make([]string, 0, len(s.final.Settled))
	for m := range s.final.Settled {
		settled = append(settled, m)
	}
	sort.Strings(settled)
	return map[string]any{
		"vault": v, "faults": s.faults, "pending_blocks": len(s.pending),
		"mempool": len(s.mempool), "events": len(s.log),
		"escrow_total": v.EscrowTotal(), "settled_markets": strings.Join(settled, ","),
	}
}
