// Package httpapi exposes the engine over HTTP and serves the operator panel.
package httpapi

import (
	"context"
	"embed"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/ranranrunforit/prediction-margin-reconciler/internal/app"
	"github.com/ranranrunforit/prediction-margin-reconciler/internal/chain"
	"github.com/ranranrunforit/prediction-margin-reconciler/internal/core"
)

//go:embed panel.html
var assets embed.FS

type Server struct{ A *app.App }

func (s *Server) Routes() *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("/", s.panel)
	m.HandleFunc("/api/state", s.state)
	m.HandleFunc("/api/deposit", s.deposit)
	m.HandleFunc("/api/withdraw", s.withdraw)
	m.HandleFunc("/api/position", s.position)
	m.HandleFunc("/api/settle", s.settle)
	m.HandleFunc("/api/faults", s.faults)
	m.HandleFunc("/api/verify", s.verify)
	m.HandleFunc("/api/unfreeze", s.unfreeze)
	m.HandleFunc("/api/inject-shortfall", s.injectShortfall)
	return m
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
}

func (s *Server) panel(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	b, err := assets.ReadFile("panel.html")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(b)
}

type balanceRow struct {
	Account string `json:"account"`
	Balance int64  `json:"balance"`
}

func (s *Server) state(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rep, err := s.A.Reconcile(ctx)
	if err != nil {
		fail(w, err)
		return
	}
	viol, err := s.A.Verify(ctx, false)
	if err != nil {
		fail(w, err)
		return
	}
	inflight, err := s.A.Store.InFlight(ctx)
	if err != nil {
		fail(w, err)
		return
	}
	type intentRow struct {
		ID       string `json:"id"`
		Kind     string `json:"kind"`
		User     string `json:"user"`
		Amount   int64  `json:"amount"`
		State    string `json:"state"`
		Attempts int    `json:"attempts"`
		OnChain  string `json:"on_chain"`
		Err      string `json:"last_error"`
	}
	rows := make([]intentRow, 0, len(inflight))
	for _, it := range inflight {
		st := s.A.Chain.TxStatus(it.ID)
		onChain := "unknown"
		switch {
		case st.Processed && st.Finalized:
			onChain = "finalized"
		case st.Processed:
			onChain = "in block " + strconv.FormatInt(st.Height, 10)
		default:
			onChain = "not seen"
		}
		rows = append(rows, intentRow{ID: it.ID[:8], Kind: it.Kind, User: it.UserID,
			Amount: it.Amount, State: it.State, Attempts: it.Attempts,
			OnChain: onChain, Err: it.LastError})
	}

	bals := []balanceRow{}
	if br, err := s.A.DB().QueryContext(ctx, `
        select account_code, balance from account_balances
         where balance <> 0 order by account_code limit 60`); err == nil {
		for br.Next() {
			var b balanceRow
			if br.Scan(&b.Account, &b.Balance) == nil {
				bals = append(bals, b)
			}
		}
		br.Close()
	}

	freezes, _ := s.A.Store.Freezes(ctx)
	markets, _ := s.A.Store.Markets(ctx)
	margin, _ := s.A.Store.MarginTotal(ctx)
	collateral, _ := s.A.Store.OpenCollateral(ctx)

	type marketRow struct {
		ID    string `json:"id"`
		Mark  int64  `json:"mark"`
		State string `json:"state"`
	}
	mrows := make([]marketRow, 0, len(markets))
	for _, m := range markets {
		mrows = append(mrows, marketRow{ID: m.ID, Mark: m.Mark, State: m.State})
	}

	writeJSON(w, 200, map[string]any{
		"reconciliation": rep,
		"violations":     viol,
		"invariants":     app.Invariants,
		"intents":        rows,
		"balances":       bals,
		"freezes":        freezes,
		"markets":        mrows,
		"chain":          s.A.Chain.Snapshot(),
		"margin":         margin,
		"collateral":     collateral,
		"feed":           s.A.Feed.Name(),
	})
}

func body(r *http.Request) map[string]any {
	var m map[string]any
	_ = json.NewDecoder(r.Body).Decode(&m)
	if m == nil {
		m = map[string]any{}
	}
	return m
}

func str(m map[string]any, k, def string) string {
	if v, ok := m[k].(string); ok && v != "" {
		return v
	}
	return def
}

func amount(m map[string]any, k string, def int64) int64 {
	if v, ok := m[k].(float64); ok {
		return int64(v * float64(core.One))
	}
	return def
}

func (s *Server) deposit(w http.ResponseWriter, r *http.Request) {
	m := body(r)
	id := s.A.Chain.Deposit(str(m, "user", "alice"), amount(m, "amount", 100*core.One))
	writeJSON(w, 200, map[string]string{"fact_id": id,
		"note": "sent to the vault contract; it will appear once mined and observed"})
}

func (s *Server) withdraw(w http.ResponseWriter, r *http.Request) {
	m := body(r)
	user := str(m, "user", "alice")
	id, replayed, err := s.A.RequestWithdraw(r.Context(), user,
		amount(m, "amount", 10*core.One), str(m, "key", ""))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"intent": id, "replayed": replayed})
}

func (s *Server) position(w http.ResponseWriter, r *http.Request) {
	m := body(r)
	id, err := s.A.Store.OpenPosition(r.Context(), str(m, "user", "alice"),
		str(m, "market", "m00"), str(m, "side", "long"), amount(m, "size", 50*core.One), "")
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"position": id})
}

func (s *Server) settle(w http.ResponseWriter, r *http.Request) {
	m := body(r)
	var outcome int64
	if v, ok := m["outcome"].(float64); ok && v >= 0.5 {
		outcome = core.One
	}
	id, replayed, err := s.A.Store.RequestSettlement(r.Context(), str(m, "market", "m00"), outcome)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"intent": id, "replayed": replayed})
}

func (s *Server) faults(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, 200, s.A.Chain.Faults())
		return
	}
	var f chain.Faults
	if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
		fail(w, err)
		return
	}
	s.A.Chain.SetFaults(f)
	writeJSON(w, 200, s.A.Chain.Faults())
}

func (s *Server) verify(w http.ResponseWriter, r *http.Request) {
	strict := r.URL.Query().Get("strict") == "1"
	vs, err := s.A.Verify(r.Context(), strict)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"violations": vs, "ok": len(vs) == 0})
}

func (s *Server) unfreeze(w http.ResponseWriter, r *http.Request) {
	if err := s.A.Store.Unfreeze(r.Context(), str(body(r), "subject", "*")); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
}

// injectShortfall deliberately credits a user without any matching on-chain
// deposit. It keeps double entry intact, so the ledger alone still looks
// perfectly healthy -- which is the point. Only reconciliation against the
// chain can catch it.
func (s *Server) injectShortfall(w http.ResponseWriter, r *http.Request) {
	m := body(r)
	amt := amount(m, "amount", 500*core.One)
	user := str(m, "user", "alice")
	id, _, err := s.A.Store.Post(r.Context(), core.Transfer{
		Kind:           "INJECTED_BUG.phantom_credit",
		IdempotencyKey: "bug:" + core.NewID(),
		Meta:           map[string]any{"injected": true},
		Legs: []core.Leg{
			{Account: core.AcctExternal, Amount: -amt},
			{Account: core.AvailableAcct(user), Amount: amt},
		},
	})
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"transfer": id,
		"note": "phantom credit written; the next reconciliation should report unsafe_shortfall and halt withdrawals"})
}

// Serve is a convenience wrapper.
func Serve(ctx context.Context, a *app.App, addr string) error {
	s := &Server{A: a}
	srv := &http.Server{Addr: addr, Handler: s.Routes()}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	return srv.ListenAndServe()
}
