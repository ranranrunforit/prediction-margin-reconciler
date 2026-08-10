// Package pricefeed holds the hot mark-price cache and the liquidation lease.
//
// Everything in here is deliberately reconstructible. Redis holds no state
// that could not be rebuilt from Postgres and the chain, which is why losing
// Redis is an availability problem and never a correctness problem.
//
// The Redis client is ~80 lines of RESP rather than a dependency, so the repo
// builds with exactly one third-party module (the Postgres driver).
package pricefeed

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Feed interface {
	// Publish updates the hot mark price for a market.
	Publish(ctx context.Context, marketID string, mark int64) error
	// Marks returns every cached mark price.
	Marks(ctx context.Context) (map[string]int64, error)
	// Lease grants short-lived exclusivity, used to stop two liquidator
	// passes from working the same position. It is an optimisation: the
	// authoritative guard is the version check on the positions row.
	Lease(ctx context.Context, key string, ttl time.Duration) bool
	Name() string
	Close() error
}

// ---------------------------------------------------------------- in-memory

type mem struct {
	mu     sync.Mutex
	marks  map[string]int64
	leases map[string]time.Time
}

func NewMemory() Feed {
	return &mem{marks: map[string]int64{}, leases: map[string]time.Time{}}
}

func (m *mem) Publish(_ context.Context, id string, mark int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.marks[id] = mark
	return nil
}

func (m *mem) Marks(context.Context) (map[string]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int64, len(m.marks))
	for k, v := range m.marks {
		out[k] = v
	}
	return out, nil
}

func (m *mem) Lease(_ context.Context, key string, ttl time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if exp, ok := m.leases[key]; ok && time.Now().Before(exp) {
		return false
	}
	m.leases[key] = time.Now().Add(ttl)
	return true
}

func (m *mem) Name() string { return "memory" }
func (m *mem) Close() error { return nil }

// ---------------------------------------------------------------- redis

type redisFeed struct {
	mu   sync.Mutex
	addr string
	conn net.Conn
	rw   *bufio.ReadWriter
	key  string
}

// NewRedis dials redis://host:port. On any failure the caller should fall back
// to NewMemory: this cache is never the source of truth.
func NewRedis(rawURL string) (Feed, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	host := u.Host
	if host == "" {
		host = rawURL
	}
	if !strings.Contains(host, ":") {
		host += ":6379"
	}
	r := &redisFeed{addr: host, key: "pmr:marks"}
	if err := r.dial(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *redisFeed) dial() error {
	c, err := net.DialTimeout("tcp", r.addr, 2*time.Second)
	if err != nil {
		return err
	}
	r.conn = c
	r.rw = bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c))
	return nil
}

func (r *redisFeed) do(args ...string) (any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn == nil {
		if err := r.dial(); err != nil {
			return nil, err
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := r.rw.WriteString(b.String()); err != nil {
		r.reset()
		return nil, err
	}
	if err := r.rw.Flush(); err != nil {
		r.reset()
		return nil, err
	}
	v, err := readReply(r.rw.Reader)
	if err != nil {
		r.reset()
	}
	return v, err
}

func (r *redisFeed) reset() {
	if r.conn != nil {
		_ = r.conn.Close()
	}
	r.conn = nil
}

func readReply(rd *bufio.Reader) (any, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return nil, errors.New("empty reply")
	}
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return nil, errors.New(line[1:])
	case ':':
		return strconv.ParseInt(line[1:], 10, 64)
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := ioReadFull(rd, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil, nil
		}
		out := make([]any, 0, n)
		for i := 0; i < n; i++ {
			v, err := readReply(rd)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	}
	return nil, fmt.Errorf("unexpected reply prefix %q", line[0])
}

func ioReadFull(rd *bufio.Reader, b []byte) (int, error) {
	n := 0
	for n < len(b) {
		m, err := rd.Read(b[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (r *redisFeed) Publish(_ context.Context, id string, mark int64) error {
	_, err := r.do("HSET", r.key, id, strconv.FormatInt(mark, 10))
	return err
}

func (r *redisFeed) Marks(context.Context) (map[string]int64, error) {
	v, err := r.do("HGETALL", r.key)
	if err != nil {
		return nil, err
	}
	arr, _ := v.([]any)
	out := map[string]int64{}
	for i := 0; i+1 < len(arr); i += 2 {
		k, _ := arr[i].(string)
		s, _ := arr[i+1].(string)
		n, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			out[k] = n
		}
	}
	return out, nil
}

func (r *redisFeed) Lease(_ context.Context, key string, ttl time.Duration) bool {
	v, err := r.do("SET", "pmr:lease:"+key, "1", "NX", "PX", strconv.FormatInt(ttl.Milliseconds(), 10))
	if err != nil {
		// Redis being unavailable must not stop liquidation; the version
		// check on the position row is the real safety net.
		return true
	}
	s, _ := v.(string)
	return s == "OK"
}

func (r *redisFeed) Name() string { return "redis:" + r.addr }

func (r *redisFeed) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reset()
	return nil
}
