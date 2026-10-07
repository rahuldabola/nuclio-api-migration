package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Snapshot is the result of one sampling round.
type Snapshot struct {
	Samples   int            `json:"samples"`
	Errors    int            `json:"errors"`
	ByBackend map[string]int `json:"by_backend"`
	P95Ms     float64        `json:"p95_ms"`
	UpdatedAt time.Time      `json:"updated_at"`
}

// Prober samples the gateway on an interval and reports how traffic is split
// between backends (read from the X-Backend response header) and how fast it is.
// It keeps cumulative counters for /metrics and the latest round for /status.
type Prober struct {
	client  *http.Client
	url     string
	apiKey  string
	samples int

	mu       sync.RWMutex
	latest   Snapshot
	requests map[string]int
	errors   int
}

func NewProber(client *http.Client, url, apiKey string, samples int) *Prober {
	return &Prober{client: client, url: url, apiKey: apiKey, samples: samples, requests: map[string]int{}}
}

// Sample sends p.samples requests and records the outcome.
func (p *Prober) Sample(ctx context.Context) Snapshot {
	snap := Snapshot{ByBackend: map[string]int{}}
	var latencies []float64
	for i := 0; i < p.samples; i++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url, nil)
		if err != nil {
			snap.Errors++
			continue
		}
		if p.apiKey != "" {
			req.Header.Set("apikey", p.apiKey)
		}
		start := time.Now()
		resp, err := p.client.Do(req)
		elapsed := float64(time.Since(start).Microseconds()) / 1000
		if err != nil {
			snap.Errors++
			continue
		}
		backend := resp.Header.Get("X-Backend")
		resp.Body.Close()
		if resp.StatusCode >= 500 {
			snap.Errors++
			continue
		}
		if backend == "" {
			backend = "unknown"
		}
		snap.ByBackend[backend]++
		snap.Samples++
		latencies = append(latencies, elapsed)
	}
	snap.P95Ms = percentile(latencies, 0.95)
	snap.UpdatedAt = time.Now().UTC()

	p.mu.Lock()
	p.latest = snap
	for b, n := range snap.ByBackend {
		p.requests[b] += n
	}
	p.errors += snap.Errors
	p.mu.Unlock()
	return snap
}

// Run samples every interval until ctx is cancelled.
func (p *Prober) Run(ctx context.Context, interval time.Duration) {
	p.Sample(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.Sample(ctx)
		}
	}
}

func percentile(values []float64, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	idx := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[idx]
}

// Handler serves /healthz, /status (JSON) and /metrics (Prometheus text format).
func (p *Prober) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"ok"}`)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		p.mu.RLock()
		defer p.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p.latest)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		p.mu.RLock()
		defer p.mu.RUnlock()
		var b strings.Builder
		b.WriteString("# TYPE probe_requests_total counter\n")
		backends := make([]string, 0, len(p.requests))
		for name := range p.requests {
			backends = append(backends, name)
		}
		sort.Strings(backends)
		for _, name := range backends {
			fmt.Fprintf(&b, "probe_requests_total{backend=%q} %d\n", name, p.requests[name])
		}
		b.WriteString("# TYPE probe_errors_total counter\n")
		fmt.Fprintf(&b, "probe_errors_total %d\n", p.errors)
		b.WriteString("# TYPE probe_latency_p95_ms gauge\n")
		fmt.Fprintf(&b, "probe_latency_p95_ms %.2f\n", p.latest.P95Ms)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, b.String())
	})
	return mux
}
