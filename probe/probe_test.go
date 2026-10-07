package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// backend alternates X-Backend between legacy and nuclio, like a 50/50 canary.
func backend(t *testing.T, status int) *httptest.Server {
	t.Helper()
	var n atomic.Int64
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("apikey") != "k" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if n.Add(1)%2 == 0 {
			w.Header().Set("X-Backend", "legacy")
		} else {
			w.Header().Set("X-Backend", "nuclio")
		}
		w.WriteHeader(status)
	}))
}

func TestSampleCountsTrafficSplit(t *testing.T) {
	srv := backend(t, http.StatusNotFound) // the probed route answers 404, which is a healthy reply
	defer srv.Close()

	snap := NewProber(srv.Client(), srv.URL, "k", 10).Sample(context.Background())

	if snap.Samples != 10 || snap.Errors != 0 {
		t.Fatalf("samples=%d errors=%d", snap.Samples, snap.Errors)
	}
	if snap.ByBackend["legacy"] != 5 || snap.ByBackend["nuclio"] != 5 {
		t.Fatalf("split = %v", snap.ByBackend)
	}
	if snap.P95Ms <= 0 {
		t.Fatalf("p95 should be positive, got %v", snap.P95Ms)
	}
}

func TestServerErrorsAndUnreachableCountAsErrors(t *testing.T) {
	srv := backend(t, http.StatusBadGateway)
	snap := NewProber(srv.Client(), srv.URL, "k", 4).Sample(context.Background())
	srv.Close()
	if snap.Errors != 4 || snap.Samples != 0 {
		t.Fatalf("5xx: samples=%d errors=%d", snap.Samples, snap.Errors)
	}

	down := NewProber(http.DefaultClient, srv.URL, "k", 3).Sample(context.Background())
	if down.Errors != 3 {
		t.Fatalf("unreachable: errors=%d", down.Errors)
	}
}

func TestEndpoints(t *testing.T) {
	srv := backend(t, http.StatusNotFound)
	defer srv.Close()
	p := NewProber(srv.Client(), srv.URL, "k", 6)
	p.Sample(context.Background())
	api := httptest.NewServer(p.Handler())
	defer api.Close()

	resp, err := http.Get(api.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	var snap Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil || snap.Samples != 6 {
		t.Fatalf("status: %v %+v", err, snap)
	}

	metrics, _ := http.Get(api.URL + "/metrics")
	raw, _ := io.ReadAll(metrics.Body)
	body := string(raw)
	for _, want := range []string{
		`probe_requests_total{backend="legacy"} 3`,
		`probe_requests_total{backend="nuclio"} 3`,
		"probe_errors_total 0",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q in:\n%s", want, body)
		}
	}

	if h, _ := http.Get(api.URL + "/healthz"); h.StatusCode != http.StatusOK {
		t.Errorf("healthz status %d", h.StatusCode)
	}
}

func TestPercentile(t *testing.T) {
	if got := percentile([]float64{10, 20, 30, 40, 50}, 0.95); got != 50 {
		t.Fatalf("got %v", got)
	}
	if percentile(nil, 0.95) != 0 {
		t.Fatal("empty input should be 0")
	}
}
