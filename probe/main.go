// Command probe watches the Quote API gateway during a canary cutover: it samples the
// gateway, reports the legacy/Nuclio traffic split and p95 latency, and exposes them as
// JSON (/status) and Prometheus metrics (/metrics).
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	target := env("PROBE_TARGET", "http://localhost:8080/v1/quotes/none")
	samples, err := strconv.Atoi(env("PROBE_SAMPLES", "20"))
	if err != nil || samples < 1 {
		log.Fatalf("invalid PROBE_SAMPLES")
	}
	interval, err := time.ParseDuration(env("PROBE_INTERVAL", "10s"))
	if err != nil {
		log.Fatalf("invalid PROBE_INTERVAL: %v", err)
	}

	prober := NewProber(&http.Client{Timeout: 5 * time.Second}, target, env("PROBE_API_KEY", ""), samples)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go prober.Run(ctx, interval)

	srv := &http.Server{Addr: env("PROBE_ADDR", ":9090"), Handler: prober.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()

	log.Printf("probing %s every %s, listening on %s", target, interval, srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
