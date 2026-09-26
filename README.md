# Quote API migration: FastAPI → Nuclio, with Kafka and Kong

[![CI](https://github.com/rahuldabola/nuclio-api-migration/actions/workflows/ci.yml/badge.svg)](https://github.com/rahuldabola/nuclio-api-migration/actions/workflows/ci.yml)

A worked migration of a running REST service onto **Nuclio** serverless functions without changing
its API contract. The legacy FastAPI service and the new functions run side by side behind **Kong**,
which splits traffic between them with weighted canary routing. Bulk work that used to run inside the
web process now goes through **Kafka** to a Kafka-triggered Nuclio function. A parity suite replays
the same requests against both implementations and fails the build on any difference in status code
or response body.

```
                         ┌──────────────── Kong (DB-less) ───────────────┐
 client ── apikey ──►    │ key-auth · rate-limiting · correlation-id ·   │
                         │ prometheus · active health checks             │
                         │                                               │
                         │  /v1/*        weighted: legacy 20 / nuclio 80 │
                         │  /legacy/v1/* pinned to legacy                │
                         │  /next/v1/*   pinned to nuclio                │
                         └───────┬──────────────────────────┬────────────┘
                                 │                          │
                  ┌──────────────▼─────────┐   ┌────────────▼───────────────┐
                  │ legacy (FastAPI, async)│   │ Nuclio fn: quote-api (HTTP)│
                  │ BackgroundTasks for    │   │ publishes bulk jobs ──────►│── Kafka topic
                  │ bulk jobs              │   └────────────────────────────┘   quote-jobs
                  └──────────────┬─────────┘                │                    │
                                 │        ┌─────────────────▼──────────────┐     │
                                 └──────► │ Redis (shared quote/job store) │ ◄───┤
                                          └────────────────────────────────┘     │
                                                   ┌─────────────────────────────▼──┐
                                                   │ Nuclio fn: quote-worker        │
                                                   │ (kafka-cluster trigger)        │
                                                   └────────────────────────────────┘
```

## The API being migrated

| Method | Path | Behaviour |
|---|---|---|
| `POST` | `/v1/quotes` | Price a cart (region tax, coupons, shipping threshold) → `201` quote |
| `GET` | `/v1/quotes/{id}` | Fetch a quote → `200`, or `404 {"detail": "Quote not found"}` |
| `POST` | `/v1/quote-jobs` | Submit up to 50 quotes for async pricing → `202` pending job |
| `GET` | `/v1/quote-jobs/{id}` | Job status: `pending` / `done` / `failed`, with the resulting quote ids |

Money is sent and returned as fixed two-decimal strings. Validation errors use FastAPI's
`422 {"detail": [...]}` envelope, and clients parse it.

## How the migration was done

1. **Moved the business logic into a shared package.** Pricing, models and persistence moved into
   [`quotecore/`](quotecore), which has no framework imports. The legacy service
   ([`legacy/app.py`](legacy/app.py)) and the Nuclio handler
   ([`functions/quote_api/main.py`](functions/quote_api/main.py)) are both thin adapters over it.
   The legacy rounding rules were kept exactly as they were: HALF_UP at each step, discount before
   tax, and the free-shipping check on the discounted subtotal. See
   [`quotecore/pricing.py`](quotecore/pricing.py).
2. **Reproduced the HTTP contract outside FastAPI.** Nuclio has no FastAPI layer, so
   [`quotecore/http_compat.py`](quotecore/http_compat.py) re-implements what clients observe:
   routing, 404/405 bodies, and FastAPI's validation error format.
3. **Wrote the parity corpus before cutover.** [`tests/cases.py`](tests/cases.py) holds 27 requests:
   pricing edge cases, every class of validation error, malformed JSON, empty bodies, and unknown
   routes and methods. Each one runs against both implementations, and the responses must match
   after normalising ids and timestamps.
4. **Replaced in-process background work with Kafka.** Legacy runs bulk jobs in FastAPI
   `BackgroundTasks`, so a pod restart loses them and they share the event loop with request
   handling. The Nuclio API now publishes each job to Kafka with an acknowledged send
   (`acks=all`, idempotent producer) and returns `202` only after Kafka confirms the write. The
   `quote-worker` function consumes the topic. It is idempotent, because Kafka can redeliver a
   message after a rebalance, and it marks malformed messages `failed` instead of retrying them
   forever.
5. **Cut over through Kong.** Both backends share Redis, so a quote created on one can be read
   through the other at any canary split. Moving traffic means changing two target weights
   ([`scripts/set_canary.sh`](scripts/set_canary.sh)), and rolling back means setting the Nuclio
   weight to 0. Clients never see a URL change.

### Drift the parity suite caught

The first parity run failed 2 of 27 cases, and both were differences a client would have hit:

| Case | Legacy (FastAPI) | First Nuclio version | Cause |
|---|---|---|---|
| `unit_price: "-1"` | `"ctx": {"gt": 0}` | `"ctx": {"gt": "0"}` | FastAPI's `jsonable_encoder` turns a whole-number `Decimal` into an int; I had stringified it |
| body is `[1,2,3]` | `model_attributes_type` | `model_type` | FastAPI validates request bodies with `from_attributes=True`, which changes pydantic's error type |

Another difference came from Nuclio itself. The Nuclio processor already decodes
`application/json` bodies before the handler runs, but passes the raw bytes through when the JSON
is invalid. The adapter handles both cases so that malformed JSON still produces FastAPI's
`json_invalid` error with the same character position.

## Verification

| Layer | What runs | Where |
|---|---|---|
| Unit | Pricing rules, rounding ties, coupon caps, money serialisation | `pytest` (local, CI) |
| In-process parity | All 27 corpus cases against the real FastAPI app vs. the Nuclio handler (via `nuclio-sdk`'s test platform), reads across backends, bulk-job parity through the worker | `pytest` (local, CI); no Docker needed |
| Nuclio-specific | Kafka publish failure → `503` and job marked failed, idempotent redelivery, malformed messages | `pytest` |
| Live parity | Corpus replayed through Kong to `/legacy` vs `/next`, bulk jobs through real Kafka, canary split, key-auth / rate-limit / correlation-id | CI `integration` job |
| Cutover drill | Kong weights → 0/100 (all Nuclio) → 100/0 (rollback), checking the `X-Backend` header on every response | CI `integration` job |
| Load | Async load test, p50/p95/p99 per backend | CI `integration` job (results in the run summary) |

## Run it

**Tests only (no Docker):**

```bash
python -m venv .venv && . .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
pytest
```

**Full stack** (needs Docker and [`nuctl`](https://github.com/nuclio/nuclio/releases) 1.17.x):

```bash
bash scripts/deploy.sh                 # redis, kafka, legacy → nuctl deploy both functions → kong
python -m parity.run_live              # live parity through Kong
bash scripts/set_canary.sh 0 100       # full cutover to Nuclio
bash scripts/set_canary.sh 100 0       # rollback
python -m perf.load                    # latency comparison
curl -H 'apikey: demo-key-change-me' localhost:8080/v1/quotes -H 'content-type: application/json' \
     -d '{"items":[{"sku":"A","qty":2,"unit_price":"199.99"}],"region":"IN"}'
```

Kong's metrics are at `http://localhost:8001/metrics`. The API key and open admin port are
local-development defaults.

## Layout

```
quotecore/            framework-free domain: models (the contract), pricing, Redis store, FastAPI-compat layer
legacy/               the original FastAPI service + Dockerfile
functions/quote_api/  Nuclio HTTP function + function.yaml
functions/quote_worker/ Nuclio Kafka-triggered function + function.yaml
gateway/kong.yml      DB-less Kong: canary upstream, pinned routes, auth, rate limits, metrics
tests/                unit, in-process parity, Nuclio-specific behaviour, shared parity corpus
parity/run_live.py    live parity + canary checks through the gateway
perf/load.py          async load test
scripts/              deploy.sh (compose + nuctl), set_canary.sh (weight shift / rollback)
```
