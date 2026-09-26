"""Async load test: compare latency of the legacy and Nuclio backends through Kong.

    python -m perf.load --gateway http://localhost:8080 --requests 400 --concurrency 20

Writes a markdown table to stdout (and to $GITHUB_STEP_SUMMARY when set).
"""

import argparse
import asyncio
import os
import statistics
import time

import httpx

BODY = {"items": [{"sku": "A", "qty": 3, "unit_price": "12.35"}, {"sku": "B", "qty": 1, "unit_price": "499.00"}],
        "region": "IN", "coupon": "SAVE10"}


async def run(client: httpx.AsyncClient, prefix: str, total: int, concurrency: int) -> dict:
    latencies: list[float] = []
    errors = 0
    sem = asyncio.Semaphore(concurrency)

    async def one():
        nonlocal errors
        async with sem:
            start = time.perf_counter()
            r = await client.post(f"{prefix}/v1/quotes", json=BODY)
            latencies.append((time.perf_counter() - start) * 1000)
            if r.status_code != 201:
                errors += 1

    # warm up connections and worker processes
    await asyncio.gather(*(one() for _ in range(concurrency)))
    latencies.clear()
    errors = 0

    started = time.perf_counter()
    await asyncio.gather(*(one() for _ in range(total)))
    elapsed = time.perf_counter() - started

    q = statistics.quantiles(latencies, n=100)
    return {"rps": total / elapsed, "p50": q[49], "p95": q[94], "p99": q[98], "errors": errors}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", default="http://localhost:8080")
    parser.add_argument("--apikey", default="demo-key-change-me")
    parser.add_argument("--requests", type=int, default=400)
    parser.add_argument("--concurrency", type=int, default=20)
    args = parser.parse_args()

    limits = httpx.Limits(max_connections=args.concurrency)
    async with httpx.AsyncClient(base_url=args.gateway, headers={"apikey": args.apikey},
                                 timeout=30, limits=limits) as client:
        results = {name: await run(client, prefix, args.requests, args.concurrency)
                   for name, prefix in [("legacy (FastAPI)", "/legacy"), ("nuclio", "/next")]}

    lines = [f"POST /v1/quotes: {args.requests} requests, concurrency {args.concurrency}, via Kong", "",
             "| backend | req/s | p50 ms | p95 ms | p99 ms | errors |", "|---|---|---|---|---|---|"]
    for name, r in results.items():
        lines.append(f"| {name} | {r['rps']:.0f} | {r['p50']:.1f} | {r['p95']:.1f} | {r['p99']:.1f} | {r['errors']} |")
    report = "\n".join(lines)
    print(report)
    if summary := os.getenv("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as fh:
            fh.write(report + "\n")


if __name__ == "__main__":
    asyncio.run(main())
