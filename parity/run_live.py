"""Live parity and cutover checks through Kong, against the real deployments.

    python -m parity.run_live --gateway http://localhost:8080

1. Replays the parity corpus against /legacy and /next and diffs status + body.
2. Creates quotes on each backend and reads them back through the other.
3. Submits a bulk job to each backend and waits for both to finish (Nuclio's goes
   through Kafka and the quote-worker function).
4. Sends traffic to the canary route /v1 and checks both backends served it with
   identical results.

Exits non-zero on any mismatch.
"""

import argparse
import sys
import time
from collections import Counter

import httpx

from tests.cases import CASES, item, normalize

failures: list[str] = []


def check(condition: bool, message: str) -> None:
    print(("  ok    " if condition else "  FAIL  ") + message)
    if not condition:
        failures.append(message)


def send(client: httpx.Client, prefix: str, method: str, path: str, json_body=None, raw=None) -> httpx.Response:
    url = prefix + path
    if raw is not None:
        return client.request(method, url, content=raw, headers={"content-type": "application/json"})
    if json_body is not None:
        return client.request(method, url, json=json_body)
    return client.request(method, url)


def replay_corpus(client: httpx.Client) -> None:
    print("parity corpus:")
    for case in CASES:
        old = send(client, "/legacy", case.method, case.path, case.json, case.raw)
        new = send(client, "/next", case.method, case.path, case.json, case.raw)
        same = old.status_code == new.status_code and normalize(old.json()) == normalize(new.json())
        detail = "" if same else (
            f"  legacy={old.status_code} {old.text[:200]} | nuclio={new.status_code} {new.text[:200]}")
        check(same, f"{case.name}{detail}")
        check(old.headers.get("x-backend") == "legacy" and new.headers.get("x-backend") == "nuclio",
              f"{case.name}: routed to the right backend")


def cross_reads(client: httpx.Client) -> None:
    print("shared state across backends:")
    for writer, reader in [("/legacy", "/next"), ("/next", "/legacy")]:
        created = send(client, writer, "POST", "/v1/quotes", {"items": [item(qty=2)], "region": "EU"})
        fetched = send(client, reader, "GET", f"/v1/quotes/{created.json()['id']}")
        check(created.status_code == 201 and fetched.json() == created.json(), f"write {writer} -> read {reader}")


def wait_for_job(client: httpx.Client, prefix: str, job_id: str, timeout: float = 60) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        job = send(client, prefix, "GET", f"/v1/quote-jobs/{job_id}").json()
        if job["status"] != "pending" or time.monotonic() > deadline:
            return job
        time.sleep(0.5)


def bulk_jobs(client: httpx.Client) -> None:
    print("bulk jobs (legacy BackgroundTask vs Kafka + Nuclio worker):")
    payload = {"quotes": [{"items": [item(qty=q, unit_price="45.50")], "region": "US"} for q in range(1, 6)]}
    results = {}
    for prefix in ("/legacy", "/next"):
        started = time.monotonic()
        accepted = send(client, prefix, "POST", "/v1/quote-jobs", payload)
        check(accepted.status_code == 202, f"{prefix} accepted job")
        job = wait_for_job(client, prefix, accepted.json()["job_id"])
        check(job["status"] == "done", f"{prefix} job finished in {time.monotonic() - started:.2f}s")
        results[prefix] = [normalize(send(client, prefix, "GET", f"/v1/quotes/{q}").json()) for q in job["quote_ids"]]
    check(results["/legacy"] == results["/next"], "bulk job quotes identical on both backends")


def canary(client: httpx.Client, requests: int = 60) -> None:
    print("canary route /v1:")
    served, bodies = Counter(), set()
    for _ in range(requests):
        r = send(client, "", "POST", "/v1/quotes", {"items": [item(qty=3, unit_price="12.35")], "region": "IN"})
        served[r.headers.get("x-backend")] += 1
        bodies.add(repr(normalize(r.json())))
        if r.status_code != 201:
            check(False, f"canary request failed: {r.status_code} {r.text[:200]}")
    print(f"        traffic split: {dict(served)}")
    check(served["legacy"] > 0 and served["nuclio"] > 0, "both backends received canary traffic")
    check(len(bodies) == 1, "every canary response identical regardless of backend")


def gateway_policies(base: str) -> None:
    print("gateway policies:")
    with httpx.Client(base_url=base, timeout=10) as anon:
        r = anon.get("/v1/quotes/x")
        check(r.status_code == 401, f"request without apikey rejected ({r.status_code})")
    with httpx.Client(base_url=base, timeout=10, headers={"apikey": "demo-key-change-me"}) as c:
        r = c.get("/next/v1/quotes/x")
        check(bool(r.headers.get("x-request-id")), "correlation id echoed")
        check("x-ratelimit-limit-minute" in r.headers, "rate-limit headers present")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", default="http://localhost:8080")
    parser.add_argument("--apikey", default="demo-key-change-me")
    args = parser.parse_args()

    with httpx.Client(base_url=args.gateway, timeout=20, headers={"apikey": args.apikey}) as client:
        replay_corpus(client)
        cross_reads(client)
        bulk_jobs(client)
        canary(client)
    gateway_policies(args.gateway)

    print(f"\n{'PARITY FAILED' if failures else 'PARITY OK'}: {len(failures)} failure(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
