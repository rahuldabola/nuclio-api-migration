"""Behaviour specific to the Nuclio side: queue failures, redelivery, poison messages."""

import json

from quotecore.jobs import pending_job
from tests.cases import item


def test_enqueue_failure_returns_503_and_marks_job_failed(nuclio, publisher, sync_store):
    publisher.fail = True
    r = nuclio.request("POST", "/v1/quote-jobs", {"quotes": [{"items": [item()], "region": "IN"}]})
    assert r.status == 503
    assert r.body == {"detail": "Job queue unavailable"}

    (key,) = sync_store._r.keys("quotejob:*")
    job = sync_store.get_job(key.split(":", 1)[1])
    assert (job.status, job.error) == ("failed", "could not enqueue job")


def test_worker_is_idempotent_on_redelivery(nuclio, publisher, worker, sync_store):
    r = nuclio.request("POST", "/v1/quote-jobs", {"quotes": [{"items": [item()], "region": "US"}]})
    (_, _, value), = publisher.messages

    worker(value)
    first = sync_store.get_job(r.body["job_id"])
    worker(value)  # Kafka redelivers after a rebalance
    second = sync_store.get_job(r.body["job_id"])

    assert first.status == "done"
    assert second.quote_ids == first.quote_ids
    assert len(sync_store._r.keys("quote:*")) == 1


def test_worker_marks_poison_message_failed(worker, sync_store):
    sync_store.save_job(pending_job("job-1", 1))
    worker(json.dumps({"job_id": "job-1", "quotes": [{"items": [], "region": "IN"}]}).encode())

    job = sync_store.get_job("job-1")
    assert (job.status, job.error) == ("failed", "invalid job payload")


def test_healthz(nuclio):
    r = nuclio.request("GET", "/healthz")
    assert (r.status, r.body) == (200, {"status": "ok"})
