"""Nuclio Kafka-triggered function: processes bulk quote jobs from the `quote-jobs` topic.

Replaces the legacy in-process BackgroundTask. Kafka gives at-least-once delivery, so
the handler is idempotent: a redelivered message for a finished job is skipped rather
than pricing the job twice.
"""

import json
import os

from pydantic import ValidationError

from quotecore.jobs import run_job
from quotecore.models import QuoteJobRequest
from quotecore.store import QuoteStore


def init_context(context):
    context.user_data.store = QuoteStore.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"))


def handler(context, event):
    store = context.user_data.store
    body = event.body
    message = json.loads(body) if isinstance(body, (bytes, bytearray, str)) else body
    job_id = message["job_id"]

    existing = store.get_job(job_id)
    if existing is not None and existing.status == "done":
        context.logger.info_with("Skipping already processed job", job_id=job_id)
        return

    try:
        request = QuoteJobRequest.model_validate({"quotes": message["quotes"]})
    except ValidationError as exc:
        # A malformed message would fail forever; record it and move on instead of
        # blocking the partition.
        context.logger.error_with("Invalid job payload", job_id=job_id, error=str(exc))
        if existing is not None:
            existing.status, existing.error = "failed", "invalid job payload"
            store.save_job(existing)
        return

    quotes, job = run_job(job_id, request.quotes)
    for quote in quotes:
        store.save_quote(quote)
    store.save_job(job)
    context.logger.info_with("Job processed", job_id=job_id, quotes=len(quotes))
