"""Nuclio HTTP function: the migrated Quote API.

Serves the same four routes as legacy/app.py with the same contract. Differences are
operational only:
  * bulk jobs are published to Kafka instead of running in-process;
  * clients are created once per worker in init_context, not per request.
"""

import json
import os
import re

from quotecore.http_compat import RequestError, parse_model
from quotecore.jobs import pending_job
from quotecore.models import QuoteJobRequest, QuoteRequest
from quotecore.pricing import price_quote
from quotecore.store import QuoteStore, new_id

JOBS_TOPIC = os.getenv("JOBS_TOPIC", "quote-jobs")


class KafkaPublisher:
    """Synchronous, acknowledged publish: a 202 is only returned once Kafka has the job."""

    def __init__(self, brokers: str):
        from confluent_kafka import Producer

        self._producer = Producer({"bootstrap.servers": brokers, "acks": "all", "enable.idempotence": True})

    def publish(self, topic: str, key: str, value: bytes) -> bool:
        errors = []
        self._producer.produce(topic, key=key, value=value,
                               on_delivery=lambda err, _msg: err and errors.append(err))
        remaining = self._producer.flush(10)
        return remaining == 0 and not errors


def init_context(context):
    context.user_data.store = QuoteStore.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"))
    context.user_data.publisher = KafkaPublisher(os.getenv("KAFKA_BROKERS", "kafka:9092"))


def _json(context, status: int, payload) -> object:
    body = payload if isinstance(payload, str) else json.dumps(payload)
    return context.Response(body=body, headers={"X-Backend": "nuclio"},
                            content_type="application/json", status_code=status)


def create_quote(context, event):
    request = parse_model(QuoteRequest, event.body)
    quote = price_quote(request, new_id())
    context.user_data.store.save_quote(quote)
    return _json(context, 201, quote.model_dump_json())


def get_quote(context, event, quote_id):
    quote = context.user_data.store.get_quote(quote_id)
    if quote is None:
        raise RequestError(404, "Quote not found")
    return _json(context, 200, quote.model_dump_json())


def create_quote_job(context, event):
    request = parse_model(QuoteJobRequest, event.body)
    job = pending_job(new_id(), len(request.quotes))
    store = context.user_data.store
    store.save_job(job)

    message = json.dumps({"job_id": job.job_id, "quotes": request.model_dump(mode="json")["quotes"]})
    if not context.user_data.publisher.publish(JOBS_TOPIC, job.job_id, message.encode()):
        job.status, job.error = "failed", "could not enqueue job"
        store.save_job(job)
        context.logger.error_with("Kafka publish failed", job_id=job.job_id)
        raise RequestError(503, "Job queue unavailable")
    return _json(context, 202, job.model_dump_json())


def get_quote_job(context, event, job_id):
    job = context.user_data.store.get_job(job_id)
    if job is None:
        raise RequestError(404, "Job not found")
    return _json(context, 200, job.model_dump_json())


def healthz(context, event):
    return _json(context, 200, {"status": "ok"})


ROUTES = [
    ("POST", re.compile(r"^/v1/quotes$"), create_quote),
    ("GET", re.compile(r"^/v1/quotes/(?P<quote_id>[^/]+)$"), get_quote),
    ("POST", re.compile(r"^/v1/quote-jobs$"), create_quote_job),
    ("GET", re.compile(r"^/v1/quote-jobs/(?P<job_id>[^/]+)$"), get_quote_job),
    ("GET", re.compile(r"^/healthz$"), healthz),
]


def handler(context, event):
    path = (event.path or "/").split("?", 1)[0]
    method = (event.method or "GET").upper()

    path_matched = False
    for route_method, pattern, fn in ROUTES:
        match = pattern.match(path)
        if not match:
            continue
        path_matched = True
        if route_method != method:
            continue
        try:
            return fn(context, event, **match.groupdict())
        except RequestError as exc:
            return _json(context, exc.status_code, {"detail": exc.detail})

    if path_matched:
        return _json(context, 405, {"detail": "Method Not Allowed"})
    return _json(context, 404, {"detail": "Not Found"})
