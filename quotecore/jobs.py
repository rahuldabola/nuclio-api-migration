"""Bulk quote jobs: price every request in a job and report the outcome.

Legacy ran this in a FastAPI BackgroundTask inside the web process; after the
migration it runs in a Kafka-triggered Nuclio function. Both call this function.
"""

from .models import Quote, QuoteJob, QuoteRequest
from .pricing import price_quote
from .store import new_id


def run_job(job_id: str, requests: list[QuoteRequest]) -> tuple[list[Quote], QuoteJob]:
    quotes = [price_quote(req, new_id()) for req in requests]
    job = QuoteJob(
        job_id=job_id,
        status="done",
        total_requests=len(requests),
        quote_ids=[q.id for q in quotes],
    )
    return quotes, job


def pending_job(job_id: str, total_requests: int) -> QuoteJob:
    return QuoteJob(job_id=job_id, status="pending", total_requests=total_requests)
