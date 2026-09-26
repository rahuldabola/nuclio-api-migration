"""The legacy Quote API: a long-running FastAPI service.

This is the "before" side of the migration and the reference implementation for
parity: whatever this service returns (status, body shape, error envelope) is the
contract the Nuclio functions must reproduce.
"""

import os

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request

from quotecore.jobs import pending_job, run_job
from quotecore.models import Quote, QuoteJob, QuoteJobRequest, QuoteRequest
from quotecore.pricing import price_quote
from quotecore.store import AsyncQuoteStore, new_id

app = FastAPI(title="Quote API (legacy)", version="1.0.0")
app.state.store = AsyncQuoteStore.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))


def store(request: Request) -> AsyncQuoteStore:
    return request.app.state.store


@app.middleware("http")
async def tag_backend(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Backend"] = "legacy"
    return response


@app.get("/healthz")
async def healthz(request: Request):
    await store(request).ping()
    return {"status": "ok"}


@app.post("/v1/quotes", response_model=Quote, status_code=201)
async def create_quote(body: QuoteRequest, request: Request):
    quote = price_quote(body, new_id())
    await store(request).save_quote(quote)
    return quote


@app.get("/v1/quotes/{quote_id}", response_model=Quote)
async def get_quote(quote_id: str, request: Request):
    quote = await store(request).get_quote(quote_id)
    if quote is None:
        raise HTTPException(status_code=404, detail="Quote not found")
    return quote


async def _process_job(s: AsyncQuoteStore, job_id: str, requests: list[QuoteRequest]) -> None:
    quotes, job = run_job(job_id, requests)
    for quote in quotes:
        await s.save_quote(quote)
    await s.save_job(job)


@app.post("/v1/quote-jobs", response_model=QuoteJob, status_code=202)
async def create_quote_job(body: QuoteJobRequest, request: Request, background: BackgroundTasks):
    job = pending_job(new_id(), len(body.quotes))
    await store(request).save_job(job)
    # In-process background work: lost if the pod restarts, and it competes with
    # request handling for the same event loop. This is what the migration replaced
    # with Kafka + a Nuclio consumer function.
    background.add_task(_process_job, store(request), job.job_id, body.quotes)
    return job


@app.get("/v1/quote-jobs/{job_id}", response_model=QuoteJob)
async def get_quote_job(job_id: str, request: Request):
    job = await store(request).get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
