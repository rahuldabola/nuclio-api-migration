"""Redis persistence shared by both implementations.

During the canary both the legacy service and the Nuclio functions read and write the
same keys, so a quote created on one backend can be fetched through the other. That
only works because the key layout and the JSON encoding live here, in one place.

The legacy service is async (redis.asyncio); Nuclio's Python handlers are sync, so
there are two thin clients over the same format.
"""

import uuid

import redis
import redis.asyncio as aioredis

from .models import Quote, QuoteJob

TTL_SECONDS = 7 * 24 * 3600


def new_id() -> str:
    return uuid.uuid4().hex


def quote_key(quote_id: str) -> str:
    return f"quote:{quote_id}"


def job_key(job_id: str) -> str:
    return f"quotejob:{job_id}"


class QuoteStore:
    def __init__(self, client: redis.Redis):
        self._r = client

    @classmethod
    def from_url(cls, url: str) -> "QuoteStore":
        return cls(redis.Redis.from_url(url, decode_responses=True))

    def save_quote(self, quote: Quote) -> None:
        self._r.set(quote_key(quote.id), quote.model_dump_json(), ex=TTL_SECONDS)

    def get_quote(self, quote_id: str) -> Quote | None:
        raw = self._r.get(quote_key(quote_id))
        return Quote.model_validate_json(raw) if raw else None

    def save_job(self, job: QuoteJob) -> None:
        self._r.set(job_key(job.job_id), job.model_dump_json(), ex=TTL_SECONDS)

    def get_job(self, job_id: str) -> QuoteJob | None:
        raw = self._r.get(job_key(job_id))
        return QuoteJob.model_validate_json(raw) if raw else None


class AsyncQuoteStore:
    def __init__(self, client: aioredis.Redis):
        self._r = client

    @classmethod
    def from_url(cls, url: str) -> "AsyncQuoteStore":
        return cls(aioredis.Redis.from_url(url, decode_responses=True))

    async def save_quote(self, quote: Quote) -> None:
        await self._r.set(quote_key(quote.id), quote.model_dump_json(), ex=TTL_SECONDS)

    async def get_quote(self, quote_id: str) -> Quote | None:
        raw = await self._r.get(quote_key(quote_id))
        return Quote.model_validate_json(raw) if raw else None

    async def save_job(self, job: QuoteJob) -> None:
        await self._r.set(job_key(job.job_id), job.model_dump_json(), ex=TTL_SECONDS)

    async def get_job(self, job_id: str) -> QuoteJob | None:
        raw = await self._r.get(job_key(job_id))
        return QuoteJob.model_validate_json(raw) if raw else None

    async def ping(self) -> bool:
        return await self._r.ping()
