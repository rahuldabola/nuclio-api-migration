"""In-process harness: legacy FastAPI app and both Nuclio functions over one fake Redis.

No Docker, Kafka or Redis needed. Nuclio handlers are driven through nuclio-sdk's
mock platform, with request bodies decoded the way the real Nuclio processor does it.
"""

import importlib.util
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

import fakeredis
import fakeredis.aioredis
import pytest
from fastapi.testclient import TestClient
from nuclio_sdk import Event
from nuclio_sdk.test import Platform

from quotecore.store import AsyncQuoteStore, QuoteStore

ROOT = Path(__file__).resolve().parents[1]


def load_function(name: str):
    """Import functions/<name>/main.py under a unique module name (both are called main.py)."""
    module_name = f"nuclio_fn_{name}"
    spec = importlib.util.spec_from_file_location(module_name, ROOT / "functions" / name / "main.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module  # the mock platform looks init_context up here
    spec.loader.exec_module(module)
    return module


@dataclass
class FakePublisher:
    fail: bool = False
    messages: list = field(default_factory=list)

    def publish(self, topic: str, key: str, value: bytes) -> bool:
        if self.fail:
            return False
        self.messages.append((topic, key, value))
        return True


@dataclass
class HttpResult:
    status: int
    body: object
    backend: str


class NuclioClient:
    """Calls the quote_api handler like the Nuclio HTTP trigger would."""

    def __init__(self, platform: Platform, module):
        self._platform = platform
        self._module = module

    def request(self, method: str, path: str, json_body=None, raw: bytes | None = None) -> HttpResult:
        body = raw if raw is not None else (json.dumps(json_body).encode() if json_body is not None else b"")
        # The processor pre-decodes application/json bodies and passes raw bytes on failure.
        decoded = body
        if body:
            try:
                decoded = json.loads(body)
            except ValueError:
                decoded = body
        event = Event(body=decoded, content_type="application/json", method=method, path=path)
        response = self._platform.call_handler(self._module.handler, event)
        return HttpResult(response.status_code, json.loads(response.body), response.headers.get("X-Backend"))


class LegacyClient:
    def __init__(self, client: TestClient):
        self._client = client

    def request(self, method: str, path: str, json_body=None, raw: bytes | None = None) -> HttpResult:
        if raw is not None:
            r = self._client.request(method, path, content=raw, headers={"content-type": "application/json"})
        elif json_body is not None:
            r = self._client.request(method, path, json=json_body)
        else:
            r = self._client.request(method, path)
        return HttpResult(r.status_code, r.json(), r.headers.get("X-Backend"))


@pytest.fixture
def redis_server():
    return fakeredis.FakeServer()


@pytest.fixture
def sync_store(redis_server):
    return QuoteStore(fakeredis.FakeRedis(server=redis_server, decode_responses=True))


@pytest.fixture
def publisher():
    return FakePublisher()


@pytest.fixture
def legacy(redis_server):
    from legacy.app import app

    app.state.store = AsyncQuoteStore(fakeredis.aioredis.FakeRedis(server=redis_server, decode_responses=True))
    with TestClient(app) as client:
        yield LegacyClient(client)


@pytest.fixture
def api_module(sync_store, publisher):
    module = load_function("quote_api")

    def init_context(context):
        context.user_data.store = sync_store
        context.user_data.publisher = publisher

    module.init_context = init_context
    return module


@pytest.fixture
def nuclio(api_module):
    return NuclioClient(Platform(), api_module)


@pytest.fixture
def worker(sync_store):
    module = load_function("quote_worker")

    def init_context(context):
        context.user_data.store = sync_store

    module.init_context = init_context
    platform = Platform()

    def deliver(value: bytes):
        return platform.call_handler(module.handler, Event(body=value, method="POST", path="/"))

    return deliver
