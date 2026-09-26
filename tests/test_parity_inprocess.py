"""Functional parity: every corpus case must produce the same status and body on both backends."""

import json

import pytest

from tests.cases import CASES, item, normalize


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
def test_same_response(case, legacy, nuclio):
    old = legacy.request(case.method, case.path, json_body=case.json, raw=case.raw)
    new = nuclio.request(case.method, case.path, json_body=case.json, raw=case.raw)

    assert (old.backend, new.backend) == ("legacy", "nuclio")
    assert new.status == old.status, f"status drift: legacy={old.status} nuclio={new.status}"
    assert normalize(new.body) == normalize(old.body)


@pytest.mark.parametrize("writer, reader", [("legacy", "nuclio"), ("nuclio", "legacy")])
def test_quotes_readable_across_backends(writer, reader, legacy, nuclio):
    backends = {"legacy": legacy, "nuclio": nuclio}
    created = backends[writer].request("POST", "/v1/quotes", {"items": [item(qty=2)], "region": "EU"})
    assert created.status == 201

    fetched = backends[reader].request("GET", f"/v1/quotes/{created.body['id']}")
    assert fetched.status == 200
    assert fetched.body == created.body


def test_bulk_job_parity(legacy, nuclio, publisher, worker):
    payload = {"quotes": [
        {"items": [item(qty=3, unit_price="45.50")], "region": "US"},
        {"items": [item(unit_price="1200.00")], "region": "IN", "coupon": "SAVE10"},
    ]}

    old_job = legacy.request("POST", "/v1/quote-jobs", payload)
    new_job = nuclio.request("POST", "/v1/quote-jobs", payload)
    assert old_job.status == new_job.status == 202
    assert normalize(new_job.body) == normalize(old_job.body)

    # The Nuclio side only enqueued; run the Kafka consumer on what was published.
    (topic, key, value), = publisher.messages
    assert (topic, key) == ("quote-jobs", new_job.body["job_id"])
    worker(value)

    old_done = legacy.request("GET", f"/v1/quote-jobs/{old_job.body['job_id']}")
    new_done = nuclio.request("GET", f"/v1/quote-jobs/{new_job.body['job_id']}")
    assert old_done.body["status"] == new_done.body["status"] == "done"

    old_quotes = [normalize(legacy.request("GET", f"/v1/quotes/{q}").body) for q in old_done.body["quote_ids"]]
    new_quotes = [normalize(nuclio.request("GET", f"/v1/quotes/{q}").body) for q in new_done.body["quote_ids"]]
    assert new_quotes == old_quotes


def test_job_message_is_replayable_json(nuclio, publisher):
    nuclio.request("POST", "/v1/quote-jobs", {"quotes": [{"items": [item()], "region": "IN"}]})
    (_, _, value), = publisher.messages
    message = json.loads(value)
    assert message["quotes"][0]["items"][0]["unit_price"] == "10.00"
