"""The parity corpus: requests replayed against both implementations.

Shared by the in-process parity tests and by parity/run_parity.py, which replays the
same cases against the live deployments over HTTP. Add a case here whenever a client
reports a behaviour the new service must keep.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Case:
    name: str
    method: str
    path: str
    json: object = None
    raw: bytes | None = None


def item(sku="SKU-1", qty=1, unit_price="10.00"):
    return {"sku": sku, "qty": qty, "unit_price": unit_price}


CASES = [
    # --- happy paths and pricing rules -------------------------------------------------
    Case("simple IN quote", "POST", "/v1/quotes", {"items": [item(qty=2, unit_price="199.99")], "region": "IN"}),
    Case("US free shipping boundary", "POST", "/v1/quotes", {"items": [item(unit_price="100.00")], "region": "US"}),
    Case("US just below free shipping", "POST", "/v1/quotes", {"items": [item(unit_price="99.99")], "region": "US"}),
    Case("EU multi-line", "POST", "/v1/quotes",
         {"items": [item("A", 3, "12.35"), item("B", 1, "0.01"), item("C", 7, "4.99")], "region": "EU"}),
    Case("SAVE10 coupon", "POST", "/v1/quotes", {"items": [item(qty=4, unit_price="250.00")], "region": "IN", "coupon": "SAVE10"}),
    Case("SAVE10 hits cap", "POST", "/v1/quotes", {"items": [item(qty=10, unit_price="999.00")], "region": "IN", "coupon": "save10"}),
    Case("FLAT50 below minimum", "POST", "/v1/quotes", {"items": [item(unit_price="499.99")], "region": "EU", "coupon": "FLAT50"}),
    Case("FLAT50 applied", "POST", "/v1/quotes", {"items": [item(unit_price="500.00")], "region": "EU", "coupon": "FLAT50"}),
    Case("unknown coupon ignored", "POST", "/v1/quotes", {"items": [item()], "region": "US", "coupon": "BOGUS"}),
    Case("half-up rounding on tax", "POST", "/v1/quotes", {"items": [item(qty=3, unit_price="0.35")], "region": "US"}),
    Case("price as JSON number", "POST", "/v1/quotes", {"items": [item(unit_price=19.99)], "region": "IN"}),
    # --- validation errors (clients parse these envelopes) -----------------------------
    Case("missing region", "POST", "/v1/quotes", {"items": [item()]}),
    Case("bad region", "POST", "/v1/quotes", {"items": [item()], "region": "UK"}),
    Case("empty items", "POST", "/v1/quotes", {"items": [], "region": "IN"}),
    Case("qty zero", "POST", "/v1/quotes", {"items": [item(qty=0)], "region": "IN"}),
    Case("qty not int", "POST", "/v1/quotes", {"items": [item(qty="two")], "region": "IN"}),
    Case("negative price", "POST", "/v1/quotes", {"items": [item(unit_price="-1")], "region": "IN"}),
    Case("too many decimals", "POST", "/v1/quotes", {"items": [item(unit_price="1.999")], "region": "IN"}),
    Case("unknown field", "POST", "/v1/quotes", {"items": [item()], "region": "IN", "priority": True}),
    Case("body is a list", "POST", "/v1/quotes", [1, 2, 3]),
    Case("malformed JSON", "POST", "/v1/quotes", raw=b'{"items": [,], "region": "IN"}'),
    Case("empty body", "POST", "/v1/quotes", raw=b""),
    # --- lookups and routing ------------------------------------------------------------
    Case("unknown quote", "GET", "/v1/quotes/does-not-exist"),
    Case("unknown job", "GET", "/v1/quote-jobs/does-not-exist"),
    Case("unknown route", "GET", "/v1/nope"),
    Case("wrong method", "DELETE", "/v1/quotes"),
    Case("job validation error", "POST", "/v1/quote-jobs", {"quotes": []}),
]

VOLATILE_KEYS = {"id", "created_at", "job_id", "quote_ids"}


def normalize(value):
    """Blank out generated identifiers and timestamps so responses can be compared."""
    if isinstance(value, dict):
        return {k: ("<volatile>" if k in VOLATILE_KEYS else normalize(v)) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize(v) for v in value]
    return value
