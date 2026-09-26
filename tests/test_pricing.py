from datetime import UTC, datetime
from decimal import Decimal

import pytest

from quotecore.models import QuoteRequest
from quotecore.pricing import price_quote

NOW = datetime(2026, 9, 26, 12, 0, tzinfo=UTC)


def quote(items, region, coupon=None):
    request = QuoteRequest.model_validate({"items": items, "region": region, "coupon": coupon})
    return price_quote(request, "q1", NOW)


def test_in_quote_with_shipping():
    q = quote([{"sku": "A", "qty": 2, "unit_price": "199.99"}], "IN")
    assert (q.subtotal, q.discount, q.tax, q.shipping, q.total) == (
        Decimal("399.98"), Decimal("0.00"), Decimal("72.00"), Decimal("49.00"), Decimal("520.98"))
    assert q.currency == "INR"
    assert q.created_at == "2026-09-26T12:00:00Z"


def test_tax_rounds_half_up_not_bankers():
    # 0.25 * 18% = 0.045, an exact tie: HALF_UP gives 0.05, banker's rounding would give 0.04.
    q = quote([{"sku": "A", "qty": 1, "unit_price": "0.25"}], "IN")
    assert q.tax == Decimal("0.05")
    # 0.10 * 8.25% = 0.00825 -> 0.01
    q = quote([{"sku": "A", "qty": 1, "unit_price": "0.10"}], "US")
    assert q.tax == Decimal("0.01")


@pytest.mark.parametrize("coupon, expected", [
    ("SAVE10", Decimal("100.00")),
    ("save10", Decimal("100.00")),
    ("FLAT50", Decimal("50.00")),
    ("NOPE", Decimal("0.00")),
    (None, Decimal("0.00")),
])
def test_coupons(coupon, expected):
    assert quote([{"sku": "A", "qty": 1, "unit_price": "1000.00"}], "IN", coupon).discount == expected


def test_save10_is_capped():
    q = quote([{"sku": "A", "qty": 10, "unit_price": "999.00"}], "IN", "SAVE10")
    assert q.discount == Decimal("500.00")


def test_free_shipping_uses_discounted_subtotal():
    # 105.00 qualifies in the US, but SAVE10 brings it to 94.50, so shipping is charged.
    q = quote([{"sku": "A", "qty": 1, "unit_price": "105.00"}], "US", "SAVE10")
    assert q.shipping == Decimal("15.00")


def test_money_serializes_as_two_place_strings():
    q = quote([{"sku": "A", "qty": 1, "unit_price": "5"}], "US")
    dumped = q.model_dump(mode="json")
    assert dumped["subtotal"] == "5.00"
    assert dumped["line_items"][0]["unit_price"] == "5.00"
