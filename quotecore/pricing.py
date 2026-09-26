"""Quote pricing rules, extracted verbatim from the legacy service during the migration.

Keeping this module free of I/O and framework imports is what made the migration
safe: FastAPI and Nuclio are just two thin adapters around the same pure function.

Legacy behaviours that are deliberately preserved (clients depend on them):
  * every monetary step rounds HALF_UP to 2 places (not banker's rounding);
  * the discount is applied before tax, and tax is charged on the discounted subtotal;
  * the free-shipping threshold is checked against the *discounted* subtotal;
  * an unknown coupon is ignored rather than rejected.
"""

from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal

from .models import LineItemOut, Quote, QuoteRequest, Region

CENT = Decimal("0.01")

CURRENCY = {Region.IN: "INR", Region.US: "USD", Region.EU: "EUR"}
TAX_RATE = {Region.IN: Decimal("0.18"), Region.US: Decimal("0.0825"), Region.EU: Decimal("0.20")}
SHIPPING_FEE = {Region.IN: Decimal("49.00"), Region.US: Decimal("15.00"), Region.EU: Decimal("12.00")}
FREE_SHIPPING_FROM = {Region.IN: Decimal("999.00"), Region.US: Decimal("100.00"), Region.EU: Decimal("90.00")}

SAVE10_CAP = Decimal("500.00")
FLAT50_MIN_SUBTOTAL = Decimal("500.00")


def _round(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def discount_for(coupon: str | None, subtotal: Decimal) -> Decimal:
    code = (coupon or "").strip().upper()
    if code == "SAVE10":
        return min(_round(subtotal * Decimal("0.10")), SAVE10_CAP)
    if code == "FLAT50" and subtotal >= FLAT50_MIN_SUBTOTAL:
        return Decimal("50.00")
    return Decimal("0.00")


def price_quote(request: QuoteRequest, quote_id: str, now: datetime | None = None) -> Quote:
    now = now or datetime.now(UTC)
    region = request.region

    line_items = [
        LineItemOut(
            sku=item.sku,
            qty=item.qty,
            unit_price=item.unit_price,
            line_total=_round(item.unit_price * item.qty),
        )
        for item in request.items
    ]
    subtotal = sum((li.line_total for li in line_items), Decimal("0.00"))
    discount = discount_for(request.coupon, subtotal)
    taxable = subtotal - discount
    tax = _round(taxable * TAX_RATE[region])
    shipping = Decimal("0.00") if taxable >= FREE_SHIPPING_FROM[region] else SHIPPING_FEE[region]

    return Quote(
        id=quote_id,
        region=region,
        currency=CURRENCY[region],
        coupon=request.coupon,
        line_items=line_items,
        subtotal=subtotal,
        discount=discount,
        tax=tax,
        shipping=shipping,
        total=taxable + tax + shipping,
        created_at=now.isoformat(timespec="seconds").replace("+00:00", "Z"),
    )
