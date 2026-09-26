"""Request/response models shared by the legacy FastAPI service and the Nuclio functions.

These models *are* the API contract. Both implementations validate with them and
serialize through them, so a field rename or type change breaks both sides at once
instead of silently drifting.
"""

from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer

# Money is carried as Decimal internally and rendered as a fixed two-place string on
# the wire ("1234.50"), which is what the legacy API always returned. Floats would
# leak binary rounding into totals.
Money = Annotated[Decimal, PlainSerializer(lambda v: f"{v:.2f}", return_type=str)]


class Region(StrEnum):
    IN = "IN"
    US = "US"
    EU = "EU"


class LineItemIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sku: str = Field(min_length=1, max_length=64)
    qty: int = Field(ge=1, le=1000)
    unit_price: Decimal = Field(gt=0, le=100000, max_digits=8, decimal_places=2)


class QuoteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[LineItemIn] = Field(min_length=1, max_length=100)
    region: Region
    coupon: str | None = Field(default=None, max_length=32)


class LineItemOut(BaseModel):
    sku: str
    qty: int
    unit_price: Money
    line_total: Money


class Quote(BaseModel):
    id: str
    region: Region
    currency: str
    coupon: str | None
    line_items: list[LineItemOut]
    subtotal: Money
    discount: Money
    tax: Money
    shipping: Money
    total: Money
    created_at: str


class QuoteJobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    quotes: list[QuoteRequest] = Field(min_length=1, max_length=50)


class QuoteJob(BaseModel):
    job_id: str
    status: Literal["pending", "done", "failed"]
    total_requests: int
    quote_ids: list[str] = []
    error: str | None = None
