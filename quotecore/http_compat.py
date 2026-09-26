"""Reproduce FastAPI's request-parsing and error envelopes outside FastAPI.

Clients of the legacy API parse its 422/404/405 bodies, so the Nuclio functions have
to emit byte-for-byte compatible errors. This module is the whole compatibility layer;
tests/test_parity_inprocess.py proves it against a real FastAPI app.
"""

import json
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ValidationError

_MISSING = object()


class RequestError(Exception):
    def __init__(self, status_code: int, detail: Any):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def _jsonable(value: Any) -> Any:
    # Mirrors fastapi.encoders.jsonable_encoder for what shows up in error ctx:
    # integral Decimals become ints (ctx {"gt": 0}, not "0"), others floats.
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return str(value)


def _jsonable_errors(exc: ValidationError) -> list[dict]:
    # FastAPI prefixes every location with "body" and drops the pydantic docs URL.
    errors = []
    for err in exc.errors(include_url=False):
        err = dict(err)
        err["loc"] = ["body", *err["loc"]]
        errors.append(_jsonable(err))
    return errors


def decode_body(body: Any) -> Any:
    """Turn a Nuclio event body into the parsed JSON value FastAPI would have seen.

    Nuclio pre-decodes `application/json` bodies into Python objects, but hands the
    raw bytes through when decoding fails, so both shapes have to be handled.
    """
    if body is None or body == b"" or body == "":
        return _MISSING
    if isinstance(body, (bytes, bytearray)):
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise RequestError(422, [{
                "type": "json_invalid",
                "loc": ["body", exc.pos],
                "msg": "JSON decode error",
                "input": {},
                "ctx": {"error": exc.msg},
            }]) from None
    return body


def parse_model(model: type[BaseModel], body: Any) -> BaseModel:
    value = decode_body(body)
    if value is _MISSING:
        raise RequestError(422, [{"type": "missing", "loc": ["body"], "msg": "Field required", "input": None}])
    try:
        # FastAPI validates bodies with from_attributes=True, which is why a non-object
        # body reports `model_attributes_type` rather than pydantic's usual `model_type`.
        return model.model_validate(value, from_attributes=True)
    except ValidationError as exc:
        raise RequestError(422, _jsonable_errors(exc)) from None
