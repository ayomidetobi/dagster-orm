"""Pydantic request validation for query-time value-query parameters."""

from __future__ import annotations

from datetime import datetime

import structlog
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator, model_validator

from rewrite.data_api.columns import ValueColumns, normalize_ticker_source
from rewrite.data_api.errors import (
    InvalidQueryError,
    SnapshotConflictError,
    TickerSourceRequiredError,
)

logger = structlog.get_logger(__name__)


class ValueQueryRequest(BaseModel):
    """Validates the scalar parameters shared by DataAPI.get_values and QuerySet.value.

    pydantic handles type coercion and simple scalar rules (start <= end,
    limit > 0); cross-field business rules that deserve their own precise
    exception type (snapshot conflict, missing ticker_source) are checked
    separately in validate_value_query() rather than folded into pydantic's
    generic ValidationError.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    ticker_source: str | None = None
    start: datetime | None = None
    end: datetime | None = None
    order_by: str | None = ValueColumns.TIMESTAMP
    ascending: bool = True
    limit: int | None = None
    version: int | None = None
    as_of: datetime | None = None
    out_of_cache: bool = False

    @field_validator("ticker_source", mode="before")
    @classmethod
    def _normalize_ticker_source(cls, value: str | None) -> str | None:
        return normalize_ticker_source(value) if value else value

    @model_validator(mode="after")
    def _check_scalar_rules(self) -> "ValueQueryRequest":
        if self.start is not None and self.end is not None and self.start > self.end:
            raise ValueError(f"start ({self.start}) must be <= end ({self.end})")
        if self.limit is not None and self.limit <= 0:
            raise ValueError(f"limit must be positive, got {self.limit}")
        return self


def validate_value_query(**kwargs: object) -> ValueQueryRequest:
    """Validate value-query parameters, raising a domain-specific error per failure mode."""

    try:
        request = ValueQueryRequest(**kwargs)
    except ValidationError as exc:
        logger.warning("invalid_query_parameters", errors=str(exc))
        raise InvalidQueryError(str(exc)) from exc

    if request.version is not None and request.as_of is not None:
        logger.warning("snapshot_conflict", version=request.version, as_of=request.as_of)
        raise SnapshotConflictError("Specify only one of version or as_of.")

    if request.out_of_cache and not request.ticker_source:
        logger.warning("ticker_source_required_for_out_of_cache")
        raise TickerSourceRequiredError("out_of_cache=True requires ticker_source")

    return request
