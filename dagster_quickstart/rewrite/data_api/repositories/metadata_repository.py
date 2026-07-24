"""Business repository for metadata."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime

import pandas as pd
import structlog

from rewrite.data_api.errors import InvalidFilterFieldError, InvalidFilterValueError
from rewrite.data_api.repositories.storage_repository import MetadataStorageRepository

logger = structlog.get_logger(__name__)


class MetadataRepository:
    """
    Business repository responsible for metadata operations.

    Storage concerns are delegated to a MetadataStorageRepository.
    """

    def __init__(
        self,
        storage: MetadataStorageRepository,
    ) -> None:
        self._storage = storage

    def get_metadata(
        self,
        filters: Mapping[str, Sequence[str]] | None = None,
        *,
        exclude: bool = False,
        version: int | None = None,
        as_of: datetime | None = None,
        strict: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieve metadata matching supplied filters.

        strict controls how unrecognized filter *values* (e.g. a typo'd
        asset_class) are handled: False (default) drops them with a logged
        warning and proceeds with the valid subset, True raises
        InvalidFilterValueError.
        """

        filters = self._normalize_filters(filters)
        self._validate_filter_fields(filters)
        filters = self._validate_filter_values(filters, strict=strict)

        return self._storage.get_metadata(
            filters=filters,
            exclude=exclude,
            version=version,
            as_of=as_of,
        )

    def get_columns(self) -> list[str]:
        """
        Return the available column names, for filter validation/discovery.
        """

        return self._storage.get_columns()

    def get_filter_options(
        self,
        fields: str | Sequence[str] | None = None,
        *,
        filters: Mapping[str, Sequence[str]] | None = None,
        exclude: bool = False,
        strict: bool = False,
    ) -> dict[str, list[str]] | list[str]:
        """
        Return the distinct values available for one or more metadata
        columns, so a caller can see what they can actually filter by.

        fields=None returns options for every column. Pass filters to narrow
        the options to a subset (e.g. currency values within
        asset_class=Equity) -- computed via SELECT DISTINCT, not by pulling
        the whole table into pandas. strict controls how unrecognized values
        in that narrowing filter are handled (see get_metadata()).
        """

        available_columns = self._storage.get_columns()

        if fields is None:
            requested_fields = list(available_columns)
        else:
            requested_fields = [fields] if isinstance(fields, str) else list(fields)

        if not requested_fields:
            raise ValueError("get_filter_options() requires at least one field")

        self._raise_if_invalid_fields(requested_fields, available_columns)

        normalized_filters = self._normalize_filters(filters)
        self._validate_filter_fields(normalized_filters)
        normalized_filters = self._validate_filter_values(normalized_filters, strict=strict)
        options_by_field = {
            field: self._storage.get_distinct_values(
                field, filters=normalized_filters, exclude=exclude
            )
            for field in requested_fields
        }

        if fields is not None and len(requested_fields) == 1:
            return options_by_field[requested_fields[0]]

        return options_by_field

    def save_metadata(
        self,
        frame: pd.DataFrame,
        *,
        fresh: bool = False,
    ) -> None:
        if frame.empty:
            return

        logger.info("metadata_repository_save", row_count=len(frame), fresh=fresh)
        self._storage.save_metadata(frame, fresh=fresh)

    def refresh(self) -> None:
        logger.info("metadata_repository_refresh")
        self._storage.refresh_metadata()

    def _validate_filter_fields(
        self,
        filters: Mapping[str, Sequence[str]],
    ) -> None:
        """
        Raise InvalidFilterFieldError (listing available columns) if any
        filter key isn't an actual metadata column.
        """

        if not filters:
            return

        self._raise_if_invalid_fields(list(filters.keys()), self._storage.get_columns())

    @staticmethod
    def _raise_if_invalid_fields(
        fields: Sequence[str],
        available_columns: Sequence[str],
    ) -> None:
        """
        Raise InvalidFilterFieldError (listing available columns) if any of
        the given fields isn't an actual metadata column.
        """

        invalid_fields = [field for field in fields if field not in available_columns]

        if invalid_fields:
            logger.warning(
                "invalid_filter_field",
                invalid_fields=invalid_fields,
                available_columns=sorted(available_columns),
            )
            raise InvalidFilterFieldError(
                f"Invalid field(s): {invalid_fields}. Available columns: {sorted(available_columns)}"
            )

    def _validate_filter_values(
        self,
        filters: Mapping[str, Sequence[str]],
        *,
        strict: bool,
    ) -> dict[str, list[str]]:
        """
        Check each filter's values against the column's actual distinct
        values, raising or dropping-with-a-warning per `strict` (see
        _raise_or_drop_invalid_values).
        """

        if not filters:
            return dict(filters)

        return {
            field: self._raise_or_drop_invalid_values(
                field, values, self._storage.get_distinct_values(field), strict=strict
            )
            for field, values in filters.items()
        }

    @staticmethod
    def _raise_or_drop_invalid_values(
        field: str,
        values: Sequence[str],
        valid_options: Sequence[str],
        *,
        strict: bool,
    ) -> list[str]:
        """
        Compare requested filter values against the valid options for a
        column. In strict mode, raise InvalidFilterValueError naming the bad
        value(s) and the valid options. Otherwise, log a warning and return
        only the valid subset so the query still runs successfully.
        """

        invalid_values = [value for value in values if value not in valid_options]

        if not invalid_values:
            return list(values)

        if strict:
            logger.warning(
                "invalid_filter_value",
                field=field,
                invalid_values=invalid_values,
                available_options=sorted(valid_options),
            )
            raise InvalidFilterValueError(
                f"Invalid value(s) for field {field!r}: {invalid_values}. "
                f"Valid options: {sorted(valid_options)}"
            )

        logger.warning(
            "invalid_filter_value_dropped",
            field=field,
            invalid_values=invalid_values,
            available_options=sorted(valid_options),
        )
        return [value for value in values if value in valid_options]

    @staticmethod
    def _normalize_filters(
        filters: Mapping[str, Sequence[str]] | None,
    ) -> Mapping[str, Sequence[str]]:
        if filters is None:
            return {}

        normalized = {}

        for key, values in filters.items():
            if not values:
                continue

            normalized[key] = list(dict.fromkeys(values))

        return normalized
