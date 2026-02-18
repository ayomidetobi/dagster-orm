"""QuerySet class for building and executing metadata and value queries."""

from typing import Any, Dict, List, Optional

import pandas as pd

from dagster_quickstart.orm.domain.metadata_repository import MetadataRepository
from dagster_quickstart.orm.domain.validation_repository import ValidationRepository
from dagster_quickstart.orm.domain.value_repository import ValueRepository
from dagster_quickstart.orm.exceptions import (
    InvalidFilterFieldError,
    MetadataResolutionError,
    SeriesNotFoundError,
    ValueQueryParameterError,
)
from dagster_quickstart.orm.query_params import ValueQueryParams
from dagster_quickstart.orm.schema import (
    VALID_METADATA_FILTER_COLUMNS,
    MetadataColumns,
    TickerSource,
)


class QuerySet:
    """QuerySet for building and executing metadata and value queries.

    Responsibilities:
    - Normalize filters
    - Validate filter fields against MetadataColumns
    - Resolve series_code exactly once (cached)
    - Orchestrate metadata + value queries
    - Return pandas DataFrames

    QuerySet must:
    - Depend ONLY on MetadataRepository and ValueRepository
    - NOT depend on DuckDB connection
    - NOT depend on S3Operations
    - NOT build SQL
    - NOT know about Parquet or S3
    """

    def __init__(
        self,
        metadata_repository: MetadataRepository,
        value_repository: ValueRepository,
        metadata_filters: Optional[Dict[str, List[str]]] = None,
        validation_repository: Optional[ValidationRepository] = None,
        exclude: bool = False,
        series_codes: Optional[List[str]] = None,
    ):
        """Initialize QuerySet with repositories and filters.

        Args:
            metadata_repository: MetadataRepository for loading metadata
            value_repository: ValueRepository for loading value data
            metadata_filters: Dictionary mapping metadata column names to filter values.
                If None and series_codes is provided, filters will be empty.
            validation_repository: Optional ValidationRepository instance for validation
            exclude: If True, invert filter logic (exclude matching values)
            series_codes: Optional list of series codes to override filter-based resolution.
                If set, metadata filtering is bypassed and these codes are used directly.

        Raises:
            InvalidFilterFieldError: If any filter field is invalid
        """
        self._metadata_repository = metadata_repository
        self._value_repository = value_repository
        self._series_codes: Optional[List[str]] = series_codes
        self._metadata_filters = (
            self._normalize_filters(metadata_filters) if metadata_filters is not None else {}
        )
        self._resolved_series_codes: Optional[List[str]] = None
        self._validation_repository = validation_repository
        self._exclude = exclude

    def _normalize_filters(self, filters: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """Normalize filter dictionary to ensure consistent format.

        Converts single values to lists and validates filter fields.

        Args:
            filters: Raw filter dictionary

        Returns:
            Normalized filter dictionary with all values as lists

        Raises:
            InvalidFilterFieldError: If any filter field is not a valid metadata column
        """
        if not filters:
            return {}

        normalized_filters: Dict[str, List[str]] = {}

        for filter_field, filter_values in filters.items():
            if filter_field not in VALID_METADATA_FILTER_COLUMNS:
                raise InvalidFilterFieldError(
                    f"'{filter_field}' is not a valid metadata filter field. "
                    f"Valid fields: {sorted(VALID_METADATA_FILTER_COLUMNS)}"
                )

            if isinstance(filter_values, str):
                normalized_filters[filter_field] = [filter_values]
            elif isinstance(filter_values, list):
                normalized_filters[filter_field] = filter_values
            else:
                normalized_filters[filter_field] = [str(filter_values)]

        return normalized_filters

    def _build_metadata_query(self) -> pd.DataFrame:
        """Build metadata query using metadata repository.

        If _series_codes is set, filters by series_code instead of using metadata_filters.
        If validation_repository is provided, uses filter_with_validation() which
        performs both filtering and validation in a single SQL query.

        Returns:
            DataFrame with filtered (and validated if validation_repository provided) metadata

        Raises:
            MetadataResolutionError: If query execution fails
        """
        # If series_codes override is set, filter by series_code instead
        if self._series_codes is not None:
            filters = {MetadataColumns.SERIES_CODE: self._series_codes}
        else:
            filters = self._metadata_filters

        if self._validation_repository is not None:
            metadata_df = self._validation_repository.filter_with_validation(
                filters=filters, exclude=self._exclude
            )
        else:
            metadata_df = self._metadata_repository.filter(filters=filters, exclude=self._exclude)

        return metadata_df

    def resolve_series_codes(self) -> List[str]:
        """Resolve series codes for this QuerySet.

        If _series_codes is set, returns it directly (override mode).
        Otherwise, resolves from metadata query and caches the result.

        Returns:
            List of series_code strings

        Raises:
            MetadataResolutionError: If query execution fails
            SeriesNotFoundError: If no series match the filters
        """
        # If series_codes override is set, return it directly
        if self._series_codes is not None:
            return self._series_codes

        # Otherwise, use cached result or resolve from metadata
        if self._resolved_series_codes is not None:
            return self._resolved_series_codes

        try:
            metadata_result = self._build_metadata_query()

            if metadata_result.empty:
                raise SeriesNotFoundError(
                    f"No series found matching filters: {self._metadata_filters}"
                )

            if MetadataColumns.SERIES_CODE not in metadata_result.columns:
                raise MetadataResolutionError("Metadata DataFrame missing series_code column")

            resolved_codes = metadata_result[MetadataColumns.SERIES_CODE].dropna().unique().tolist()
            resolved_codes = [str(code).strip() for code in resolved_codes if code]

            if not resolved_codes:
                raise SeriesNotFoundError(
                    f"No series found matching filters: {self._metadata_filters}"
                )

            self._resolved_series_codes = resolved_codes
            return resolved_codes

        except SeriesNotFoundError:
            raise
        except Exception as exc:
            raise MetadataResolutionError(
                f"Failed to resolve series codes from metadata: {exc}"
            ) from exc

    def _validate_time_params(self, params: Optional[ValueQueryParams]) -> None:
        """Validate time query parameters.

        Args:
            params: ValueQueryParams with time filtering options

        Raises:
            ValueQueryParameterError: If time parameters are invalid
        """
        if params is None:
            return

        if params.start and params.end:
            if params.start > params.end:
                raise ValueQueryParameterError(
                    f"Start timestamp '{params.start}' must be <= end timestamp '{params.end}'"
                )

    def info(self) -> pd.DataFrame:
        """Get metadata information for matching series.

        Returns:
            DataFrame with metadata columns for all series matching the filters (validated)

        Raises:
            MetadataResolutionError: If query execution fails
            SeriesNotFoundError: If no series match the filters
        """
        try:
            metadata_df = self._build_metadata_query()

            if metadata_df.empty:
                if self._series_codes is not None:
                    raise SeriesNotFoundError(
                        f"No metadata found for series codes: {self._series_codes}"
                    )
                else:
                    raise SeriesNotFoundError(
                        f"No series found matching filters: {self._metadata_filters}"
                    )

            return metadata_df

        except SeriesNotFoundError:
            raise
        except Exception as exc:
            raise MetadataResolutionError(f"Failed to retrieve metadata info: {exc}") from exc

    def value(
        self,
        params: Optional[ValueQueryParams] = None,
        tickersource: TickerSource = TickerSource.BLOOMBERG,
    ) -> pd.DataFrame:
        """Get value data for matching series.

        Args:
            params: Optional ValueQueryParams for time filtering and pagination
            tickersource: Ticker source (default: TickerSource.BLOOMBERG)

        Returns:
            DataFrame with series_code, timestamp, and value columns

        Raises:
            SeriesNotFoundError: If no series match the filters
            ValueQueryParameterError: If time parameters are invalid
        """
        resolved_series_codes = self.resolve_series_codes()

        if not resolved_series_codes:
            raise SeriesNotFoundError(f"No series found matching filters: {self._metadata_filters}")

        self._validate_time_params(params)

        value_df = self._value_repository.get_batch_series_data(
            series_codes=resolved_series_codes,
            tickersource=tickersource,
            start=params.start if params else None,
            end=params.end if params else None,
            order_by=params.order_by if params else None,
            limit=params.limit if params else None,
        )

        return value_df

    def filter(self, **filters: Any) -> "QuerySet":
        """Apply additional filters to this QuerySet, creating a new filtered QuerySet.

        Resolves the current QuerySet's series codes, applies the new filters to their
        metadata, and returns a new QuerySet with the filtered series codes.

        Args:
            **filters: Keyword arguments mapping metadata column names to filter values.
                Values can be single strings or lists of strings.
                Example: country=["usa"], asset_class=["Equity"]

        Returns:
            New QuerySet instance with filtered series codes

        Raises:
            InvalidFilterFieldError: If any filter field is not a valid metadata column
            SeriesNotFoundError: If no series match the combined filters
        """
        # Normalize the new filters
        new_filters = {}
        for filter_field, filter_value in filters.items():
            if filter_field not in VALID_METADATA_FILTER_COLUMNS:
                raise InvalidFilterFieldError(
                    f"'{filter_field}' is not a valid metadata filter field. "
                    f"Valid fields: {sorted(VALID_METADATA_FILTER_COLUMNS)}"
                )

            if isinstance(filter_value, str):
                new_filters[filter_field] = [filter_value]
            elif isinstance(filter_value, list):
                new_filters[filter_field] = filter_value
            else:
                new_filters[filter_field] = [str(filter_value)]

        # Resolve current series codes
        current_codes = self.resolve_series_codes()

        if not current_codes:
            raise SeriesNotFoundError("Cannot filter empty QuerySet")

        # Get metadata for current series codes
        metadata_filters = {MetadataColumns.SERIES_CODE: current_codes}
        if self._validation_repository is not None:
            metadata_df = self._validation_repository.filter_with_validation(
                filters=metadata_filters, exclude=False
            )
        else:
            metadata_df = self._metadata_repository.filter(filters=metadata_filters, exclude=False)

        if metadata_df.empty:
            raise SeriesNotFoundError(f"No metadata found for series codes: {current_codes}")

        # Apply new filters to the metadata
        for filter_field, filter_values in new_filters.items():
            if filter_values:
                mask = metadata_df[filter_field].isin(filter_values)
                metadata_df = metadata_df[mask]

        if metadata_df.empty:
            raise SeriesNotFoundError(f"No series match the combined filters: {new_filters}")

        # Extract filtered series codes
        filtered_codes = sorted(metadata_df[MetadataColumns.SERIES_CODE].dropna().unique().tolist())

        # Create new QuerySet with filtered series codes
        return QuerySet(
            metadata_repository=self._metadata_repository,
            value_repository=self._value_repository,
            metadata_filters=None,
            validation_repository=self._validation_repository,
            exclude=False,
            series_codes=filtered_codes,
        )

    def filter_exclude(self, **filters: Any) -> "QuerySet":
        """Apply exclude filters to this QuerySet, creating a new filtered QuerySet.

        Resolves the current QuerySet's series codes, excludes series matching the
        filters from their metadata, and returns a new QuerySet with the remaining
        series codes.

        Args:
            **filters: Keyword arguments mapping metadata column names to filter values.
                Values can be single strings or lists of strings.
                Series matching these filters will be excluded.
                Example: country=["usa"], asset_class=["Equity"]

        Returns:
            New QuerySet instance with filtered series codes (excluding matching series)

        Raises:
            InvalidFilterFieldError: If any filter field is not a valid metadata column
            SeriesNotFoundError: If no series remain after exclusion
        """
        # Normalize the new filters
        new_filters = {}
        for filter_field, filter_value in filters.items():
            if filter_field not in VALID_METADATA_FILTER_COLUMNS:
                raise InvalidFilterFieldError(
                    f"'{filter_field}' is not a valid metadata filter field. "
                    f"Valid fields: {sorted(VALID_METADATA_FILTER_COLUMNS)}"
                )

            if isinstance(filter_value, str):
                new_filters[filter_field] = [filter_value]
            elif isinstance(filter_value, list):
                new_filters[filter_field] = filter_value
            else:
                new_filters[filter_field] = [str(filter_value)]

        # Resolve current series codes
        current_codes = self.resolve_series_codes()

        if not current_codes:
            raise SeriesNotFoundError("Cannot filter empty QuerySet")

        # Get metadata for current series codes
        metadata_filters = {MetadataColumns.SERIES_CODE: current_codes}
        if self._validation_repository is not None:
            metadata_df = self._validation_repository.filter_with_validation(
                filters=metadata_filters, exclude=False
            )
        else:
            metadata_df = self._metadata_repository.filter(filters=metadata_filters, exclude=False)

        if metadata_df.empty:
            raise SeriesNotFoundError(f"No metadata found for series codes: {current_codes}")

        # Apply exclude filters to the metadata (exclude series matching the filters)
        for filter_field, filter_values in new_filters.items():
            if filter_values:
                mask = ~metadata_df[filter_field].isin(filter_values)
                metadata_df = metadata_df[mask]

        if metadata_df.empty:
            raise SeriesNotFoundError(f"No series remain after excluding filters: {new_filters}")

        # Extract filtered series codes
        filtered_codes = sorted(metadata_df[MetadataColumns.SERIES_CODE].dropna().unique().tolist())

        # Create new QuerySet with filtered series codes
        return QuerySet(
            metadata_repository=self._metadata_repository,
            value_repository=self._value_repository,
            metadata_filters=None,
            validation_repository=self._validation_repository,
            exclude=False,
            series_codes=filtered_codes,
        )

    def union(self, other: "QuerySet") -> "QuerySet":
        """Create a new QuerySet representing the union of two QuerySets.

        Resolves series codes from both QuerySets and creates a new QuerySet
        with the union of their series codes. The result bypasses metadata filtering.

        Args:
            other: Another QuerySet to union with

        Returns:
            New QuerySet instance with unioned series codes

        Raises:
            ValueError: If QuerySets don't share the same repositories
        """
        # Validate that both QuerySets use the same repositories
        if (
            self._metadata_repository is not other._metadata_repository
            or self._value_repository is not other._value_repository
        ):
            raise ValueError(
                "Cannot union QuerySets with different repository instances. "
                "Both QuerySets must be created from the same DataAPI instance."
            )

        # Resolve series codes from both QuerySets
        self_codes = set(self.resolve_series_codes())
        other_codes = set(other.resolve_series_codes())

        # Union the series codes
        unioned_codes = sorted(list(self_codes | other_codes))

        # Create new QuerySet with unioned series codes
        # Filters are None, exclude is False (series_code mode bypasses filtering)
        return QuerySet(
            metadata_repository=self._metadata_repository,
            value_repository=self._value_repository,
            metadata_filters=None,
            validation_repository=self._validation_repository,
            exclude=False,
            series_codes=unioned_codes,
        )
