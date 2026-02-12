"""QuerySet class for building and executing metadata and value queries."""

from typing import Dict, List, Optional

import pandas as pd

from dagster_quickstart.orm.domain.metadata_repository import MetadataRepository
from dagster_quickstart.orm.domain.value_repository import ValueRepository
from dagster_quickstart.orm.exceptions import (
    InvalidFilterFieldError,
    MetadataResolutionError,
    SeriesNotFoundError,
    ValueQueryParameterError,
)
from dagster_quickstart.orm.query_params import ValueQueryParams
from dagster_quickstart.orm.schema import (
    MetadataColumns,
    TickerSource,
    VALID_METADATA_FILTER_COLUMNS,
)
from dagster_quickstart.orm.validation import MetadataValidator


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
        metadata_filters: Dict[str, List[str]],
        validator: Optional[MetadataValidator] = None,
    ):
        """Initialize QuerySet with repositories and filters.

        Args:
            metadata_repository: MetadataRepository for loading metadata
            value_repository: ValueRepository for loading value data
            metadata_filters: Dictionary mapping metadata column names to filter values
            validator: Optional MetadataValidator instance for validation

        Raises:
            InvalidFilterFieldError: If any filter field is invalid
        """
        self._metadata_repository = metadata_repository
        self._value_repository = value_repository
        self._metadata_filters = self._normalize_filters(metadata_filters)
        self._resolved_series_codes: Optional[List[str]] = None
        self._validator = validator

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

        Returns:
            DataFrame with filtered metadata

        Raises:
            MetadataResolutionError: If query execution fails
        """
        metadata_df = self._metadata_repository.filter(filters=self._metadata_filters)

        if self._validator is not None:
            metadata_df = self._validator.validate_metadata_dataframe(metadata_df)

        return metadata_df

    def _resolve_series_codes(self) -> List[str]:
        """Resolve series codes from metadata query.

        Executes the metadata query and extracts series_code values.
        Results are cached to avoid re-execution.

        Returns:
            List of series_code strings

        Raises:
            MetadataResolutionError: If query execution fails
            SeriesNotFoundError: If no series match the filters
        """
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
        resolved_series_codes = self._resolve_series_codes()

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
