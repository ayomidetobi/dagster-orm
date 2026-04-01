"""QuerySet class for building and executing metadata and value queries."""

from typing import Any, Dict, List, Optional, Iterable, Tuple

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
    TableNames,
    TickerSource,
    ValueColumns,
    COLUMN_GROUPS,
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
        control_table: Optional[str] = None,
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
            control_table: ``None`` uses ``TableNames.METADATA_WILDCARD`` (``control/metadata*/``).
                Otherwise ``TableNames.METADATA`` for validated primary catalog only, or
                ``TableNames.METADATA_DERIVED`` for dependency definitions only.

        Raises:
            InvalidFilterFieldError: If any filter field is invalid
        """
        self._metadata_repository = metadata_repository
        self._value_repository = value_repository
        self._series_codes: Optional[List[str]] = series_codes
        self._control_table = control_table or TableNames.METADATA_WILDCARD
        self._metadata_filters = (
            self._normalize_filters(metadata_filters) if metadata_filters is not None else {}
        )
        self._resolved_series_codes: Optional[List[str]] = None
        self._validation_repository = validation_repository
        self._exclude = exclude

    def __repr__(self) -> str:
        segments = [
            f"filters={self._metadata_filters!r}",
            f"exclude={self._exclude!r}",
            f"control_table={self._control_table!r}",
        ]
        if self._series_codes is not None:
            segments.append(f"series_codes={self._series_codes!r}")
        return f"QuerySet({', '.join(segments)})"

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

    def _load_metadata_rows(
        self,
        filters: Dict[str, List[str]],
        *,
        exclude: bool,
    ) -> pd.DataFrame:
        """Load metadata for this QuerySet's control table (URI resolved in repositories).

        Primary catalog ``metadata`` optionally goes through lookup validation; other
        control types use :meth:`MetadataRepository.filter` only.
        """
        if self._validation_repository is not None and self._control_table == TableNames.METADATA:
            return self._validation_repository.filter_with_validation(
                filters=filters,
                exclude=exclude,
                control_type=self._control_table,
            )
        return self._metadata_repository.filter(
            filters=filters,
            control_type=self._control_table,
            exclude=exclude,
        )

    def _build_metadata_query(self) -> pd.DataFrame:
        """Build metadata query using metadata repository.

        If _series_codes is set, filters by series_code instead of using metadata_filters.
        If validation_repository is provided and control table is primary ``metadata``,
        uses filter_with_validation() which performs filtering and lookup validation.

        Returns:
            DataFrame with filtered (and validated when applicable) metadata

        Raises:
            MetadataResolutionError: If query execution fails
        """
        if self._series_codes is not None:
            filters = {MetadataColumns.SERIES_CODE: self._series_codes}
        else:
            filters = self._metadata_filters

        return self._load_metadata_rows(filters, exclude=self._exclude)
    def _get_name_map(self, field: str) -> Dict[str, str]:
        metadata_df = self.info(allow_empty=True)

        if metadata_df.empty or field not in metadata_df.columns:
            return {}

        return dict (
            metadata_df[
                [MetadataColumns.SERIES_CODE, field]
            ].dropna().values
        )
    def _apply_column_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply column groups to a DataFrame.

        Args:
            df: DataFrame to apply column groups to

        Returns:
            DataFrame with column groups applied
        """
        column_to_group: Dict[str, str] = {}
        for group_name, columns_in_group in COLUMN_GROUPS.items():
            for column_name in columns_in_group:
                column_to_group.setdefault(column_name, group_name)
        tuples = [
            (column_to_group.get(column_name, "UNGROUPED"), column_name)
            for column_name in df.columns
        ]
        df = df.copy()
        df.columns = pd.MultiIndex.from_tuples(tuples)
        return df

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

    def info(self, *, allow_empty: bool = False ,detailed: bool = False) -> pd.DataFrame:
        """Get metadata information for matching series.

        Args:
            allow_empty: If True, return an empty DataFrame when no rows match instead of raising
            detailed: If True, apply column groups to the DataFrame
        Returns:
            DataFrame with metadata columns for all series matching the filters (validated)

        Raises:
            MetadataResolutionError: If query execution fails
            SeriesNotFoundError: If no series match the filters (unless allow_empty)
        """
        try:
            metadata_df = self._build_metadata_query()

            if metadata_df.empty and not allow_empty:
                if self._series_codes is not None:
                    raise SeriesNotFoundError(
                        f"No metadata found for series codes: {self._series_codes}"
                    )
                else:
                    raise SeriesNotFoundError(
                        f"No series found matching filters: {self._metadata_filters}"
                    )

            if detailed:
                metadata_df = self._apply_column_groups(metadata_df)

            return metadata_df

        except SeriesNotFoundError:
            raise
        except Exception as exc:
            raise MetadataResolutionError(f"Failed to retrieve metadata info: {exc}") from exc

    def value(
        self,
        params: Optional[ValueQueryParams] = None,
        tickersource: TickerSource = TickerSource.BLOOMBERG,
        humanize: bool = False,
    ) -> pd.DataFrame:
        """Get value data for matching series.

        Args:
            params: Optional ValueQueryParams for time filtering and pagination
            tickersource: Ticker source (default: TickerSource.BLOOMBERG)
            humanize: If True, rename series_code to editorial_short_default
        Returns:
            DataFrame with timestamp as index, series_code as columns, and values as cell values.
            Timestamps are timezone-aware UTC and sorted ascending.

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

        # Return empty DataFrame unchanged
        if value_df.empty:
            return value_df

        # Pivot to wide format: timestamp as index, series_code as columns
        pivoted_df = value_df.pivot(
            index=ValueColumns.TIMESTAMP,
            columns=ValueColumns.SERIES_CODE,
            values=ValueColumns.VALUE,
        )
        if humanize:
            name_map = self._get_name_map(MetadataColumns.SERIES_NAME)
            pivoted_df = pivoted_df.rename(columns=name_map)

        return pivoted_df

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

        metadata_filters = {MetadataColumns.SERIES_CODE: current_codes}
        metadata_df = self._load_metadata_rows(metadata_filters, exclude=False)

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
            control_table=self._control_table,
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

        metadata_df = self._load_metadata_rows(
            {MetadataColumns.SERIES_CODE: current_codes},
            exclude=False,
        )

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
            control_table=self._control_table,
        )

    def union(self, *others: "QuerySet") -> "QuerySet":
        """Create a new QuerySet representing the union of multiple QuerySets.

        Resolves series codes from this QuerySet and all provided QuerySets, then creates
        a new QuerySet with the union of all their series codes. The result bypasses metadata filtering.

        Args:
            *others: One or more QuerySets to union with this QuerySet

        Returns:
            New QuerySet instance with unioned series codes from all QuerySets

        Raises:
            ValueError: If any QuerySets don't share the same repositories, or if no QuerySets provided

        Examples:
            # Union two datasets
            data_equity = data_api.get(asset_class=["Equity"])
            data_fx = data_api.get(asset_class=["FX"])
            combined = data_equity.union(data_fx)

            # Union multiple datasets
            data_equity = data_api.get(asset_class=["Equity"])
            data_fx = data_api.get(asset_class=["FX"])
            data_usa = data_api.get(country=["USA"])
            combined = data_equity.union(data_fx, data_usa)
        """
        if not others:
            raise ValueError("At least one QuerySet must be provided to union()")

        # Validate that all QuerySets use the same repositories
        all_querysets = [self] + list(others)
        for other in others:
            if (
                self._metadata_repository is not other._metadata_repository
                or self._value_repository is not other._value_repository
            ):
                raise ValueError(
                    "Cannot union QuerySets with different repository instances. "
                    "All QuerySets must be created from the same DataAPI instance."
                )
            if self._control_table != other._control_table:
                raise ValueError(
                    "Cannot union QuerySets with different control_table (metadata sources)."
                )

        # Resolve series codes from all QuerySets
        all_codes = set()
        for queryset in all_querysets:
            all_codes.update(queryset.resolve_series_codes())

        # Union the series codes
        unioned_codes = sorted(list(all_codes))

        # Create new QuerySet with unioned series codes
        # Filters are None, exclude is False (series_code mode bypasses filtering)
        return QuerySet(
            metadata_repository=self._metadata_repository,
            value_repository=self._value_repository,
            metadata_filters=None,
            validation_repository=self._validation_repository,
            exclude=False,
            series_codes=unioned_codes,
            control_table=self._control_table,
        )

    def get_last_values(
        self,
        ticker_source: TickerSource = TickerSource.BLOOMBERG,
        humanize: bool = False,
    ) -> pd.DataFrame:
        """Get latest (max timestamp) value for each series_code in this QuerySet.

        Args:
            ticker_source: Ticker source (default: TickerSource.BLOOMBERG)
            humanize: If True, rename series_code to series_name
        Returns:
            DataFrame with timestamp as index, series_code as columns, and values as cell values.
            Timestamps are timezone-aware UTC and sorted ascending.

        Raises:
            SeriesNotFoundError: If no series match the filters
        """
        resolved_series_codes = self.resolve_series_codes()

        if not resolved_series_codes:
            raise SeriesNotFoundError(f"No series found matching filters: {self._metadata_filters}")

        result_df = self._value_repository.get_last_values(resolved_series_codes, ticker_source)

        # Return empty DataFrame unchanged
        if result_df.empty:
            return result_df

        # Pivot to wide format: timestamp as index, series_code as columns
        pivoted_df = result_df.pivot(
            index=ValueColumns.TIMESTAMP,
            columns=ValueColumns.SERIES_CODE,
            values=ValueColumns.VALUE,
        )
        if humanize:
            name_map = self._get_name_map(MetadataColumns.SERIES_NAME)
            pivoted_df = pivoted_df.rename(columns=name_map)
        # Ensure timestamps remain timezone-aware UTC and sort ascending
        # if not pivoted_df.empty:
        #     pivoted_df = pivoted_df.sort_index(ascending=True)

        return pivoted_df

    def get_values(
        self,
        ticker_source: Optional[TickerSource] = None,
        humanize: bool = False,
    ) -> pd.DataFrame:
        """Get all values for all series in this QuerySet (optionally filtered by ticker_source).

        Args:
            ticker_source: Optional ticker source filter (default: None, uses BLOOMBERG)
            humanize: If True, rename series_code to series_name
        Returns:
            DataFrame with timestamp as index, series_code as columns, and values as cell values.
            Timestamps are timezone-aware UTC and sorted ascending.
        """
        # Default to BLOOMBERG if not specified
        if ticker_source is None:
            ticker_source = TickerSource.BLOOMBERG

        resolved_series_codes = self.resolve_series_codes()

        if not resolved_series_codes:
            return pd.DataFrame()

        # Get all values for these series
        result_df = self._value_repository.get_batch_series_data(
            resolved_series_codes,
            ticker_source,
            start=None,
            end=None,
            order_by=None,
            limit=None,
        )

        # Return empty DataFrame unchanged
        if result_df.empty:
            return result_df

        # Pivot to wide format: timestamp as index, series_code as columns
        pivoted_df = result_df.pivot(
            index=ValueColumns.TIMESTAMP,
            columns=ValueColumns.SERIES_CODE,
            values=ValueColumns.VALUE,
        )

        if humanize:
            name_map = self._get_name_map(MetadataColumns.SERIES_NAME)
            pivoted_df = pivoted_df.rename(columns=name_map)

        return pivoted_df

    def groupby(self, by: List[str]) -> Iterable[Tuple[tuple, "QuerySet"]]:
        """Group QuerySet by metadata columns.

        Args:
            by: List of metadata columns to group by

        Yields:
            Tuple of (group_key, QuerySet)
            where group_key is a tuple of values
        """
        if not by:
            raise ValueError("groupby 'by' cannot be empty")

        # Validate columns
        for col in by:
            if col not in VALID_METADATA_FILTER_COLUMNS:
                raise InvalidFilterFieldError(
                    f"'{col}' is not a valid metadata column for grouping"
                )

        metadata_df = self.info(allow_empty=True)

        if metadata_df.empty:
            return

        grouped = metadata_df.groupby(by, dropna=False)

        for group_values, group_df in grouped:
            # Normalize single key → tuple
            if not isinstance(group_values, tuple):
                group_values = (group_values,)

            series_codes = (
                group_df[MetadataColumns.SERIES_CODE]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )

            yield group_values, QuerySet(
                metadata_repository=self._metadata_repository,
                value_repository=self._value_repository,
                metadata_filters=None,
                validation_repository=self._validation_repository,
                exclude=False,
                series_codes=series_codes,
                control_table=self._control_table,
            )