"""QuerySet class for building and executing metadata and value queries."""

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd

from dagster_quickstart.orm.derived_fetch import get_derived_out_of_cache_values
from dagster_quickstart.orm.direct_source_fetch import get_direct_source_values
from dagster_quickstart.orm.domain.metadata_repository import MetadataRepository
from dagster_quickstart.orm.domain.validation_repository import ValidationRepository
from dagster_quickstart.orm.domain.value_repository import ValueRepository
from dagster_quickstart.orm.exceptions import (
    InvalidFilterFieldError,
    MetadataResolutionError,
    SeriesNotFoundError,
    ValueQueryParameterError,
)
from dagster_quickstart.orm.option_utils import dataframe_filter_options
from dagster_quickstart.orm.query_params import ValueQueryParams
from dagster_quickstart.orm.schema import (
    COLUMN_GROUPS,
    MetadataColumns,
    TableNames,
    TickerSource,
    VALID_METADATA_FILTER_COLUMNS,
    ValueColumns,
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
        out_of_cache: bool = False,
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
            out_of_cache: Default ``out_of_cache`` behavior for value retrieval methods.
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
        normalized_filters = (
            self._normalize_filter_input(metadata_filters) if metadata_filters is not None else {}
        )
        self._include_filters = {} if exclude else normalized_filters
        self._exclude_filters = normalized_filters if exclude else {}
        self._resolved_series_codes: Optional[List[str]] = None
        self._validation_repository = validation_repository
        self._out_of_cache = out_of_cache

    def __repr__(self) -> str:
        segments = [
            f"include_filters={self._include_filters!r}",
            f"exclude_filters={self._exclude_filters!r}",
            f"control_table={self._control_table!r}",
            f"out_of_cache={self._out_of_cache!r}",
        ]
        if (sc := self._series_codes) is not None:
            n = len(sc)
            segments.append(
                f"series_codes={sc!r}"
                if n <= 5
                else f"series_codes={repr(sc[:5])[:-1]}, ...] (n={n})"
            )
        if self._resolved_series_codes is not None:
            segments.append(f"resolved_series_codes={len(self._resolved_series_codes)} cached")
        return f"QuerySet({', '.join(segments)})"

    def _normalize_filter_input(self, filters: Optional[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Normalize a raw filter dictionary into ``Dict[str, List[str]]``.

        Converts scalar values to single-item lists and validates field names.

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

    def _merge_filters(
        self,
        current_filters: Dict[str, List[str]],
        new_filters: Dict[str, List[str]],
    ) -> Dict[str, List[str]]:
        """Merge filter dictionaries.

        Repeated fields are intersected so chained filters narrow the result set.
        New fields are added normally.
        """
        merged = {field: values[:] for field, values in current_filters.items()}
        for field, values in new_filters.items():
            if field in merged:
                allowed = set(values)
                merged[field] = [value for value in merged[field] if value in allowed]
            else:
                merged[field] = list(pd.unique(values))
        return merged

    def _effective_include_filters(self) -> Dict[str, List[str]]:
        """Return include filters plus explicit series-code scoping, if any."""
        include_filters = {field: values[:] for field, values in self._include_filters.items()}
        if self._series_codes is not None:
            include_filters[MetadataColumns.SERIES_CODE] = list(self._series_codes)
        return include_filters

    def _apply_exclude_filters_to_dataframe(self, metadata_df: pd.DataFrame) -> pd.DataFrame:
        """Apply exclude filters to a metadata DataFrame after include filtering."""
        filtered_df = metadata_df
        for filter_field, filter_values in self._exclude_filters.items():
            if filter_values:
                filtered_df = filtered_df[~filtered_df[filter_field].isin(filter_values)]
        return filtered_df

    def _filter_state_for_error(self) -> Dict[str, Dict[str, List[str]]]:
        """Return current filter state for error messages."""
        return {
            "include_filters": self._include_filters,
            "exclude_filters": self._exclude_filters,
        }

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

        Include filters are applied first at repository level where possible. Exclude
        filters are then applied lazily against the resulting metadata DataFrame.
        Explicit ``series_codes`` continue to scope the query without forcing eager
        resolution during filter chaining.

        Returns:
            DataFrame with filtered (and validated when applicable) metadata

        Raises:
            MetadataResolutionError: If query execution fails
        """
        metadata_df = self._load_metadata_rows(self._effective_include_filters(), exclude=False)
        if metadata_df.empty:
            return metadata_df
        return self._apply_exclude_filters_to_dataframe(metadata_df)

    def _get_name_map(self, field: str) -> Dict[str, str]:
        metadata_df = self.info(allow_empty=True)

        if metadata_df.empty or field not in metadata_df.columns:
            return {}

        return dict(metadata_df[[MetadataColumns.SERIES_CODE, field]].dropna().values)

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
                    f"No series found matching filters: {self._filter_state_for_error()}"
                )

            if MetadataColumns.SERIES_CODE not in metadata_result.columns:
                raise MetadataResolutionError("Metadata DataFrame missing series_code column")

            resolved_codes = metadata_result[MetadataColumns.SERIES_CODE].dropna().unique().tolist()
            resolved_codes = [str(code).strip() for code in resolved_codes if code]

            if not resolved_codes:
                raise SeriesNotFoundError(
                    f"No series found matching filters: {self._filter_state_for_error()}"
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

    def info(self, *, allow_empty: bool = False, detailed: bool = False) -> pd.DataFrame:
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
                        f"No series found matching filters: {self._filter_state_for_error()}"
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
        tickersource: Optional[TickerSource] = None,
        humanize: bool = False,
        out_of_cache: Optional[bool] = None,
        business_days: bool = False,
    ) -> pd.DataFrame:
        """Get value data for matching series.

        Args:
            params: Optional ValueQueryParams for time filtering and pagination
            tickersource: Optional ticker source override. When provided, all resolved
                series are loaded from that source. When ``None``, values are loaded
                from each series' metadata ``default_source``.
            humanize: If True, rename series_code to editorial_short_default
            out_of_cache: If provided, overrides this QuerySet's default and controls
                whether cached parquet values are bypassed in favor of direct source fetches.
            business_days: If True, drop rows where all selected series values are NaN
                after pivoting to wide format. This does not use a holiday calendar
                and does not remove rows where only some series are NaN.

        Returns:
            DataFrame with timestamp as index, series_code as columns, and values as cell values.
            Timestamps are timezone-aware UTC and sorted ascending.

        Raises:
            SeriesNotFoundError: If no series match the filters
            ValueQueryParameterError: If time parameters are invalid
        """
        resolved_series_codes = self.resolve_series_codes()

        if not resolved_series_codes:
            raise SeriesNotFoundError(
                f"No series found matching filters: {self._filter_state_for_error()}"
            )

        self._validate_time_params(params)

        effective_out_of_cache = self._out_of_cache if out_of_cache is None else out_of_cache

        if effective_out_of_cache:
            value_df = self._get_values_out_of_cache(
                resolved_series_codes,
                tickersource=tickersource,
                params=params,
            )
        elif tickersource is None:
            value_df = self._get_values_by_default_source(resolved_series_codes, params=params)
        else:
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
        if business_days:
            pivoted_df = pivoted_df.dropna(how="all")
        if humanize:
            name_map = self._get_name_map(MetadataColumns.SERIES_NAME)
            pivoted_df = pivoted_df.rename(columns=name_map)

        return pivoted_df

    def _split_derived_and_primary_codes(
        self, series_codes: List[str]
    ) -> Tuple[List[str], List[str]]:
        """Partition codes into derived (metadata_derived) vs primary catalog series."""
        derived_df = self._metadata_repository.filter(
            filters={MetadataColumns.SERIES_CODE: series_codes},
            control_type=TableNames.METADATA_DERIVED,
            exclude=False,
        )
        if derived_df.empty:
            return [], list(series_codes)

        derived_set = set(derived_df[MetadataColumns.SERIES_CODE].astype(str).str.strip())
        derived = [code for code in series_codes if str(code).strip() in derived_set]
        primary = [code for code in series_codes if str(code).strip() not in derived_set]
        return derived, primary

    def _load_primary_metadata_rows(self, filters: Dict[str, List[str]]) -> pd.DataFrame:
        return self._metadata_repository.filter(
            filters=filters,
            control_type=TableNames.METADATA,
            exclude=False,
        )

    def _load_derived_dependency_rows(self, series_codes: List[str]) -> pd.DataFrame:
        return self._metadata_repository.filter(
            filters={MetadataColumns.SERIES_CODE: series_codes},
            control_type=TableNames.METADATA_DERIVED,
            exclude=False,
        )

    def _resolve_out_of_cache_tickersource(
        self,
        series_codes: List[str],
        tickersource: Optional[TickerSource],
    ) -> TickerSource:
        if tickersource is not None:
            return tickersource
        metadata_df = self._load_primary_metadata_rows({MetadataColumns.SERIES_CODE: series_codes})
        if MetadataColumns.DEFAULT_SOURCE not in metadata_df.columns:
            return TickerSource.BLOOMBERG
        sources = (
            metadata_df[MetadataColumns.DEFAULT_SOURCE]
            .dropna()
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )
        if len(sources) == 1:
            try:
                return TickerSource(sources[0])
            except ValueError:
                pass
        return TickerSource.BLOOMBERG

    def _get_values_out_of_cache(
        self,
        resolved_series_codes: List[str],
        *,
        tickersource: Optional[TickerSource],
        params: Optional[ValueQueryParams] = None,
    ) -> pd.DataFrame:
        """Load values bypassing parquet cache; compute derived series from parent fetches."""
        derived_codes, primary_codes = self._split_derived_and_primary_codes(resolved_series_codes)

        result_frames: List[pd.DataFrame] = []

        if primary_codes:
            source = self._resolve_out_of_cache_tickersource(primary_codes, tickersource)
            result_frames.append(
                get_direct_source_values(
                    load_metadata_rows=self._load_primary_metadata_rows,
                    series_codes=primary_codes,
                    tickersource=source,
                    params=params,
                )
            )

        if derived_codes:
            parent_codes: List[str] = []
            dependencies_df = self._load_derived_dependency_rows(derived_codes)
            for _, row in dependencies_df.iterrows():
                parent_str = row.get(MetadataColumns.PARENT_SERIES_CODE, "")
                if parent_str and not pd.isna(parent_str):
                    parent_codes.extend(
                        code.strip() for code in str(parent_str).split("|") if code.strip()
                    )
            parent_codes = list(dict.fromkeys(parent_codes))
            source = self._resolve_out_of_cache_tickersource(
                parent_codes or derived_codes, tickersource
            )
            result_frames.append(
                get_derived_out_of_cache_values(
                    load_primary_metadata_rows=self._load_primary_metadata_rows,
                    load_derived_dependency_rows=self._load_derived_dependency_rows,
                    derived_series_codes=derived_codes,
                    tickersource=source,
                    params=params,
                )
            )

        if not result_frames:
            return pd.DataFrame(
                columns=[
                    ValueColumns.SERIES_CODE,
                    ValueColumns.TIMESTAMP,
                    ValueColumns.VALUE,
                ]
            )

        non_empty = [frame for frame in result_frames if not frame.empty]
        if not non_empty:
            return pd.DataFrame(
                columns=[
                    ValueColumns.SERIES_CODE,
                    ValueColumns.TIMESTAMP,
                    ValueColumns.VALUE,
                ]
            )

        return pd.concat(non_empty, ignore_index=True)

    def _get_values_by_default_source(
        self,
        resolved_series_codes: List[str],
        params: Optional[ValueQueryParams] = None,
    ) -> pd.DataFrame:
        """Load values by each series' metadata ``default_source``.

        Args:
            resolved_series_codes: Already-resolved series codes to fetch.
            params: Optional value-query params forwarded to the repository.

        Returns:
            Long-form DataFrame with ``series_code``, ``timestamp``, and ``value`` columns.

        Raises:
            ValueQueryParameterError: If metadata lacks ``default_source`` or contains
                missing / invalid ticker source values for any selected series.
        """
        metadata_df = self._load_metadata_rows(
            {MetadataColumns.SERIES_CODE: resolved_series_codes},
            exclude=False,
        )

        if MetadataColumns.DEFAULT_SOURCE not in metadata_df.columns:
            raise ValueQueryParameterError(
                "Metadata is missing required 'default_source' column for per-series value loading."
            )

        grouped_series_codes: Dict[TickerSource, List[str]] = defaultdict(list)

        for series_code in resolved_series_codes:
            matching_rows = metadata_df[
                metadata_df[MetadataColumns.SERIES_CODE].astype(str) == str(series_code)
            ]
            if matching_rows.empty:
                raise ValueQueryParameterError(
                    f"Missing metadata row for series '{series_code}' while resolving default_source."
                )

            source_value = matching_rows.iloc[0].get(MetadataColumns.DEFAULT_SOURCE)
            if pd.isna(source_value) or not str(source_value).strip():
                raise ValueQueryParameterError(
                    f"Series '{series_code}' is missing metadata default_source."
                )

            try:
                ticker_source = TickerSource(str(source_value).strip())
            except ValueError as exc:
                raise ValueQueryParameterError(
                    f"Series '{series_code}' has invalid metadata default_source "
                    f"value {source_value!r}."
                ) from exc

            grouped_series_codes[ticker_source].append(str(series_code))

        result_frames: List[pd.DataFrame] = []
        for ticker_source, series_codes in grouped_series_codes.items():
            group_df = self._value_repository.get_batch_series_data(
                series_codes=series_codes,
                tickersource=ticker_source,
                start=params.start if params else None,
                end=params.end if params else None,
                order_by=params.order_by if params else None,
                limit=params.limit if params else None,
            )
            if not group_df.empty:
                result_frames.append(group_df)

        if not result_frames:
            return pd.DataFrame(
                columns=[
                    ValueColumns.SERIES_CODE,
                    ValueColumns.TIMESTAMP,
                    ValueColumns.VALUE,
                ]
            )

        return pd.concat(result_frames, ignore_index=True)

    def filter(self, **filters: Any) -> "QuerySet":
        """Lazily add include filters while preserving the existing filter chain.

        Args:
            **filters: Keyword arguments mapping metadata column names to filter values.
                Values can be single strings or lists of strings.
                Example: country=["usa"], asset_class=["Equity"]

        Returns:
            New QuerySet instance with merged include filters

        Raises:
            InvalidFilterFieldError: If any filter field is not a valid metadata column
        """
        new_filters = self._normalize_filter_input(filters)
        return QuerySet(
            metadata_repository=self._metadata_repository,
            value_repository=self._value_repository,
            metadata_filters=self._merge_filters(self._effective_include_filters(), new_filters),
            validation_repository=self._validation_repository,
            exclude=False,
            out_of_cache=self._out_of_cache,
            control_table=self._control_table,
        ).filter_exclude(**self._exclude_filters)

    def filter_exclude(self, **filters: Any) -> "QuerySet":
        """Lazily add exclude filters while preserving existing include filters.

        Args:
            **filters: Metadata filters whose matching rows should be excluded.

        Returns:
            New QuerySet instance with merged exclude filters.
        """
        new_filters = self._normalize_filter_input(filters)
        base_queryset = QuerySet(
            metadata_repository=self._metadata_repository,
            value_repository=self._value_repository,
            metadata_filters=self._effective_include_filters(),
            validation_repository=self._validation_repository,
            exclude=False,
            out_of_cache=self._out_of_cache,
            control_table=self._control_table,
        )
        base_queryset._exclude_filters = self._merge_filters(self._exclude_filters, new_filters)
        return base_queryset

    def _options_from_dataframe(
        self,
        dataframe: pd.DataFrame,
        fields: Optional[Union[str, List[str]]] = None,
        *,
        as_dataframe: bool = False,
    ) -> Union[List[str], Dict[str, List[str]], pd.DataFrame]:
        """Return unique option values from a DataFrame."""
        return dataframe_filter_options(dataframe, fields=fields, as_dataframe=as_dataframe)

    def filter_options(
        self,
        fields: Optional[Union[str, List[str]]] = None,
        *,
        as_dataframe: bool = False,
    ) -> Union[List[str], Dict[str, List[str]], pd.DataFrame]:
        """Return contextual filter options from this QuerySet's metadata result.

        This method is lazy with respect to filter chaining: it uses the current
        QuerySet state and only resolves metadata at call time.

        Args:
            fields: Metadata field name, list of field names, or ``None`` for all fields.
            as_dataframe: When ``True``, return ``field`` / ``value`` rows.

        Returns:
            Context-specific filter options derived from ``self.info(allow_empty=True)``.
        """
        metadata_df = self.info(allow_empty=True)
        return self._options_from_dataframe(metadata_df, fields=fields, as_dataframe=as_dataframe)

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
            out_of_cache=self._out_of_cache,
            control_table=self._control_table,
        )

    def get_last_values(
        self,
        ticker_source: TickerSource = TickerSource.BLOOMBERG,
        humanize: bool = False,
        business_days: bool = True,
    ) -> pd.DataFrame:
        """Get latest (max timestamp) value for each series_code in this QuerySet.

        Args:
            ticker_source: Ticker source (default: TickerSource.BLOOMBERG)
            humanize: If True, rename series_code to series_name
            business_days: If True, drop rows where all selected series values are NaN
                after pivoting to wide format. This does not use a holiday calendar
                and does not remove rows where only some series are NaN.
        Returns:
            DataFrame with timestamp as index, series_code as columns, and values as cell values.
            Timestamps are timezone-aware UTC and sorted ascending.

        Raises:
            SeriesNotFoundError: If no series match the filters
        """
        resolved_series_codes = self.resolve_series_codes()

        if not resolved_series_codes:
            raise SeriesNotFoundError(
                f"No series found matching filters: {self._filter_state_for_error()}"
            )

        result_df = self._value_repository.get_last_values(
            resolved_series_codes,
            ticker_source,
            latest_non_null=business_days,
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
        if business_days:
            pivoted_df = pivoted_df.dropna(how="all")
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
        business_days: bool = True,
    ) -> pd.DataFrame:
        """Get all values for all series in this QuerySet (optionally filtered by ticker_source).

        Args:
            ticker_source: Optional ticker source override. When provided, all resolved
                series are loaded from that source. When ``None``, values are loaded
                from each series' metadata ``default_source``.
            humanize: If True, rename series_code to series_name
            business_days: If True, drop rows where all selected series values are NaN
                after pivoting to wide format. This does not use a holiday calendar
                and does not remove rows where only some series are NaN.
        Returns:
            DataFrame with timestamp as index, series_code as columns, and values as cell values.
            Timestamps are timezone-aware UTC and sorted ascending.
        """
        resolved_series_codes = self.resolve_series_codes()

        if not resolved_series_codes:
            return pd.DataFrame()

        if ticker_source is None:
            result_df = self._get_values_by_default_source(resolved_series_codes)
        else:
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
        if business_days:
            pivoted_df = pivoted_df.dropna(how="all")

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
                group_df[MetadataColumns.SERIES_CODE].dropna().astype(str).unique().tolist()
            )

            yield (
                group_values,
                QuerySet(
                    metadata_repository=self._metadata_repository,
                    value_repository=self._value_repository,
                    metadata_filters=None,
                    validation_repository=self._validation_repository,
                    exclude=False,
                    series_codes=series_codes,
                    out_of_cache=self._out_of_cache,
                    control_table=self._control_table,
                ),
            )
