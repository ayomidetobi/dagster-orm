"""Value repository: load series data from wide monthly Parquet partitions only."""

from collections import defaultdict
from datetime import datetime, timezone
import logging
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from dagster_quickstart.orm.domain.metadata_repository import MetadataRepository
from dagster_quickstart.orm.domain.validation_repository import ValidationRepository
from dagster_quickstart.orm.exceptions import MetadataResolutionError
from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.parquet_adapter import ParquetAdapter
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.schema import (
    MetadataColumns,
    TableNames,
    TickerSource,
    ValueColumns,
    get_storage_field_column,
    ticker_source_uses_wide_storage,
)
from dagster_quickstart.utils.datetime_utils import (
    ensure_utc,
    iter_year_months,
    normalize_date_to_utc,
    normalize_pandas_timestamp_to_utc,
    utc_now,
)
from dagster_quickstart.utils.pandas_wide import select_series_columns_as_long_df


logger = logging.getLogger(__name__)


def _is_missing_parquet_partition_error(exc: Exception) -> bool:
    """Return True when DuckDB could not find a parquet partition."""
    message = str(exc).lower()
    return any(
        needle in message
        for needle in (
            "no files found that match the pattern",
            "could not open file",
            "no such file",
            "not found",
            "cannot open file",
            "failed to open",
        )
    )



class ValueRepository:
    """Repository for loading value data from parquet files.

    Bloomberg and MDS values are read from wide monthly partitions under
    ``value-data/wide/{source}/field_type=.../year=.../month=.../``.
    Series defined only in ``metadata_derived`` (empty vendor field, ``calc_type`` set) use that
    value as the wide ``field_type`` partition (aligned with derived asset materialization).
    Internal ticker source uses the ``calc_type`` column as the wide partition key when set.
    """

    def __init__(
        self,
        duckdb_repository: DuckDbRepository,
        parquet_adapter: ParquetAdapter,
        s3_adapter: S3Adapter,
        validation_repository: Optional[ValidationRepository] = None,
        metadata_repository: Optional[MetadataRepository] = None,
    ):
        """Initialize value repository.

        Args:
            duckdb_repository: DuckDbRepository for executing queries
            parquet_adapter: ParquetAdapter for building parquet sources
            s3_adapter: S3Adapter for URI resolution
            validation_repository: Used to resolve ``bbg_field`` / ``mds_field`` for wide reads
            metadata_repository: Used to resolve ``calc_type`` for series in ``metadata_derived`` only
        """
        self._repository = duckdb_repository
        self._parquet_adapter = parquet_adapter
        self._s3_adapter = s3_adapter
        self._validation_repository = validation_repository
        self._metadata_repository = metadata_repository
        self._wide_schema_cache: Dict[str, set[str]] = {}

    @staticmethod
    def _uses_wide_storage(tickersource: TickerSource) -> bool:
        return ticker_source_uses_wide_storage(tickersource)

    @staticmethod
    def _vendor_field_column(tickersource: TickerSource) -> str:
        return get_storage_field_column(tickersource)

    @staticmethod
    def _wide_partition_key_from_derived_metadata(row: pd.Series) -> Optional[str]:
        """Map a metadata row to wide ``field_type`` from ``MetadataColumns.CALC_TYPE``."""
        calc_type = row.get(MetadataColumns.CALC_TYPE)
        if calc_type is None or pd.isna(calc_type):
            return None
        ck = str(calc_type).strip().upper()
        return ck if ck else None

    def _merge_wide_partition_keys_from_derived(
        self, out: Dict[str, str], codes: List[str]
    ) -> None:
        """Fill partition keys for series that exist only on ``metadata_derived``."""
        if self._metadata_repository is None:
            return
        missing = [c for c in codes if c not in out]
        if not missing:
            return
        derived_df = self._metadata_repository.filter(
            filters={MetadataColumns.SERIES_CODE: missing},
            control_type=TableNames.METADATA_DERIVED,
            exclude=False,
            allow_empty=True,
        )
        if derived_df.empty:
            return
        for _, row in derived_df.iterrows():
            sc = row.get(MetadataColumns.SERIES_CODE)
            if sc is None or pd.isna(sc):
                continue
            sc_str = str(sc).strip()
            pk = self._wide_partition_key_from_derived_metadata(row)
            if pk:
                out[sc_str] = pk

    def _resolve_vendor_field_map(
        self, series_codes: List[str], tickersource: TickerSource
    ) -> Dict[str, str]:
        """Map each series_code to wide storage ``field_type`` (vendor field or ``calc_type``)."""
        out: Dict[str, str] = {}
        if not series_codes or not self._uses_wide_storage(tickersource):
            return out
        codes = list(dict.fromkeys(series_codes))
        col = self._vendor_field_column(tickersource)

        if self._validation_repository is not None:
            df = self._validation_repository.filter_with_validation(
                filters={MetadataColumns.SERIES_CODE: codes}
            )
            if not df.empty and col in df.columns:
                for _, row in df.iterrows():
                    sc = row.get(MetadataColumns.SERIES_CODE)
                    v = row.get(col)
                    if sc is None or pd.isna(sc):
                        continue
                    sc_str = str(sc).strip()
                    if pd.isna(v) or not str(v).strip():
                        pk = self._wide_partition_key_from_derived_metadata(row)
                        if pk:
                            out[sc_str] = pk
                        continue
                    out[sc_str] = str(v).strip()

        self._merge_wide_partition_keys_from_derived(out, codes)
        return out

    def _read_wide_parquet_parts_to_df(self, uris: List[str]) -> pd.DataFrame:
        parts: List[pd.DataFrame] = []
        for uri in uris:
            try:
                esc = uri.replace("'", "''")
                sql = f"SELECT * FROM read_parquet('{esc}')"
                part = self._repository.execute_raw_sql(sql)
                if not part.empty:
                    parts.append(part)
            except Exception:
                continue
        if not parts:
            return pd.DataFrame()
        return pd.concat(parts, ignore_index=True)

    def _cache_wide_schema_columns(self, uri: str, columns: List[str]) -> List[str]:
        """Cache parquet columns for a URI and return the cached set as a sorted list."""
        cached = {col for col in columns if col}
        self._wide_schema_cache[uri] = cached
        return sorted(cached)

    def _parquet_schema_column_names(self, uri: str) -> List[str]:
        """Return parquet column names for one URI using DuckDB schema discovery."""
        if uri in self._wide_schema_cache:
            return sorted(self._wide_schema_cache[uri])

        esc = uri.replace("'", "''")
        try:
            schema_df = self._repository.execute_raw_sql(
                f"SELECT * FROM parquet_schema('{esc}')"
            )
        except Exception as exc:
            if _is_missing_parquet_partition_error(exc):
                return []
            return []

        if schema_df.empty:
            self._wide_schema_cache[uri] = set()
            return []

        for candidate in ("name", "column_name", "column", "field_name"):
            if candidate in schema_df.columns:
                names = (
                    schema_df[candidate]
                    .dropna()
                    .astype(str)
                    .map(str.strip)
                    .tolist()
                )
                return self._cache_wide_schema_columns(uri, [name for name in names if name])

        return self._cache_wide_schema_columns(
            uri, [str(col).strip() for col in schema_df.columns if str(col).strip()]
        )

    def _resolve_existing_wide_columns(
        self,
        uris: List[str],
        requested_columns: List[str],
    ) -> List[str]:
        """Return requested columns that exist in at least one parquet URI, preserving order."""
        requested = list(dict.fromkeys(requested_columns))
        if not uris or not requested:
            return []

        existing: List[str] = []
        seen = set()
        for uri in uris:
            uri_columns = set(self._parquet_schema_column_names(uri))
            if not uri_columns:
                continue
            for column in requested:
                if column in uri_columns and column not in seen:
                    existing.append(column)
                    seen.add(column)
        return existing

    def debug_wide_partition_schema(
        self,
        field_type: str,
        year: int,
        month: int,
        tickersource: TickerSource = TickerSource.BLOOMBERG,
    ) -> pd.DataFrame:
        """Debug helper for inspecting a wide parquet partition schema."""
        uri = self._s3_adapter.get_wide_value_partition_uri(field_type, year, month, tickersource)
        esc = uri.replace("'", "''")
        sql = f"SELECT * FROM parquet_schema('{esc}')"
        logger.debug(
            "wide partition schema debug: field_type=%s year=%s month=%s uri=%s",
            field_type,
            year,
            month,
            uri,
        )
        return self._repository.execute_raw_sql(sql)

    @staticmethod
    def _is_missing_wide_column_error(exc: Exception) -> bool:
        """Return True when DuckDB rejected a projection because a column was missing."""
        message = str(exc).lower()
        return any(
            needle in message
            for needle in (
                "column not found",
                "referenced column",
                "does not have a column named",
                "binder error",
                "unknown column",
                "no column named",
            )
        )

    @staticmethod
    def _is_date_only_bound(value: Optional[str]) -> bool:
        """Return True when a bound string appears to be date-only."""
        if value is None:
            return False
        if not isinstance(value, str):
            return False
        stripped = value.strip()
        return bool(
            stripped
            and (
                stripped.isdigit() and len(stripped) == 8
                or (
                    len(stripped) == 10
                    and stripped[4] == "-"
                    and stripped[7] == "-"
                    and "T" not in stripped
                    and ":" not in stripped
                )
            )
        )

    def _build_wide_source_expr(self, uris: List[str]) -> str:
        """Build a DuckDB parquet source expression for one or more URIs."""
        if len(uris) == 1:
            return f"read_parquet({self._quote_sql_literal(uris[0])})"
        return (
            f"read_parquet([{', '.join(self._quote_sql_literal(uri) for uri in uris)}], "
            "union_by_name=true)"
        )

    def _read_wide_selected_columns_to_df(
        self,
        uris: List[str],
        series_codes: List[str],
        start: Optional[str],
        end: Optional[str],
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Read only projected wide columns from one or more parquet URIs."""
        requested_series = list(dict.fromkeys(series_codes))
        if not uris or not requested_series:
            return pd.DataFrame()

        timestamp_col = ValueColumns.TIMESTAMP
        source_expr = self._build_wide_source_expr(uris)
        projected_columns = [timestamp_col] + requested_series
        select_sql = ", ".join(self._quote_identifier(col) for col in projected_columns)

        sql = f"SELECT {select_sql} FROM {source_expr}"
        order_column = order_by or timestamp_col
        sql += f" ORDER BY {self._quote_identifier(order_column)}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"

        params: List[object] = []

        logger.debug(
            "wide value read attempt: uris=%s first_uri=%s requested=%s selected=%s params=%s",
            len(uris),
            uris[0] if uris else None,
            requested_series,
            projected_columns,
            params,
        )

        try:
            wide_df = self._repository.execute_raw_sql(sql, params or None)
        except Exception as exc:
            if _is_missing_parquet_partition_error(exc):
                logger.debug(
                    "wide value read skipped missing partition: uri_count=%s requested_columns=%s returned_shape=%s",
                    len(uris),
                    len(requested_series),
                    None,
                )
                return pd.DataFrame()
            if not self._is_missing_wide_column_error(exc):
                logger.debug("wide value read failed without missing-column fallback: %s", exc)
                raise

            available_series = self._resolve_existing_wide_columns(uris, requested_series)
            if not available_series:
                logger.warning(
                    "No requested series columns found in wide parquet. Requested: %s",
                    requested_series,
                )
                logger.debug(
                    "wide value read skipped missing columns: uri_count=%s requested_columns=%s returned_shape=%s",
                    len(uris),
                    len(requested_series),
                    None,
                )
                return pd.DataFrame()

            fallback_source_expr = self._build_wide_source_expr(uris)
            fallback_columns = [timestamp_col] + available_series
            fallback_select_sql = ", ".join(
                self._quote_identifier(col) for col in fallback_columns
            )
            fallback_sql = f"SELECT {fallback_select_sql} FROM {fallback_source_expr}"
            fallback_sql += f" ORDER BY {self._quote_identifier(order_column)}"
            if limit is not None:
                fallback_sql += f" LIMIT {int(limit)}"

            logger.debug(
                "wide value read fallback: schema_columns=%s matched=%s selected=%s",
                len(available_series),
                available_series,
                fallback_columns,
            )

            try:
                wide_df = self._repository.execute_raw_sql(fallback_sql, params or None)
            except Exception as fallback_exc:
                if _is_missing_parquet_partition_error(fallback_exc):
                    logger.debug("wide value read fallback hit missing partition: %s", fallback_exc)
                    return pd.DataFrame()
                raise

        logger.debug(
            "wide value read result shape before normalization: %s",
            getattr(wide_df, "shape", None),
        )
        logger.debug(
            "wide value read result columns before normalization: %s",
            list(getattr(wide_df, "columns", [])),
        )

        if len(wide_df.columns) == 0:
            logger.debug(
                "wide value read returned no rows: uri_count=%s requested_columns=%s returned_shape=%s",
                len(uris),
                len(requested_series),
                getattr(wide_df, "shape", None),
            )
            return pd.DataFrame()

        if timestamp_col not in wide_df.columns:
            raise MetadataResolutionError(
                f"Wide parquet data is missing required column '{timestamp_col}'"
            )

        wide_df = normalize_pandas_timestamp_to_utc(wide_df, timestamp_col)
        wide_df = wide_df.sort_values(timestamp_col).drop_duplicates(
            subset=[timestamp_col], keep="last"
        )
        wide_df = wide_df.set_index(timestamp_col)
        wide_df.index.name = timestamp_col
        logger.debug(
            "wide value read final shape=%s final_columns=%s",
            wide_df.shape,
            list(wide_df.columns),
        )

        if wide_df.empty and logger.isEnabledFor(logging.DEBUG):
            try:
                debug_sql = (
                    f'SELECT MIN({self._quote_identifier(timestamp_col)}) AS min_ts, '
                    f'MAX({self._quote_identifier(timestamp_col)}) AS max_ts, '
                    "COUNT(*) AS row_count "
                    f"FROM {source_expr}"
                )
                debug_stats = self._repository.execute_raw_sql(debug_sql)
                logger.debug(
                    "wide value debug stats uri=%s stats=%s",
                    uris[0] if uris else None,
                    debug_stats.to_dict("records") if not debug_stats.empty else [],
                )
            except Exception as exc:
                logger.debug("wide value debug stats query failed: %s", exc)

        return wide_df

    def _read_wide_as_wide(
        self,
        series_codes: List[str],
        vendor_field: str,
        tickersource: TickerSource,
        start: Optional[str],
        end: Optional[str],
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Read wide parquet data and return a timestamp-indexed wide DataFrame."""
        empty = pd.DataFrame()
        if not series_codes:
            return empty

        codes = list(dict.fromkeys(series_codes))
        requested_column_count = len(codes)
        bounded = start is not None or end is not None
        logger.debug(
            "get_batch_series_data_wide reading field_type=%s requested=%s bounded=%s start=%s end=%s order_by=%s limit=%s",
            vendor_field,
            codes,
            bounded,
            start,
            end,
            order_by,
            limit,
        )

        if bounded:
            if start is not None:
                s_dt = normalize_date_to_utc(start).to_pydatetime()
            else:
                s_dt = datetime(2000, 1, 1, tzinfo=timezone.utc)
            if end is not None:
                e_dt = normalize_date_to_utc(end).to_pydatetime()
            else:
                e_dt = ensure_utc(utc_now())

            months = iter_year_months(s_dt, e_dt)
            uris = [
                self._s3_adapter.get_wide_value_partition_uri(vendor_field, y, m, tickersource)
                for y, m in months
            ]
            uri_count = len(uris)
            logger.debug(
                "get_batch_series_data_wide URIs for field_type=%s: count=%s first_uri=%s",
                vendor_field,
                uri_count,
                uris[0] if uris else None,
            )
            wide_df = self._read_wide_selected_columns_to_df(
                uris=uris,
                series_codes=codes,
                start=start,
                end=end,
                order_by=order_by,
                limit=limit,
            )
        else:
            glob_uri = self._s3_adapter.get_wide_field_glob_uri(vendor_field, tickersource)
            logger.debug(
                "get_batch_series_data_wide glob URI for field_type=%s: %s",
                vendor_field,
                glob_uri,
            )
            uri_count = 1
            wide_df = self._read_wide_selected_columns_to_df(
                uris=[glob_uri],
                series_codes=codes,
                start=start,
                end=end,
                order_by=order_by,
                limit=limit,
            )

        if wide_df.empty:
            logger.debug(
                "get_batch_series_data_wide skipped empty part: field_type=%s uri_count=%s requested_columns=%s returned_shape=%s",
                vendor_field,
                uri_count,
                requested_column_count,
                wide_df.shape,
            )
        return wide_df

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Quote a DuckDB identifier."""
        return '"' + identifier.replace('"', '""') + '"'

    @staticmethod
    def _quote_sql_literal(value: str) -> str:
        """Quote a SQL string literal."""
        return "'" + value.replace("'", "''") + "'"

    def _month_iter_desc(
        self,
        start_year_month: Optional[Tuple[int, int]] = None,
        max_lookback_months: int = 2400,
    ) -> Iterable[Tuple[int, int]]:
        """Yield year/month pairs from newest to oldest.

        Args:
            start_year_month: Optional starting (year, month). Defaults to current UTC month.
            max_lookback_months: Maximum number of months to yield.
        """
        if max_lookback_months <= 0:
            return

        if start_year_month is None:
            now = ensure_utc(utc_now())
            year, month = now.year, now.month
        else:
            year, month = start_year_month

        for _ in range(max_lookback_months):
            yield year, month
            month -= 1
            if month < 1:
                month = 12
                year -= 1

    def _read_wide_month_selected_columns(
        self,
        vendor_field: str,
        year: int,
        month: int,
        tickersource: TickerSource,
        series_codes: List[str],
    ) -> pd.DataFrame:
        """Read one monthly wide parquet partition with only the requested series columns."""
        empty = pd.DataFrame(columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE])
        if not series_codes:
            return empty

        uri = self._s3_adapter.get_wide_value_partition_uri(vendor_field, year, month, tickersource)
        esc_uri = uri.replace("'", "''")

        try:
            schema_df = self._repository.execute_raw_sql(
                f"SELECT * FROM read_parquet('{esc_uri}') LIMIT 0"
            )
        except Exception as exc:
            if _is_missing_parquet_partition_error(exc):
                return empty
            raise

        if ValueColumns.TIMESTAMP not in schema_df.columns:
            raise MetadataResolutionError(
                f"Wide parquet partition is missing required column '{ValueColumns.TIMESTAMP}'"
            )

        available_columns = set(schema_df.columns)
        selected_series = [sc for sc in series_codes if sc in available_columns]
        if not selected_series:
            return empty

        selected_columns = [ValueColumns.TIMESTAMP] + selected_series
        select_sql = ", ".join(self._quote_identifier(col) for col in selected_columns)

        try:
            wide_df = self._repository.execute_raw_sql(
                f"SELECT {select_sql} FROM read_parquet('{esc_uri}')"
            )
        except Exception as exc:
            if _is_missing_parquet_partition_error(exc):
                return empty
            raise

        if wide_df.empty:
            return empty

        wide_df = normalize_pandas_timestamp_to_utc(wide_df, ValueColumns.TIMESTAMP)
        return wide_df

    @staticmethod
    def _latest_values_from_wide_df(
        wide_df: pd.DataFrame,
        series_codes: List[str],
        latest_non_null: bool,
    ) -> Dict[str, Tuple[pd.Timestamp, object]]:
        """Extract latest values for the requested series from one wide month frame."""
        results: Dict[str, Tuple[pd.Timestamp, object]] = {}
        if wide_df.empty or not series_codes:
            return results

        timestamp_col = ValueColumns.TIMESTAMP
        last_row = wide_df.iloc[-1]
        last_timestamp = last_row[timestamp_col]

        for series_code in series_codes:
            if series_code not in wide_df.columns:
                continue

            if latest_non_null:
                last_valid_index = wide_df[series_code].last_valid_index()
                if last_valid_index is None:
                    continue
                results[series_code] = (
                    wide_df.loc[last_valid_index, timestamp_col],
                    wide_df.loc[last_valid_index, series_code],
                )
            else:
                results[series_code] = (last_timestamp, last_row[series_code])

        return results

    def _get_last_values_for_vendor_field(
        self,
        series_codes: List[str],
        vendor_field: str,
        tickersource: TickerSource,
        latest_non_null: bool,
    ) -> pd.DataFrame:
        """Fast path for one vendor field using month-by-month reverse scanning."""
        remaining = list(dict.fromkeys(series_codes))
        found_rows: List[Dict[str, object]] = []

        for year, month in self._month_iter_desc():
            if not remaining:
                break

            month_df = self._read_wide_month_selected_columns(
                vendor_field=vendor_field,
                year=year,
                month=month,
                tickersource=tickersource,
                series_codes=remaining,
            )
            if month_df.empty:
                continue

            latest_rows = self._latest_values_from_wide_df(
                month_df,
                remaining,
                latest_non_null=latest_non_null,
            )
            if not latest_rows:
                continue

            for series_code, (timestamp, value) in latest_rows.items():
                found_rows.append(
                    {
                        ValueColumns.SERIES_CODE: series_code,
                        ValueColumns.TIMESTAMP: timestamp,
                        ValueColumns.VALUE: value,
                    }
                )
            found_series = set(latest_rows.keys())
            remaining = [series_code for series_code in remaining if series_code not in found_series]

        if not found_rows:
            return pd.DataFrame(
                columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
            )

        return pd.DataFrame(found_rows)

    def _read_wide_as_long(
        self,
        series_codes: List[str],
        vendor_field: str,
        tickersource: TickerSource,
        start: Optional[str],
        end: Optional[str],
    ) -> pd.DataFrame:
        empty = pd.DataFrame(
            columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
        )
        if not series_codes:
            return empty

        codes = list(dict.fromkeys(series_codes))
        bounded = start is not None or end is not None

        if bounded:
            if start is not None and end is not None:
                s_dt = normalize_date_to_utc(start).to_pydatetime()
                e_dt = normalize_date_to_utc(end).to_pydatetime()
            elif start is not None:
                s_dt = normalize_date_to_utc(start).to_pydatetime()
                e_dt = ensure_utc(utc_now())
            else:
                e_dt = normalize_date_to_utc(end).to_pydatetime()
                s_dt = datetime(2000, 1, 1, tzinfo=timezone.utc)

            months = iter_year_months(s_dt, e_dt)
            uris = [
                self._s3_adapter.get_wide_value_partition_uri(vendor_field, y, m, tickersource)
                for y, m in months
            ]
            wide_df = self._read_wide_parquet_parts_to_df(uris)
        else:
            glob_uri = self._s3_adapter.get_wide_field_glob_uri(vendor_field, tickersource)
            esc = glob_uri.replace("'", "''")
            try:
                wide_df = self._repository.execute_raw_sql(
                    f"SELECT * FROM read_parquet('{esc}')"
                )
            except Exception:
                wide_df = pd.DataFrame()

        if wide_df.empty:
            return empty

        return select_series_columns_as_long_df(wide_df, codes, start=start, end=end)

    def get_batch_series_data_wide(
        self,
        series_codes: List[str],
        tickersource: TickerSource = TickerSource.BLOOMBERG,
        start: Optional[str] = None,
        end: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Load value data for multiple series_codes as a wide DataFrame."""
        if not series_codes:
            return pd.DataFrame()

        if not self._uses_wide_storage(tickersource):
            return pd.DataFrame()

        try:
            logger.debug(
                "get_batch_series_data_wide input: series_codes=%s tickersource=%s start=%s end=%s order_by=%s limit=%s",
                series_codes,
                tickersource,
                start,
                end,
                order_by,
                limit,
            )
            fmap = self._resolve_vendor_field_map(series_codes, tickersource)
            logger.debug("get_batch_series_data_wide field map: %s", fmap)
            by_field: Dict[str, List[str]] = defaultdict(list)
            for sc in series_codes:
                vf = fmap.get(sc)
                if vf:
                    by_field[vf].append(sc)
            logger.debug("get_batch_series_data_wide grouped by_field: %s", dict(by_field))

            parts: List[pd.DataFrame] = []
            for vf, codes in by_field.items():
                logger.debug("get_batch_series_data_wide reading field_type=%s", vf)
                part_df = self._read_wide_as_wide(
                    codes,
                    vf,
                    tickersource,
                    start,
                    end,
                    order_by=order_by,
                    limit=limit,
                )
                logger.debug(
                    "get_batch_series_data_wide field_type=%s returned shape=%s columns=%s",
                    vf,
                    part_df.shape,
                    list(part_df.columns),
                )
                if not part_df.empty:
                    parts.append(part_df)

            if not parts:
                logger.warning(
                    "No requested series columns found in wide parquet. Requested: %s",
                    series_codes,
                )
                empty = pd.DataFrame()
                empty.index.name = ValueColumns.TIMESTAMP
                return empty

            combined = pd.concat(parts, axis=1, join="outer")
            combined = combined.sort_index()
            combined.index.name = ValueColumns.TIMESTAMP
            logger.debug(
                "get_batch_series_data_wide combined shape=%s columns=%s",
                combined.shape,
                list(combined.columns),
            )
            return combined
        except Exception as exc:
            raise MetadataResolutionError(f"Failed to load wide value data batch: {exc}") from exc

    def _finalize_long_df(
        self,
        long_df: pd.DataFrame,
        order_by: Optional[str],
        limit: Optional[int],
    ) -> pd.DataFrame:
        if long_df.empty:
            return long_df
        ob = order_by or ValueColumns.TIMESTAMP
        long_df = long_df.sort_values(ob, ascending=True).reset_index(drop=True)
        if limit is not None and limit > 0:
            long_df = long_df.head(limit).reset_index(drop=True)
        return long_df

    def get_series_data(
        self,
        series_code: str,
        tickersource: TickerSource = TickerSource.BLOOMBERG,
        start: Optional[str] = None,
        end: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Load value data for a single series_code."""
        if not self._uses_wide_storage(tickersource):
            return pd.DataFrame(
                columns=[
                    ValueColumns.SERIES_CODE,
                    ValueColumns.TIMESTAMP,
                    ValueColumns.VALUE,
                ]
            )
        try:
            fmap = self._resolve_vendor_field_map([series_code], tickersource)
            vf = fmap.get(series_code)
            if not vf:
                return pd.DataFrame(
                    columns=[
                        ValueColumns.SERIES_CODE,
                        ValueColumns.TIMESTAMP,
                        ValueColumns.VALUE,
                    ]
                )
            long_df = self._read_wide_as_long([series_code], vf, tickersource, start, end)
            return self._finalize_long_df(long_df, order_by, limit)
        except Exception:
            return pd.DataFrame(
                columns=[
                    ValueColumns.SERIES_CODE,
                    ValueColumns.TIMESTAMP,
                    ValueColumns.VALUE,
                ]
            )

    def get_batch_series_data(
        self,
        series_codes: List[str],
        tickersource: TickerSource = TickerSource.BLOOMBERG,
        start: Optional[str] = None,
        end: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Load value data for multiple series_codes."""
        if not series_codes:
            return pd.DataFrame(
                columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
            )

        if not self._uses_wide_storage(tickersource):
            return pd.DataFrame(
                columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
            )

        try:
            fmap = self._resolve_vendor_field_map(series_codes, tickersource)
            by_field: Dict[str, List[str]] = defaultdict(list)
            for sc in series_codes:
                vf = fmap.get(sc)
                if vf:
                    by_field[vf].append(sc)
            parts: List[pd.DataFrame] = []
            for vf, codes in by_field.items():
                parts.append(self._read_wide_as_long(codes, vf, tickersource, start, end))
            if not parts:
                return pd.DataFrame(
                    columns=[
                        ValueColumns.SERIES_CODE,
                        ValueColumns.TIMESTAMP,
                        ValueColumns.VALUE,
                    ]
                )
            out = pd.concat(parts, ignore_index=True)
            return self._finalize_long_df(out, order_by, limit)
        except Exception as exc:
            raise MetadataResolutionError(f"Failed to load value data batch: {exc}") from exc

    def get_last_values(
        self,
        series_codes: List[str],
        tickersource: TickerSource = TickerSource.BLOOMBERG,
        latest_non_null: bool = True,
    ) -> pd.DataFrame:
        """Latest row per series_code.

        Args:
            series_codes: Series identifiers to load.
            tickersource: Value source to read from.
            latest_non_null: If True, ignore NaN values when selecting the latest
                value per series.
        """
        if not series_codes:
            return pd.DataFrame(
                columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
            )

        if not self._uses_wide_storage(tickersource):
            return pd.DataFrame(
                columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
            )

        try:
            fmap = self._resolve_vendor_field_map(series_codes, tickersource)
            if not fmap:
                return pd.DataFrame(
                    columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
                )

            by_field: Dict[str, List[str]] = defaultdict(list)
            for series_code in series_codes:
                vendor_field = fmap.get(series_code)
                if vendor_field:
                    by_field[vendor_field].append(series_code)

            parts: List[pd.DataFrame] = []
            for vendor_field, field_series_codes in by_field.items():
                part_df = self._get_last_values_for_vendor_field(
                    series_codes=field_series_codes,
                    vendor_field=vendor_field,
                    tickersource=tickersource,
                    latest_non_null=latest_non_null,
                )
                if not part_df.empty:
                    parts.append(part_df)

            if not parts:
                return pd.DataFrame(
                    columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
                )

            return (
                pd.concat(parts, ignore_index=True)
                .sort_values(ValueColumns.SERIES_CODE, ascending=True)
                .reset_index(drop=True)
            )
        except Exception as exc:
            raise MetadataResolutionError(f"Failed to load last values: {exc}") from exc

    def get_all_values(
        self, series_codes: List[str], tickersource: TickerSource = TickerSource.BLOOMBERG
    ) -> pd.DataFrame:
        """All values for the given series."""
        return self.get_batch_series_data(
            series_codes=series_codes,
            tickersource=tickersource,
            start=None,
            end=None,
            order_by=None,
            limit=None,
        )
