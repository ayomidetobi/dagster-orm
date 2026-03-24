"""Value repository for loading value data from parquet files."""

from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
from duckdb_tinyorm_py import QueryBuilder

from dagster_quickstart.orm.domain.validation_repository import ValidationRepository
from dagster_quickstart.orm.exceptions import MetadataResolutionError
from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.parquet_adapter import ParquetAdapter
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.schema import MetadataColumns, TickerSource, ValueColumns
from dagster_quickstart.orm.wide_value_storage import (
    iter_year_months,
    wide_table_to_long,
)
from dagster_quickstart.utils.datetime_utils import ensure_utc, normalize_date_to_utc, utc_now


_WIDE_TICKER_SOURCES = frozenset({TickerSource.BLOOMBERG, TickerSource.MDS})


class ValueRepository:
    """Repository for loading value data from parquet files.

    Bloomberg and MDS values are read from wide monthly partitions under
    ``value-data/wide/{source}/field_type=.../year=.../month=.../``.
    Other sources (e.g. INTERNAL derived series) use legacy per-series long Parquet paths.
    """

    def __init__(
        self,
        duckdb_repository: DuckDbRepository,
        parquet_adapter: ParquetAdapter,
        s3_adapter: S3Adapter,
        validation_repository: Optional[ValidationRepository] = None,
    ):
        """Initialize value repository.

        Args:
            duckdb_repository: DuckDbRepository for executing queries
            parquet_adapter: ParquetAdapter for building parquet sources
            s3_adapter: S3Adapter for URI resolution
            validation_repository: Used to resolve ``bbg_field`` / ``mds_field`` for wide reads
        """
        self._repository = duckdb_repository
        self._parquet_adapter = parquet_adapter
        self._s3_adapter = s3_adapter
        self._validation_repository = validation_repository

    @staticmethod
    def _uses_wide_storage(tickersource: TickerSource) -> bool:
        return tickersource in _WIDE_TICKER_SOURCES

    @staticmethod
    def _vendor_field_column(tickersource: TickerSource) -> str:
        if tickersource == TickerSource.BLOOMBERG:
            return MetadataColumns.BBG_FIELD
        if tickersource == TickerSource.MDS:
            return MetadataColumns.MDS_FIELD
        raise ValueError(f"No vendor field column for ticker source {tickersource!r}")

    def get_vendor_field_map(
        self, series_codes: List[str], tickersource: TickerSource
    ) -> Dict[str, str]:
        """Public: series_code → vendor field partition key (``bbg_field`` / ``mds_field``)."""
        return self._resolve_vendor_field_map(series_codes, tickersource)

    def _resolve_vendor_field_map(
        self, series_codes: List[str], tickersource: TickerSource
    ) -> Dict[str, str]:
        """Map series_code → vendor field code (PX_LAST, YIELD, …) from validated metadata."""
        out: Dict[str, str] = {}
        if (
            not series_codes
            or self._validation_repository is None
            or not self._uses_wide_storage(tickersource)
        ):
            return out
        col = self._vendor_field_column(tickersource)
        df = self._validation_repository.filter_with_validation(
            filters={MetadataColumns.SERIES_CODE: list(dict.fromkeys(series_codes))}
        )
        if df.empty or col not in df.columns:
            return out
        for _, row in df.iterrows():
            sc = row.get(MetadataColumns.SERIES_CODE)
            v = row.get(col)
            if sc is None or pd.isna(sc):
                continue
            if pd.isna(v) or not str(v).strip():
                continue
            out[str(sc).strip()] = str(v).strip()
        return out

    def _read_wide_parquet_parts_to_df(self, uris: List[str]) -> pd.DataFrame:
        """Load and concatenate Parquet files; skip missing or failing objects."""
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

    def _read_wide_combined_long(
        self,
        series_codes: List[str],
        vendor_field: str,
        tickersource: TickerSource,
        start: Optional[str],
        end: Optional[str],
    ) -> pd.DataFrame:
        """Read wide layout for one vendor field partition set; return long rows for series_codes."""
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

        return wide_table_to_long(wide_df, codes, start=start, end=end)

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
        if self._uses_wide_storage(tickersource):
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
                long_df = self._read_wide_combined_long(
                    [series_code], vf, tickersource, start, end
                )
                return self._finalize_long_df(long_df, order_by, limit)
            except Exception:
                return pd.DataFrame(
                    columns=[
                        ValueColumns.SERIES_CODE,
                        ValueColumns.TIMESTAMP,
                        ValueColumns.VALUE,
                    ]
                )

        try:
            uri = self._s3_adapter.get_value_data_uri(series_code, tickersource)
            sql, params = self._build_series_query_sql(
                series_code, uri, start, end, order_by, limit
            )

            if params:
                result = self._repository.execute_raw_sql(sql, params)
            else:
                result = self._repository.execute_raw_sql(sql)

            return result

        except Exception:
            return pd.DataFrame(
                columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
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

        if self._uses_wide_storage(tickersource):
            try:
                fmap = self._resolve_vendor_field_map(series_codes, tickersource)
                by_field: Dict[str, List[str]] = defaultdict(list)
                for sc in series_codes:
                    vf = fmap.get(sc)
                    if vf:
                        by_field[vf].append(sc)
                parts: List[pd.DataFrame] = []
                for vf, codes in by_field.items():
                    parts.append(
                        self._read_wide_combined_long(
                            codes, vf, tickersource, start, end
                        )
                    )
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

        try:
            union_parts = []
            all_params: List = []

            for series_code in series_codes:
                try:
                    uri = self._s3_adapter.get_value_data_uri(series_code, tickersource)
                    sql, params = self._build_series_query_sql(
                        series_code, uri, start, end, None, None
                    )

                    union_parts.append(f"({sql})")
                    if params:
                        all_params.extend(params)

                except Exception:
                    continue

            if not union_parts:
                return pd.DataFrame(
                    columns=[
                        ValueColumns.SERIES_CODE,
                        ValueColumns.TIMESTAMP,
                        ValueColumns.VALUE,
                    ]
                )

            order_by_column = order_by or ValueColumns.TIMESTAMP
            order_by_clause = f" ORDER BY {order_by_column}"
            limit_clause = f" LIMIT {limit}" if limit else ""

            union_query = f"""
                SELECT * FROM (
                    {' UNION ALL '.join(union_parts)}
                )
                {order_by_clause}
                {limit_clause}
            """

            if all_params:
                result = self._repository.execute_raw_sql(union_query, all_params)
            else:
                result = self._repository.execute_raw_sql(union_query)

            return result

        except Exception as exc:
            raise MetadataResolutionError(f"Failed to load value data batch: {exc}") from exc

    def _build_series_query_sql(
        self,
        series_code: str,
        uri: str,
        start: Optional[str],
        end: Optional[str],
        order_by: Optional[str],
        limit: Optional[int],
    ) -> Tuple[str, List]:
        """Build SQL query for a single legacy long-format series using QueryBuilder."""
        query_builder = QueryBuilder("_parquet_source")

        if start:
            query_builder.where(ValueColumns.TIMESTAMP, ">=", start)
        if end:
            query_builder.where(ValueColumns.TIMESTAMP, "<=", end)

        order_by_column = order_by or ValueColumns.TIMESTAMP
        query_builder.order_by(order_by_column)

        if limit:
            query_builder.limit(limit)

        base_sql, builder_params = query_builder.build()

        parquet_source = self._parquet_adapter.build_parquet_source(uri)
        adapted_sql = base_sql.replace("FROM _parquet_source", f"FROM {parquet_source}")

        custom_select = f"""
            SELECT 
                '{series_code}' AS {ValueColumns.SERIES_CODE},
                {ValueColumns.TIMESTAMP},
                {ValueColumns.VALUE}
        """
        final_sql = adapted_sql.replace("SELECT *", custom_select.strip())

        return final_sql, builder_params

    def get_last_values(
        self,
        series_codes: List[str],
        tickersource: TickerSource = TickerSource.BLOOMBERG,
    ) -> pd.DataFrame:
        """Get latest (max timestamp) row per series_code."""
        if not series_codes:
            return pd.DataFrame(
                columns=[ValueColumns.SERIES_CODE, ValueColumns.TIMESTAMP, ValueColumns.VALUE]
            )

        if self._uses_wide_storage(tickersource):
            try:
                all_long = self.get_batch_series_data(
                    series_codes=series_codes,
                    tickersource=tickersource,
                    start=None,
                    end=None,
                    order_by=None,
                    limit=None,
                )
                if all_long.empty:
                    return all_long
                all_long = all_long.sort_values(ValueColumns.TIMESTAMP, ascending=False)
                return (
                    all_long.groupby(ValueColumns.SERIES_CODE, as_index=False)
                    .head(1)
                    .sort_values(ValueColumns.SERIES_CODE)
                    .reset_index(drop=True)
                )
            except Exception as exc:
                raise MetadataResolutionError(f"Failed to load last values: {exc}") from exc

        try:
            union_parts = []
            all_params: List = []

            for series_code in series_codes:
                try:
                    uri = self._s3_adapter.get_value_data_uri(series_code, tickersource)
                    sql, params = self._build_series_query_sql(
                        series_code, uri, None, None, None, None
                    )

                    union_parts.append(f"({sql})")
                    if params:
                        all_params.extend(params)

                except Exception:
                    continue

            if not union_parts:
                return pd.DataFrame(
                    columns=[
                        ValueColumns.SERIES_CODE,
                        ValueColumns.TIMESTAMP,
                        ValueColumns.VALUE,
                    ]
                )

            union_query = f"""
                SELECT series_code, timestamp, value
                FROM (
                    {' UNION ALL '.join(union_parts)}
                ) AS all_data
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY series_code
                    ORDER BY timestamp DESC
                ) = 1
                ORDER BY series_code
            """

            if all_params:
                result = self._repository.execute_raw_sql(union_query, all_params)
            else:
                result = self._repository.execute_raw_sql(union_query)

            return result

        except Exception as exc:
            raise MetadataResolutionError(f"Failed to load last values: {exc}") from exc

    def get_all_values(
        self, series_codes: List[str], tickersource: TickerSource = TickerSource.BLOOMBERG
    ) -> pd.DataFrame:
        """Get all values from all specified series for a given ticker source."""
        return self.get_batch_series_data(
            series_codes=series_codes,
            tickersource=tickersource,
            start=None,
            end=None,
            order_by=None,
            limit=None,
        )
