"""Value repository: load series data from wide monthly Parquet partitions only."""

from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd

from dagster_quickstart.orm.domain.metadata_repository import MetadataRepository
from dagster_quickstart.orm.domain.validation_repository import ValidationRepository
from dagster_quickstart.orm.exceptions import MetadataResolutionError
from dagster_quickstart.orm.infrastructure.duckdb_repository import DuckDbRepository
from dagster_quickstart.orm.infrastructure.parquet_adapter import ParquetAdapter
from dagster_quickstart.orm.infrastructure.s3_adapter import S3Adapter
from dagster_quickstart.orm.schema import MetadataColumns, TableNames, TickerSource, ValueColumns
from dagster_quickstart.utils.datetime_utils import (
    ensure_utc,
    iter_year_months,
    normalize_date_to_utc,
    utc_now,
)
from dagster_quickstart.utils.pandas_wide import select_series_columns_as_long_df

_WIDE_TICKER_SOURCES = frozenset(
    {TickerSource.BLOOMBERG, TickerSource.MDS, TickerSource.HAWKEYE, TickerSource.INTERNAL}
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

    @staticmethod
    def _uses_wide_storage(tickersource: TickerSource) -> bool:
        return tickersource in _WIDE_TICKER_SOURCES

    @staticmethod
    def _vendor_field_column(tickersource: TickerSource) -> str:
        if tickersource == TickerSource.BLOOMBERG:
            return MetadataColumns.BBG_FIELD
        if tickersource == TickerSource.MDS:
            return MetadataColumns.MDS_FIELD
        if tickersource == TickerSource.HAWKEYE:
            return MetadataColumns.HAWK_FIELD
        if tickersource == TickerSource.INTERNAL:
            return MetadataColumns.CALC_TYPE
        raise ValueError(f"No vendor field column for ticker source {tickersource!r}")

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
            if latest_non_null:
                all_long = all_long.dropna(subset=[ValueColumns.VALUE])
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
