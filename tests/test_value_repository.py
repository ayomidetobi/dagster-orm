import re

import pandas as pd

from dagster_quickstart.orm.domain.value_repository import ValueRepository
from dagster_quickstart.orm.schema import MetadataColumns, TickerSource, ValueColumns



class FakeDuckDbRepository:
    def __init__(self, schema_frames, data_frames):
        self.schema_frames = schema_frames
        self.data_frames = data_frames
        self.calls = []

    @staticmethod
    def _extract_uris(sql: str) -> list[str]:
        schema_match = re.search(r"parquet_schema\('(.+?)'\)", sql)
        if schema_match:
            return [schema_match.group(1).replace("''", "'")]

        list_match = re.search(r"read_parquet\(\[(.*?)\]", sql)
        if list_match:
            inner = list_match.group(1)
            return [m.replace("''", "'") for m in re.findall(r"'((?:''|[^'])+)'", inner)]

        single_match = re.search(r"read_parquet\('(.+?)'\)", sql)
        if single_match:
            return [single_match.group(1).replace("''", "'")]

        raise AssertionError(f"Could not extract URI(s) from SQL: {sql}")

    @staticmethod
    def _extract_selected_columns(sql: str) -> list[str]:
        select_match = re.search(r"SELECT\s+(.*?)\s+FROM\s+read_parquet", sql, re.S | re.I)
        if not select_match:
            return []
        select_clause = select_match.group(1)
        return [m.replace('""', '"') for m in re.findall(r'"((?:[^"]|"")+)"', select_clause)]

    @staticmethod
    def _extract_limit(sql: str) -> int | None:
        match = re.search(r"LIMIT\s+(\d+)", sql, re.I)
        return int(match.group(1)) if match else None

    @staticmethod
    def _extract_order_by(sql: str) -> str | None:
        match = re.search(r"ORDER BY\s+\"((?:[^\"]|\"\")+?)\"", sql, re.I)
        return match.group(1).replace('""', '"') if match else None

    def execute_raw_sql(self, sql: str, params=None) -> pd.DataFrame:
        self.calls.append({"sql": sql, "params": params})

        if "parquet_schema" in sql:
            uri = self._extract_uris(sql)[0]
            frame = self.schema_frames.get(uri)
            if frame is None:
                raise FileNotFoundError(f"No files found that match the pattern: {uri}")
            return pd.DataFrame({"name": list(frame.columns)})

        if "LIMIT 0" in sql:
            uri = self._extract_uris(sql)[0]
            frame = self.schema_frames.get(uri)
            if frame is None:
                raise FileNotFoundError(f"No files found that match the pattern: {uri}")
            return frame.copy()

        uris = self._extract_uris(sql)
        frames = []
        for uri in uris:
            frame = self.data_frames.get(uri)
            if frame is None:
                raise FileNotFoundError(f"No files found that match the pattern: {uri}")
            frames.append(frame.copy())

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True, sort=False)

        if ValueColumns.TIMESTAMP in combined.columns:
            combined[ValueColumns.TIMESTAMP] = pd.to_datetime(combined[ValueColumns.TIMESTAMP], utc=True)

        if params:
            param_idx = 0
            if ">=" in sql:
                start = pd.Timestamp(params[param_idx])
                combined = combined[combined[ValueColumns.TIMESTAMP] >= start]
                param_idx += 1
            if "<=" in sql and param_idx < len(params):
                end = pd.Timestamp(params[param_idx])
                combined = combined[combined[ValueColumns.TIMESTAMP] <= end]

        selected_columns = self._extract_selected_columns(sql)
        if selected_columns:
            missing_columns = [col for col in selected_columns if col not in combined.columns]
            if missing_columns:
                raise RuntimeError(
                    f"Binder Error: Referenced column(s) {missing_columns!r} does not have a column named in parquet"
                )
            combined = combined[selected_columns]

        order_by = self._extract_order_by(sql)
        if order_by and order_by in combined.columns:
            combined = combined.sort_values(order_by)

        limit = self._extract_limit(sql)
        if limit is not None:
            combined = combined.head(limit)

        return combined.reset_index(drop=True)



class FakeS3Adapter:
    def __init__(self):
        self.calls = []

    def get_wide_value_partition_uri(self, field_type, year, month, tickersource=TickerSource.BLOOMBERG):
        uri = f"s3://bucket/{tickersource.value}/{field_type}/{year:04d}-{month:02d}.parquet"
        self.calls.append(
            {
                "method": "get_wide_value_partition_uri",
                "field_type": field_type,
                "year": year,
                "month": month,
                "tickersource": tickersource,
                "uri": uri,
            }
        )
        return uri

    def get_wide_field_glob_uri(self, field_type, tickersource=TickerSource.BLOOMBERG):
        uri = f"s3://bucket/{tickersource.value}/{field_type}/**/*.parquet"
        self.calls.append(
            {
                "method": "get_wide_field_glob_uri",
                "field_type": field_type,
                "tickersource": tickersource,
                "uri": uri,
            }
        )
        return uri



class FakeValidationRepository:
    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe

    def filter_with_validation(self, filters=None):
        df = self.dataframe.copy()
        if filters and MetadataColumns.SERIES_CODE in filters:
            df = df[df[MetadataColumns.SERIES_CODE].isin(filters[MetadataColumns.SERIES_CODE])]
        return df.reset_index(drop=True)



def make_repository(schema_frames, data_frames, mapping_df) -> ValueRepository:
    return ValueRepository(
        duckdb_repository=FakeDuckDbRepository(schema_frames, data_frames),
        parquet_adapter=object(),
        s3_adapter=FakeS3Adapter(),
        validation_repository=FakeValidationRepository(mapping_df),
        metadata_repository=None,
    )


def _fail_if_batch_is_used(*args, **kwargs):
    raise AssertionError("get_last_values should not call get_batch_series_data")


def test_get_last_values_scans_months_backwards_and_stops_when_all_found() -> None:
    mapping_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1", "S2"],
            MetadataColumns.BBG_FIELD: ["PX_LAST", "PX_LAST"],
        }
    )
    may_uri = "s3://bucket/Bloomberg/PX_LAST/2024-05.parquet"
    april_uri = "s3://bucket/Bloomberg/PX_LAST/2024-04.parquet"
    schema_frames = {
        may_uri: pd.DataFrame(
            columns=[ValueColumns.TIMESTAMP, "S2"],
        ),
        april_uri: pd.DataFrame(
            columns=[ValueColumns.TIMESTAMP, "S1", "S2"],
        ),
    }
    data_frames = {
        may_uri: pd.DataFrame(
            {
                ValueColumns.TIMESTAMP: pd.to_datetime(
                    ["2024-05-29", "2024-05-31"],
                    utc=True,
                ),
                "S2": [1.0, 2.0],
            }
        ),
        april_uri: pd.DataFrame(
            {
                ValueColumns.TIMESTAMP: pd.to_datetime(
                    ["2024-04-29", "2024-04-30"],
                    utc=True,
                ),
                "S1": [10.0, 11.0],
                "S2": [20.0, 21.0],
            }
        ),
    }
    repository = make_repository(schema_frames, data_frames, mapping_df)
    repository._month_iter_desc = lambda start_year_month=None, max_lookback_months=2400: iter(
        [(2024, 5), (2024, 4), (2024, 3)]
    )

    repository._resolve_vendor_field_map = lambda series_codes, tickersource: {
        "S1": "PX_LAST",
        "S2": "PX_LAST",
    }
    repository.get_batch_series_data = _fail_if_batch_is_used

    result = repository.get_last_values(["S1", "S2"], TickerSource.BLOOMBERG, latest_non_null=True)

    assert list(result[ValueColumns.SERIES_CODE]) == ["S1", "S2"]
    assert result.loc[result[ValueColumns.SERIES_CODE] == "S1", ValueColumns.TIMESTAMP].iloc[0] == pd.Timestamp(
        "2024-04-30", tz="UTC"
    )
    assert result.loc[result[ValueColumns.SERIES_CODE] == "S1", ValueColumns.VALUE].iloc[0] == 11.0
    assert result.loc[result[ValueColumns.SERIES_CODE] == "S2", ValueColumns.TIMESTAMP].iloc[0] == pd.Timestamp(
        "2024-05-31", tz="UTC"
    )
    assert result.loc[result[ValueColumns.SERIES_CODE] == "S2", ValueColumns.VALUE].iloc[0] == 2.0
    assert len([call for call in repository._repository.calls if "2024-03" in call]) == 0


def test_get_last_values_latest_non_null_false_keeps_latest_row_even_if_null() -> None:
    mapping_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1"],
            MetadataColumns.BBG_FIELD: ["PX_LAST"],
        }
    )
    may_uri = "s3://bucket/Bloomberg/PX_LAST/2024-05.parquet"
    schema_frames = {
        may_uri: pd.DataFrame(columns=[ValueColumns.TIMESTAMP, "S1"]),
    }
    data_frames = {
        may_uri: pd.DataFrame(
            {
                ValueColumns.TIMESTAMP: pd.to_datetime(
                    ["2024-05-30", "2024-05-31"],
                    utc=True,
                ),
                "S1": [5.0, None],
            }
        )
    }
    repository = make_repository(schema_frames, data_frames, mapping_df)
    repository._month_iter_desc = lambda start_year_month=None, max_lookback_months=2400: iter(
        [(2024, 5)]
    )
    repository._resolve_vendor_field_map = lambda series_codes, tickersource: {"S1": "PX_LAST"}
    repository.get_batch_series_data = _fail_if_batch_is_used

    result = repository.get_last_values(["S1"], TickerSource.BLOOMBERG, latest_non_null=False)

    assert result.loc[0, ValueColumns.TIMESTAMP] == pd.Timestamp("2024-05-31", tz="UTC")
    assert pd.isna(result.loc[0, ValueColumns.VALUE])



def test_read_wide_month_selected_columns_skips_missing_partition() -> None:
    mapping_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1"],
            MetadataColumns.BBG_FIELD: ["PX_LAST"],
        }
    )
    repository = make_repository({}, {}, mapping_df)

    empty = repository._read_wide_month_selected_columns(
        vendor_field="PX_LAST",
        year=2024,
        month=5,
        tickersource=TickerSource.BLOOMBERG,
        series_codes=["S1"],
    )

    assert empty.empty


def test_read_wide_selected_columns_pushes_projection_and_returns_rows() -> None:
    mapping_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1", "S2", "S3"],
            MetadataColumns.BBG_FIELD: ["PX_LAST", "PX_LAST", "PX_LAST"],
        }
    )
    may_uri = "s3://bucket/Bloomberg/PX_LAST/2024-05.parquet"
    june_uri = "s3://bucket/Bloomberg/PX_LAST/2024-06.parquet"
    schema_frames = {
        may_uri: pd.DataFrame(columns=[ValueColumns.TIMESTAMP, "S1", "S2"]),
        june_uri: pd.DataFrame(columns=[ValueColumns.TIMESTAMP, "S1"]),
    }
    data_frames = {
        may_uri: pd.DataFrame(
            {
                ValueColumns.TIMESTAMP: pd.to_datetime(["2024-05-01", "2024-05-02"], utc=True),
                "S1": [1.0, 2.0],
                "S2": [10.0, 20.0],
            }
        ),
        june_uri: pd.DataFrame(
            {
                ValueColumns.TIMESTAMP: pd.to_datetime(["2024-06-01"], utc=True),
                "S1": [3.0],
            }
        ),
    }
    repository = make_repository(schema_frames, data_frames, mapping_df)

    result = repository._read_wide_selected_columns_to_df(
        uris=[may_uri, june_uri],
        series_codes=["S1", "S2", "S3"],
        start="2024-05-01",
        end="2024-06-30",
        order_by=ValueColumns.TIMESTAMP,
        limit=10,
    )

    read_calls = [call for call in repository._repository.calls if "read_parquet" in call["sql"]]
    assert read_calls
    assert all("SELECT *" not in call["sql"] for call in read_calls)
    assert any('"timestamp", "S1", "S2"' in call["sql"] for call in read_calls)
    assert result.index.name == ValueColumns.TIMESTAMP
    assert list(result.columns) == ["S1", "S2"]
    assert pd.Timestamp("2024-06-01", tz="UTC") in result.index


def test_read_wide_selected_columns_skips_missing_columns_gracefully() -> None:
    mapping_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1", "S2"],
            MetadataColumns.BBG_FIELD: ["PX_LAST", "PX_LAST"],
        }
    )
    may_uri = "s3://bucket/Bloomberg/PX_LAST/2024-05.parquet"
    schema_frames = {
        may_uri: pd.DataFrame(columns=[ValueColumns.TIMESTAMP, "S1"]),
    }
    data_frames = {
        may_uri: pd.DataFrame(
            {
                ValueColumns.TIMESTAMP: pd.to_datetime(["2024-05-01"], utc=True),
                "S1": [1.0],
            }
        ),
    }
    repository = make_repository(schema_frames, data_frames, mapping_df)

    result = repository._read_wide_selected_columns_to_df(
        uris=[may_uri],
        series_codes=["S1", "S2"],
        start=None,
        end=None,
    )

    assert list(result.columns) == ["S1"]
    assert result.index.name == ValueColumns.TIMESTAMP


def test_read_wide_selected_columns_returns_true_empty_for_missing_partition() -> None:
    mapping_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1"],
            MetadataColumns.BBG_FIELD: ["PX_LAST"],
        }
    )
    missing_uri = "s3://bucket/Bloomberg/PX_LAST/2024-05.parquet"
    repository = make_repository({}, {}, mapping_df)

    result = repository._read_wide_selected_columns_to_df(
        uris=[missing_uri],
        series_codes=["S1"],
        start=None,
        end=None,
    )

    assert result.empty
    assert list(result.columns) == []


def test_read_wide_selected_columns_preserves_requested_order() -> None:
    mapping_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1", "S2", "S3"],
            MetadataColumns.BBG_FIELD: ["PX_LAST", "PX_LAST", "PX_LAST"],
        }
    )
    may_uri = "s3://bucket/Bloomberg/PX_LAST/2024-05.parquet"
    schema_frames = {
        may_uri: pd.DataFrame(columns=[ValueColumns.TIMESTAMP, "S2", "S1"]),
    }
    data_frames = {
        may_uri: pd.DataFrame(
            {
                ValueColumns.TIMESTAMP: pd.to_datetime(["2024-05-01", "2024-05-02"], utc=True),
                "S1": [1.0, 2.0],
                "S2": [10.0, 20.0],
            }
        ),
    }
    repository = make_repository(schema_frames, data_frames, mapping_df)

    result = repository._read_wide_selected_columns_to_df(
        uris=[may_uri],
        series_codes=["S1", "S2", "S3"],
        start=None,
        end=None,
    )

    assert not result.empty
    assert list(result.columns) == ["S1", "S2"]
    assert result.index.name == ValueColumns.TIMESTAMP


def test_read_wide_selected_columns_returns_empty_when_no_requested_columns_exist() -> None:
    mapping_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1"],
            MetadataColumns.BBG_FIELD: ["PX_LAST"],
        }
    )
    may_uri = "s3://bucket/Bloomberg/PX_LAST/2024-05.parquet"
    schema_frames = {
        may_uri: pd.DataFrame(columns=[ValueColumns.TIMESTAMP, "OTHER"]),
    }
    data_frames = {
        may_uri: pd.DataFrame(
            {
                ValueColumns.TIMESTAMP: pd.to_datetime(["2024-05-01"], utc=True),
                "OTHER": [1.0],
            }
        ),
    }
    repository = make_repository(schema_frames, data_frames, mapping_df)

    result = repository._read_wide_selected_columns_to_df(
        uris=[may_uri],
        series_codes=["S1"],
        start=None,
        end=None,
    )

    assert result.empty
    assert list(result.columns) == []


def test_get_batch_series_data_wide_skips_empty_column_only_parts() -> None:
    mapping_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1", "S2"],
            MetadataColumns.BBG_FIELD: ["PX_LAST", "PX_LAST"],
        }
    )
    repository = make_repository({}, {}, mapping_df)

    def fake_read_wide_as_wide(*args, **kwargs):
        return pd.DataFrame(columns=[ValueColumns.TIMESTAMP, "S1"])

    repository._read_wide_as_wide = fake_read_wide_as_wide  # type: ignore[method-assign]

    result = repository.get_batch_series_data_wide(
        series_codes=["S1", "S2"],
        tickersource=TickerSource.BLOOMBERG,
    )

    assert result.empty
    assert list(result.columns) == []


def test_read_wide_selected_columns_avoids_schema_discovery_on_success() -> None:
    mapping_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1", "S2"],
            MetadataColumns.BBG_FIELD: ["PX_LAST", "PX_LAST"],
        }
    )
    may_uri = "s3://bucket/Bloomberg/PX_LAST/2024-05.parquet"
    schema_frames = {
        may_uri: pd.DataFrame(columns=[ValueColumns.TIMESTAMP, "S1", "S2"]),
    }
    data_frames = {
        may_uri: pd.DataFrame(
            {
                ValueColumns.TIMESTAMP: pd.to_datetime(["2024-05-01", "2024-05-02"], utc=True),
                "S1": [1.0, 2.0],
                "S2": [10.0, 20.0],
            }
        ),
    }
    repository = make_repository(schema_frames, data_frames, mapping_df)

    result = repository._read_wide_selected_columns_to_df(
        uris=[may_uri],
        series_codes=["S1", "S2"],
        start="2024-05-01",
        end="2024-05-31",
    )

    assert result.index.name == ValueColumns.TIMESTAMP
    assert all("parquet_schema" not in call["sql"] for call in repository._repository.calls)


def test_read_wide_selected_columns_ignores_time_filter_for_now() -> None:
    mapping_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1", "S2"],
            MetadataColumns.BBG_FIELD: ["PX_LAST", "PX_LAST"],
        }
    )
    may_uri = "s3://bucket/Bloomberg/PX_LAST/2024-05.parquet"
    schema_frames = {
        may_uri: pd.DataFrame(columns=[ValueColumns.TIMESTAMP, "S1", "S2"]),
    }
    data_frames = {
        may_uri: pd.DataFrame(
            {
                ValueColumns.TIMESTAMP: pd.to_datetime(["2024-05-01"], utc=True),
                "S1": [1.0],
                "S2": [2.0],
            }
        ),
    }
    repository = make_repository(schema_frames, data_frames, mapping_df)

    result = repository._read_wide_selected_columns_to_df(
        uris=[may_uri],
        series_codes=["S1", "S2"],
        start="2024-06-01",
        end="2024-06-30",
    )

    assert not result.empty
    assert list(result.columns) == ["S1", "S2"]
    assert result.index.name == ValueColumns.TIMESTAMP


def test_read_wide_as_wide_uses_december_month_uri_and_returns_rows_for_date_range() -> None:
    mapping_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1", "S2"],
            MetadataColumns.BBG_FIELD: ["PX_LAST", "PX_LAST"],
        }
    )
    uri = "s3://bucket/Bloomberg/PX_LAST/2025-12.parquet"
    schema_frames = {
        uri: pd.DataFrame(columns=[ValueColumns.TIMESTAMP, "S1", "S2"]),
    }
    data_frames = {
        uri: pd.DataFrame(
            {
                ValueColumns.TIMESTAMP: pd.to_datetime(
                    [
                        "2025-12-01 00:00:00+00:00",
                        "2025-12-02 00:00:00+00:00",
                        "2025-12-03 00:00:00+00:00",
                    ]
                ),
                "S1": [1.0, 2.0, 3.0],
                "S2": [10.0, 20.0, 30.0],
            }
        ),
    }
    repository = make_repository(schema_frames, data_frames, mapping_df)

    result = repository._read_wide_as_wide(
        series_codes=["S1", "S2"],
        vendor_field="PX_LAST",
        tickersource=TickerSource.BLOOMBERG,
        start="2025-12-01",
        end="2025-12-03",
    )

    assert any(
        call["method"] == "get_wide_value_partition_uri"
        and call["year"] == 2025
        and call["month"] == 12
        for call in repository._s3_adapter.calls
    )
    assert list(result.columns) == ["S1", "S2"]
    assert list(result.index.strftime("%Y-%m-%d")) == ["2025-12-01", "2025-12-02", "2025-12-03"]


def test_read_wide_as_wide_datetime_end_returns_month_rows() -> None:
    mapping_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1"],
            MetadataColumns.BBG_FIELD: ["PX_LAST"],
        }
    )
    uri = "s3://bucket/Bloomberg/PX_LAST/2025-12.parquet"
    schema_frames = {
        uri: pd.DataFrame(columns=[ValueColumns.TIMESTAMP, "S1"]),
    }
    data_frames = {
        uri: pd.DataFrame(
            {
                ValueColumns.TIMESTAMP: pd.to_datetime(
                    [
                        "2025-12-03 00:00:00+00:00",
                        "2025-12-03 12:00:00+00:00",
                        "2025-12-03 23:59:59+00:00",
                    ]
                ),
                "S1": [1.0, 2.0, 3.0],
            }
        ),
    }
    repository = make_repository(schema_frames, data_frames, mapping_df)

    result = repository._read_wide_as_wide(
        series_codes=["S1"],
        vendor_field="PX_LAST",
        tickersource=TickerSource.BLOOMBERG,
        start="2025-12-03 00:00:00",
        end="2025-12-03 23:59:59",
    )

    assert not result.empty
    assert len(result) == 3
    assert set(result.index.strftime("%Y-%m-%d")) == {"2025-12-03"}


def test_read_wide_selected_columns_caches_schema_after_fallback() -> None:
    mapping_df = pd.DataFrame(
        {
            MetadataColumns.SERIES_CODE: ["S1", "S2"],
            MetadataColumns.BBG_FIELD: ["PX_LAST", "PX_LAST"],
        }
    )
    may_uri = "s3://bucket/Bloomberg/PX_LAST/2024-05.parquet"
    schema_frames = {
        may_uri: pd.DataFrame(columns=[ValueColumns.TIMESTAMP, "S1"]),
    }
    data_frames = {
        may_uri: pd.DataFrame(
            {
                ValueColumns.TIMESTAMP: pd.to_datetime(["2024-05-01"], utc=True),
                "S1": [1.0],
            }
        ),
    }
    repository = make_repository(schema_frames, data_frames, mapping_df)

    first = repository._read_wide_selected_columns_to_df(
        uris=[may_uri],
        series_codes=["S1", "S2"],
        start=None,
        end=None,
    )
    first_schema_calls = [call for call in repository._repository.calls if "parquet_schema" in call["sql"]]
    assert not first.empty
    assert first.index.name == ValueColumns.TIMESTAMP
    assert len(first_schema_calls) == 1

    repository._repository.calls.clear()
    second = repository._read_wide_selected_columns_to_df(
        uris=[may_uri],
        series_codes=["S1", "S2"],
        start=None,
        end=None,
    )
    second_schema_calls = [call for call in repository._repository.calls if "parquet_schema" in call["sql"]]
    assert not second.empty
    assert second.index.name == ValueColumns.TIMESTAMP
    assert len(second_schema_calls) == 0
