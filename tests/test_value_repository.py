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
    def _extract_uri(sql: str) -> str:
        match = re.search(r"read_parquet\('(.+?)'\)", sql)
        if not match:
            raise AssertionError(f"Could not extract URI from SQL: {sql}")
        return match.group(1).replace("''", "'")

    def execute_raw_sql(self, sql: str) -> pd.DataFrame:
        self.calls.append(sql)
        uri = self._extract_uri(sql)
        if "LIMIT 0" in sql:
            frame = self.schema_frames.get(uri)
            if frame is None:
                raise FileNotFoundError(f"No files found that match the pattern: {uri}")
            return frame.copy()
        frame = self.data_frames.get(uri)
        if frame is None:
            raise FileNotFoundError(f"No files found that match the pattern: {uri}")
        return frame.copy()



class FakeS3Adapter:
    def get_wide_value_partition_uri(self, field_type, year, month, tickersource=TickerSource.BLOOMBERG):
        return f"s3://bucket/{tickersource.value}/{field_type}/{year:04d}-{month:02d}.parquet"



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
