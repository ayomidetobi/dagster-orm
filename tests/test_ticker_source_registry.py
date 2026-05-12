from dagster_quickstart.orm.schema import (
    MetadataColumns,
    TICKER_SOURCE_REGISTRY,
    TickerSource,
    get_storage_field_column,
    get_ticker_source_config,
    get_vendor_field_column,
    get_vendor_ticker_column,
    ticker_source_uses_wide_storage,
)


def test_registry_contains_known_sources() -> None:
    assert TickerSource.BLOOMBERG in TICKER_SOURCE_REGISTRY
    assert TickerSource.MDS in TICKER_SOURCE_REGISTRY
    assert TickerSource.HAWKEYE in TICKER_SOURCE_REGISTRY
    assert TickerSource.INTERNAL in TICKER_SOURCE_REGISTRY


def test_vendor_columns_come_from_registry() -> None:
    assert get_vendor_ticker_column(TickerSource.BLOOMBERG) == MetadataColumns.BBG_TICKER
    assert get_vendor_field_column(TickerSource.MDS) == MetadataColumns.MDS_FIELD


def test_storage_helpers_support_internal_and_wide_flags() -> None:
    assert get_storage_field_column(TickerSource.INTERNAL) == MetadataColumns.CALC_TYPE
    assert ticker_source_uses_wide_storage(TickerSource.INTERNAL) is True
    assert ticker_source_uses_wide_storage(TickerSource.LSEG) is False


def test_onetick_reuses_mds_metadata_columns() -> None:
    config = get_ticker_source_config(TickerSource.ONETICK)

    assert config.ticker_column == MetadataColumns.MDS_TICKER
    assert config.field_column == MetadataColumns.MDS_FIELD
