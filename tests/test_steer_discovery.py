"""Unit tests for steer.discovery: currency/country parsing and pair availability assessment."""

from __future__ import annotations

import pandas as pd

from dagster_quickstart.steer.discovery import (
    assess_pair_availability,
    build_currency_to_equity_series,
    build_currency_to_fi_series,
    parse_equity_currency,
    parse_fi_country,
    parse_fi_currency,
)


def test_parse_fi_currency_resolves_sovereign_yield_via_country_prefix():
    assert parse_fi_currency("US2Y_YIELD_0021") == "USD"
    assert parse_fi_currency("DE2Y_YIELD_0076") == "EUR"


def test_parse_fi_currency_resolves_swap_series_via_explicit_mnemonic_table():
    assert parse_fi_currency("EUSA2_PX_LAST") == "EUR"
    assert parse_fi_currency("USOSFR2_PX_LAST") == "USD"
    assert parse_fi_currency("BPSW2_PX_LAST") == "GBP"
    assert parse_fi_currency("JYSO2_PX_LAST") == "JPY"
    assert parse_fi_currency("SFSW2_PX_LAST") == "CHF"
    assert parse_fi_currency("ADSW2_PX_LAST") == "AUD"
    assert parse_fi_currency("NKSW2_PX_LAST") == "NOK"
    assert parse_fi_currency("SKSW2_PX_LAST") == "SEK"


def test_parse_fi_currency_none_for_unrecognized_series_code():
    assert parse_fi_currency("SX0001_PX_LAST") is None


def test_swap_series_do_not_match_the_country_prefix_regex_directly():
    """Confirms the swap mnemonics genuinely need the explicit table --
    they aren't just an unnoticed match of the sovereign-yield pattern."""
    for series_code in (
        "EUSA2_PX_LAST",
        "USOSFR2_PX_LAST",
        "BPSW2_PX_LAST",
        "JYSO2_PX_LAST",
        "SFSW2_PX_LAST",
        "ADSW2_PX_LAST",
    ):
        assert parse_fi_country(series_code) is None


def test_build_currency_to_fi_series_merges_yield_and_swap_coverage():
    metadata = pd.DataFrame(
        {
            "series_code": [
                "US2Y_YIELD_0021",
                "USOSFR2_PX_LAST",
                "SFSW2_PX_LAST",
            ]
        }
    )

    by_currency = build_currency_to_fi_series(metadata)

    assert set(by_currency["USD"]) == {"US2Y_YIELD_0021", "USOSFR2_PX_LAST"}
    assert by_currency["CHF"] == ["SFSW2_PX_LAST"]


def test_chfjpy_pair_gets_rate_data_from_swap_only_coverage():
    """CHF has no sovereign-yield series in this catalog -- SFSW2_PX_LAST
    (the swap) is its only rate-data source, so a CHF pair should only be
    unblocked on rate_data once that swap series is present."""
    metadata = pd.DataFrame({"series_code": ["JP5Y_YIELD_0047", "SFSW2_PX_LAST"]})
    currency_to_fi_series = build_currency_to_fi_series(metadata)

    availability = assess_pair_availability(
        "CHFJPY_SPOT_0099",
        "G10",
        currency_to_fi_series=currency_to_fi_series,
        currency_to_equity_series={},
    )

    assert availability.rate_data_available is True
    assert availability.base_rate_series == "SFSW2_PX_LAST"
    assert availability.quote_rate_series == "JP5Y_YIELD_0047"


def test_pair_missing_swap_and_yield_coverage_is_reported_unavailable():
    availability = assess_pair_availability(
        "CHFNZD_SPOT_0100", "G10", currency_to_fi_series={}, currency_to_equity_series={}
    )

    assert availability.rate_data_available is False
    assert "CHF" in availability.rate_data_reason
    assert "NZD" in availability.rate_data_reason


def test_eurnok_pair_gets_rate_data_from_swap_only_coverage():
    """NOK (like CHF) has no sovereign-yield series in this catalog --
    NKSW2_PX_LAST is its only rate-data source."""
    metadata = pd.DataFrame({"series_code": ["DE2Y_YIELD_0076", "NKSW2_PX_LAST"]})
    currency_to_fi_series = build_currency_to_fi_series(metadata)

    availability = assess_pair_availability(
        "EURNOK_SPOT_0265",
        "G10",
        currency_to_fi_series=currency_to_fi_series,
        currency_to_equity_series={},
    )

    assert availability.rate_data_available is True
    assert availability.base_rate_series == "DE2Y_YIELD_0076"
    assert availability.quote_rate_series == "NKSW2_PX_LAST"


def test_seknok_pair_gets_rate_data_from_both_swap_series():
    """SEKNOK needs both placeholder swap series -- neither leg has a sovereign yield."""
    metadata = pd.DataFrame({"series_code": ["SKSW2_PX_LAST", "NKSW2_PX_LAST"]})
    currency_to_fi_series = build_currency_to_fi_series(metadata)

    availability = assess_pair_availability(
        "SEKNOK_SPOT_0616",
        "G10",
        currency_to_fi_series=currency_to_fi_series,
        currency_to_equity_series={},
    )

    assert availability.rate_data_available is True
    assert availability.base_rate_series == "SKSW2_PX_LAST"
    assert availability.quote_rate_series == "NKSW2_PX_LAST"


def test_parse_equity_currency_resolves_via_explicit_mnemonic_table():
    assert parse_equity_currency("AUD_PX_LAST") == "AUD"
    assert parse_equity_currency("ZAR_PX_LAST") == "ZAR"


def test_parse_equity_currency_none_for_generic_equity_series():
    """The bulk of this catalog's Equity metadata (Common Stock / generic
    "Regional Index" rows) carries no country/currency signal at all --
    only the 14 explicit per-currency MSCI series resolve."""
    assert parse_equity_currency("SX0001_PX_LAST") is None
    assert parse_equity_currency("IDX0005_INDEX") is None


def test_build_currency_to_equity_series_only_matches_explicit_table():
    metadata = pd.DataFrame(
        {"series_code": ["AUD_PX_LAST", "JPY_PX_LAST", "SX0001_PX_LAST", "IDX0005_INDEX"]}
    )

    by_currency = build_currency_to_equity_series(metadata)

    assert by_currency == {"AUD": ["AUD_PX_LAST"], "JPY": ["JPY_PX_LAST"]}


def test_audjpy_pair_fully_unblocked_with_both_rate_and_equity_coverage():
    """AUD and JPY both have sovereign-yield and local-equity coverage in
    the real catalog -- a pair between them should come off the blocked
    list entirely once both metadata sets are wired in."""
    fi_metadata = pd.DataFrame({"series_code": ["AU2Y_YIELD_0208", "JP5Y_YIELD_0047"]})
    equity_metadata = pd.DataFrame({"series_code": ["AUD_PX_LAST", "JPY_PX_LAST"]})

    availability = assess_pair_availability(
        "AUDJPY_SPOT_0004",
        "G10",
        currency_to_fi_series=build_currency_to_fi_series(fi_metadata),
        currency_to_equity_series=build_currency_to_equity_series(equity_metadata),
    )

    assert availability.local_equity_available is True
    assert availability.base_equity_series == "AUD_PX_LAST"
    assert availability.quote_equity_series == "JPY_PX_LAST"
    assert availability.rate_data_available is True
    assert availability.blocked is False


def test_pair_with_rate_data_but_no_equity_coverage_stays_blocked():
    """Both drivers are required -- rate_data alone isn't enough to unblock a pair."""
    fi_metadata = pd.DataFrame({"series_code": ["AU2Y_YIELD_0208", "JP5Y_YIELD_0047"]})

    availability = assess_pair_availability(
        "AUDJPY_SPOT_0004",
        "G10",
        currency_to_fi_series=build_currency_to_fi_series(fi_metadata),
        currency_to_equity_series={},
    )

    assert availability.rate_data_available is True
    assert availability.local_equity_available is False
    assert availability.blocked is True
