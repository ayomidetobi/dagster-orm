"""Unit tests for steer.discovery: FX-leg parsing, RoleResolver, and pair availability assessment."""

from __future__ import annotations

import pandas as pd
import pytest

from dagster_quickstart.steer.source.discovery import (
    REQUIRED_ROLES,
    ROLE_FILTERS,
    PairAvailability,
    RoleResolver,
    assess_pair_availability,
    build_availability_report,
    parse_fx_legs,
)


def test_parse_fx_legs_extracts_base_and_quote():
    assert parse_fx_legs("EURUSD_PX_LAST") == ("EUR", "USD")
    assert parse_fx_legs("USDZAR_PX_LAST") == ("USD", "ZAR")


def test_parse_fx_legs_none_for_unrecognized_series_code():
    assert parse_fx_legs("SX0001_PX_LAST") is None


def _role_row(role: str, currency: str, series_code: str, *, is_synthetic: bool = False) -> dict:
    row = {"currency": currency, "series_code": series_code, "is_synthetic": is_synthetic}
    row.update({key: value[0] for key, value in ROLE_FILTERS[role].items()})
    return row


class _FakeMetadataAPI:
    """Just enough of DataAPI's surface for RoleResolver.from_data_api -- one unfiltered
    get_metadata() call returning the whole frame."""

    def __init__(self, frame: pd.DataFrame):
        self._frame = frame
        self.call_count = 0

    def get_metadata(self, *args, **kwargs):
        assert not args and not kwargs, "RoleResolver.from_data_api must call get_metadata() with no filters"
        self.call_count += 1
        return type("Result", (), {"frame": self._frame})()


def test_role_resolver_finds_matching_series():
    resolver = RoleResolver(pd.DataFrame([_role_row("swap_2y", "EUR", "EUR2YSW_PX_LAST")]))

    code, reason = resolver.resolve("swap_2y", "EUR")

    assert code == "EUR2YSW_PX_LAST"
    assert "EUR" in reason


def test_role_resolver_none_when_nothing_matches():
    resolver = RoleResolver(pd.DataFrame([_role_row("swap_2y", "EUR", "EUR2YSW_PX_LAST")]))

    code, reason = resolver.resolve("swap_2y", "GBP")

    assert code is None
    assert "GBP" in reason


def test_role_resolver_prefers_real_row_over_synthetic_duplicate():
    """CNH's local_equity role matches 2 rows in the real catalog -- the real
    CNHLIVEMSCI_PX_LAST and the synthetic CNHMSCI_PX_LAST. The real one wins --
    this is the ONLY thing is_synthetic still influences (see module docstring)."""
    resolver = RoleResolver(
        pd.DataFrame(
            [
                _role_row("local_equity", "CNH", "CNHMSCI_PX_LAST", is_synthetic=True),
                _role_row("local_equity", "CNH", "CNHLIVEMSCI_PX_LAST", is_synthetic=False),
            ]
        )
    )

    code, _ = resolver.resolve("local_equity", "CNH")

    assert code == "CNHLIVEMSCI_PX_LAST"


def test_role_resolver_from_data_api_calls_get_metadata_exactly_once_with_no_filters():
    frame = pd.DataFrame([_role_row("swap_2y", "EUR", "EUR2YSW_PX_LAST")])
    api = _FakeMetadataAPI(frame)

    resolver = RoleResolver.from_data_api(api)

    assert api.call_count == 1
    code, _ = resolver.resolve("swap_2y", "EUR")
    assert code == "EUR2YSW_PX_LAST"


def _g10_catalog() -> RoleResolver:
    rows = []
    for role, currencies in (
        ("swap_2y", ["EUR", "USD"]),
        ("rate_3m", ["EUR", "USD"]),
        ("yield_10y", ["EUR", "USD"]),
        ("local_equity", ["EUR", "USD"]),
    ):
        for ccy in currencies:
            rows.append(_role_row(role, ccy, f"{ccy}_{role}_PX_LAST"))
    return RoleResolver(pd.DataFrame(rows))


def test_g10_pair_fully_available_needs_all_four_roles_both_legs():
    resolver = _g10_catalog()

    availability = assess_pair_availability("EURUSD_PX_LAST", "G10", resolver)

    assert availability.blocked is False
    assert availability.base_currency == "EUR"
    assert availability.quote_currency == "USD"
    for role in REQUIRED_ROLES["G10"][0]:
        assert availability.get("base", role) is not None
        assert availability.get("quote", role) is not None


def test_g10_pair_missing_one_leg_one_role_is_blocked_with_readable_reason():
    resolver = _g10_catalog()
    # Drop GBP's rate_3m entirely by using a pair whose quote (GBP) has no coverage at all.
    availability = assess_pair_availability("EURGBP_PX_LAST", "G10", resolver)

    assert availability.blocked is True
    reasons = availability.block_reasons
    assert any("GBP" in reason for reason in reasons)


def _em_catalog() -> RoleResolver:
    rows = []
    for role, currencies in (
        ("swap_2y", ["USD", "ZAR"]),
        ("local_equity", ["USD", "ZAR"]),
        ("cds_5y", ["ZAR"]),
    ):
        for ccy in currencies:
            rows.append(_role_row(role, ccy, f"{ccy}_{role}_PX_LAST"))
    return RoleResolver(pd.DataFrame(rows))


def test_em_pair_cds_role_required_only_for_non_usd_leg():
    resolver = _em_catalog()

    availability = assess_pair_availability("USDZAR_PX_LAST", "EM", resolver)

    assert availability.blocked is False
    assert availability.get("base", "cds_5y") is None  # USD leg -- cds_5y never requested for it
    assert availability.get("quote", "cds_5y") == "ZAR_cds_5y_PX_LAST"


def test_em_pair_missing_non_usd_cds_is_blocked():
    resolver = RoleResolver(
        pd.DataFrame(
            [
                _role_row("swap_2y", "USD", "USD_swap_PX_LAST"),
                _role_row("swap_2y", "ZAR", "ZAR_swap_PX_LAST"),
                _role_row("local_equity", "USD", "USD_eq_PX_LAST"),
                _role_row("local_equity", "ZAR", "ZAR_eq_PX_LAST"),
            ]
        )
    )

    availability = assess_pair_availability("USDZAR_PX_LAST", "EM", resolver)

    assert availability.blocked is True
    assert any("cds_5y" in key for key in availability.missing_reasons)


def test_em_cross_with_no_usd_leg_is_blocked_as_a_structural_invariant():
    """TRYZAR has two non-USD legs -- EM driver 2 (the non-USD leg's 5Y CDS, a
    single-country level) has no principled candidate when both legs are
    non-USD. Blocked outright, without even attempting role resolution."""
    resolver = _em_catalog()  # has full TRY/ZAR role coverage if it were ever queried

    availability = assess_pair_availability("TRYZAR_PX_LAST", "EM", resolver)

    assert availability.blocked is True
    assert any("USD-quoted by construction" in reason for reason in availability.block_reasons)
    assert availability.resolved == {}  # no role resolution attempted at all


def test_chn_cross_with_no_usd_leg_is_blocked_as_a_structural_invariant():
    resolver = RoleResolver(pd.DataFrame())

    availability = assess_pair_availability("CNHJPY_PX_LAST", "CHN", resolver)

    assert availability.blocked is True
    assert any("USD-quoted by construction" in reason for reason in availability.block_reasons)


def test_unparseable_series_code_reports_blocked():
    resolver = RoleResolver(pd.DataFrame())

    availability = assess_pair_availability("NOT_A_PAIR", "G10", resolver)

    assert availability.blocked is True
    assert availability.base_currency is None
    assert availability.quote_currency is None


# --- build_availability_report / PairAvailability.from_report_row round-trip ---


def test_build_availability_report_includes_role_columns():
    resolver_frame = pd.DataFrame(
        row
        for role, currencies in (
            ("swap_2y", ["EUR", "USD"]),
            ("rate_3m", ["EUR", "USD"]),
            ("yield_10y", ["EUR", "USD"]),
            ("local_equity", ["EUR", "USD"]),
        )
        for ccy in currencies
        for row in [_role_row(role, ccy, f"{ccy}_{role}_PX_LAST")]
    )

    class _Api:
        def get_metadata(self):
            return type("R", (), {"frame": resolver_frame})()

    report = build_availability_report(
        {"G10": pd.DataFrame({"series_code": ["EURUSD_PX_LAST"]})}, _Api()
    )

    assert len(report) == 1
    row = report.iloc[0]
    assert row["base_swap_2y"] == "EUR_swap_2y_PX_LAST"
    assert row["quote_swap_2y"] == "USD_swap_2y_PX_LAST"
    assert pd.isna(row["base_cds_5y"])  # G10 never populates cds_5y


@pytest.mark.parametrize(
    "variant,series_code,resolver_rows",
    [
        (
            "G10",
            "EURUSD_PX_LAST",
            [
                _role_row(role, ccy, f"{ccy}_{role}_PX_LAST")
                for role in ("swap_2y", "rate_3m", "yield_10y", "local_equity")
                for ccy in ("EUR", "USD")
            ],
        ),
        (
            "EM",
            "USDZAR_PX_LAST",
            [
                _role_row(role, ccy, f"{ccy}_{role}_PX_LAST")
                for role in ("swap_2y", "local_equity")
                for ccy in ("USD", "ZAR")
            ]
            + [_role_row("cds_5y", "ZAR", "ZAR_cds_5y_PX_LAST")],
        ),
        (
            "CHN",
            "USDCNH_PX_LAST",
            [
                _role_row(role, ccy, f"{ccy}_{role}_PX_LAST")
                for role in ("swap_2y", "local_equity")
                for ccy in ("USD", "CNH")
            ]
            + [_role_row("cds_5y", "CNH", "CNHCDS_PX_LAST", is_synthetic=True)],
        ),
    ],
)
def test_from_report_row_round_trips_resolved_and_blocked(variant, series_code, resolver_rows):
    resolver = RoleResolver(pd.DataFrame(resolver_rows))
    original = assess_pair_availability(series_code, variant, resolver)

    class _Api:
        def get_metadata(self):
            return type("R", (), {"frame": pd.DataFrame(resolver_rows)})()

    report = build_availability_report({variant: pd.DataFrame({"series_code": [series_code]})}, _Api())
    row = report.iloc[0]

    rebuilt = PairAvailability.from_report_row(row)

    assert rebuilt.series_code == original.series_code
    assert rebuilt.variant == original.variant
    assert rebuilt.base_currency == original.base_currency
    assert rebuilt.quote_currency == original.quote_currency
    assert rebuilt.resolved == original.resolved
    assert rebuilt.blocked == original.blocked
    assert rebuilt.block_reasons == original.block_reasons


def test_from_report_row_round_trips_a_blocked_pair():
    resolver = RoleResolver(pd.DataFrame())  # nothing resolves

    class _Api:
        def get_metadata(self):
            return type("R", (), {"frame": pd.DataFrame()})()

    report = build_availability_report({"EM": pd.DataFrame({"series_code": ["USDZAR_PX_LAST"]})}, _Api())
    row = report.iloc[0]

    rebuilt = PairAvailability.from_report_row(row)
    original = assess_pair_availability("USDZAR_PX_LAST", "EM", resolver)

    assert rebuilt.blocked is True
    assert original.blocked is True
    assert rebuilt.resolved == original.resolved == {}
