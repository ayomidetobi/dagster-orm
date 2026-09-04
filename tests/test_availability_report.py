"""Unit tests for dagster_quickstart.availability: FX-leg parsing, RoleResolver, and pair availability assessment.

Uses steer.config.STEER_AVAILABILITY_SPEC as the AvailabilitySpec under test throughout -- these
are regression tests for availability/'s generic machinery against STEER's real role/variant
vocabulary, not tests of an abstract spec nobody uses."""

from __future__ import annotations

import pandas as pd
import pytest

from dagster_quickstart.availability.pairs import parse_fx_legs
from dagster_quickstart.availability.report import (
    PairAvailability,
    assess_pair_availability,
    build_availability_report,
)
from dagster_quickstart.availability.roles import RoleResolver
from dagster_quickstart.availability.spec import AvailabilitySpec
from dagster_quickstart.steer.config import STEER_AVAILABILITY_SPEC

REQUIRED_ROLES = STEER_AVAILABILITY_SPEC.required_roles
ROLE_FILTERS = STEER_AVAILABILITY_SPEC.role_filters


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
        assert not args and not kwargs, (
            "RoleResolver.from_data_api must call get_metadata() with no filters"
        )
        self.call_count += 1
        return type("Result", (), {"frame": self._frame})()


def test_role_resolver_finds_matching_series():
    resolver = RoleResolver(
        pd.DataFrame([_role_row("swap_2y", "EUR", "EUR2YSW_PX_LAST")]), STEER_AVAILABILITY_SPEC
    )

    code, reason = resolver.resolve("swap_2y", "EUR")

    assert code == "EUR2YSW_PX_LAST"
    assert "EUR" in reason


def test_role_resolver_none_when_nothing_matches():
    resolver = RoleResolver(
        pd.DataFrame([_role_row("swap_2y", "EUR", "EUR2YSW_PX_LAST")]), STEER_AVAILABILITY_SPEC
    )

    code, reason = resolver.resolve("swap_2y", "GBP")

    assert code is None
    assert "GBP" in reason


def _spec_with(*, excluded_series_codes=None) -> AvailabilitySpec:
    """A minimal AvailabilitySpec for testing RoleResolver's ambiguity/exclusion mechanism
    directly -- generic, not STEER's real vocabulary (that's what STEER_AVAILABILITY_SPEC-based
    tests elsewhere in this file, and tests/test_steer_universe_datasets.py's real-catalog CNH
    test, are for)."""
    return AvailabilitySpec(
        role_filters={"swap_2y": {"sub_asset_class": ["Interest Rate Swap"], "tenor": ["2Y"]}},
        required_roles={"G10": (("swap_2y",), ())},
        single_non_usd_leg={"G10": False},
        single_non_usd_leg_reason="unused in this test",
        excluded_series_codes=excluded_series_codes or {},
        variants=("G10",),
    )


def test_role_resolver_records_and_logs_a_genuine_ambiguity(capfd):
    """resolve_role must record when more than one row matched, rather than silently taking the
    first row -- ambiguous role resolution is a catalog problem, visible the moment it happens
    (a logged warning naming the role, currency and candidates), not discovered later when a
    coefficient looks wrong."""
    spec = _spec_with()
    resolver = RoleResolver(
        pd.DataFrame(
            [
                _role_row("swap_2y", "EUR", "EUR2YSW_B_PX_LAST"),
                _role_row("swap_2y", "EUR", "EUR2YSW_A_PX_LAST"),
            ]
        ),
        spec,
    )

    assert resolver.ambiguities[("swap_2y", "EUR")] == (
        "EUR2YSW_A_PX_LAST",
        "EUR2YSW_B_PX_LAST",
    )
    # Still resolves deterministically (alphabetically-first) -- ambiguous doesn't mean broken,
    # it means visible.
    code, _ = resolver.resolve("swap_2y", "EUR")
    assert code == "EUR2YSW_A_PX_LAST"

    captured = capfd.readouterr()
    combined = captured.out + captured.err
    assert "availability_role_ambiguous" in combined
    assert "swap_2y" in combined
    assert "EUR" in combined
    assert "EUR2YSW_A_PX_LAST" in combined
    assert "EUR2YSW_B_PX_LAST" in combined


def test_role_resolver_does_not_flag_a_currency_with_only_one_match():
    spec = _spec_with()
    resolver = RoleResolver(pd.DataFrame([_role_row("swap_2y", "EUR", "EUR2YSW_PX_LAST")]), spec)

    assert resolver.ambiguities == {}


def test_excluded_series_codes_prevents_the_ambiguity_from_ever_being_recorded():
    """The fix for a real ambiguity (see STEER_AVAILABILITY_SPEC.excluded_series_codes / CNH's
    local_equity) is an explicit exclusion, applied before candidates are grouped -- so the
    excluded row never even reaches the point where it could create an ambiguity, rather than
    winning some other tie-break."""
    spec = _spec_with(excluded_series_codes={"swap_2y": ("EUR2YSW_B_PX_LAST",)})
    resolver = RoleResolver(
        pd.DataFrame(
            [
                _role_row("swap_2y", "EUR", "EUR2YSW_B_PX_LAST"),
                _role_row("swap_2y", "EUR", "EUR2YSW_A_PX_LAST"),
            ]
        ),
        spec,
    )

    assert ("swap_2y", "EUR") not in resolver.ambiguities
    code, _ = resolver.resolve("swap_2y", "EUR")
    assert code == "EUR2YSW_A_PX_LAST"


def test_role_resolver_from_data_api_calls_get_metadata_exactly_once_with_no_filters():
    frame = pd.DataFrame([_role_row("swap_2y", "EUR", "EUR2YSW_PX_LAST")])
    api = _FakeMetadataAPI(frame)

    resolver = RoleResolver.from_data_api(api, STEER_AVAILABILITY_SPEC)

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
    return RoleResolver(pd.DataFrame(rows), STEER_AVAILABILITY_SPEC)


def test_g10_pair_fully_available_needs_all_four_roles_both_legs():
    resolver = _g10_catalog()

    availability = assess_pair_availability(
        "EURUSD_PX_LAST", "G10", resolver, STEER_AVAILABILITY_SPEC
    )

    assert availability.blocked is False
    assert availability.base_currency == "EUR"
    assert availability.quote_currency == "USD"
    for role in REQUIRED_ROLES["G10"][0]:
        assert availability.get("base", role) is not None
        assert availability.get("quote", role) is not None


def test_g10_pair_missing_one_leg_one_role_is_blocked_with_readable_reason():
    resolver = _g10_catalog()
    # Drop GBP's rate_3m entirely by using a pair whose quote (GBP) has no coverage at all.
    availability = assess_pair_availability(
        "EURGBP_PX_LAST", "G10", resolver, STEER_AVAILABILITY_SPEC
    )

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
    return RoleResolver(pd.DataFrame(rows), STEER_AVAILABILITY_SPEC)


def test_em_pair_cds_role_required_only_for_non_usd_leg():
    resolver = _em_catalog()

    availability = assess_pair_availability(
        "USDZAR_PX_LAST", "EM", resolver, STEER_AVAILABILITY_SPEC
    )

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
        ),
        STEER_AVAILABILITY_SPEC,
    )

    availability = assess_pair_availability(
        "USDZAR_PX_LAST", "EM", resolver, STEER_AVAILABILITY_SPEC
    )

    assert availability.blocked is True
    assert any("cds_5y" in key for key in availability.missing_reasons)


def test_em_cross_with_no_usd_leg_is_blocked_as_a_structural_invariant():
    """TRYZAR has two non-USD legs -- EM driver 2 (the non-USD leg's 5Y CDS, a
    single-country level) has no principled candidate when both legs are
    non-USD. Blocked outright, without even attempting role resolution."""
    resolver = _em_catalog()  # has full TRY/ZAR role coverage if it were ever queried

    availability = assess_pair_availability(
        "TRYZAR_PX_LAST", "EM", resolver, STEER_AVAILABILITY_SPEC
    )

    assert availability.blocked is True
    assert any("USD-quoted by construction" in reason for reason in availability.block_reasons)
    assert availability.resolved == {}  # no role resolution attempted at all


def test_chn_cross_with_no_usd_leg_is_blocked_as_a_structural_invariant():
    resolver = RoleResolver(pd.DataFrame(), STEER_AVAILABILITY_SPEC)

    availability = assess_pair_availability(
        "CNHJPY_PX_LAST", "CHN", resolver, STEER_AVAILABILITY_SPEC
    )

    assert availability.blocked is True
    assert any("USD-quoted by construction" in reason for reason in availability.block_reasons)


def test_unparseable_series_code_reports_blocked():
    resolver = RoleResolver(pd.DataFrame(), STEER_AVAILABILITY_SPEC)

    availability = assess_pair_availability("NOT_A_PAIR", "G10", resolver, STEER_AVAILABILITY_SPEC)

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
        {"G10": pd.DataFrame({"series_code": ["EURUSD_PX_LAST"]})}, _Api(), STEER_AVAILABILITY_SPEC
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
    resolver = RoleResolver(pd.DataFrame(resolver_rows), STEER_AVAILABILITY_SPEC)
    original = assess_pair_availability(series_code, variant, resolver, STEER_AVAILABILITY_SPEC)

    class _Api:
        def get_metadata(self):
            return type("R", (), {"frame": pd.DataFrame(resolver_rows)})()

    report = build_availability_report(
        {variant: pd.DataFrame({"series_code": [series_code]})}, _Api(), STEER_AVAILABILITY_SPEC
    )
    row = report.iloc[0]

    rebuilt = PairAvailability.from_report_row(row, STEER_AVAILABILITY_SPEC)

    assert rebuilt.series_code == original.series_code
    assert rebuilt.variant == original.variant
    assert rebuilt.base_currency == original.base_currency
    assert rebuilt.quote_currency == original.quote_currency
    assert rebuilt.resolved == original.resolved
    assert rebuilt.blocked == original.blocked
    assert rebuilt.block_reasons == original.block_reasons


def test_from_report_row_round_trips_a_blocked_pair():
    resolver = RoleResolver(pd.DataFrame(), STEER_AVAILABILITY_SPEC)  # nothing resolves

    class _Api:
        def get_metadata(self):
            return type("R", (), {"frame": pd.DataFrame()})()

    report = build_availability_report(
        {"EM": pd.DataFrame({"series_code": ["USDZAR_PX_LAST"]})}, _Api(), STEER_AVAILABILITY_SPEC
    )
    row = report.iloc[0]

    rebuilt = PairAvailability.from_report_row(row, STEER_AVAILABILITY_SPEC)
    original = assess_pair_availability("USDZAR_PX_LAST", "EM", resolver, STEER_AVAILABILITY_SPEC)

    assert rebuilt.blocked is True
    assert original.blocked is True
    assert rebuilt.resolved == original.resolved == {}
