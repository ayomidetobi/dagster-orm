"""Regression tests for steer.discovery.discover_pairs against the real catalog.

Loads dagster_quickstart/data/meta_series_steer.csv directly (the actual
shipped catalog, not a hand-built fixture) through the real QuerySet/
MetadataService stack -- same FakeMetadataStorage/FakeValueStorage pattern
as tests/test_dataset.py and tests/test_steer_assets.py, just pointed at
the real file so a future edit to the CSV (e.g. accidentally marking a
pair row is_synthetic=True) is caught here, not just in unit tests against
hand-built fixtures.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from dagster_quickstart.rewrite.data_api.dataset import DatasetBase
from dagster_quickstart.rewrite.data_api.factory import create_data_api
from dagster_quickstart.steer.source.discovery import (
    RoleResolver,
    assess_pair_availability,
    build_availability_report,
    discover_pairs,
)

CATALOG_PATH = Path(__file__).resolve().parents[1] / "dagster_quickstart" / "data" / "meta_series_steer.csv"


class FakeMetadataStorage:
    def __init__(self, frame: pd.DataFrame):
        self._frame = frame

    def get_columns(self) -> list[str]:
        return list(self._frame.columns)

    def _filtered(self, filters, *, exclude=False):
        frame = self._frame
        if filters:
            mask = pd.Series(True, index=frame.index)
            for field, values in filters.items():
                mask &= frame[field].isin(values)
            frame = frame[~mask] if exclude else frame[mask]
        return frame

    def get_metadata(self, filters=None, *, exclude=False, version=None, as_of=None):
        return self._filtered(filters, exclude=exclude).reset_index(drop=True)

    def get_distinct_values(self, column, *, filters=None, exclude=False):
        return sorted(self._filtered(filters, exclude=exclude)[column].dropna().unique().tolist())

    def save_metadata(self, frame, *, fresh=False):
        raise NotImplementedError

    def refresh_metadata(self):
        pass


class FakeValueStorage:
    def __init__(self):
        self._frame = pd.DataFrame(columns=["series_code", "timestamp", "value"])

    def get_values(self, series_codes, **kwargs):
        return self._frame

    def get_last_values(self, series_codes, **kwargs):
        return self._frame

    def value_exists(self, series_codes, **kwargs):
        return {code: False for code in series_codes}

    def save_values(self, frame):
        raise NotImplementedError

    def delete_values(self, filters):
        raise NotImplementedError

    def get_storage_path(self):
        return None


@pytest.fixture
def real_catalog_data_api():
    frame = pd.read_csv(CATALOG_PATH)
    api = create_data_api(
        duckdb_connection=object(),
        metadata_repository=FakeMetadataStorage(frame),
        value_repository=FakeValueStorage(),
    )
    DatasetBase.configure(api)
    yield api
    DatasetBase._api = None


@pytest.mark.parametrize("variant,expected_count", [("G10", 45), ("EM", 21), ("CHN", 1)])
def test_discover_pairs_finds_every_pair_in_the_real_catalog(
    real_catalog_data_api, variant, expected_count
):
    pairs = discover_pairs(variant, real_catalog_data_api)

    assert len(pairs) == expected_count


@pytest.mark.parametrize("variant", ["G10", "EM", "CHN"])
def test_require_real_still_returns_every_pair(real_catalog_data_api, variant):
    """The regression this guards against: pairs marked is_synthetic=True
    would vanish under require_real=True, and a production run that filters
    is_synthetic=False (the normal way to exclude the 24 placeholder driver
    rows) would discover zero pairs and estimate nothing."""
    pairs = discover_pairs(variant, real_catalog_data_api, require_real=True)

    assert not pairs.empty


def test_g10_is_the_full_45_pair_cross_with_no_reciprocals():
    frame = pd.read_csv(CATALOG_PATH)
    g10 = frame[(frame["sub_asset_class"] == "FX Spot") & (frame["market_development"] == "G10")]

    assert len(g10) == 45
    underlyings = set(g10["underlying"])
    for pair in ("EURNOK", "AUDJPY", "GBPSEK", "CHFJPY", "NOKSEK"):
        assert pair in underlyings

    legs = {(u[:3], u[3:]) for u in underlyings}
    reciprocals = [(base, quote) for base, quote in legs if (quote, base) in legs]
    assert reciprocals == []


def test_em_and_chn_pairs_are_all_usd_quoted():
    frame = pd.read_csv(CATALOG_PATH)
    em_chn = frame[
        (frame["sub_asset_class"] == "FX Spot") & (frame["market_development"].isin(["EM", "CHN"]))
    ]

    assert len(em_chn) == 22
    assert (em_chn["underlying"].str.startswith("USD")).all()


def test_no_fx_pair_row_is_synthetic():
    frame = pd.read_csv(CATALOG_PATH)
    fx = frame[frame["sub_asset_class"] == "FX Spot"]

    assert len(fx) == 67
    assert not fx["is_synthetic"].any()


@pytest.mark.parametrize("variant", ["G10", "EM", "CHN"])
def test_every_pair_in_the_real_catalog_resolves_with_no_blocks(real_catalog_data_api, variant):
    """Driver 2 (yield_curve_or_cds) is the part most at risk of a coverage
    gap: G10 needs rate_3m+yield_10y for both legs, EM/CHN need cds_5y for
    the non-USD leg. This proves every currency actually pulled into a pair
    -- all 10 G10, all 21 EM, CNH -- has full role coverage in the catalog,
    not just that the roles exist somewhere."""
    pairs = discover_pairs(variant, real_catalog_data_api)
    resolver = RoleResolver.from_data_api(real_catalog_data_api)

    blocked = [
        (series_code, assess_pair_availability(series_code, variant, resolver).block_reasons)
        for series_code in pairs["series_code"]
        if assess_pair_availability(series_code, variant, resolver).blocked
    ]

    assert blocked == []


def test_cnh_local_equity_resolves_to_the_real_series_not_the_synthetic_duplicate(
    real_catalog_data_api,
):
    """Acceptance criterion: CNH's local_equity role matches 2 real-catalog rows --
    CNHLIVEMSCI_PX_LAST (real) and CNHMSCI_PX_LAST (synthetic). is_synthetic no longer
    filters anything, but it must still decide this tie-break: real before synthetic."""
    resolver = RoleResolver.from_data_api(real_catalog_data_api)

    code, _ = resolver.resolve("local_equity", "CNH")

    assert code == "CNHLIVEMSCI_PX_LAST"


def test_a_full_availability_run_issues_one_get_metadata_call_for_role_resolution(
    real_catalog_data_api,
):
    """Acceptance criterion: role resolution across every G10 (45), EM (21) and CHN (1) pair
    -- ~106 distinct (role, currency) combinations -- costs exactly one get_metadata() call
    (RoleResolver.from_data_api), not one per combination. Pair discovery itself (one
    get_metadata() call per variant, to find the pairs at all) happens before the counter
    starts, since that's a separate concern from role resolution."""
    pairs_by_variant = {
        variant: discover_pairs(variant, real_catalog_data_api) for variant in ("G10", "EM", "CHN")
    }

    original_get_metadata = real_catalog_data_api.get_metadata
    call_count = {"n": 0}

    def counting_get_metadata(*args, **kwargs):
        call_count["n"] += 1
        return original_get_metadata(*args, **kwargs)

    real_catalog_data_api.get_metadata = counting_get_metadata

    report = build_availability_report(pairs_by_variant, real_catalog_data_api)

    assert call_count["n"] == 1
    assert len(report) == 45 + 21 + 1
