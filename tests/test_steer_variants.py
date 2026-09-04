"""Tests for steer.config's FXVariant, FX_G10/FX_EM/FX_CHN, VARIANTS, default_data_api().

Acceptance criterion 1 (FX_G10.fit() matches the explicit load_strategy_config +
Steer.from_data_api path) is proven by wiring both through the identical FXVariant instance
and the identical fake data_api -- .steer()/.fit() are a thin delegation onto
Steer.from_data_api(), not a second implementation, so this is enough to catch a regression in
the wiring itself; the pipeline's actual correctness is tests/test_steer_model.py's job.
"""

from __future__ import annotations

import importlib
import sys

import pytest
from pydantic import ValidationError

from dagster_quickstart.steer.config import DRIVER_NAMES, FX_CHN, FX_EM, FX_G10, VARIANTS
from dagster_quickstart.steer.model import Steer
from tests.test_steer_assets import (
    FakeRewriteDataAPIResource,
    _unblocked_g10_metadata,
    _unblocked_g10_values,
)
from tests.test_steer_model import (
    _chn_metadata,
    _chn_values,
    _em_metadata,
    _em_values,
    _write_availability_report,
)


def test_variants_dict_keys_by_variant():
    assert VARIANTS == {"G10": FX_G10, "EM": FX_EM, "CHN": FX_CHN}


def test_g10_and_em_use_the_five_canonical_drivers():
    assert FX_G10.drivers == DRIVER_NAMES
    assert FX_EM.drivers == DRIVER_NAMES


def test_chn_has_seven_drivers_and_the_tightened_cointegration_significance():
    """Acceptance criterion 6."""
    assert FX_CHN.drivers == DRIVER_NAMES + ("offshore_spread", "flows")
    assert FX_CHN.cointegration_significance == 0.01


def test_fx_variant_is_frozen():
    """Acceptance criterion 5: FX_G10/FX_EM/FX_CHN are shared module-level singletons --
    mutating one must be rejected, not silently change every other caller's behavior."""
    with pytest.raises(ValidationError):
        FX_G10.z_threshold = 2.0


def test_model_copy_produces_an_independent_variant():
    """The docstring's supported way to experiment with a variant, without mutating FX_G10."""
    variant = FX_G10.model_copy(update={"z_threshold": 2.0})

    assert variant.z_threshold == 2.0
    assert FX_G10.z_threshold == 1.5


@pytest.mark.parametrize(
    "variant_obj,build_metadata,build_values,series_code",
    [
        (FX_G10, _unblocked_g10_metadata, _unblocked_g10_values, "EURUSD_PX_LAST"),
        (FX_EM, _em_metadata, _em_values, "USDZAR_PX_LAST"),
        (FX_CHN, _chn_metadata, _chn_values, "USDCNH_PX_LAST"),
    ],
)
def test_fit_matches_the_explicit_steer_from_data_api_path(
    variant_obj, build_metadata, build_values, series_code
):
    """Acceptance criterion 1, for one G10, one EM, and one CHN pair."""
    resource = FakeRewriteDataAPIResource(build_metadata(), build_values())
    _write_availability_report(resource.api, variant_obj.variant)

    via_variant = variant_obj.fit(data_api=resource.api, lookback_days=1, cointegration="each")
    via_explicit = Steer.from_data_api(
        resource.api, variant=variant_obj.variant, strategy_config=variant_obj
    ).fit(lookback_days=1, cointegration="each")

    fitted_via_variant = via_variant[series_code]
    fitted_via_explicit = via_explicit[series_code]

    assert fitted_via_variant.coefficient.equals(fitted_via_explicit.coefficient)
    assert fitted_via_variant.z_score == fitted_via_explicit.z_score
    assert fitted_via_variant.cointegration_passed == fitted_via_explicit.cointegration_passed
    assert fitted_via_variant.dropped_variables == fitted_via_explicit.dropped_variables


def test_importing_config_constructs_no_data_api(monkeypatch):
    """Acceptance criterion 3: `import steer.config` must never open a Postgres/S3 connection."""

    def _boom(self, *args, **kwargs):
        raise AssertionError("DataAPI must not be constructed at import time")

    monkeypatch.setattr("dagster_quickstart.rewrite.data_api.api.data_api.DataAPI.__init__", _boom)

    sys.modules.pop("dagster_quickstart.steer.config", None)
    module = importlib.import_module("dagster_quickstart.steer.config")

    assert module.FX_G10.variant == "G10"
