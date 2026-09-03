"""Unit tests for steer.config: StrategyConfig validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dagster_quickstart.steer.config import DRIVER_NAMES, GLOBAL_DRIVERS, StrategyConfig

_CHN_DRIVERS = DRIVER_NAMES + ("offshore_spread", "flows")

_VALID_KWARGS = dict(
    variant="G10",
    window_months=12,
    stop_reward_ratio=2.0,
    logged_rate_threshold=0.01,
    expected_signs={
        "interest_rate_differential": 1,
        "yield_curve_or_cds": -1,
        "local_equity": 1,
        "global_equity": 1,
        "commodity": 1,
    },
)


def test_valid_config_round_trips():
    config = StrategyConfig(**_VALID_KWARGS)

    assert config.variant == "G10"
    assert config.z_threshold == 1.5  # default
    assert config.global_equity_series == GLOBAL_DRIVERS.global_equity_series


def test_missing_driver_in_expected_signs_raises():
    kwargs = dict(_VALID_KWARGS)
    kwargs["expected_signs"] = {
        k: v for k, v in _VALID_KWARGS["expected_signs"].items() if k != "commodity"
    }

    with pytest.raises(ValidationError, match="missing driver"):
        StrategyConfig(**kwargs)


def test_global_equity_series_is_the_shared_instance_not_a_field():
    """global_equity_series/commodity_series aren't StrategyConfig fields --
    passing them explicitly is rejected (extra field forbidden), the same
    as any other typo'd/removed field."""
    kwargs = dict(_VALID_KWARGS, global_equity_series="SOME_OTHER_SERIES")

    with pytest.raises(ValidationError):
        StrategyConfig(**kwargs)


def test_every_variant_gets_the_identical_global_drivers():
    g10 = StrategyConfig(**{**_VALID_KWARGS, "variant": "G10"})
    em = StrategyConfig(**{**_VALID_KWARGS, "variant": "EM"})

    assert g10.global_equity_series == em.global_equity_series == GLOBAL_DRIVERS.global_equity_series
    assert g10.commodity_series == em.commodity_series == GLOBAL_DRIVERS.commodity_series


def test_unknown_variant_rejected():
    kwargs = dict(_VALID_KWARGS, variant="APAC")

    with pytest.raises(ValidationError):
        StrategyConfig(**kwargs)


def test_chn_is_a_valid_variant():
    config = StrategyConfig(**{**_VALID_KWARGS, "variant": "CHN"})

    assert config.variant == "CHN"


def test_extra_field_rejected():
    kwargs = dict(_VALID_KWARGS, made_up_field=123)

    with pytest.raises(ValidationError):
        StrategyConfig(**kwargs)


def test_drivers_defaults_to_the_five_canonical_names():
    config = StrategyConfig(**_VALID_KWARGS)

    assert config.drivers == DRIVER_NAMES


def test_chn_config_validates_with_seven_drivers():
    kwargs = dict(
        _VALID_KWARGS,
        variant="CHN",
        drivers=_CHN_DRIVERS,
        expected_signs={
            **_VALID_KWARGS["expected_signs"],
            "offshore_spread": 0,
            "flows": 0,
        },
    )

    config = StrategyConfig(**kwargs)

    assert config.drivers == _CHN_DRIVERS


def test_chn_drivers_reject_g10s_five_driver_expected_signs():
    """A 7-driver config's expected_signs must cover offshore_spread/flows too --
    G10's 5-key dict is missing them."""
    kwargs = dict(_VALID_KWARGS, variant="CHN", drivers=_CHN_DRIVERS)

    with pytest.raises(ValidationError, match="missing driver"):
        StrategyConfig(**kwargs)


def test_g10_drivers_reject_chns_seven_driver_expected_signs():
    """A 5-driver config's expected_signs can't carry offshore_spread/flows --
    they aren't in its drivers."""
    kwargs = dict(
        _VALID_KWARGS,
        expected_signs={**_VALID_KWARGS["expected_signs"], "offshore_spread": 0, "flows": 0},
    )

    with pytest.raises(ValidationError, match="not in this config's drivers"):
        StrategyConfig(**kwargs)
