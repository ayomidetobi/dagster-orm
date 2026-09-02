"""Unit tests for steer.config: StrategyConfig validation, YAML loading."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dagster_quickstart.steer.config import (
    DRIVER_NAMES,
    GLOBAL_DRIVERS,
    StrategyConfig,
    load_all_strategy_configs,
    load_strategy_config,
)

_CHN_DRIVERS = DRIVER_NAMES + ("offshore_spread", "flows")

_VALID_KWARGS = dict(
    universe="G10",
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

    assert config.universe == "G10"
    assert config.z_threshold == 1.5  # default
    assert config.global_equity_series == GLOBAL_DRIVERS.global_equity_series


def test_missing_driver_in_expected_signs_raises():
    kwargs = dict(_VALID_KWARGS)
    kwargs["expected_signs"] = {
        k: v for k, v in _VALID_KWARGS["expected_signs"].items() if k != "commodity"
    }

    with pytest.raises(ValidationError, match="missing driver"):
        StrategyConfig(**kwargs)


def test_global_equity_series_is_the_shared_instance_not_a_yaml_field():
    """global_equity_series/commodity_series aren't StrategyConfig fields --
    passing them explicitly is rejected (extra field forbidden), the same
    as any other typo'd/removed field."""
    kwargs = dict(_VALID_KWARGS, global_equity_series="SOME_OTHER_SERIES")

    with pytest.raises(ValidationError):
        StrategyConfig(**kwargs)


def test_every_universe_gets_the_identical_global_drivers():
    g10 = StrategyConfig(**{**_VALID_KWARGS, "universe": "G10"})
    em = StrategyConfig(**{**_VALID_KWARGS, "universe": "EM"})

    assert g10.global_equity_series == em.global_equity_series == GLOBAL_DRIVERS.global_equity_series
    assert g10.commodity_series == em.commodity_series == GLOBAL_DRIVERS.commodity_series


def test_unknown_universe_rejected():
    kwargs = dict(_VALID_KWARGS, universe="APAC")

    with pytest.raises(ValidationError):
        StrategyConfig(**kwargs)


def test_chn_is_a_valid_universe():
    config = StrategyConfig(**{**_VALID_KWARGS, "universe": "CHN"})

    assert config.universe == "CHN"


def test_extra_field_rejected():
    kwargs = dict(_VALID_KWARGS, made_up_field=123)

    with pytest.raises(ValidationError):
        StrategyConfig(**kwargs)


def test_load_strategy_config_from_yaml(tmp_path):
    yaml_path = tmp_path / "g10.yaml"
    yaml_path.write_text(
        """
universe: G10
window_months: 12
stop_reward_ratio: 2.0
logged_rate_threshold: 0.01
expected_signs:
  interest_rate_differential: 1
  yield_curve_or_cds: -1
  local_equity: 1
  global_equity: 1
  commodity: 1
"""
    )

    config = load_strategy_config(yaml_path)

    assert config.universe == "G10"
    assert config.commodity_series == GLOBAL_DRIVERS.commodity_series


def _yaml_text(universe: str, window_months: int) -> str:
    return (
        f"universe: {universe}\nwindow_months: {window_months}\nstop_reward_ratio: 2.0\n"
        "logged_rate_threshold: 0.01\n"
        "expected_signs: {interest_rate_differential: 1, yield_curve_or_cds: -1, local_equity: 1, "
        "global_equity: 1, commodity: 1}\n"
    )


def test_load_all_strategy_configs_keys_by_universe(tmp_path):
    (tmp_path / "g10.yaml").write_text(_yaml_text("G10", 12))
    (tmp_path / "em.yaml").write_text(_yaml_text("EM", 6))
    (tmp_path / "chn.yaml").write_text(_yaml_text("CHN", 6))

    configs = load_all_strategy_configs(tmp_path)

    assert set(configs) == {"G10", "EM", "CHN"}
    assert configs["G10"].window_months == 12
    assert configs["EM"].window_months == 6


def test_load_all_strategy_configs_rejects_duplicate_universe(tmp_path):
    (tmp_path / "a.yaml").write_text(_yaml_text("G10", 12))
    (tmp_path / "b.yaml").write_text(_yaml_text("G10", 12))

    with pytest.raises(ValueError, match="Duplicate"):
        load_all_strategy_configs(tmp_path)


def test_real_strategy_configs_load_and_validate():
    """The actual dagster_quickstart/steer/strategy_configs/*.yaml files this app ships."""
    configs = load_all_strategy_configs()

    assert set(configs) == {"G10", "EM", "CHN"}
    assert configs["G10"].drivers == DRIVER_NAMES
    assert configs["EM"].drivers == DRIVER_NAMES
    assert configs["CHN"].drivers == _CHN_DRIVERS
    assert configs["CHN"].cointegration_significance == 0.01
    assert configs["CHN"].expected_signs["offshore_spread"] == 0


def test_drivers_defaults_to_the_five_canonical_names():
    config = StrategyConfig(**_VALID_KWARGS)

    assert config.drivers == DRIVER_NAMES


def test_chn_config_validates_with_seven_drivers():
    kwargs = dict(
        _VALID_KWARGS,
        universe="CHN",
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
    kwargs = dict(_VALID_KWARGS, universe="CHN", drivers=_CHN_DRIVERS)

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
