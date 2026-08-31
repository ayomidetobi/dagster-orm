"""Unit tests for steer.config: StrategyConfig validation, YAML loading."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dagster_quickstart.steer.config import (
    StrategyConfig,
    load_all_strategy_configs,
    load_strategy_config,
)

_VALID_KWARGS = dict(
    universe="G10",
    window_months=12,
    stop_reward_ratio=2.0,
    logged_rate_threshold=0.01,
    global_equity_series="IDX0005_INDEX",
    commodity_series="XAU_PX_0032",
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
    assert config.global_equity_series == "IDX0005_INDEX"


def test_missing_driver_in_expected_signs_raises():
    kwargs = dict(_VALID_KWARGS)
    kwargs["expected_signs"] = {
        k: v for k, v in _VALID_KWARGS["expected_signs"].items() if k != "commodity"
    }

    with pytest.raises(ValidationError, match="missing driver"):
        StrategyConfig(**kwargs)


def test_missing_global_equity_series_raises():
    kwargs = {k: v for k, v in _VALID_KWARGS.items() if k != "global_equity_series"}

    with pytest.raises(ValidationError):
        StrategyConfig(**kwargs)


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
global_equity_series: IDX0005_INDEX
commodity_series: XAU_PX_0032
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
    assert config.commodity_series == "XAU_PX_0032"


def _yaml_text(universe: str, window_months: int) -> str:
    return (
        f"universe: {universe}\nwindow_months: {window_months}\nstop_reward_ratio: 2.0\n"
        "logged_rate_threshold: 0.01\nglobal_equity_series: IDX0005_INDEX\ncommodity_series: XAU_PX_0032\n"
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
