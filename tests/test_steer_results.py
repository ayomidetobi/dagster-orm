"""Unit tests for steer.results: SteerResult / build_steer_result."""

from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd
import pytest

from dagster_quickstart.rewrite.data_api.factory import create_data_api
from dagster_quickstart.steer.analytics.estimation import sign_check_and_reestimate
from dagster_quickstart.steer.analytics.results import SteerResult, build_steer_result


class _EmptyMetadataStorage:
    def get_metadata(self, **kwargs):
        return pd.DataFrame()

    def get_columns(self):
        return []

    def get_distinct_values(self, *args, **kwargs):
        return []

    def save_metadata(self, *args, **kwargs):
        raise NotImplementedError

    def refresh_metadata(self):
        pass


class _EmptyValueStorage:
    def get_values(self, *args, **kwargs):
        return pd.DataFrame()

    def get_last_values(self, *args, **kwargs):
        return pd.DataFrame()

    def value_exists(self, *args, **kwargs):
        return {}

    def save_values(self, *args, **kwargs):
        raise NotImplementedError

    def delete_values(self, *args, **kwargs):
        raise NotImplementedError

    def get_storage_path(self):
        return None


@pytest.fixture
def data_api():
    """A real DataAPI (create_data_api) over a real in-memory duckdb connection -- the metadata/
    value repositories are never touched by SteerResult.save()/.load(), which only ever use
    data_api.write_table()/.read_table() (see steer/results.py)."""
    return create_data_api(
        duckdb_connection=duckdb.connect(":memory:"),
        metadata_repository=_EmptyMetadataStorage(),
        value_repository=_EmptyValueStorage(),
    )


@pytest.fixture
def cointegrated_system():
    """Same construction as test_steer_estimation.py's fixture -- rate is a real
    linear combination of the 5 drivers plus stationary noise."""
    rng = np.random.default_rng(42)
    n = 400
    dates = pd.bdate_range("2023-01-02", periods=n)

    ird = np.cumsum(rng.normal(0, 0.02, n))
    yc = np.cumsum(rng.normal(0, 0.02, n))
    leq = np.cumsum(rng.normal(0, 0.02, n))
    geq = np.cumsum(rng.normal(0, 0.02, n))
    comm = 50 + np.cumsum(rng.normal(0, 0.3, n))
    noise = rng.normal(0, 0.01, n)

    rate = 1.1 + 0.5 * ird - 0.3 * yc + 0.2 * leq + 0.1 * geq + 0.004 * comm + noise

    rate_series = pd.Series(rate, index=dates)
    drivers = pd.DataFrame(
        {
            "interest_rate_differential": ird,
            "yield_curve_or_cds": yc,
            "local_equity": leq,
            "global_equity": geq,
            "commodity": comm,
        },
        index=dates,
    )
    return rate_series, drivers


def _estimate(cointegrated_system, **overrides):
    rate, drivers = cointegrated_system
    as_of = overrides.pop("as_of", rate.index[-1])
    return sign_check_and_reestimate(
        rate,
        drivers,
        as_of=as_of,
        window_months=overrides.pop("window_months", 12),
        is_logged=False,
        expected_signs={
            "interest_rate_differential": 1,
            "yield_curve_or_cds": -1,
            "local_equity": 1,
            "global_equity": 1,
            "commodity": 1,
        },
        min_observations=40,
    )


def _build(cointegrated_system, **kwargs):
    rate, drivers = cointegrated_system
    as_of = rate.index[-1]
    estimate = _estimate(cointegrated_system, as_of=as_of)
    return build_steer_result(
        "AUDJPY_SPOT_0004",
        "G10",
        rate,
        drivers,
        estimate=estimate,
        window_months=12,
        **kwargs,
    )


def test_build_steer_result_time_series_share_one_index(cointegrated_system):
    result = _build(cointegrated_system)

    index = result.spot.index
    for series in list(result.drivers.values()) + [
        result.response,
        result.fitted,
        result.residual,
        result.upper_bound,
        result.lower_bound,
    ]:
        assert series.index.equals(index)


def _build_logged(cointegrated_system, **kwargs):
    """Same fixture/pair, fit in log space -- rate stays comfortably positive (~1.11-1.42)
    throughout this fixture's window, so log() is well-defined."""
    rate, drivers = cointegrated_system
    as_of = rate.index[-1]
    estimate = sign_check_and_reestimate(
        rate,
        drivers,
        as_of=as_of,
        window_months=12,
        is_logged=True,
        expected_signs={
            "interest_rate_differential": 1,
            "yield_curve_or_cds": -1,
            "local_equity": 1,
            "global_equity": 1,
            "commodity": 1,
        },
        min_observations=40,
    )
    return build_steer_result(
        "AUDJPY_SPOT_0004", "G10", rate, drivers, estimate=estimate, window_months=12, **kwargs
    )


def test_fair_value_is_on_the_same_scale_as_spot_when_logged(cointegrated_system):
    """Regression test for the bug: fitted lives in log space when is_logged=True (~log(1.2)
    =~0.18 here), while spot is always a raw rate level (~1.2) -- plot() used to draw them on
    the same axes. fair_value undoes the log, so the ratio to spot should be plausible (close
    to 1), never off by orders of magnitude."""
    result = _build_logged(cointegrated_system)

    ratio = result.fair_value.iloc[-1] / result.spot.iloc[-1]

    assert 0.5 <= ratio <= 2.0
    # fitted itself (log space) is NOT on the same scale as spot -- the bug this fixes.
    assert not (0.5 <= result.fitted.iloc[-1] / result.spot.iloc[-1] <= 2.0)


def test_fair_value_equals_fitted_when_not_logged(cointegrated_system):
    result = _build(cointegrated_system)

    pd.testing.assert_series_equal(result.fair_value, result.fitted, check_names=False)


def test_fair_value_upper_lower_bracket_fair_value(cointegrated_system):
    """exp() of a symmetric log-space band is asymmetric around exp(fitted) -- the log-normal
    shape, not a bug -- but fair_value_lower <= fair_value <= fair_value_upper must still hold."""
    result = _build_logged(cointegrated_system)

    assert (result.fair_value_lower <= result.fair_value + 1e-9).all()
    assert (result.fair_value - 1e-9 <= result.fair_value_upper).all()
    # The asymmetry itself: the gap up and the gap down are NOT equal (unlike upper_bound/
    # lower_bound around fitted, which are symmetric by construction in log space).
    up_gap = (result.fair_value_upper - result.fair_value).iloc[-1]
    down_gap = (result.fair_value - result.fair_value_lower).iloc[-1]
    assert up_gap != pytest.approx(down_gap)


def test_plot_only_renders_rate_level_series(cointegrated_system):
    """Every line/fill plot() draws must be a rate level (fair_value family), never a
    log-space series (fitted/upper_bound/lower_bound) -- checked by comparing the actual
    y-data matplotlib received against both candidates."""
    matplotlib = pytest.importorskip("matplotlib")
    matplotlib.use("Agg")

    result = _build_logged(cointegrated_system, signal_target=1.3, signal_stop_loss=1.1)
    ax = result.plot()

    lines_by_label = {line.get_label(): line for line in ax.get_lines()}
    assert lines_by_label["spot"].get_ydata() == pytest.approx(result.spot.to_numpy())
    assert lines_by_label["fair value (STEER)"].get_ydata() == pytest.approx(
        result.fair_value.to_numpy()
    )
    # NOT the log-space series -- this is what would have regressed if plot() still used fitted.
    assert lines_by_label["fair value (STEER)"].get_ydata() != pytest.approx(
        result.fitted.to_numpy()
    )

    fill = ax.collections[0]
    fill_ymin, fill_ymax = fill.get_paths()[0].vertices[:, 1].min(), fill.get_paths()[0].vertices[:, 1].max()
    assert fill_ymax == pytest.approx(result.fair_value_upper.max(), rel=1e-6)
    assert fill_ymin == pytest.approx(result.fair_value_lower.min(), rel=1e-6)


def test_fx_is_an_alias_for_spot(cointegrated_system):
    result = _build(cointegrated_system)

    assert result.fx is result.spot


def test_z_score_and_dropped_variables_agree_with_the_estimate_it_was_built_from(
    cointegrated_system,
):
    """Regression: build_steer_result used to independently re-fit with every
    driver and no sign check, so its z_score could silently disagree with the
    real SteerEstimate whenever a driver had been dropped. Taking the
    estimate as input makes them agree by construction."""
    estimate = _estimate(cointegrated_system)
    rate, drivers = cointegrated_system

    result = build_steer_result(
        "AUDJPY_SPOT_0004", "G10", rate, drivers, estimate=estimate, window_months=12
    )

    assert result.z_score == estimate.z_score
    assert result.dropped_variables == estimate.dropped_variables


def test_dropped_driver_is_excluded_from_the_design_matrix(cointegrated_system):
    """A driver sign_check_and_reestimate dropped shouldn't be in the regression's
    X matrix -- refitting on it would be exactly the disagreement bug this fixes."""
    rate, drivers = cointegrated_system
    as_of = rate.index[-1]
    # Force a drop: expected sign for local_equity contradicts its true (positive) coefficient.
    estimate = sign_check_and_reestimate(
        rate,
        drivers,
        as_of=as_of,
        window_months=12,
        is_logged=False,
        expected_signs={
            "interest_rate_differential": 1,
            "yield_curve_or_cds": -1,
            "local_equity": -1,
            "global_equity": 1,
            "commodity": 1,
        },
        min_observations=40,
    )
    assert estimate.dropped_variables  # sanity: a drop actually happened

    result = build_steer_result(
        "AUDJPY_SPOT_0004", "G10", rate, drivers, estimate=estimate, window_months=12
    )

    assert set(result.dropped_variables) <= set(result.drivers)  # still reported as a time series
    for dropped in result.dropped_variables:
        assert dropped not in result.design.columns


def test_design_matches_kept_driver_columns(cointegrated_system):
    result = _build(cointegrated_system)

    assert list(result.design.columns) == [
        "const",
        "interest_rate_differential",
        "yield_curve_or_cds",
        "local_equity",
        "global_equity",
        "commodity",
    ]
    assert result.design.index.equals(result.spot.index)


def test_trigger_band_is_symmetric_around_fitted(cointegrated_system):
    """upper_bound/lower_bound = fitted +/- z_threshold * residual_std -- always
    symmetric around fitted, by construction (not a statistical confidence
    interval, which wouldn't generally be symmetric or this wide)."""
    result = _build(cointegrated_system)

    assert (result.lower_bound <= result.fitted + 1e-9).all()
    assert (result.fitted - 1e-9 <= result.upper_bound).all()


def test_trigger_band_matches_the_default_z_threshold(cointegrated_system):
    rate, drivers = cointegrated_system
    estimate = _estimate(cointegrated_system)
    residual_std = estimate.residual_std

    result = build_steer_result(
        "AUDJPY_SPOT_0004", "G10", rate, drivers, estimate=estimate, window_months=12
    )

    assert result.upper_bound.iloc[-1] == pytest.approx(result.fitted.iloc[-1] + 1.5 * residual_std)
    assert result.lower_bound.iloc[-1] == pytest.approx(result.fitted.iloc[-1] - 1.5 * residual_std)


def test_trigger_band_uses_the_given_z_threshold_not_the_default(cointegrated_system):
    rate, drivers = cointegrated_system
    estimate = _estimate(cointegrated_system)
    residual_std = estimate.residual_std

    result = build_steer_result(
        "AUDJPY_SPOT_0004", "G10", rate, drivers, estimate=estimate, window_months=12,
        z_threshold=2.0,
    )

    assert result.upper_bound.iloc[-1] == pytest.approx(result.fitted.iloc[-1] + 2.0 * residual_std)


def test_band_and_signal_trigger_agree_at_the_latest_date(cointegrated_system):
    """Acceptance criterion: abs(spot - fitted) > (upper_bound - fitted) at the latest date
    IFF abs(z_score) > z_threshold. spot and response coincide here because is_logged=False
    (see _build/_estimate) -- residual_std is computed identically (ddof=0) in both
    estimate_steer (which produces z_score) and build_steer_result (which produces the
    band), so the two are guaranteed to agree by construction, not by coincidence."""
    rate, drivers = cointegrated_system
    z_threshold = 1.5

    for expected_signs in (
        {  # signs that keep every driver -> a "normal" z_score
            "interest_rate_differential": 1, "yield_curve_or_cds": -1,
            "local_equity": 1, "global_equity": 1, "commodity": 1,
        },
        {name: 0 for name in drivers.columns},  # never drops -- exercises the other branch too
    ):
        estimate = sign_check_and_reestimate(
            rate, drivers, as_of=rate.index[-1], window_months=12, is_logged=False,
            expected_signs=expected_signs, min_observations=40,
        )
        result = build_steer_result(
            "AUDJPY_SPOT_0004", "G10", rate, drivers, estimate=estimate, window_months=12,
            z_threshold=z_threshold,
        )

        band_says_beyond = abs(result.spot.iloc[-1] - result.fitted.iloc[-1]) > (
            result.upper_bound.iloc[-1] - result.fitted.iloc[-1]
        )
        z_says_beyond = abs(estimate.z_score) > z_threshold

        assert band_says_beyond == z_says_beyond


def test_coefficient_recovers_true_relationship(cointegrated_system):
    result = _build(cointegrated_system)

    assert result.coefficient["interest_rate_differential"] == pytest.approx(0.5, abs=0.05)
    assert result.p_values["interest_rate_differential"] < 0.05
    assert result.standard_error["interest_rate_differential"] > 0


def test_upper_lower_default_to_none_without_a_signal(cointegrated_system):
    result = _build(cointegrated_system)

    assert result.upper is None
    assert result.lower is None
    assert result.markov_state is None


def test_upper_lower_set_from_signal_target_stop(cointegrated_system):
    result = _build(cointegrated_system, signal_target=1.5, signal_stop_loss=1.0)

    assert result.upper == 1.5
    assert result.lower == 1.0


def test_cointegration_passed_recorded_from_the_passed_in_result(cointegrated_system):
    from dagster_quickstart.steer.analytics.estimation import CointegrationResult

    cointegration = CointegrationResult(
        as_of=pd.Timestamp.now(),
        passed=True,
        p_value=0.01,
        test_statistic=-5.0,
        critical_values=(-3.5, -2.9, -2.6),
        n_obs=200,
    )

    result = _build(cointegrated_system, cointegration=cointegration)

    assert result.cointegration_passed is True


def test_cointegration_passed_none_without_a_cointegration_result(cointegrated_system):
    result = _build(cointegrated_system)

    assert result.cointegration_passed is None


def test_to_frame_is_one_row_per_date_all_series_as_columns(cointegrated_system):
    result = _build(cointegrated_system)

    frame = result.to_frame()

    assert frame.index.equals(result.spot.index)
    assert set(frame.columns) == {
        "spot",
        "local_equity",
        "interest_rate_differential",
        "yield_curve_or_cds",
        "global_equity",
        "commodity",
        "response",
        "fitted",
        "fair_value",
        "residual",
        "upper_bound",
        "lower_bound",
    }


def test_cross_section_is_one_flat_row_with_named_coefficients(cointegrated_system):
    result = _build(cointegrated_system, signal_target=1.5, signal_stop_loss=1.0)

    row = result.cross_section()

    assert row["series_code"] == "AUDJPY_SPOT_0004"
    assert row["universe"] == "G10"
    assert row["upper"] == 1.5
    assert row["coefficient_interest_rate_differential"] == pytest.approx(0.5, abs=0.05)
    assert "standard_error_interest_rate_differential" in row
    assert "p_value_interest_rate_differential" in row
    assert "dropped_variables" in row


def test_cross_section_rows_concat_into_a_comparison_table(cointegrated_system):
    """The documented cross-pair use case: stack several pairs' cross_section() rows."""
    result_a = _build(cointegrated_system)
    result_b = _build(cointegrated_system)

    table = pd.DataFrame([result_a.cross_section(), result_b.cross_section()])

    assert len(table) == 2
    assert "z_score" in table.columns


def test_save_and_load_round_trips_every_field(cointegrated_system, data_api):
    result = _build(cointegrated_system, signal_target=1.5, signal_stop_loss=1.0)

    result.save(data_api)
    loaded = SteerResult.load(data_api, "AUDJPY_SPOT_0004")

    assert loaded.series_code == result.series_code
    assert loaded.universe == result.universe
    assert loaded.is_logged == result.is_logged
    assert loaded.upper == result.upper
    assert loaded.lower == result.lower
    assert loaded.markov_state == result.markov_state
    assert loaded.dropped_variables == result.dropped_variables
    assert loaded.z_score == pytest.approx(result.z_score)
    pd.testing.assert_frame_equal(
        loaded.to_frame(), result.to_frame(), check_names=False, check_freq=False
    )
    pd.testing.assert_series_equal(
        loaded.fair_value, result.fair_value, check_names=False, check_freq=False
    )
    assert loaded.cross_section()["fair_value"] == pytest.approx(result.cross_section()["fair_value"])
    pd.testing.assert_series_equal(
        loaded.coefficient.sort_index(),
        result.coefficient.sort_index(),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        loaded.p_values.sort_index(), result.p_values.sort_index(), check_names=False
    )


def test_save_twice_appends_a_new_snapshot_load_returns_the_latest(cointegrated_system, data_api):
    rate, drivers = cointegrated_system
    earlier_as_of = rate.index[-30]
    later_as_of = rate.index[-1]

    earlier = build_steer_result(
        "AUDJPY_SPOT_0004",
        "G10",
        rate,
        drivers,
        estimate=_estimate(cointegrated_system, as_of=earlier_as_of),
        window_months=12,
    )
    later = build_steer_result(
        "AUDJPY_SPOT_0004",
        "G10",
        rate,
        drivers,
        estimate=_estimate(cointegrated_system, as_of=later_as_of),
        window_months=12,
    )
    earlier.save(data_api)
    later.save(data_api)

    loaded_latest = SteerResult.load(data_api, "AUDJPY_SPOT_0004")
    assert loaded_latest.as_of == later_as_of

    loaded_earlier = SteerResult.load(data_api, "AUDJPY_SPOT_0004", as_of=earlier_as_of)
    assert loaded_earlier.as_of == earlier_as_of


def test_load_missing_pair_raises_lookup_error(data_api):
    with pytest.raises(LookupError):
        SteerResult.load(data_api, "NOT_A_REAL_PAIR")


def test_a_chn_style_seven_driver_result_round_trips_through_storage(data_api):
    """SteerResult used to hardcode 5 driver dataclass fields -- no room for
    CHN's 2 extras. A 7-driver result must save/load cleanly, and coexist
    in the same summary table as a 5-driver G10 result (DataAPI.write_table()'s
    column widening -- see rewrite.data_api.repositories.generic_table_repository)."""
    rng = np.random.default_rng(3)
    n = 300
    dates = pd.bdate_range("2023-01-02", periods=n)
    drivers = pd.DataFrame(
        {
            "interest_rate_differential": np.cumsum(rng.normal(0, 0.02, n)),
            "yield_curve_or_cds": np.cumsum(rng.normal(0, 0.02, n)),
            "local_equity": np.cumsum(rng.normal(0, 0.02, n)),
            "global_equity": np.cumsum(rng.normal(0, 0.02, n)),
            "commodity": np.cumsum(rng.normal(0, 0.02, n)),
            "offshore_spread": np.cumsum(rng.normal(0, 0.02, n)),
            "flows": np.cumsum(rng.normal(0, 0.02, n)),
        },
        index=dates,
    )
    rate = pd.Series(7.0 + drivers.sum(axis=1) * 0.05 + rng.normal(0, 0.01, n), index=dates)
    as_of = dates[-1]
    estimate = sign_check_and_reestimate(
        rate,
        drivers,
        as_of=as_of,
        window_months=6,
        is_logged=False,
        expected_signs={name: 0 for name in drivers.columns},
        min_observations=40,
    )

    result = build_steer_result(
        "USDCNH_PX_LAST", "CHN", rate, drivers, estimate=estimate, window_months=6
    )
    result.save(data_api)

    loaded = SteerResult.load(data_api, "USDCNH_PX_LAST")
    assert set(loaded.drivers) == set(drivers.columns)
    assert loaded.drivers["offshore_spread"].index.equals(loaded.spot.index)


def test_g10_and_chn_summaries_coexist_with_different_driver_columns(data_api, cointegrated_system):
    g10_result = _build(cointegrated_system)
    g10_result.save(data_api)

    rng = np.random.default_rng(9)
    n = 300
    dates = pd.bdate_range("2023-01-02", periods=n)
    drivers = pd.DataFrame(
        {name: np.cumsum(rng.normal(0, 0.02, n)) for name in [
            "interest_rate_differential", "yield_curve_or_cds", "local_equity",
            "global_equity", "commodity", "offshore_spread", "flows",
        ]},
        index=dates,
    )
    rate = pd.Series(7.0 + drivers.sum(axis=1) * 0.05 + rng.normal(0, 0.01, n), index=dates)
    as_of = dates[-1]
    estimate = sign_check_and_reestimate(
        rate, drivers, as_of=as_of, window_months=6, is_logged=False,
        expected_signs={name: 0 for name in drivers.columns}, min_observations=40,
    )
    chn_result = build_steer_result(
        "USDCNH_PX_LAST", "CHN", rate, drivers, estimate=estimate, window_months=6
    )
    chn_result.save(data_api)

    g10_loaded = SteerResult.load(data_api, "AUDJPY_SPOT_0004")
    chn_loaded = SteerResult.load(data_api, "USDCNH_PX_LAST")
    assert set(g10_loaded.drivers) == {
        "interest_rate_differential", "yield_curve_or_cds", "local_equity", "global_equity", "commodity",
    }
    assert "offshore_spread" in chn_loaded.drivers
