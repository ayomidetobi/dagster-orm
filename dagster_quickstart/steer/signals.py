"""BUY/SELL/NONE signal generation from a SteerEstimate + CointegrationResult.

Pure function, no Dagster/DuckLake -- see tests/test_steer_signals.py.

Sign convention (stated explicitly since it's a self-consistent choice, not
a law of physics): a positive z-score means the actual rate is trading
*above* its STEER fair value (rich) -- mean-reversion says SELL. A negative
z-score means the rate is *below* fair value (cheap) -- BUY. This matches
the residual convention in steer.estimation.estimate_steer
(z = (actual - fitted) / residual_std).

Target/stop-loss convention: target is the fitted STEER value itself (the
level the signal expects the rate to revert to). reward = |current -
target|; risk = reward / stop_reward_ratio; stop_loss is `risk` further
from current, in the direction that would prove the trade wrong.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

import pandas as pd

from dagster_quickstart.steer.constants import SIGNAL_BUY, SIGNAL_NONE, SIGNAL_SELL
from dagster_quickstart.steer.estimation import CointegrationResult, SteerEstimate

Signal = Literal["BUY", "SELL", "NONE"]


@dataclass(frozen=True)
class SteerSignal:
    """One day's trading signal for one currency pair."""

    as_of: pd.Timestamp
    signal: Signal
    entry_z_score: float
    target: Optional[float]
    stop_loss: Optional[float]
    reason: str


def generate_signal(
    estimate: SteerEstimate,
    cointegration: CointegrationResult,
    *,
    current_rate: float,
    z_threshold: float,
    stop_reward_ratio: float,
) -> SteerSignal:
    """BUY/SELL/NONE from an estimate + cointegration result.

    NONE (no target/stop-loss) whenever cointegration fails OR |z-score| is
    below z_threshold -- both conditions must hold for a real signal, per
    the spec. `current_rate` is the live rate level (not log-transformed,
    even when estimate.is_logged) -- this function does the log/level
    conversion itself via estimate.fitted_value_level.
    """
    if not cointegration.passed:
        return SteerSignal(
            as_of=estimate.as_of,
            signal=SIGNAL_NONE,
            entry_z_score=estimate.z_score,
            target=None,
            stop_loss=None,
            reason=f"cointegration failed (p={cointegration.p_value:.4f})",
        )

    if abs(estimate.z_score) < z_threshold:
        return SteerSignal(
            as_of=estimate.as_of,
            signal=SIGNAL_NONE,
            entry_z_score=estimate.z_score,
            target=None,
            stop_loss=None,
            reason=f"|z|={abs(estimate.z_score):.2f} below threshold {z_threshold}",
        )

    target = estimate.fitted_value_level
    direction: Signal = SIGNAL_SELL if estimate.z_score > 0 else SIGNAL_BUY
    reward = abs(current_rate - target)
    risk = reward / stop_reward_ratio
    stop_loss = current_rate + risk if direction == "SELL" else current_rate - risk

    return SteerSignal(
        as_of=estimate.as_of,
        signal=direction,
        entry_z_score=estimate.z_score,
        target=target,
        stop_loss=stop_loss,
        reason=f"|z|={abs(estimate.z_score):.2f} >= threshold {z_threshold}, cointegrated",
    )
