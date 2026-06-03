"""Minimal stubs for MQL demos. Replace imports in ``hawk.py`` with your real integration."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

from dagster_quickstart.utils.demo_random_timeseries import demo_random_wide_frame


def build_celery_config() -> Dict[str, Any]:
    """Demo Celery / broker settings; must include ``CeleryConnection`` for :class:`HawkStrategy`."""
    return {"CeleryConnection": "demo://localhost"}


def getHawkTimeseriesToDf(
    *,
    celery: Dict[str, Any],
    fameCodes: List[str],
    fromDate: dt.datetime,
    toDate: dt.datetime,
) -> pd.DataFrame:
    """Demo: random wide frame; swap for real Hawk client."""
    return demo_random_wide_frame(fromDate, toDate, fameCodes)


@dataclass
class DataRawResult:
    timestamp: dt.datetime
    payload: Any
    source: str
    symbols: List[Any]
    from_date: Optional[dt.datetime]
    to_date: Optional[dt.datetime]
    metadata: Dict[str, Any]


@dataclass
class DataResult:
    raw: DataRawResult


class BaseDataStrategy:
    """Base for vendor-specific pull strategies."""

    _source_name: str = "unknown"

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
