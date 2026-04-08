"""Demo Hawk data strategy — swap ``base_demo`` imports for your production package."""

from __future__ import annotations

import datetime as dt
from typing import Any, List

from dagster_quickstart.MQL.base_demo import (
    BaseDataStrategy,
    DataRawResult,
    DataResult,
    build_celery_config,
    getHawkTimeseriesToDf,
)


class HawkStrategy(BaseDataStrategy):
    _source_name = "Hawk"

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        if config is None:
            config = build_celery_config()
        super().__init__(config)

        if "CeleryConnection" not in self.config:
            raise ValueError("Hawk Data retrieval requires CeleryConnection param")

    def get_history(
        self,
        symbols: List[str],
        fromDate: dt.datetime,
        toDate: dt.datetime,
        **kwargs: Any,
    ) -> DataResult:
        payload = getHawkTimeseriesToDf(
            celery=self.config,
            fameCodes=symbols,
            fromDate=fromDate,
            toDate=toDate,
        )

        if "frequency" in kwargs:
            payload = payload.resample(kwargs["frequency"]).last()

        raw = DataRawResult(
            timestamp=dt.datetime.now(dt.timezone.utc),
            payload=payload,
            source=self._source_name,
            symbols=symbols,
            from_date=fromDate,
            to_date=toDate,
            metadata=self.config,
        )

        return DataResult(raw)

    def get_live(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("Hawk live feed not implemented in demo")
