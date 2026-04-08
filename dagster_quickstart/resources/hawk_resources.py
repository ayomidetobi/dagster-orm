"""Demo Dagster resource for Hawk-style history pulls (MQL ``HawkStrategy``)."""

from __future__ import annotations

import datetime as dt
from typing import Any, List, Optional

from dagster import ConfigurableResource, InitResourceContext, get_dagster_logger

from dagster_quickstart.MQL.base_demo import DataResult
from dagster_quickstart.MQL.hawk import HawkStrategy

logger = get_dagster_logger()


class HawkResource(ConfigurableResource):
    """Wraps demo :class:`~dagster_quickstart.MQL.hawk.HawkStrategy` for assets/ops.

    Set ``celery_connection`` to your broker URL in real deployments; the default is a
    placeholder for local demos only.
    """

    celery_connection: str = "demo://localhost"

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._hawk_strategy: Optional[HawkStrategy] = HawkStrategy(
            config={"CeleryConnection": self.celery_connection},
        )
        logger.info("HawkResource ready (demo MQL HawkStrategy)")

    def get_strategy(self) -> HawkStrategy:
        """Return the initialized :class:`HawkStrategy` (only valid during execution)."""
        strategy = getattr(self, "_hawk_strategy", None)
        if strategy is None:
            raise RuntimeError(
                "HawkResource is not initialized; use only inside a Dagster run "
                "(resource setup_for_execution must have run)."
            )
        return strategy

    def fetch_history(
        self,
        symbols: List[str],
        start: dt.datetime,
        end: dt.datetime,
        **kwargs: Any,
    ) -> DataResult:
        """Historical pull for ``symbols`` in ``[start, end)`` (pass-through to ``get_history``)."""
        return self.get_strategy().get_history(
            symbols,
            fromDate=start,
            toDate=end,
            **kwargs,
        )
