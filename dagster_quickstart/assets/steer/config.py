"""Per-run Dagster Config for the STEER asset graph.

Distinct from steer.config.StrategyConfig (the code-defined, per-variant
model parameters -- see FX_G10/FX_EM/FX_CHN) -- this is Dagster's own
per-run config system (see
assets/ingestion/bloomberg_rewrite/config.py's BloombergValuesConfig for
the same pattern), used for the one thing that varies per *run* rather
than per *variant*: which date to evaluate as of.
"""

from typing import Optional

from dagster import Config


class StrategyRunConfig(Config):
    """Run-level override for the STEER estimation/signal assets.

    Attributes:
        as_of: ISO date string ("2024-06-03") to evaluate the model as of,
            instead of the run's own date. None (default) uses the latest
            date in that partition's silver/features data -- this is how a
            normal daily scheduled run behaves. Set this to backfill a
            specific historical date for a specific pair (see the README's
            "backfill a single pair/date" section) without needing a date
            partition axis -- the confirmed 2D partition scheme
            (variant x currency_pair) re-evaluates fresh "as of today" on
            every materialization by default.
    """

    as_of: Optional[str] = None
