"""Static universe partitions for the STEER asset graph.

G10, EM, and CHN are independently fetched, so each is its own Dagster
partition -- but currency_pair is NOT a partition dimension. Every pair in
a universe (identified by series_code, stored together in the same Parquet
dataset) is processed as data within that universe's single partition run
(see assets/steer/universe_datasets.py's discover_pairs()), not as a
separate Dagster partition -- one call per universe covers every pair's
complete history.

Purely static -- no live datalake query at import time, or ever, for this
module: universe membership (G10/EM/CHN) is fixed by StrategyConfig's own
Literal type, not discovered. This intentionally avoids
DynamicPartitionsDefinition and the pair-registration machinery
(steer_pair_discovery_sensor) an earlier per-pair-partition design needed --
static partitions need no sensor to keep them in sync with the datalake.
"""

from dagster import StaticPartitionsDefinition

STEER_PARTITIONS = StaticPartitionsDefinition(["G10", "EM", "CHN"])
