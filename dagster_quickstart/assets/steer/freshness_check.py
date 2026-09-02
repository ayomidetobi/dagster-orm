"""The Dagster identifier for steer_silver_prices' bronze-freshness AssetCheckSpec.

The freshness *rule* (assess_freshness, FRESHNESS_TOLERANCE_DAYS) lives in
steer/pipeline.py -- it's a domain decision ("is this pair's data current
enough to use"), not orchestration. Only the check's name, which
steer/silver_asset.py's AssetCheckSpec and AssetCheckResult both need to
agree on, is a Dagster concern.
"""

FRESHNESS_CHECK_NAME = "validate_bronze_freshness"
