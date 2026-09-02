> [!NOTE]
> This project has been archived, please reference the [Quickstart](https://docs.dagster.io/getting-started/quickstart) available in the Dagster documentation.

---

<div align="center">
  <a target="_blank" href="https://dagster.io" style="background:none">
    <img alt="dagster logo" src="https://github.com/dagster-io/dagster-quickstart/assets/5807118/7010804c-05a6-4ef4-bfc8-d9c88d458906" width="auto" height="120px">
  </a>
</div>

# Dagster Quickstart

Get up-and-running with the Dagster quickstart project -- open the project in a GitHub Codespace and start building data pipelines with no local installation required.

For more information on how to use this project, please reference the [Dagster Quickstart guide](https://docs.dagster.io/getting-started/quickstart).

## Running The Project

### Option 1. Using GitHub Codespaces

1. Fork this repository

2. From the **Code** dropdown, select **Create codespace on main**

<img width="300" alt="Create codespace" src="https://github.com/dagster-io/dagster-quickstart/assets/5807118/954493f0-99ac-4aa9-884b-3b2800d2a0d8">

3. Once the codespace has loaded, run `dagster dev` in the terminal to start Dagster:

    ```bash
    dagster dev
    ```

4. When prompted, click **Open in Browser**.

<img width="400" alt="Codespace Open In Browser" src="https://github.com/dagster-io/dagster-quickstart/assets/5807118/2d598c56-2bf5-4ffb-927f-5d2e4a5e6967">

> [!TIP]  
> If the popup to open Dagster is not visible, you can navigate to the **Forwarded Ports** tab, and open the **Forwarded Address** for port 3000.

5. **Success!** You'll be presented with the lineage of assets in the quickstart project.

![image](https://github.com/dagster-io/dagster-quickstart/assets/5807118/fe5dcf40-a086-42a3-974c-42c252e3a705)

### Option 2. Running Locally

1. Clone the Dagster Quickstart repository:

    ```sh
    git clone https://github.com/dagster-io/dagster-quickstart

    cd dagster-quickstart
    ```

2. Install the required dependencies.

    Here we are using `-e`, for ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs), so that when Dagster code is modified, the changes automatically apply. 

    ```sh
    pip install -e ".[dev]"
    ```

3. Run the project!

    ```sh
    dagster dev
    ```

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `dagster_quickstart_tests` directory and you can run tests using `pytest`.

## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.

## STEER daily model pipeline

`dagster_quickstart/steer/` computes daily STEER-style FX fair values (rolling
OLS on 5 drivers + Engle-Granger cointegration + z-score signals) for the G10,
EM, and CHN (CNY/CNH) universes, on top of the existing Bloomberg ingestion.
It reads bronze data via the existing `rewrite_data_api` resource and writes
new `silver`/`gold` DuckDB schemas into the same DuckLake catalog -- the
existing `metadata`/`values` tables are untouched (aside from one new
`market_development` metadata column -- see below).

Currency pairs are **not** hand-typed anywhere -- each universe's asset run
discovers every pair in that universe live from the datalake via
`rewrite/data_api/dataset/fx.py`'s
`FXDevelopedMarkets`/`FXEmergingMarkets`/`FXChina` datasets, which filter
metadata's `market_development` column (`G10`/`EM`/`CHN`/`GLOBAL`, added to
`dagster_quickstart/data/meta_series.csv`). Two of the 5 STEER drivers
(`local_equity`, and `interest_rate_differential`/`yield_curve_or_cds`)
need genuine per-country data that this demo catalog doesn't fully have --
rather than substitute a proxy, a pair missing either is explicitly
reported and skipped (see "Data availability" below), never silently
regressed on a corrupted input.

- `dagster_quickstart/steer/` -- pure business logic: config (window,
  z-threshold, risk/reward, curated global driver series -- no pairs),
  pair/driver discovery + availability assessment (`discovery.py`), feature
  engineering, OLS/cointegration estimation, signal generation, gold-layer
  storage. No Dagster dependency; see `tests/test_steer_*.py` for direct
  unit tests against synthetic cointegrated/non-cointegrated series.
- `dagster_quickstart/assets/steer/` -- the Dagster graph on top, each asset
  processing every pair in the current `universe` partition and writing one
  concatenated, `series_code`-tagged DataFrame: `steer_silver_prices`
  (discover the universe's pairs, check each one's availability -- skip
  cleanly if blocked -- fetch + conform onto a business-day calendar, plus
  the freshness check) -> `steer_features` (`build_steer_features` per pair,
  pandera validated) -> `steer_cointegration` -> `steer_estimate` (rolling
  OLS + sign-check/re-estimate per pair, writes `gold.steer_estimates`) ->
  `steer_signal` (writes `gold.steer_signals`); plus the
  `steer_data_availability` report asset, partitioned the same way.
- `dagster_quickstart/steer/universes.py` -- `FX_G10`/`FX_EM`/`FX_CHN`, one
  code-defined, frozen `FXUniverse` (a `StrategyConfig` subclass -- see
  `steer/config.py`) per universe: window, z-threshold, stop/reward ratio,
  logged-rate threshold, expected coefficient signs -- currency pairs are
  not configured here either. Each also carries the pipeline entry point
  (`FX_G10.fit(lookback_days=5, cointegration="each")` fits every pair in
  that universe -- see `steer/model.py`). The two curated global driver
  series `global_equity_series`/`commodity_series` are shared by every
  universe and live in `steer/config.py`'s `GLOBAL_DRIVERS` instead; swap
  for your own real global benchmarks if you have better ones.

### Partitioning

Partitioned by `universe` only -- `StaticPartitionsDefinition(["G10", "EM",
"CHN"])`, a fixed literal list (`assets/steer/partitions.py`). `currency_pair`
is **not** a Dagster partition dimension: each universe partition's run
discovers and fetches the complete history for every pair in that universe
in one go (`steer/discovery.py`'s `discover_pairs()`), and
those pairs live together as rows in the same output DataFrame/Parquet
dataset, identified by a `series_code` column -- not by separate partitions.
This keeps the partition set static and tiny, so no live datalake query is
needed anywhere at module-import time (`dagster definitions validate` never
touches the real catalog) and there's no partition-registration step or
sensor required before a run.

Each real `series_code` (not a "clean pair name" -- this catalog can have
several series for the same nominal pair, e.g. two AUDJPY series with
different suffixes; each is tracked and reported on individually) present in
a universe is resolved and assessed for driver availability within that
partition's run (`steer/discovery.py`'s `build_availability_report()`, consumed by
`steer_silver_prices` via `pairs_from_availability_report()`).

### Data availability

`interest_rate_differential`/`yield_curve_or_cds` need a real sovereign-yield
or interest-rate-swap series for **both** of a pair's currencies -- this
catalog's Fixed Income data covers 9 currencies (AUD, CAD, CHF, EUR, GBP,
JPY, NOK, SEK, USD): sovereign yields for AUD/CAD/EUR/GBP/JPY/USD (via
country-prefix series_codes, `steer/discovery.py`'s `COUNTRY_TO_CURRENCY`),
plus 2Y interest-rate-swap series for EUR/USD/GBP/JPY/CHF/AUD/NOK/SEK,
listed explicitly in `SWAP_SERIES_TO_CURRENCY` since they have no derivable
country-prefix structure -- CHF/NOK/SEK's only rate-data source is their
swap series. EUSA2/USOSFR2/BPSW2/JYSO2/SFSW2/ADSW2 are real Bloomberg
mnemonics; `NKSW2_PX_LAST`/`SKSW2_PX_LAST` (NOK/SEK) are placeholder demo
tickers added on request, not verified real ones -- swap in real mnemonics
when available. `local_equity` needs a real per-country equity index for
**both** of a pair's currencies -- most of this catalog's Equity metadata
(Common Stock / generic "Regional Index" rows) has no country signal at
all, but 14 real per-currency MSCI index series were added explicitly
(AUD, CNY, EUR, GBP, INR, JPY, MXN, NOK, RUB, SAR, SEK, SGD, USD, ZAR --
real Bloomberg mnemonics like `AUD_PX_LAST`, listed in
`steer/discovery.py`'s `EQUITY_SERIES_TO_CURRENCY`). Materialize
`steer_data_availability` for the full picture (which pairs are blocked and
why, per driver) -- as of the real demo catalog today, 33 of 150 pairs are
unblocked, and the entire G10 universe (33/33) is now fully unblocked, since
every G10 currency has both rate and local_equity coverage. EM and CHN stay
fully blocked -- their currencies have local_equity coverage but no rate
data yet. That's the honest, verified state of this data, not a bug (aside
from the two placeholder swap tickers, noted above). Once real rate data
exists for more currencies (even for a subset of pairs), the same logic
picks it up automatically -- see `steer/discovery.py`'s module docstring.

### Running locally

1. Start the webserver: `dagster dev` (from the repo root, where
   `pyproject.toml`'s `[tool.dagster]` block is).
2. Materialize `steer_data_availability` (for the `G10`, `EM`, or `CHN`
   partition) to see the real gap report for that universe -- no
   registration step needed first, since `universe` is a static partition.
3. For a pair the report shows as available (the entire `G10` universe, 33
   pairs, in the shipped demo catalog today -- see above), materialize `steer_silver_prices` ->
   `steer_features` -> `steer_cointegration` -> `steer_estimate` ->
   `steer_signal` for that universe's partition, or materialize the whole
   `steer_daily_run` job (which covers all 6 assets, including
   `steer_data_availability`, across all 3 partitions).
4. Inspect results: `gold.steer_estimates` / `gold.steer_signals` in the
   DuckLake catalog (query them the same way you'd query `values`/
   `metadata` -- see `rewrite/data_api`, filtering on `series_code`), or via
   each asset's materialization metadata in the UI (coefficients, z-score,
   signal, target/stop-loss, per pair).

`steer_daily_schedule` (09:00 Europe/Lisbon, weekdays -- matches this
repo's prior daily-ingestion schedule convention; runs one `RunRequest` per
universe partition) and `steer_daily_digest_schedule` (09:30, sends the
daily summary email) all start **STOPPED**, same as the existing
notification sensors -- turn them on from the Dagster UI once
`OutlookEmailResource` has real SMTP credentials (for the digest) and you're
ready for the pipeline to run unattended.

### Backfilling a date

Since there's no date partition axis, "backfill a date" means re-running a
universe's job with `as_of` overridden, not selecting a date partition.
`--partition` just takes the universe name directly -- no registration step,
since `universe` is a static partition:

```sh
dagster asset materialize \
  --select "steer_silver_prices,steer_features,steer_cointegration,steer_estimate,steer_signal" \
  --partition "G10" \
  --config-json '{"ops": {"steer_cointegration": {"config": {"as_of": "2024-06-03"}}, "steer_estimate": {"config": {"as_of": "2024-06-03"}}}}'
```

(`steer_silver_prices`/`steer_features` always fetch full history for every
pair in the universe and don't take an `as_of` themselves -- only
`steer_cointegration`/`steer_estimate` need it, via each asset's
`StrategyRunConfig`, see `assets/steer/config.py`.) This backfills every pair
in the `G10` universe for that date in one run; loop the command over a date
range to backfill several dates.
