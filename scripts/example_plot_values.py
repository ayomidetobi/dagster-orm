#!/usr/bin/env python3
"""Example: plot value/metadata data with ECharts, and export it.

Zero-config: reads DATABASE_URL / S3_* from dagster_quickstart/.env (via
python-decouple) and attaches the real Postgres+S3 DuckLake catalog. Part 1
writes a small amount of demo data under two clearly-named EXAMPLE_PLOT_*
series codes first, so it's self-contained. Part 2 uses REAL metadata
already in the catalog (Fixed Income sovereign yields) to demonstrate the
get_metadata() -> get_values() -> plot() flow end to end.

dataapi.plot is a chart-type namespace, not a single method -- pick the
chart you want and it fetches + plots in one call:

    data_api.plot.line(["SX0001_PX_LAST"])
    data_api.plot.bar([...], export_path="chart.png")
    data_api.plot.yield_curve(filters={"series_code": [...]})

...or fetch first and plot the frame directly, chained or standalone:

    data_api.get_values([...]).plot.line()
    data_api.plot(some_wide_frame)          # calling it directly defaults to line

Every method (see rewrite.data_api.plotting.PlotAccessor for the full list:
line/area/bar/scatter/candlestick/yield_curve/term_structure/correlation/
heatmap/histogram/boxplot/seasonality/calendar/dashboard) takes export_path=
to save a file -- ".html" always works, ".png"/".jpg" need the optional
"plotting" extras (`pip install -e ".[plotting]"`, plus Chrome/Chromium
installed locally -- Selenium manages its own driver).

Usage:
    python scripts/example_plot_values.py
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DAGSTER_QUICKSTART = REPO_ROOT / "dagster_quickstart"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(DAGSTER_QUICKSTART))

import numpy as np
import pandas as pd

from dagster_quickstart.rewrite.data_api.api.data_api import DataAPI

SERIES_A = "EXAMPLE_PLOT_SERIES_A"
SERIES_B = "EXAMPLE_PLOT_SERIES_B"


def print_separator(text: str = "", char: str = "=", length: int = 60) -> None:
    if text:
        print(f"\n{char * length}")
        print(text)
        print(f"{char * length}")
    else:
        print(f"\n{char * length}")


data_api = DataAPI(live=False)

# ============================================================================
# Part 1: the core interface, using self-contained demo data
# ============================================================================

print_separator("Part 1: core interface, with demo data")

dates = pd.date_range("2024-01-01", periods=60, freq="D")
rng = np.random.default_rng(7)
series_a_values = 100 + np.cumsum(rng.normal(0, 1, size=60))
series_b_values = 60 + np.cumsum(rng.normal(0, 0.6, size=60))
series_b_values[20:24] = np.nan  # a real gap -- the chart breaks the line here, not a straight connect

demo_values = pd.concat(
    [
        pd.DataFrame({"series_code": SERIES_A, "timestamp": dates, "value": series_a_values}),
        pd.DataFrame({"series_code": SERIES_B, "timestamp": dates, "value": series_b_values}),
    ]
)
data_api.write_values(demo_values)
print(f"Wrote {len(demo_values)} demo rows for {SERIES_A} and {SERIES_B}.")

# 1a: chainable -- get_values() returns a ValueFrame, which has a .plot accessor.
print_separator("1a. data_api.get_values([...]).plot.line()")
chart = data_api.get_values([SERIES_A, SERIES_B]).plot.line(title="Demo series (chained)")
print(f"Built a {type(chart).__name__} chart with {len(chart.options['series'])} series.")

# 1b: direct -- plot a frame you already have, without re-fetching. Calling
# the accessor itself (no chart-type method) defaults to a line chart.
print_separator("1b. data_api.plot(value_df)")
value_df = data_api.get_last_values([SERIES_A, SERIES_B])
print("Latest values:")
print(value_df)
chart = data_api.plot(value_df, title="Demo series (latest values only)")
print(f"Built a {type(chart).__name__} chart from an already-fetched frame.")

# 1c: fetch + plot + export in one call, via the chart-type accessor.
print_separator("1c. data_api.plot.line([...], export_path=...)")
html_path = DAGSTER_QUICKSTART / "example_plot_output.html"
data_api.plot.line([SERIES_A, SERIES_B], title="Demo series (exported)", export_path=html_path)
print(f"Exported interactive chart to {html_path} -- open it in any browser.")

# 1d: export to a static image (PNG). Needs the optional "plotting" extra
# (snapshot-selenium + selenium, plus Chrome/Chromium installed) -- falls
# back to a clear message instead of crashing if it's not set up.
print_separator("1d. export to PNG")
png_path = DAGSTER_QUICKSTART / "example_plot_output.png"
try:
    data_api.plot.line([SERIES_A, SERIES_B], title="Demo series (PNG export)", export_path=png_path)
    print(f"Exported static image to {png_path}")
except ImportError as exc:
    print(f"PNG export not available in this environment: {exc}")

# 1e: a few of the other chart types, still on the same demo series.
print_separator("1e. other chart types: bar, histogram, boxplot, correlation")
values = data_api.get_values([SERIES_A, SERIES_B])
print(f"bar:         {type(values.plot.bar()).__name__}")
print(f"histogram:   {type(values.plot.histogram(SERIES_A)).__name__}")
print(f"boxplot:     {type(values.plot.boxplot()).__name__}")
print(f"correlation: {type(values.plot.correlation()).__name__}")

# ============================================================================
# Part 2: get_metadata() -> get_values() -> plot(), using REAL catalog metadata
# ============================================================================
#
# This is the pattern to reach for whenever you don't already know the
# series_codes you want -- discover them from metadata first, then fetch
# and plot. We use the Fixed Income sovereign-yield series already in the
# catalog (from meta_series.csv) rather than another synthetic series.

print_separator("Part 2: get_metadata() -> get_values() -> plot() with real metadata")

# Discover: one Italian sovereign-yield series per tenor (2Y/5Y/10Y/30Y).
# get_metadata() filters are exact-match on column values -- asset_class
# narrows to the Fixed Income rows, and series_name is filtered in pandas
# afterwards since "issuer" isn't its own metadata column here.
fixed_income = data_api.get_metadata(asset_class=["Fixed Income"]).frame
italy_curve = (
    fixed_income[fixed_income["series_name"].str.startswith("IT ", na=False)]
    .drop_duplicates("tenor")
    .sort_values("tenor")
)
print(f"Discovered {len(italy_curve)} Italian sovereign-yield series via get_metadata():")
print(italy_curve[["series_code", "series_name", "tenor"]].to_string(index=False))

italy_codes = italy_curve["series_code"].tolist()

# This demo catalog has metadata for these series but no value history yet
# (no vendor is actually wired up) -- write some so the rest of this
# section has real data to fetch and plot, keyed to the real series_codes
# get_metadata() just returned (not hardcoded names).
rng = np.random.default_rng(11)
yield_dates = pd.date_range("2024-01-01", periods=90, freq="D")
base_yield_by_tenor = {"2Y": 3.8, "5Y": 3.6, "10Y": 4.1, "30Y": 4.5}
yield_rows = [
    pd.DataFrame(
        {
            "series_code": row.series_code,
            "timestamp": yield_dates,
            "value": base_yield_by_tenor[row.tenor] + np.cumsum(rng.normal(0, 0.03, size=90)),
        }
    )
    for row in italy_curve.itertuples()
]
data_api.write_values(pd.concat(yield_rows))
print(f"Wrote {90 * len(italy_curve)} demo yield rows across {len(italy_curve)} series.")

# 2a: fetch the whole history and plot it as a line chart.
print_separator("2a. data_api.get_values(italy_codes).plot.line()")
yield_history = data_api.get_values(italy_codes)
chart = yield_history.plot.line(title="Italy sovereign yields", subtitle="2Y / 5Y / 10Y / 30Y")
print(f"Built a {type(chart).__name__} chart over {yield_history.shape[1]} tenors.")

# 2b: the metadata-driven chart type -- yield_curve() re-runs get_metadata()
# with the same filter internally, then plots the latest value per series
# against its tenor, sorted correctly (2Y before 10Y, not alphabetically).
print_separator("2b. data_api.plot.yield_curve(filters={'series_code': italy_codes})")
yield_curve_path = DAGSTER_QUICKSTART / "example_yield_curve.html"
curve_chart = data_api.plot.yield_curve(
    filters={"series_code": italy_codes},
    title="Italy sovereign yield curve (latest)",
    export_path=yield_curve_path,
)
print(f"Built a {type(curve_chart).__name__} chart, exported to {yield_curve_path}.")

# 2c: term_structure() overlays several such curves -- e.g. today vs a
# month ago -- so you can see how the whole curve has shifted.
print_separator("2c. data_api.plot.term_structure(..., as_of_dates=[...])")
term_structure_path = DAGSTER_QUICKSTART / "example_term_structure.html"
term_chart = data_api.plot.term_structure(
    filters={"series_code": italy_codes},
    as_of_dates=["2024-02-01", "2024-03-30"],
    title="Italy yield curve: Feb vs end of Mar",
    export_path=term_structure_path,
)
print(f"Built a {type(term_chart).__name__} chart, exported to {term_structure_path}.")

print_separator("All examples completed!")
