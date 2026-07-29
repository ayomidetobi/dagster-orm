"""ECharts-based plotting for value/metadata data.

``dataapi.plot`` is a chart-type namespace -- ``.line()``, ``.bar()``,
``.scatter()``, ``.yield_curve()``, etc. -- mirroring pandas' own
``df.plot.line()``/``.bar()`` accessor convention rather than a single
``plot(type="...")`` switch, since different chart types genuinely need
different parameters (scatter needs two series, histogram needs a bin
count, yield_curve needs a tenor column). Calling the accessor directly
(``dataapi.plot(...)``) defaults to a line chart.

Every method works two ways:
    data_api.plot.line(["SX0001_PX_LAST"])       # fetches via get_values() first
    data_api.get_values([...]).plot.line()        # frame already fetched

See scripts/example_plot_values.py for a full runnable walkthrough,
including get_metadata() -> get_values() -> plot.yield_curve().
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

import numpy as np
import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Bar, Boxplot, Calendar, HeatMap, Kline, Line, Page, Scatter
from pyecharts.globals import ThemeType

if TYPE_CHECKING:
    from dagster_quickstart.rewrite.data_api.api.data_api import DataAPI

DEFAULT_WIDTH = "1000px"
DEFAULT_HEIGHT = "600px"


# ============================================================================
# Shared helpers
# ============================================================================


def _format_index(frame: pd.DataFrame) -> list[str]:
    return [
        index_value.strftime("%Y-%m-%d") if hasattr(index_value, "strftime") else str(index_value)
        for index_value in frame.index
    ]


def _clean(values: pd.Series) -> list[float | None]:
    """NaN -> None so ECharts renders a gap instead of a fabricated zero."""

    return [None if pd.isna(value) else float(value) for value in values]


def _base_toolbox() -> opts.ToolboxOpts:
    return opts.ToolboxOpts(
        is_show=True,
        feature=opts.ToolBoxFeatureOpts(
            save_as_image=opts.ToolBoxFeatureSaveAsImageOpts(title="Save as image"),
            restore=opts.ToolBoxFeatureRestoreOpts(title="Restore"),
            data_zoom=opts.ToolBoxFeatureDataZoomOpts(zoom_title="Zoom", back_title="Reset zoom"),
            data_view=opts.ToolBoxFeatureDataViewOpts(
                title="View data", lang=["Data View", "Close", "Refresh"]
            ),
        ),
    )


def export_chart(chart: Any, path: str | Path) -> Path:
    """Export a chart built by this module to a file.

    ``.html`` writes a standalone interactive file (pyecharts' own renderer
    -- opens in any browser, no extra dependencies, always available).

    ``.png``/``.jpg``/``.jpeg``/``.svg``/``.pdf``/``.gif`` render a static
    snapshot via a headless browser -- requires `pip install
    snapshot-selenium selenium` (plus Chrome/Chromium installed; Selenium
    manages its own driver) or `pip install snapshot-pyppeteer`. Neither is
    installed by default since they pull in a browser automation stack --
    raises a clear ImportError naming what to install rather than failing
    deep inside pyecharts with an opaque one.
    """

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.suffix.lower() in (".html", ".htm"):
        chart.render(str(path))
        return path

    snapshot = None
    try:
        from snapshot_selenium import snapshot  # type: ignore[no-redef]
    except ImportError:
        try:
            from snapshot_pyppeteer import snapshot  # type: ignore[no-redef]
        except ImportError:
            pass

    if snapshot is None:
        raise ImportError(
            f"Exporting to {path.suffix!r} requires a headless-browser snapshot engine. "
            "Install one of: `pip install snapshot-selenium selenium` (plus "
            "Chrome/Chromium installed locally) or `pip install "
            "snapshot-pyppeteer`, then retry -- or export to a .html file "
            "instead (always supported, no extra dependencies)."
        )

    from pyecharts.render import make_snapshot

    make_snapshot(snapshot, chart.render(), str(path))
    return path


# ============================================================================
# Chart builders -- pure functions, frame(s) in, pyecharts chart out
# ============================================================================


def build_line_chart(
    frame: pd.DataFrame,
    *,
    title: str = "",
    subtitle: str = "",
    theme: str = ThemeType.WHITE,
    smooth: bool = False,
    area: bool = False,
    stacked: bool = False,
    show_data_zoom: bool = True,
    width: str = DEFAULT_WIDTH,
    height: str = DEFAULT_HEIGHT,
    export_path: str | Path | None = None,
) -> Line:
    """Line (or, with area=True, area) chart from a wide-form value frame.

    `frame` is wide-form (a DatetimeIndex, one column per series_code) --
    exactly what DataAPI.get_values()/get_last_values() return. A missing
    value (NaN) breaks the line with a visible gap rather than connecting
    across it or plotting as zero. stacked=True stacks series on top of
    each other (only meaningful together with area=True or for comparing
    contribution to a total).
    """

    if frame.empty:
        raise ValueError("Cannot plot an empty value frame -- no data to show.")

    x_axis = _format_index(frame)
    chart = Line(init_opts=opts.InitOpts(theme=theme, width=width, height=height))
    chart.add_xaxis(x_axis)

    for series_code in frame.columns:
        chart.add_yaxis(
            series_name=str(series_code),
            y_axis=_clean(frame[series_code]),
            is_smooth=smooth,
            is_connect_nones=False,
            is_symbol_show=len(x_axis) <= 60,
            symbol_size=4,
            stack="total" if stacked else None,
            areastyle_opts=opts.AreaStyleOpts(opacity=0.25 if area else 0),
            label_opts=opts.LabelOpts(is_show=False),
        )

    chart.set_global_opts(
        title_opts=opts.TitleOpts(title=title or "Value series", subtitle=subtitle),
        legend_opts=opts.LegendOpts(is_show=True, pos_top="8%", type_="scroll"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
        yaxis_opts=opts.AxisOpts(type_="value", is_scale=True),
        datazoom_opts=(
            [opts.DataZoomOpts(range_start=0, range_end=100), opts.DataZoomOpts(type_="inside")]
            if show_data_zoom
            else None
        ),
        toolbox_opts=_base_toolbox(),
    )

    if export_path is not None:
        export_chart(chart, export_path)

    return chart


def build_bar_chart(
    frame: pd.DataFrame,
    *,
    title: str = "",
    subtitle: str = "",
    theme: str = ThemeType.WHITE,
    stacked: bool = False,
    show_data_zoom: bool = True,
    width: str = DEFAULT_WIDTH,
    height: str = DEFAULT_HEIGHT,
    export_path: str | Path | None = None,
) -> Bar:
    """Bar chart from a wide-form value frame -- one bar group per timestamp.

    Best for a small number of timestamps (e.g. month-end snapshots); a
    long daily series is usually clearer as a line -- see build_line_chart().
    """

    if frame.empty:
        raise ValueError("Cannot plot an empty value frame -- no data to show.")

    x_axis = _format_index(frame)
    chart = Bar(init_opts=opts.InitOpts(theme=theme, width=width, height=height))
    chart.add_xaxis(x_axis)

    for series_code in frame.columns:
        chart.add_yaxis(
            series_name=str(series_code),
            y_axis=_clean(frame[series_code]),
            stack="total" if stacked else None,
            label_opts=opts.LabelOpts(is_show=False),
        )

    chart.set_global_opts(
        title_opts=opts.TitleOpts(title=title or "Value series", subtitle=subtitle),
        legend_opts=opts.LegendOpts(is_show=True, pos_top="8%", type_="scroll"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        xaxis_opts=opts.AxisOpts(type_="category"),
        yaxis_opts=opts.AxisOpts(type_="value", is_scale=True),
        datazoom_opts=(
            [opts.DataZoomOpts(range_start=0, range_end=100), opts.DataZoomOpts(type_="inside")]
            if show_data_zoom
            else None
        ),
        toolbox_opts=_base_toolbox(),
    )

    if export_path is not None:
        export_chart(chart, export_path)

    return chart


def build_scatter_chart(
    frame: pd.DataFrame,
    x_series: str,
    y_series: str,
    *,
    title: str = "",
    subtitle: str = "",
    theme: str = ThemeType.WHITE,
    width: str = DEFAULT_WIDTH,
    height: str = DEFAULT_HEIGHT,
    export_path: str | Path | None = None,
) -> Scatter:
    """Scatter of one series' values against another's, matched by timestamp.

    Useful for eyeballing how two series move together (a visual
    complement to plot.heatmap()'s numeric correlation).
    """

    if x_series not in frame.columns or y_series not in frame.columns:
        raise ValueError(f"frame must have both {x_series!r} and {y_series!r} columns.")

    paired = frame[[x_series, y_series]].dropna()
    if paired.empty:
        raise ValueError(f"No overlapping (non-null) timestamps between {x_series!r} and {y_series!r}.")

    chart = Scatter(init_opts=opts.InitOpts(theme=theme, width=width, height=height))
    chart.add_xaxis(paired[x_series].tolist())
    chart.add_yaxis(
        series_name=y_series,
        y_axis=paired[y_series].tolist(),
        label_opts=opts.LabelOpts(is_show=False),
    )
    chart.set_global_opts(
        title_opts=opts.TitleOpts(title=title or f"{x_series} vs {y_series}", subtitle=subtitle),
        tooltip_opts=opts.TooltipOpts(trigger="item"),
        xaxis_opts=opts.AxisOpts(type_="value", name=x_series, is_scale=True),
        yaxis_opts=opts.AxisOpts(type_="value", name=y_series, is_scale=True),
        toolbox_opts=_base_toolbox(),
    )

    if export_path is not None:
        export_chart(chart, export_path)

    return chart


def build_histogram_chart(
    series: pd.Series,
    *,
    bins: int = 20,
    title: str = "",
    subtitle: str = "",
    theme: str = ThemeType.WHITE,
    width: str = DEFAULT_WIDTH,
    height: str = DEFAULT_HEIGHT,
    export_path: str | Path | None = None,
) -> Bar:
    """Histogram of a single series' value distribution."""

    clean = series.dropna()
    if clean.empty:
        raise ValueError("Cannot plot a histogram of an all-null series.")

    counts, edges = np.histogram(clean, bins=bins)
    labels = [f"{edges[i]:.2f} to {edges[i + 1]:.2f}" for i in range(len(edges) - 1)]

    chart = Bar(init_opts=opts.InitOpts(theme=theme, width=width, height=height))
    chart.add_xaxis(labels)
    chart.add_yaxis(
        series_name=series.name or "count",
        y_axis=[int(count) for count in counts],
        category_gap=0,
        label_opts=opts.LabelOpts(is_show=False),
    )
    chart.set_global_opts(
        title_opts=opts.TitleOpts(title=title or f"{series.name} distribution", subtitle=subtitle),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        xaxis_opts=opts.AxisOpts(type_="category", axislabel_opts=opts.LabelOpts(rotate=45)),
        yaxis_opts=opts.AxisOpts(type_="value", name="count"),
        toolbox_opts=_base_toolbox(),
    )

    if export_path is not None:
        export_chart(chart, export_path)

    return chart


def build_boxplot_chart(
    frame: pd.DataFrame,
    *,
    title: str = "",
    subtitle: str = "",
    theme: str = ThemeType.WHITE,
    width: str = DEFAULT_WIDTH,
    height: str = DEFAULT_HEIGHT,
    export_path: str | Path | None = None,
) -> Boxplot:
    """Boxplot comparing each series' value distribution, side by side."""

    if frame.empty:
        raise ValueError("Cannot plot an empty value frame -- no data to show.")

    series_codes = list(frame.columns)
    raw_data = [frame[code].dropna().tolist() for code in series_codes]
    if not any(raw_data):
        raise ValueError("Cannot plot a boxplot with no non-null values in any series.")

    chart = Boxplot(init_opts=opts.InitOpts(theme=theme, width=width, height=height))
    chart.add_xaxis(series_codes)
    chart.add_yaxis("distribution", Boxplot.prepare_data(raw_data))
    chart.set_global_opts(
        title_opts=opts.TitleOpts(title=title or "Value distribution", subtitle=subtitle),
        tooltip_opts=opts.TooltipOpts(trigger="item"),
        xaxis_opts=opts.AxisOpts(type_="category"),
        yaxis_opts=opts.AxisOpts(type_="value", is_scale=True),
        toolbox_opts=_base_toolbox(),
    )

    if export_path is not None:
        export_chart(chart, export_path)

    return chart


def build_heatmap_chart(
    frame: pd.DataFrame,
    *,
    method: str = "pearson",
    title: str = "",
    subtitle: str = "",
    theme: str = ThemeType.WHITE,
    width: str = DEFAULT_WIDTH,
    height: str = DEFAULT_HEIGHT,
    export_path: str | Path | None = None,
) -> HeatMap:
    """Correlation matrix heatmap across every series in `frame`.

    method is passed straight to DataFrame.corr() -- "pearson" (default),
    "spearman", or "kendall".
    """

    if frame.shape[1] < 2:
        raise ValueError("Need at least 2 series to compute a correlation heatmap.")

    correlation = frame.corr(method=method)
    series_codes = list(correlation.columns)

    cells = [
        [i, j, round(float(correlation.iloc[j, i]), 4)]
        for i in range(len(series_codes))
        for j in range(len(series_codes))
        if not pd.isna(correlation.iloc[j, i])
    ]

    chart = HeatMap(init_opts=opts.InitOpts(theme=theme, width=width, height=height))
    chart.add_xaxis(series_codes)
    chart.add_yaxis(method, series_codes, cells, label_opts=opts.LabelOpts(is_show=True, position="inside"))
    chart.set_global_opts(
        title_opts=opts.TitleOpts(title=title or f"{method.title()} correlation", subtitle=subtitle),
        tooltip_opts=opts.TooltipOpts(trigger="item"),
        xaxis_opts=opts.AxisOpts(type_="category", axislabel_opts=opts.LabelOpts(rotate=45)),
        yaxis_opts=opts.AxisOpts(type_="category"),
        visualmap_opts=opts.VisualMapOpts(min_=-1, max_=1, is_calculable=True, orient="horizontal", pos_left="center"),
        toolbox_opts=_base_toolbox(),
    )

    if export_path is not None:
        export_chart(chart, export_path)

    return chart


def build_calendar_chart(
    series: pd.Series,
    *,
    year: int | None = None,
    title: str = "",
    subtitle: str = "",
    theme: str = ThemeType.WHITE,
    width: str = DEFAULT_WIDTH,
    height: str = "220px",
    export_path: str | Path | None = None,
) -> Calendar:
    """GitHub-style calendar heatmap of one series' daily values.

    year defaults to the most recent year present in `series`'s index.
    """

    clean = series.dropna()
    if clean.empty:
        raise ValueError("Cannot plot a calendar heatmap of an all-null series.")

    if year is None:
        year = clean.index.max().year
    year_values = clean[clean.index.year == year]
    if year_values.empty:
        raise ValueError(f"No data for year {year} -- available years: {sorted(clean.index.year.unique())}")

    data = [[ts.strftime("%Y-%m-%d"), round(float(value), 4)] for ts, value in year_values.items()]

    chart = Calendar(init_opts=opts.InitOpts(theme=theme, width=width, height=height))
    chart.add(
        str(series.name or "value"),
        data,
        calendar_opts=opts.CalendarOpts(range_=str(year)),
    )
    chart.set_global_opts(
        title_opts=opts.TitleOpts(title=title or f"{series.name} ({year})", subtitle=subtitle),
        visualmap_opts=opts.VisualMapOpts(
            min_=float(year_values.min()), max_=float(year_values.max()), orient="horizontal", pos_left="center"
        ),
        tooltip_opts=opts.TooltipOpts(trigger="item"),
    )

    if export_path is not None:
        export_chart(chart, export_path)

    return chart


def build_candlestick_chart(
    series: pd.Series,
    *,
    freq: str = "W",
    title: str = "",
    subtitle: str = "",
    theme: str = ThemeType.WHITE,
    width: str = DEFAULT_WIDTH,
    height: str = DEFAULT_HEIGHT,
    export_path: str | Path | None = None,
) -> Kline:
    """Candlestick chart derived from a single scalar value series.

    Our datalake stores one value per (series_code, timestamp), not real
    OHLC bars -- so open/high/low/close are derived by resampling to
    `freq` (default weekly: first/max/min/last within each period). This
    is a legitimate way to read a daily series' volatility/range, but it
    isn't the same thing as a genuine intraday OHLC bar.
    """

    clean = series.dropna()
    if clean.empty:
        raise ValueError("Cannot build a candlestick chart from an all-null series.")

    ohlc = clean.resample(freq).agg(["first", "max", "min", "last"]).dropna()
    if ohlc.empty:
        raise ValueError(f"No complete {freq!r} periods to build candles from.")

    x_axis = [ts.strftime("%Y-%m-%d") for ts in ohlc.index]
    # pyecharts Kline expects each item as [open, close, low, high].
    candles = ohlc[["first", "last", "min", "max"]].to_numpy().tolist()

    chart = Kline(init_opts=opts.InitOpts(theme=theme, width=width, height=height))
    chart.add_xaxis(x_axis)
    chart.add_yaxis(str(series.name or "value"), candles)
    chart.set_global_opts(
        title_opts=opts.TitleOpts(
            title=title or f"{series.name} ({freq} candles, derived from daily values)", subtitle=subtitle
        ),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        xaxis_opts=opts.AxisOpts(type_="category"),
        yaxis_opts=opts.AxisOpts(type_="value", is_scale=True),
        datazoom_opts=[opts.DataZoomOpts(range_start=0, range_end=100), opts.DataZoomOpts(type_="inside")],
        toolbox_opts=_base_toolbox(),
    )

    if export_path is not None:
        export_chart(chart, export_path)

    return chart


def build_seasonality_chart(
    series: pd.Series,
    *,
    period: str = "month",
    title: str = "",
    subtitle: str = "",
    theme: str = ThemeType.WHITE,
    width: str = DEFAULT_WIDTH,
    height: str = DEFAULT_HEIGHT,
    export_path: str | Path | None = None,
) -> Line:
    """Seasonality chart: one line per year, x-axis is month (or day-of-week).

    period="month" (default) groups by calendar month across years --
    period="dayofweek" groups by weekday instead. Needs at least 2 distinct
    years of data to be meaningful.
    """

    clean = series.dropna()
    if clean.empty:
        raise ValueError("Cannot plot seasonality for an all-null series.")

    if period == "month":
        x_values, x_labels = clean.index.month - 1, [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
        ]
    elif period == "dayofweek":
        x_values, x_labels = clean.index.dayofweek, ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    else:
        raise ValueError(f"period must be 'month' or 'dayofweek', got {period!r}")

    grouped = pd.DataFrame({"year": clean.index.year, "x": x_values, "value": clean.to_numpy()})
    pivoted = grouped.groupby(["year", "x"])["value"].mean().unstack("year")

    chart = Line(init_opts=opts.InitOpts(theme=theme, width=width, height=height))
    chart.add_xaxis([x_labels[i] for i in pivoted.index])
    for year in pivoted.columns:
        chart.add_yaxis(
            series_name=str(year),
            y_axis=_clean(pivoted[year]),
            is_smooth=True,
            is_connect_nones=True,
            label_opts=opts.LabelOpts(is_show=False),
        )
    chart.set_global_opts(
        title_opts=opts.TitleOpts(title=title or f"{series.name} seasonality (by {period})", subtitle=subtitle),
        legend_opts=opts.LegendOpts(is_show=True, pos_top="8%"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
        yaxis_opts=opts.AxisOpts(type_="value", is_scale=True),
        toolbox_opts=_base_toolbox(),
    )

    if export_path is not None:
        export_chart(chart, export_path)

    return chart


_TENOR_PATTERN = re.compile(r"(\d+(?:\.\d+)?)\s*([DWMY])", re.IGNORECASE)
_TENOR_UNIT_TO_MONTHS = {"D": 1 / 30, "W": 1 / 4.345, "M": 1.0, "Y": 12.0}


def tenor_to_months(tenor: str) -> float:
    """Convert a tenor string ("1M", "2Y", "6M", "10Y", ...) to months, for sorting.

    Unparseable tenors sort last (float("inf")) rather than raising, so one
    bad label doesn't break the whole curve.
    """

    match = _TENOR_PATTERN.match(str(tenor).strip())
    if not match:
        return float("inf")
    value, unit = match.groups()
    return float(value) * _TENOR_UNIT_TO_MONTHS[unit.upper()]


def build_yield_curve_chart(
    tenor_by_series: pd.Series,
    value_by_series: pd.Series,
    *,
    title: str = "",
    subtitle: str = "",
    theme: str = ThemeType.WHITE,
    width: str = DEFAULT_WIDTH,
    height: str = DEFAULT_HEIGHT,
    export_path: str | Path | None = None,
) -> Bar:
    """Value-vs-tenor curve (e.g. a yield curve), sorted by tenor.

    tenor_by_series/value_by_series are both indexed by series_code --
    build these from get_metadata()'s tenor column and get_last_values(),
    respectively (see DataAPI.plot.yield_curve() for the full pipeline).
    """

    combined = pd.DataFrame({"tenor": tenor_by_series, "value": value_by_series}).dropna()
    if combined.empty:
        raise ValueError("No series had both a tenor and a value to plot.")

    combined["_sort_key"] = combined["tenor"].map(tenor_to_months)
    combined = combined.sort_values("_sort_key")

    chart = Bar(init_opts=opts.InitOpts(theme=theme, width=width, height=height))
    chart.add_xaxis(combined["tenor"].tolist())
    chart.add_yaxis(
        "value", [round(float(v), 4) for v in combined["value"]], label_opts=opts.LabelOpts(is_show=True)
    )
    chart.set_global_opts(
        title_opts=opts.TitleOpts(title=title or "Yield curve", subtitle=subtitle),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        xaxis_opts=opts.AxisOpts(type_="category", name="tenor"),
        yaxis_opts=opts.AxisOpts(type_="value", is_scale=True, name="value"),
        toolbox_opts=_base_toolbox(),
    )

    if export_path is not None:
        export_chart(chart, export_path)

    return chart


def build_term_structure_chart(
    tenor_by_series: pd.Series,
    value_by_series_by_label: dict[str, pd.Series],
    *,
    title: str = "",
    subtitle: str = "",
    theme: str = ThemeType.WHITE,
    width: str = DEFAULT_WIDTH,
    height: str = DEFAULT_HEIGHT,
    export_path: str | Path | None = None,
) -> Line:
    """Multiple value-vs-tenor curves overlaid (e.g. today's curve vs a month ago's).

    value_by_series_by_label maps a label (e.g. "2024-06-01") to a
    series_code -> value Series for that snapshot -- see
    DataAPI.plot.term_structure() for the full pipeline.
    """

    if not value_by_series_by_label:
        raise ValueError("Need at least one snapshot to plot a term structure.")

    tenor_frame = tenor_by_series.dropna()
    tenor_order = sorted(tenor_frame.index, key=lambda code: tenor_to_months(tenor_frame[code]))
    x_labels = [tenor_frame[code] for code in tenor_order]

    chart = Line(init_opts=opts.InitOpts(theme=theme, width=width, height=height))
    chart.add_xaxis(x_labels)
    for label, values in value_by_series_by_label.items():
        aligned = values.reindex(tenor_order)
        chart.add_yaxis(
            series_name=str(label),
            y_axis=_clean(aligned),
            is_smooth=True,
            is_connect_nones=True,
            label_opts=opts.LabelOpts(is_show=False),
        )
    chart.set_global_opts(
        title_opts=opts.TitleOpts(title=title or "Term structure", subtitle=subtitle),
        legend_opts=opts.LegendOpts(is_show=True, pos_top="8%"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        xaxis_opts=opts.AxisOpts(type_="category", name="tenor", boundary_gap=False),
        yaxis_opts=opts.AxisOpts(type_="value", is_scale=True, name="value"),
        toolbox_opts=_base_toolbox(),
    )

    if export_path is not None:
        export_chart(chart, export_path)

    return chart


def build_dashboard(charts: Sequence[Any], *, page_title: str = "Dashboard") -> Page:
    """Combine several already-built charts into one scrollable page.

    Export with export_chart(page, "dashboard.html") (or an image
    extension -- a multi-chart Page snapshots as one tall image).
    """

    if not charts:
        raise ValueError("dashboard needs at least one chart.")

    page = Page(page_title=page_title, layout=Page.SimplePageLayout)
    for chart in charts:
        page.add(chart)
    return page


# ============================================================================
# Accessor: dataapi.plot.line()/.bar()/... and value_df.plot.line()/.bar()/...
# ============================================================================


class PlotAccessor:
    """Chart-type namespace, bound to either a DataAPI or an already-fetched frame.

    ``data_api.plot`` and ``value_frame.plot`` both return one of these --
    the fetch-then-plot vs. plot-directly behavior is decided per call by
    whether a frame is available. yield_curve()/term_structure() need
    get_metadata(), so they're only available via data_api.plot.
    """

    def __init__(self, data: "DataAPI | pd.DataFrame") -> None:
        self._data = data

    def _resolve(
        self,
        series_codes: Sequence[str] | None,
        frame: pd.DataFrame | None,
        *,
        ticker_source: str | None,
        start=None,
        end=None,
        out_of_cache: bool | None = None,
    ) -> pd.DataFrame:
        if frame is not None:
            return frame
        if isinstance(self._data, pd.DataFrame):
            return self._data
        if series_codes is None:
            raise ValueError(
                "Provide series_codes to fetch, or frame= if you already have one "
                "(e.g. from data_api.get_values())."
            )
        return self._data.get_values(
            series_codes, ticker_source=ticker_source, start=start, end=end, out_of_cache=out_of_cache
        )

    def _data_api(self) -> "DataAPI":
        if isinstance(self._data, pd.DataFrame):
            raise TypeError(
                "This chart needs metadata (get_metadata()), so it's only available via "
                "data_api.plot, not value_frame.plot."
            )
        return self._data

    def __call__(self, series_codes=None, *, frame: pd.DataFrame | None = None, **kwargs) -> Line:
        """Defaults to a line chart -- see .line() for all parameters.

        A DataFrame passed positionally (data_api.plot(value_df)) is treated
        as `frame`, not `series_codes`, so plotting an already-fetched frame
        directly works without naming the keyword.
        """
        if isinstance(series_codes, pd.DataFrame):
            frame, series_codes = series_codes, None
        return self.line(series_codes, frame=frame, **kwargs)

    def line(
        self,
        series_codes: Sequence[str] | None = None,
        *,
        frame: pd.DataFrame | None = None,
        ticker_source: str | None = None,
        start=None,
        end=None,
        out_of_cache: bool | None = None,
        **kwargs,
    ) -> Line:
        """Line chart -- see build_line_chart() for chart options (title, smooth, export_path, ...)."""
        resolved = self._resolve(series_codes, frame, ticker_source=ticker_source, start=start, end=end, out_of_cache=out_of_cache)
        return build_line_chart(resolved, **kwargs)

    def area(
        self,
        series_codes: Sequence[str] | None = None,
        *,
        frame: pd.DataFrame | None = None,
        ticker_source: str | None = None,
        start=None,
        end=None,
        out_of_cache: bool | None = None,
        **kwargs,
    ) -> Line:
        """Area chart (a line chart with the area beneath it filled) -- see build_line_chart()."""
        resolved = self._resolve(series_codes, frame, ticker_source=ticker_source, start=start, end=end, out_of_cache=out_of_cache)
        return build_line_chart(resolved, area=True, **kwargs)

    def bar(
        self,
        series_codes: Sequence[str] | None = None,
        *,
        frame: pd.DataFrame | None = None,
        ticker_source: str | None = None,
        start=None,
        end=None,
        out_of_cache: bool | None = None,
        **kwargs,
    ) -> Bar:
        """Bar chart -- see build_bar_chart() for chart options (stacked, export_path, ...)."""
        resolved = self._resolve(series_codes, frame, ticker_source=ticker_source, start=start, end=end, out_of_cache=out_of_cache)
        return build_bar_chart(resolved, **kwargs)

    def scatter(
        self,
        x_series: str,
        y_series: str,
        *,
        frame: pd.DataFrame | None = None,
        ticker_source: str | None = None,
        start=None,
        end=None,
        out_of_cache: bool | None = None,
        **kwargs,
    ) -> Scatter:
        """Scatter of x_series against y_series, matched by timestamp -- see build_scatter_chart()."""
        resolved = self._resolve([x_series, y_series], frame, ticker_source=ticker_source, start=start, end=end, out_of_cache=out_of_cache)
        return build_scatter_chart(resolved, x_series, y_series, **kwargs)

    def histogram(
        self,
        series_code: str,
        *,
        frame: pd.DataFrame | None = None,
        bins: int = 20,
        ticker_source: str | None = None,
        start=None,
        end=None,
        out_of_cache: bool | None = None,
        **kwargs,
    ) -> Bar:
        """Histogram of one series' value distribution -- see build_histogram_chart()."""
        resolved = self._resolve([series_code], frame, ticker_source=ticker_source, start=start, end=end, out_of_cache=out_of_cache)
        if series_code not in resolved.columns:
            raise ValueError(f"{series_code!r} not found in the resolved frame's columns.")
        return build_histogram_chart(resolved[series_code], bins=bins, **kwargs)

    def boxplot(
        self,
        series_codes: Sequence[str] | None = None,
        *,
        frame: pd.DataFrame | None = None,
        ticker_source: str | None = None,
        start=None,
        end=None,
        out_of_cache: bool | None = None,
        **kwargs,
    ) -> Boxplot:
        """Boxplot comparing each series' distribution -- see build_boxplot_chart()."""
        resolved = self._resolve(series_codes, frame, ticker_source=ticker_source, start=start, end=end, out_of_cache=out_of_cache)
        return build_boxplot_chart(resolved, **kwargs)

    def heatmap(
        self,
        series_codes: Sequence[str] | None = None,
        *,
        frame: pd.DataFrame | None = None,
        method: str = "pearson",
        ticker_source: str | None = None,
        start=None,
        end=None,
        out_of_cache: bool | None = None,
        **kwargs,
    ) -> HeatMap:
        """Correlation heatmap across series -- see build_heatmap_chart()."""
        resolved = self._resolve(series_codes, frame, ticker_source=ticker_source, start=start, end=end, out_of_cache=out_of_cache)
        return build_heatmap_chart(resolved, method=method, **kwargs)

    # "plot a correlation" conventionally means a correlation heatmap.
    correlation = heatmap

    def calendar(
        self,
        series_code: str,
        *,
        frame: pd.DataFrame | None = None,
        year: int | None = None,
        ticker_source: str | None = None,
        start=None,
        end=None,
        out_of_cache: bool | None = None,
        **kwargs,
    ) -> Calendar:
        """GitHub-style calendar heatmap of one series' daily values -- see build_calendar_chart()."""
        resolved = self._resolve([series_code], frame, ticker_source=ticker_source, start=start, end=end, out_of_cache=out_of_cache)
        if series_code not in resolved.columns:
            raise ValueError(f"{series_code!r} not found in the resolved frame's columns.")
        return build_calendar_chart(resolved[series_code], year=year, **kwargs)

    def candlestick(
        self,
        series_code: str,
        *,
        frame: pd.DataFrame | None = None,
        freq: str = "W",
        ticker_source: str | None = None,
        start=None,
        end=None,
        out_of_cache: bool | None = None,
        **kwargs,
    ) -> Kline:
        """Candlestick derived from one scalar series via resampling -- see build_candlestick_chart()."""
        resolved = self._resolve([series_code], frame, ticker_source=ticker_source, start=start, end=end, out_of_cache=out_of_cache)
        if series_code not in resolved.columns:
            raise ValueError(f"{series_code!r} not found in the resolved frame's columns.")
        return build_candlestick_chart(resolved[series_code], freq=freq, **kwargs)

    def seasonality(
        self,
        series_code: str,
        *,
        frame: pd.DataFrame | None = None,
        period: str = "month",
        ticker_source: str | None = None,
        start=None,
        end=None,
        out_of_cache: bool | None = None,
        **kwargs,
    ) -> Line:
        """One line per year, grouped by month/day-of-week -- see build_seasonality_chart()."""
        resolved = self._resolve([series_code], frame, ticker_source=ticker_source, start=start, end=end, out_of_cache=out_of_cache)
        if series_code not in resolved.columns:
            raise ValueError(f"{series_code!r} not found in the resolved frame's columns.")
        return build_seasonality_chart(resolved[series_code], period=period, **kwargs)

    def yield_curve(
        self,
        *,
        filters: dict | None = None,
        tenor_column: str = "tenor",
        ticker_source: str | None = None,
        **kwargs,
    ) -> Bar:
        """Value-vs-tenor curve for metadata matching `filters` -- get_metadata() + get_last_values().

            data_api.plot.yield_curve(filters={"asset_class": ["Fixed Income"]})

        Only available via data_api.plot (needs get_metadata()), not
        value_frame.plot. See build_yield_curve_chart() for chart options.
        """
        data_api = self._data_api()
        metadata_df = data_api.get_metadata(filters or {}).frame
        if tenor_column not in metadata_df.columns:
            raise ValueError(
                f"Metadata has no {tenor_column!r} column -- pass tenor_column= to name the right one."
            )
        metadata_df = metadata_df.dropna(subset=["series_code", tenor_column])
        if metadata_df.empty:
            raise ValueError("No metadata rows have both a series_code and a tenor.")

        series_codes = metadata_df["series_code"].tolist()
        last_values = data_api.get_last_values(series_codes, ticker_source=ticker_source)
        value_by_series = last_values.stack().droplevel(0) if not last_values.empty else pd.Series(dtype=float)

        tenor_by_series = metadata_df.set_index("series_code")[tenor_column]
        return build_yield_curve_chart(tenor_by_series, value_by_series, **kwargs)

    def term_structure(
        self,
        *,
        filters: dict | None = None,
        tenor_column: str = "tenor",
        ticker_source: str | None = None,
        as_of_dates: Sequence[Any] | None = None,
        **kwargs,
    ) -> Line:
        """Multiple yield-curve snapshots overlaid -- one per date in as_of_dates.

            data_api.plot.term_structure(
                filters={"asset_class": ["Fixed Income"]},
                as_of_dates=["2024-01-01", "2024-06-01"],
            )

        Falls back to a single (current) snapshot if as_of_dates isn't
        given -- in that case this is equivalent to yield_curve(). Only
        available via data_api.plot (needs get_metadata()).
        """
        data_api = self._data_api()
        metadata_df = data_api.get_metadata(filters or {}).frame
        if tenor_column not in metadata_df.columns:
            raise ValueError(
                f"Metadata has no {tenor_column!r} column -- pass tenor_column= to name the right one."
            )
        metadata_df = metadata_df.dropna(subset=["series_code", tenor_column])
        if metadata_df.empty:
            raise ValueError("No metadata rows have both a series_code and a tenor.")

        series_codes = metadata_df["series_code"].tolist()
        tenor_by_series = metadata_df.set_index("series_code")[tenor_column]

        snapshots: dict[str, pd.Series] = {}
        for as_of in as_of_dates or [None]:
            if as_of is None:
                last_values = data_api.get_last_values(series_codes, ticker_source=ticker_source)
                snapshot = last_values.stack().droplevel(0) if not last_values.empty else pd.Series(dtype=float)
            else:
                # get_values() returns the full history up to `end` -- take
                # the last known value per series (as of `as_of`, forward
                # filling through any gaps) rather than the last row of the
                # whole frame, since different series can have gaps on
                # different dates.
                values = data_api.get_values(series_codes, ticker_source=ticker_source, end=as_of)
                snapshot = values.astype(float).ffill().iloc[-1] if not values.empty else pd.Series(dtype=float)
            snapshots[str(as_of) if as_of is not None else "current"] = snapshot

        return build_term_structure_chart(tenor_by_series, snapshots, **kwargs)

    def dashboard(
        self,
        series_codes: Sequence[str] | None = None,
        *,
        frame: pd.DataFrame | None = None,
        charts: Sequence[str] = ("line", "boxplot", "heatmap"),
        ticker_source: str | None = None,
        start=None,
        end=None,
        out_of_cache: bool | None = None,
        page_title: str = "Dashboard",
        export_path: str | Path | None = None,
    ) -> Page:
        """Combine several chart types over the same series into one page.

            data_api.plot.dashboard(["A", "B", "C"], charts=("line", "boxplot", "heatmap"))

        Any name from this accessor that takes (series_codes, frame=...) as
        its first two parameters works in `charts` -- line/area/bar/
        boxplot/heatmap/histogram (histogram uses the first series_code).
        """
        resolved = self._resolve(series_codes, frame, ticker_source=ticker_source, start=start, end=end, out_of_cache=out_of_cache)

        built = []
        for chart_name in charts:
            method = getattr(self, chart_name)
            if chart_name == "histogram":
                built.append(method(resolved.columns[0], frame=resolved))
            else:
                built.append(method(frame=resolved))

        page = build_dashboard(built, page_title=page_title)
        if export_path is not None:
            export_chart(page, export_path)
        return page


class ValueFrame(pd.DataFrame):
    """Wide-form value DataFrame (DatetimeIndex, one column per series_code).

    What DataAPI.get_values()/get_last_values() return -- a normal
    DataFrame in every respect (slicing, arithmetic, write_values()
    round-trips, .attrs, etc. all still work via _constructor), plus a
    .plot accessor:

        data_api.get_values(["SX0001_PX_LAST"]).plot.line()
        data_api.get_values([...]).plot.bar(export_path="chart.png")
        data_api.get_values([...]).plot()   # calling it directly defaults to line
    """

    @property
    def _constructor(self):
        return ValueFrame

    @property
    def plot(self) -> PlotAccessor:
        return PlotAccessor(self)


# Backwards-compatible name for the pre-accessor API.
def build_value_chart(frame: pd.DataFrame, **kwargs) -> Line:
    """Deprecated alias for build_line_chart() -- kept so existing calls still work."""

    return build_line_chart(frame, **kwargs)
