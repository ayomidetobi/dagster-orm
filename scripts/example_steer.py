#!/usr/bin/env python3
"""Demo of the Steer/SteerResults model-object facade over the STEER pipeline.

Zero-config: reads DATABASE_URL / S3_* from dagster_quickstart/.env (via
python-decouple) and attaches the real Postgres+S3 DuckLake catalog, same
as scripts/example_dataapi.py. Assumes the STEER metadata catalog has
already been ingested (see assets/load_metaseries/asset.py).

Usage:
    python scripts/example_steer.py [--universe G10] [--lookback 5] [--outdir ./steer_plots]
"""

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DAGSTER_QUICKSTART = REPO_ROOT / "dagster_quickstart"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(DAGSTER_QUICKSTART))

import matplotlib

matplotlib.use("Agg")  # non-interactive backend -- this script saves PNGs, never plt.show()

from dagster_quickstart.steer.universes import UNIVERSES


def print_separator(text: str = "", char: str = "=", length: int = 60) -> None:
    if text:
        print(f"\n{char * length}")
        print(text)
        print(f"{char * length}")
    else:
        print(f"\n{char * length}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--universe", default="G10", choices=["G10", "EM", "CHN"])
    parser.add_argument("--lookback", type=int, default=5, help="lookback_days for Steer.fit()")
    parser.add_argument("--outdir", default="./steer_plots", help="directory to write PNGs into")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    print_separator(f"Steer demo -- universe={args.universe}, lookback_days={args.lookback}")

    universe = UNIVERSES[args.universe]
    # universe.fit() builds its own zero-config DataAPI(live=False) under the hood (see
    # steer/universes.py's default_data_api()) -- same DuckLake connection example_dataapi.py
    # wires up explicitly.
    results = universe.fit(lookback_days=args.lookback, cointegration="each")

    fitted_pairs = sorted(results.results.get(results.as_of_dates[-1], {})) if results.as_of_dates else []

    print_separator("Fit summary")
    print(f"Fitted pairs (as of {results.as_of_dates[-1].date() if results.as_of_dates else 'n/a'}): "
          f"{len(fitted_pairs)}")
    print(f"Blocked pairs: {len(results.blocked)}")
    for series_code, reason in results.blocked.items():
        print(f"  {series_code}: {reason}")

    if not fitted_pairs:
        print_separator("No pairs fitted -- nothing to plot")
        print("Every pair in this universe was blocked; see reasons above.", file=sys.stderr)
        return 1

    print_separator("Cross-section (latest date), sorted by |z-score| descending")
    cross_section = results.get_cross_section(-1)
    cross_section = cross_section.assign(_abs_z=cross_section["z_score"].abs()).sort_values(
        "_abs_z", ascending=False
    ).drop(columns="_abs_z")
    with_pd_option = ["series_code", "z_score", "fair_value", "cointegration_passed", "dropped_variables"]
    print(cross_section[with_pd_option].to_string(index=False))

    print_separator("Signals (latest date)")
    signals = results.signals()
    print(signals.to_string(index=False))

    non_none = signals[signals["signal"] != "NONE"]
    print_separator(f"Actionable signals ({len(non_none)} of {len(signals)})")
    if non_none.empty:
        print("(none)")
    else:
        print(non_none.to_string(index=False))

    print_separator("Writing plots")
    written = []

    fig = results.plot_z_scores().figure
    path = outdir / "z_scores.png"
    fig.savefig(path, bbox_inches="tight")
    written.append(path)

    if len(results.as_of_dates) > 1:
        fig = results.plot_z_history().figure
        path = outdir / "z_history.png"
        fig.savefig(path, bbox_inches="tight")
        written.append(path)
    else:
        print(
            "Skipping z_history.png -- lookback_days=1 gives only one fitted date "
            "(plot_z_history needs more than one); pass --lookback with a larger value."
        )

    top_pair = cross_section.iloc[0]["series_code"]
    fig = results.plot_pair(top_pair).figure
    path = outdir / f"pair_{top_pair}.png"
    fig.savefig(path, bbox_inches="tight")
    written.append(path)

    print_separator("Files written")
    for path in written:
        print(path.resolve())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
