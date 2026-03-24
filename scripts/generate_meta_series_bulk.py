"""One-off generator: writes meta_series.csv; dims align with lookup_tables.csv."""

import csv
import functools
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "dagster_quickstart" / "data"
LOOKUP_PATH = DATA / "lookup_tables.csv"
OUT = DATA / "meta_series.csv"
N_ROWS = 1000
random.seed(42)

HEADER = [
    "series_name",
    "series_code",
    "asset_class",
    "sub_asset_class",
    "product_type",
    "structure_type",
    "market_segment",
    "region",
    "currency",
    "term",
    "tenor",
    "bbg_field",
    "bbg_data_type",
    "mds_field",
    "mds_data_type",
    "bbg_ticker",
    "mds_ticker",
    "valid_from",
    "valid_to",
    "calculation_formula",
    "des_notes",
]
VALID_FROM = "2020-01-01T00:00:00"


@functools.lru_cache(maxsize=1)
def _lookup_tuple_rows(path_str: str) -> tuple[tuple[tuple[str, str], ...], ...]:
    with Path(path_str).open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return tuple(tuple(row.items()) for row in reader)


def load_lookup() -> list[dict[str, str]]:
    return [dict(t) for t in _lookup_tuple_rows(str(LOOKUP_PATH.resolve()))]


def row_for_asset_class(asset_class: str) -> dict[str, str]:
    rows = [r for r in load_lookup() if r["asset_class"] == asset_class]
    return dict(random.choice(rows))


def row_for_sub_asset_class(sub_asset_class: str) -> dict[str, str]:
    rows = [r for r in load_lookup() if r["sub_asset_class"] == sub_asset_class]
    return dict(random.choice(rows))


def _tenors_from_lookup() -> set[str]:
    return {r["tenor"] for r in load_lookup()}


def _currencies_from_lookup() -> list[str]:
    return sorted({r["currency"] for r in load_lookup()})


def _forex_pair() -> tuple[str, str, str]:
    """base, quote, pair symbol using only lookup currencies."""
    ccys = _currencies_from_lookup()
    base = random.choice(ccys)
    quote = random.choice([c for c in ccys if c != base])
    return base, quote, f"{base}{quote}"


def _equity_stock(i: int) -> list[str]:
    sym = f"SX{i:04d}"
    name = f"Synthetic Equity {i} Last Price"
    code = f"{sym}_PX_LAST"
    m = row_for_asset_class("Equity")
    return [
        name,
        code,
        m["asset_class"],
        m["sub_asset_class"],
        m["product_type"],
        m["structure_type"],
        m["market_segment"],
        m["region"],
        m["currency"],
        m["term"],
        m["tenor"],
        "PX_LAST",
        "Price",
        "",
        "",
        f"{sym} US Equity",
        sym,
        VALID_FROM,
        "",
        "",
        f"Synthetic common stock last traded price for {sym}",
    ]


def _equity_index(i: int) -> list[str]:
    idx = f"IDX{i:04d}"
    name = f"Regional Index {i}"
    code = f"{idx}_INDEX"
    tmpl = row_for_sub_asset_class("Equity Index")
    m = {**tmpl, "asset_class": "Equity", "product_type": "Index"}
    return [
        name,
        code,
        m["asset_class"],
        m["sub_asset_class"],
        m["product_type"],
        m["structure_type"],
        m["market_segment"],
        m["region"],
        m["currency"],
        m["term"],
        m["tenor"],
        "PX_LAST",
        "Price",
        "",
        "",
        f"{idx} Index",
        idx,
        VALID_FROM,
        "",
        "",
        f"Synthetic equity index level for {idx}",
    ]


def _fixed_income_yield(i: int) -> list[str]:
    m_base = row_for_asset_class("Fixed Income")
    yrs = random.choice([2, 5, 10, 30])
    tenor_allowed = {t for t in _tenors_from_lookup() if t.endswith("Y")}
    tenor = f"{yrs}Y" if f"{yrs}Y" in tenor_allowed else random.choice(sorted(tenor_allowed))
    iso = random.choice(["US", "UK", "DE", "FR", "JP", "IT", "AU", "CA"])
    mds = f"{iso}{yrs}Y"
    name = f"{iso} {yrs}Y Sovereign Yield {i}"
    code = f"{mds}_YIELD_{i:04d}"
    bbg = f"{iso}GG{yrs}YR Index" if iso != "DE" else f"GT{iso}{yrs}YR Index"
    m = {**m_base, "tenor": tenor}
    return [
        name,
        code,
        m["asset_class"],
        m["sub_asset_class"],
        m["product_type"],
        m["structure_type"],
        m["market_segment"],
        m["region"],
        m["currency"],
        m["term"],
        m["tenor"],
        "",
        "",
        "YIELD",
        "Yield",
        bbg,
        mds,
        VALID_FROM,
        "",
        "",
        f"Synthetic {yrs}-year government benchmark yield ({iso}, {m['currency']})",
    ]


def _forex(i: int) -> list[str]:
    base, quote, pair = _forex_pair()
    name = f"{base}/{quote} Spot {i}"
    code = f"{pair}_SPOT_{i:04d}"
    # Dims must match lookup rows for asset_class=Currency (only MXN row). Pair is naming/ticker only.
    m = row_for_asset_class("Currency")
    m["sub_asset_class"] = "Forex Spot"
    m["product_type"] = "Forward"
    return [
        name,
        code,
        m["asset_class"],
        m["sub_asset_class"],
        m["product_type"],
        m["structure_type"],
        m["market_segment"],
        m["region"],
        m["currency"],
        m["term"],
        m["tenor"],
        "PX_LAST",
        "Price",
        "",
        "",
        f"{pair} Curncy",
        pair,
        VALID_FROM,
        "",
        "",
        f"Synthetic spot FX rate {base} vs {quote}",
    ]


def _commodity(i: int) -> list[str]:
    # Sub-asset names must appear in lookup_tables.csv (Crude Oil, Gold, Silver, Natural Gas).
    kind = random.choice(
        [
            ("Crude Oil", "CL", "Comdty"),
            ("Natural Gas", "NG", "Comdty"),
            ("Gold", "XAU", "Curncy"),
            ("Silver", "XAG", "Curncy"),
        ]
    )
    sub, root, suffix = kind
    # Dims from asset_class=Commodity row (JPY); sub_asset_class is a valid lookup token only.
    m = row_for_asset_class("Commodity")
    m["sub_asset_class"] = sub
    sym = f"{root}{i % 10}" if suffix == "Comdty" else root
    name = f"{sub} Benchmark {i}"
    code = f"{sym}_PX_{i:04d}"
    bbg = f"{sym} {suffix}" if suffix == "Comdty" else f"{sym} Curncy"
    return [
        name,
        code,
        m["asset_class"],
        m["sub_asset_class"],
        m["product_type"],
        m["structure_type"],
        m["market_segment"],
        m["region"],
        m["currency"],
        m["term"],
        m["tenor"],
        "PX_LAST",
        "Price",
        "",
        "",
        bbg,
        sym,
        VALID_FROM,
        "",
        "",
        f"Synthetic {sub.lower()} reference price",
    ]


def build_rows() -> list[list[str]]:
    load_lookup()
    rows: list[list[str]] = []
    weights = [
        (_equity_stock, 400),
        (_equity_index, 150),
        (_fixed_income_yield, 150),
        (_forex, 150),
        (_commodity, 150),
    ]
    generators = [g for g, w in weights for _ in range(w)]
    random.shuffle(generators)
    if len(generators) > N_ROWS:
        generators = generators[:N_ROWS]
    while len(generators) < N_ROWS:
        generators.append(random.choice([t for t, _ in weights]))
    random.shuffle(generators)
    seen_codes: set[str] = set()
    i = 0
    for gen_fn in generators:
        i += 1
        row = gen_fn(i)
        code = row[1]
        while code in seen_codes:
            i += 1
            row = gen_fn(i)
            code = row[1]
        seen_codes.add(code)
        rows.append(row)
    return rows


def main() -> None:
    rows = build_rows()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        w.writerows(rows)


if __name__ == "__main__":
    main()
