from dagster_quickstart.rewrite.data_api.dataset.base import DatasetBase


class FXSpot(DatasetBase):
    """FX spot-rate series.

    asset_class="Currency" / sub_asset_class="Forex Spot" is what this
    catalog's real metadata actually uses (verified against meta_series.csv
    -- product_type is "Forward"/"Derivative" for every Currency row here,
    a data-modeling quirk of this demo catalog, not something to filter on).
    """

    _FILTERS = {
        "asset_class": "Currency",
        "sub_asset_class": "Forex Spot",
    }


class FXDevelopedMarkets(FXSpot):
    """G10/developed-market currency pairs -- both legs a G10 currency.

    See meta_series.csv's market_development column (G10/EM/CHN/GLOBAL) --
    added specifically to drive this split; asset_class=Currency has no
    other field that varies per row in this catalog (market_segment/
    sub_asset_class are both constant across every FX row).
    """

    _FILTERS = {
        **FXSpot._FILTERS,
        "market_development": ["G10"],
    }


class FXEmergingMarkets(FXSpot):
    """EM currency pairs -- at least one leg isn't a DM currency (and neither leg is CNY/CNH)."""

    _FILTERS = {
        **FXSpot._FILTERS,
        "market_development": ["EM"],
    }


class FXChina(FXSpot):
    """CNY/CNH-involving currency pairs."""

    _FILTERS = {
        **FXSpot._FILTERS,
        "market_development": ["CHN"],
    }


class FXMajor(FXSpot):
    """Developed + emerging market pairs, excluding CHN (managed/restricted currency, kept separate)."""

    _FILTERS = {
        **FXSpot._FILTERS,
        "market_development": ["G10", "EM"],
    }

    def matrix(self, *, out_of_cache: bool | None = None, ticker_source: str | None = None):
        """Return get_values(), with columns relabeled series_code -> currency.

        Raises if two matched series share a currency -- ambiguous which
        one should own that column, and silently picking one (or averaging)
        would hide a metadata problem instead of surfacing it.
        """
        values = self.get_values(out_of_cache=out_of_cache, ticker_source=ticker_source)
        if values.empty:
            return values

        currency_by_series = (
            self.query.metadata().set_index("series_code")["currency"].reindex(values.columns)
        )
        duplicated = currency_by_series[currency_by_series.duplicated(keep=False)]
        if not duplicated.empty:
            raise ValueError(
                f"matrix() needs one series per currency, but found duplicates: "
                f"{duplicated.to_dict()}"
            )

        return values.rename(columns=currency_by_series.to_dict())


class FXUSDBloc(FXSpot):
    """USD-bloc currency pairs.

    market_segment="Dollar Bloc" doesn't exist in this catalog's real data
    (Currency rows all have market_segment="Exchange Traded" -- see
    FXSpot's docstring) -- kept as originally written since fixing *this*
    filter wasn't asked for; currently matches zero rows here.
    """

    _FILTERS = {
        **FXSpot._FILTERS,
        "market_segment": ["Dollar Bloc"],
    }
