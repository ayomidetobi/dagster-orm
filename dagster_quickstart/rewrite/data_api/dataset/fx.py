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


#: Base filter for the STEER universe FX classes below -- NOT FXSpot's
#: (see FXSpot's docstring: that class targets the old demo catalog's
#: sub_asset_class="Forex Spot" rows, of which the real 128-row STEER
#: catalog has zero). "FX Spot" here is a distinct label used only by the
#: rows added specifically to unblock STEER pair discovery/rate-fetching
#: post-cutover -- one row per non-USD currency vs USD, standard Bloomberg
#: convention tickers, is_synthetic=False (real, standard-convention
#: tickers -- just not part of the source ticker sheet extract, which
#: has no FX spot rows at all -- see
#: steer/discovery.py's module docstring for the full story). Each STEER
#: universe class defines this directly rather than inheriting it, so it
#: never mixes with FXSpot/FXMajor/FXUSDBloc's old-catalog-shaped filter.
#: asset_class alone is NOT enough within a universe -- CHN's market_development
#: also covers OFFSHORE_SPREAD_PX_LAST/ONSHORE_SPREAD_PX_LAST (asset_class=
#: Currency, sub_asset_class="FX Forward"), which are driver inputs, not
#: pairs; sub_asset_class="FX Spot" is what actually narrows to real pairs.
_STEER_FX_FILTERS = {
    "asset_class": "Currency",
    "sub_asset_class": "FX Spot",
}


class FXDevelopedMarkets(DatasetBase):
    """G10 currency pairs -- one pair per non-USD G10 currency, quoted against USD."""

    _FILTERS = {
        **_STEER_FX_FILTERS,
        "market_development": ["G10"],
    }


class FXEmergingMarkets(DatasetBase):
    """EM currency pairs -- one pair per non-USD EM currency, quoted against USD."""

    _FILTERS = {
        **_STEER_FX_FILTERS,
        "market_development": ["EM"],
    }


class FXChina(DatasetBase):
    """CNH/CNY currency pairs -- one pair per CHN currency, quoted against USD."""

    _FILTERS = {
        **_STEER_FX_FILTERS,
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
