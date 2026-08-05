from dagster_quickstart.rewrite.data_api.dataset.base import DatasetBase


class FXSpot(DatasetBase):

    _FILTERS = {
        "asset_class": "FX",
        "product_type": "Spot",
    }


class FXMajor(FXSpot):

    _FILTERS = {
        **FXSpot._FILTERS,
        "sub_asset_class": ["G10 Major", "EM Major"],
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

    _FILTERS = {
        **FXSpot._FILTERS,
        "market_segment": ["Dollar Bloc"],
    }