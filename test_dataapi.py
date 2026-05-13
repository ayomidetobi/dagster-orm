def get_values(
    self,
    source: Optional[Union[str, TickerSource]] = None,
    start_date: Optional[Any] = None,
    end_date: Optional[Any] = None,
    live: Optional[bool] = None,
    business_days_only: bool = True,
    rename: bool = False,
) -> pd.DataFrame:
    resolved_series_codes = self.resolve_series_codes()

    if not resolved_series_codes:
        raise SeriesNotFoundError(
            f"No series found matching filters: {self._filter_state_for_error()}"
        )

    self._validate_time_params(start_date, end_date)

    effective_live = self._live if live is None else live

    if source is None:
        value_df = self._load_values_using_default_source(
            resolved_series_codes=resolved_series_codes,
            start_date=start_date,
            end_date=end_date,
            live=effective_live,
        )
    else:
        ticker_source = TickerSource.from_str(source)
        value_df = self._load_values_for_source(
            series_codes=resolved_series_codes,
            source=ticker_source,
            start_date=start_date,
            end_date=end_date,
            live=effective_live,
        )

    return self._format_values_dataframe(
        value_df=value_df,
        live=effective_live,
        business_days_only=business_days_only,
        rename=rename,
    )
    
def _empty_value_df(self) -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            ValueColumns.SERIES_CODE,
            ValueColumns.TIMESTAMP,
            ValueColumns.VALUE,
        ]
    )


def _load_values_for_source(
    self,
    series_codes: List[str],
    source: TickerSource,
    start_date: Optional[Any],
    end_date: Optional[Any],
    live: bool,
) -> pd.DataFrame:
    if live:
        metadata_df = self._load_metadata_rows(
            {MetadataColumns.SERIES_CODE: series_codes},
            exclude=False,
        )

        if metadata_df.empty:
            return self._empty_value_df()

        tickers = build_series_to_ticker_map(metadata_df, source)

        return get_direct_source_values(
            load_metadata_rows=lambda filters: self._load_metadata_rows(
                filters,
                exclude=False,
            ),
            series_codes=series_codes,
            tickers=tickers,
            source=source,
            start=start_date,
            end=end_date,
        )

    return self._value_repository.get_batch_series_data(
        series_codes=series_codes,
        tickersource=source,
        start=start_date,
        end=end_date,
    )
def _load_values_using_default_source(
    self,
    resolved_series_codes: List[str],
    start_date: Optional[Any],
    end_date: Optional[Any],
    live: bool,
) -> pd.DataFrame:
    metadata_df = self._load_metadata_rows(
        {MetadataColumns.SERIES_CODE: resolved_series_codes},
        exclude=False,
    )

    if metadata_df.empty:
        return self._empty_value_df()

    if MetadataColumns.DEFAULT_SOURCE not in metadata_df.columns:
        raise ValueQueryParameterError(
            f"Metadata missing required column '{MetadataColumns.DEFAULT_SOURCE}'"
        )

    parts: List[pd.DataFrame] = []

    for default_source, group_df in metadata_df.groupby(MetadataColumns.DEFAULT_SOURCE):
        if pd.isna(default_source) or not str(default_source).strip():
            bad_codes = group_df[MetadataColumns.SERIES_CODE].dropna().tolist()
            raise ValueQueryParameterError(
                f"Missing default_source for series: {bad_codes}"
            )

        try:
            source = TickerSource.from_str(str(default_source).strip())
        except Exception as exc:
            bad_codes = group_df[MetadataColumns.SERIES_CODE].dropna().tolist()
            raise ValueQueryParameterError(
                f"Invalid default_source '{default_source}' for series: {bad_codes}"
            ) from exc

        group_codes = (
            group_df[MetadataColumns.SERIES_CODE]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

        part = self._load_values_for_source(
            series_codes=group_codes,
            source=source,
            start_date=start_date,
            end_date=end_date,
            live=live,
        )

        if not part.empty:
            parts.append(part)

    if not parts:
        return self._empty_value_df()

    return pd.concat(parts, ignore_index=True)
def _format_values_dataframe(
    self,
    value_df: pd.DataFrame,
    live: bool,
    business_days_only: bool,
    rename: bool,
) -> pd.DataFrame:
    if value_df.empty:
        return value_df

    if live:
        pivoted_df = value_df
    else:
        pivoted_df = value_df.pivot(
            index=ValueColumns.TIMESTAMP,
            columns=ValueColumns.SERIES_CODE,
            values=ValueColumns.VALUE,
        )

    if business_days_only:
        pivoted_df = pivoted_df.dropna(how="all")

    if rename:
        name_map = self.get_name_map(MetadataColumns.INTERNAL)
        pivoted_df = pivoted_df.rename(columns=name_map)

    pivoted_df.index = pivoted_df.index.normalize().tz_convert(None)

    return pivoted_df