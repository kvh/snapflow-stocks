from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional, TYPE_CHECKING

from snapflow import PipeContext, pipe, DataBlock
from snapflow.core.data_formats import RecordsListGenerator
from snapflow.core.extraction.connection import JsonHttpApiConnection
from snapflow.utils.common import ensure_date

if TYPE_CHECKING:
    from snapflow_stocks import EodPrice, Ticker


MARKETSTACK_API_BASE_URL = "http://api.marketstack.com/v1/"
HTTPS_MARKETSTACK_API_BASE_URL = "https://api.marketstack.com/v1/"
MIN_DATE = date(2000, 1, 1)


@dataclass
class ExtractMarketstackConfig:
    access_key: str
    tickers: Optional[List[str]] = None
    from_date: Optional[date] = MIN_DATE
    use_https: bool = False


@dataclass
class ExtractMarketstackState:
    ticker_latest_dates_extracted: Dict[str, date]


@pipe(
    "marketstack_extract_eod_prices",
    module="stocks",
    config_class=ExtractMarketstackConfig,
    state_class=ExtractMarketstackState,
)
def marketstack_extract_eod_prices(
    ctx: PipeContext, tickers: Optional[DataBlock[Ticker]] = None
) -> RecordsListGenerator[EodPrice]:  # [MarketstackEodStockPrice]:
    access_key = ctx.get_config_value("access_key")
    use_https = ctx.get_config_value("use_https", False)
    default_from_date = ctx.get_config_value("from_date", MIN_DATE)
    assert access_key is not None
    if tickers is None:
        tickers = ctx.get_config_value("tickers")
        assert isinstance(tickers, list)
    else:
        tickers = tickers.as_dataframe()["symbol"]
    ticker_latest_dates_extracted = (
        ctx.get_state_value("ticker_latest_dates_extracted") or {}
    )
    conn = JsonHttpApiConnection(date_format="%Y-%m-%d")
    if use_https:
        endpoint_url = HTTPS_MARKETSTACK_API_BASE_URL + "eod"
    else:
        endpoint_url = MARKETSTACK_API_BASE_URL + "eod"
    for ticker in tickers:
        assert isinstance(ticker, str)
        latest_date_extracted = ensure_date(
            ticker_latest_dates_extracted.get(ticker, default_from_date)
        )
        max_date = latest_date_extracted
        params = {
            "limit": 1000,
            "offset": 0,
            "access_key": access_key,
            "symbols": ticker,
            "date_from": latest_date_extracted,
        }
        while ctx.should_continue():
            resp = conn.get(endpoint_url, params)
            json_resp = resp.json()
            assert isinstance(json_resp, dict)
            records = json_resp["data"]
            if len(records) == 0:
                # All done
                break
            yield records
            # Update state
            max_date = max(max_date, max(ensure_date(r["date"]) for r in records))
            ticker_latest_dates_extracted[ticker] = max_date + timedelta(days=1)
            ctx.emit_state_value(
                "ticker_latest_dates_extracted", ticker_latest_dates_extracted
            )
            # Setup for next page
            params["offset"] = params["offset"] + len(records)

