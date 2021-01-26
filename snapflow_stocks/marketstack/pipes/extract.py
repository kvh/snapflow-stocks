from __future__ import annotations
from base.utils import utc_now

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Dict, List, Optional

import pytz
from snapflow import DataBlock, PipeContext, pipe
from snapflow.storage.data_formats import RecordsIterator
from snapflow.core.extraction.connection import JsonHttpApiConnection
from snapflow.utils.common import ensure_date, ensure_datetime

if TYPE_CHECKING:
    from snapflow_stocks import EodPrice, MarketstackTicker, Ticker


MARKETSTACK_API_BASE_URL = "http://api.marketstack.com/v1/"
HTTPS_MARKETSTACK_API_BASE_URL = "https://api.marketstack.com/v1/"
MIN_DATE = date(2000, 1, 1)


def ensure_utc(x: datetime) -> datetime:
    try:
        return pytz.utc.localize(x)
    except:
        pass
    return x


@dataclass
class ExtractMarketstackEodConfig:
    access_key: str
    tickers: Optional[List[str]] = None
    from_date: Optional[date] = MIN_DATE
    use_https: bool = False


@dataclass
class ExtractMarketstackEodState:
    ticker_latest_dates_extracted: Dict[str, date]


@pipe(
    "marketstack_extract_eod_prices",
    module="stocks",
    config_class=ExtractMarketstackEodConfig,
    state_class=ExtractMarketstackEodState,
)
def marketstack_extract_eod_prices(
    ctx: PipeContext, tickers: Optional[DataBlock[Ticker]] = None
) -> RecordsIterator[EodPrice]:
    access_key = ctx.get_config_value("access_key")
    use_https = ctx.get_config_value("use_https", False)
    default_from_date = ctx.get_config_value("from_date", MIN_DATE)
    assert access_key is not None
    if tickers is None:
        tickers = ctx.get_config_value("tickers")
        if tickers is None:
            # We didn't get an input block for tickers AND
            # the config is empty, so we are done
            return
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


@dataclass
class ExtractMarketstackTickersConfig:
    access_key: str
    exchanges: Optional[List[str]] = field(
        default_factory=lambda: ["XNYS", "XNAS"]
    )  # Default NYSE and NASDAQ
    use_https: bool = False
    update_frequency_days: int = 1


@dataclass
class ExtractMarketstackTickersState:
    last_extracted_at: datetime


@pipe(
    "marketstack_extract_tickers",
    module="stocks",
    config_class=ExtractMarketstackTickersConfig,
    state_class=ExtractMarketstackTickersState,
)
def marketstack_extract_tickers(
    ctx: PipeContext,
) -> RecordsIterator[MarketstackTicker]:
    access_key = ctx.get_config_value("access_key")
    use_https = ctx.get_config_value("use_https", False)
    default_from_date = ensure_date(ctx.get_config_value("from_date", MIN_DATE))
    assert access_key is not None
    exchanges = ctx.get_config_value("exchanges")
    assert isinstance(exchanges, list)
    last_extracted_at = ensure_datetime(
        ctx.get_state_value("last_extracted_at") or "2020-01-01 00:00:00"
    )
    assert last_extracted_at is not None
    last_extracted_at = ensure_utc(last_extracted_at)
    if utc_now() - last_extracted_at < timedelta(days=1):  # TODO: from config
        return
    conn = JsonHttpApiConnection()
    if use_https:
        endpoint_url = HTTPS_MARKETSTACK_API_BASE_URL + "tickers"
    else:
        endpoint_url = MARKETSTACK_API_BASE_URL + "tickers"
    for exchange in exchanges:
        params = {
            "limit": 1000,
            "offset": 0,
            "access_key": access_key,
            "exchange": exchange,
        }
        while ctx.should_continue():
            resp = conn.get(endpoint_url, params)
            json_resp = resp.json()
            assert isinstance(json_resp, dict)
            records = json_resp["data"]
            if len(records) == 0:
                # All done
                break
            # Add a flattened exchange indicator
            for r in records:
                r["exchange_acronym"] = r.get("stock_exchange", {}).get("acronym")
            yield records
            # Setup for next page
            params["offset"] = params["offset"] + len(records)
    ctx.emit_state_value("last_extracted_at", utc_now())
