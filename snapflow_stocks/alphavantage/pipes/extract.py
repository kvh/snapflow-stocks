from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date, timedelta, datetime
from typing import TYPE_CHECKING, Dict, List, Optional

from snapflow import DataBlock, PipeContext, pipe
from snapflow.storage.data_formats import RecordsIterator, Records
from snapflow.core.extraction.connection import JsonHttpApiConnection
from snapflow.utils.common import (
    ensure_date,
    ensure_datetime,
    ensure_utc,
    title_to_snake_case,
    utcnow,
)
from snapflow.utils.data import read_csv

if TYPE_CHECKING:
    from snapflow_stocks import (
        Ticker,
        AlphavantageEodPrice,
        AlphavantageCompanyOverview,
    )


ALPHAVANTAGE_API_BASE_URL = "https://www.alphavantage.co/query"
MIN_DATE = date(2000, 1, 1)
MIN_DATETIME = datetime(2000, 1, 1)


@dataclass
class ExtractAlphavantageEodConfig:
    api_key: str
    tickers: Optional[List[str]] = None


@dataclass
class ExtractAlphavantageEodState:
    ticker_latest_dates_extracted: Dict[str, date]


def prepare_tickers(
    ctx: PipeContext, tickers: Optional[DataBlock[Ticker]] = None
) -> Optional[List[str]]:
    if tickers is None:
        tickers = ctx.get_config_value("tickers")
        if tickers is None:
            return
    else:
        df = tickers.as_dataframe()
        tickers = df["symbol"]
    return tickers


def prepare_params_for_ticker(
    ticker: str, ticker_latest_dates_extracted: Dict[str, date]
) -> Dict:
    latest_date_extracted = ensure_date(
        ticker_latest_dates_extracted.get(ticker, MIN_DATE)
    )
    if latest_date_extracted <= utcnow().date() - timedelta(days=100):
        # More than 100 days worth, get full
        outputsize = "full"
    else:
        # Less than 100 days, compact will suffice
        outputsize = "compact"
    params = {
        "symbol": ticker,
        "outputsize": outputsize,
        "datatype": "csv",
        "function": "TIME_SERIES_DAILY_ADJUSTED",
    }
    return params


@pipe(
    "alphavantage_extract_eod_prices",
    module="stocks",
    config_class=ExtractAlphavantageEodConfig,
    state_class=ExtractAlphavantageEodState,
)
def alphavantage_extract_eod_prices(
    ctx: PipeContext, tickers: Optional[DataBlock[Ticker]] = None
) -> RecordsIterator[AlphavantageEodPrice]:
    api_key = ctx.get_config_value("api_key")
    assert api_key is not None
    tickers = prepare_tickers(ctx, tickers)
    if tickers is None:
        # We didn't get an input block for tickers AND
        # the config is empty, so we are done
        return None
    ticker_latest_dates_extracted = (
        ctx.get_state_value("ticker_latest_dates_extracted") or {}
    )
    conn = JsonHttpApiConnection()

    def fetch_prices(params: Dict, tries: int = 0) -> Optional[Records]:
        if tries > 2:
            return None
        resp = conn.get(ALPHAVANTAGE_API_BASE_URL, params, stream=True)
        records = list(read_csv(resp.raw))
        if records:
            # Alphavantage returns 200 and json error message on failure
            if "Error Message" in str(records[0]):
                # TODO: Log this failure?
                print(f"Error for ticker {ticker}: {records[0]}")
                return None
            if "calls per minute" in str(records[0]):
                time.sleep(60)
                return fetch_prices(params, tries=tries + 1)
        return records

    for ticker in tickers:
        assert isinstance(ticker, str)
        params = prepare_params_for_ticker(ticker, ticker_latest_dates_extracted)
        params["apikey"] = api_key
        records = fetch_prices(params)
        if not records:
            continue
        # Symbol not included
        for r in records:
            r["symbol"] = ticker
        yield records
        # Update state
        ticker_latest_dates_extracted[ticker] = utcnow().date()
        ctx.emit_state_value(
            "ticker_latest_dates_extracted", ticker_latest_dates_extracted
        )
        if not ctx.should_continue():
            break


@pipe(
    "alphavantage_extract_company_overview",
    module="stocks",
    config_class=ExtractAlphavantageEodConfig,
    state_class=ExtractAlphavantageEodState,
)
def alphavantage_extract_company_overview(
    ctx: PipeContext, tickers: Optional[DataBlock[Ticker]] = None
) -> RecordsIterator[AlphavantageCompanyOverview]:
    api_key = ctx.get_config_value("api_key")
    assert api_key is not None
    tickers = prepare_tickers(ctx, tickers)
    if tickers is None:
        # We didn't get an input block for tickers AND
        # the config is empty, so we are done
        return None
    ticker_latest_dates_extracted = (
        ctx.get_state_value("ticker_latest_dates_extracted") or {}
    )
    conn = JsonHttpApiConnection()
    for i, ticker in enumerate(tickers):
        assert isinstance(ticker, str)
        latest_date_extracted = ensure_datetime(
            ticker_latest_dates_extracted.get(ticker, MIN_DATETIME)
        )
        assert latest_date_extracted is not None
        # Refresh at most once a day
        # TODO: make this configurable instead of hard-coded 1 day
        if utcnow() - ensure_utc(latest_date_extracted) < timedelta(days=1):
            continue
        params = {
            "apikey": api_key,
            "symbol": ticker,
            "function": "OVERVIEW",
        }
        resp = conn.get(ALPHAVANTAGE_API_BASE_URL, params, stream=True)
        record = resp.json()
        if not record:
            continue
        yield [record]
        # Clean up json keys to be more DB friendly
        record = {title_to_snake_case(k): v for k, v in record.items()}
        # Update state
        ticker_latest_dates_extracted[ticker] = utcnow()
        ctx.emit_state_value(
            "ticker_latest_dates_extracted", ticker_latest_dates_extracted
        )
        if not ctx.should_continue():
            break
