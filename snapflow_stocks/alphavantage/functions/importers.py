from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional

from dcp.data_format.formats.memory.records import Records
from dcp.utils.common import (
    ensure_date,
    ensure_datetime,
    ensure_utc,
    title_to_snake_case,
    utcnow,
)
from dcp.utils.data import read_csv
from snapflow import DataBlock
from snapflow import datafunction, Context
from snapflow.core.extraction.connection import JsonHttpApiConnection
from snapflow.core.function import Input
from snapflow.core.function_interface import Reference

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
class ImportAlphavantageEodState:
    ticker_latest_dates_imported: Dict[str, date]


def prepare_tickers(
    tickers_list: Optional[List] = None,
    tickers_input: Optional[DataBlock[Ticker]] = None,
) -> Optional[List[str]]:
    tickers = []
    if tickers_input is not None:
        df = tickers_input.as_dataframe()
        tickers = list(df["symbol"])
    else:
        tickers = tickers_list or []
    return tickers


def prepare_params_for_ticker(
    ticker: str, ticker_latest_dates_imported: Dict[str, datetime]
) -> Dict:
    latest_date_imported = ensure_datetime(
        ticker_latest_dates_imported.get(ticker, MIN_DATETIME)
    )
    if ensure_utc(latest_date_imported) <= utcnow() - timedelta(days=100):
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


def is_alphavantage_error(record: Dict) -> bool:
    str_record = str(record).lower()
    return "error message" in str_record or "invalid api call" in str_record


def is_alphavantage_rate_limit(record: Dict) -> bool:
    return (
        "calls per minute" in str(record).lower()
        or "api call volume" in str(record).lower()
    )


@datafunction(
    "alphavantage_import_eod_prices",
    namespace="stocks",
    state_class=ImportAlphavantageEodState,
    display_name="Import Alphavantage EOD prices",
)
def alphavantage_import_eod_prices(
    ctx: Context,
    tickers_input: Optional[Reference[Ticker]],
    api_key: str,
    tickers: Optional[List] = None,
) -> Iterator[Records[AlphavantageEodPrice]]:
    assert api_key is not None
    tickers = prepare_tickers(tickers, tickers_input)
    if not tickers:
        return None
    ticker_latest_dates_imported = (
        ctx.get_state_value("ticker_latest_dates_imported") or {}
    )
    conn = JsonHttpApiConnection()

    def fetch_prices(params: Dict, tries: int = 0) -> Optional[Records]:
        if tries > 2:
            return None
        resp = conn.get(ALPHAVANTAGE_API_BASE_URL, params, stream=True)
        try:
            record = resp.json()
            # Json response means error
            if is_alphavantage_error(record):
                # TODO: Log this failure?
                print(f"Error for {params} {record}")
                return None
            if is_alphavantage_rate_limit(record):
                time.sleep(60)
                return fetch_prices(params, tries=tries + 1)
        except:
            pass
        # print(resp.raw.read().decode("utf8"))
        # resp.raw.seek(0)
        records = list(read_csv(resp.iter_lines()))
        return records

    for ticker in tickers:
        assert isinstance(ticker, str)
        latest_date_imported = ensure_datetime(
            ticker_latest_dates_imported.get(ticker, MIN_DATETIME)
        )
        assert latest_date_imported is not None
        if utcnow() - ensure_utc(latest_date_imported) < timedelta(days=1):
            # Only check once a day
            continue
        params = prepare_params_for_ticker(ticker, ticker_latest_dates_imported)
        params["apikey"] = api_key
        records = fetch_prices(params)
        if records:
            # Symbol not included
            for r in records:
                r["symbol"] = ticker
            yield records
        # Update state
        ticker_latest_dates_imported[ticker] = utcnow()
        ctx.emit_state_value(
            "ticker_latest_dates_imported", ticker_latest_dates_imported
        )
        if not ctx.should_continue():
            break


@datafunction(
    "alphavantage_import_company_overview",
    namespace="stocks",
    state_class=ImportAlphavantageEodState,
    display_name="Import Alphavantage company overview",
)
def alphavantage_import_company_overview(
    ctx: Context,
    tickers_input: Optional[Reference[Ticker]],
    api_key: str,
    tickers: Optional[List] = None,
) -> Iterator[Records[AlphavantageCompanyOverview]]:
    assert api_key is not None
    tickers = prepare_tickers(tickers, tickers_input)
    if tickers is None:
        # We didn't get an input block for tickers AND
        # the config is empty, so we are done
        return None
    ticker_latest_dates_imported = (
        ctx.get_state_value("ticker_latest_dates_imported") or {}
    )
    conn = JsonHttpApiConnection()
    batch_size = 50
    records = []
    tickers_updated = []

    def fetch_overview(params: Dict, tries: int = 0) -> Optional[Dict]:
        if tries > 2:
            return None
        resp = conn.get(ALPHAVANTAGE_API_BASE_URL, params)
        record = resp.json()
        # Alphavantage returns 200 and json error message on failure
        if is_alphavantage_error(record):
            # TODO: Log this failure?
            # print(f"Error for ticker {params['symbol']}: {record}")
            return None
        if is_alphavantage_rate_limit(record):
            time.sleep(20)
            return fetch_overview(params, tries=tries + 1)
        return record

    for i, ticker in enumerate(tickers):
        assert isinstance(ticker, str)
        latest_date_imported = ensure_datetime(
            ticker_latest_dates_imported.get(ticker, MIN_DATETIME)
        )
        assert latest_date_imported is not None
        # Refresh at most once a day
        # TODO: make this configurable instead of hard-coded 1 day
        if utcnow() - ensure_utc(latest_date_imported) < timedelta(days=1):
            continue
        params = {
            "apikey": api_key,
            "symbol": ticker,
            "function": "OVERVIEW",
        }
        record = fetch_overview(params)
        if not record:
            continue

        # Clean up json keys to be more DB friendly
        record = {title_to_snake_case(k): v for k, v in record.items()}
        records.append(record)
        tickers_updated.append(ticker)
        if len(records) >= batch_size or i == len(tickers) - 1:
            yield records
            # Update state
            for updated_ticker in tickers_updated:
                ticker_latest_dates_imported[updated_ticker] = utcnow()
                ctx.emit_state_value(
                    "ticker_latest_dates_imported", ticker_latest_dates_imported
                )
            if not ctx.should_continue():
                break
            records = []
            tickers_updated = []
