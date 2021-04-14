from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional

import pytz
from dcp.data_format.formats.memory.records import Records
from dcp.utils.common import ensure_date, ensure_datetime, ensure_utc, utcnow
from snapflow import DataBlock, Param, Snap, SnapContext
from snapflow.core.extraction.connection import JsonHttpApiConnection
from snapflow.core.snap import Input

if TYPE_CHECKING:
    from snapflow_stocks import EodPrice, MarketstackTicker, Ticker


MARKETSTACK_API_BASE_URL = "http://api.marketstack.com/v1/"
HTTPS_MARKETSTACK_API_BASE_URL = "https://api.marketstack.com/v1/"
MIN_DATE = date(2000, 1, 1)


@dataclass
class ImportMarketstackEodState:
    ticker_latest_dates_imported: Dict[str, date]


@Snap(
    "marketstack_import_eod_prices",
    module="stocks",
    state_class=ImportMarketstackEodState,
    display_name="Import Marketstack EOD prices",
)
@Param("access_key", "str")
@Param("tickers", "json", required=False)
@Param("from_date", "date", default=MIN_DATE)
@Input("tickers", schema="Ticker", reference=True, required=False)
def marketstack_import_eod_prices(
    ctx: SnapContext, tickers: Optional[DataBlock[Ticker]] = None
) -> Iterator[Records[EodPrice]]:
    access_key = ctx.get_param("access_key")
    use_https = False  # TODO: when do we want this True?
    default_from_date = ctx.get_param("from_date", MIN_DATE)
    assert access_key is not None
    if tickers is None:
        tickers = ctx.get_param("tickers")
        if tickers is None:
            # We didn't get an input block for tickers AND
            # the params is empty, so we are done
            return
    else:
        tickers = tickers.as_dataframe()["symbol"]
    ticker_latest_dates_imported = (
        ctx.get_state_value("ticker_latest_dates_imported") or {}
    )
    conn = JsonHttpApiConnection(date_format="%Y-%m-%d")
    if use_https:
        endpoint_url = HTTPS_MARKETSTACK_API_BASE_URL + "eod"
    else:
        endpoint_url = MARKETSTACK_API_BASE_URL + "eod"
    for ticker in tickers:
        assert isinstance(ticker, str)
        latest_date_imported = ensure_date(
            ticker_latest_dates_imported.get(ticker, default_from_date)
        )
        max_date = latest_date_imported
        params = {
            "limit": 1000,
            "offset": 0,
            "access_key": access_key,
            "symbols": ticker,
            "date_from": latest_date_imported,
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
            ticker_latest_dates_imported[ticker] = max_date + timedelta(days=1)
            ctx.emit_state_value(
                "ticker_latest_dates_imported", ticker_latest_dates_imported
            )
            # Setup for next page
            params["offset"] = params["offset"] + len(records)


@dataclass
class ImportMarketstackTickersState:
    last_imported_at: datetime


@Snap(
    "marketstack_import_tickers",
    module="stocks",
    state_class=ImportMarketstackTickersState,
    display_name="Import Marketstack tickers",
)
@Param("access_key", "str")
@Param("exchanges", "json", default=["XNYS", "XNAS"])
def marketstack_import_tickers(
    ctx: SnapContext,
) -> Iterator[Records[MarketstackTicker]]:
    access_key = ctx.get_param("access_key")
    use_https = False  # TODO: when do we want this True?
    # default_from_date = ctx.get_param("from_date", MIN_DATE)
    assert access_key is not None
    exchanges = ctx.get_param("exchanges")
    assert isinstance(exchanges, list)
    last_imported_at = ensure_datetime(
        ctx.get_state_value("last_imported_at") or "2020-01-01 00:00:00"
    )
    assert last_imported_at is not None
    last_imported_at = ensure_utc(last_imported_at)
    if utcnow() - last_imported_at < timedelta(days=1):  # TODO: from config
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
    ctx.emit_state_value("last_imported_at", utcnow())
