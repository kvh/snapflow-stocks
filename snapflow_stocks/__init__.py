from snapflow_stocks.alphavantage.snaps.extract import alphavantage_extract_eod_prices
from typing import TypeVar

from snapflow import SnapflowModule
from snapflow_stocks.marketstack.snaps.extract import (
    marketstack_extract_eod_prices,
    marketstack_extract_tickers,
)
from snapflow_stocks.alphavantage.snaps.extract import alphavantage_extract_eod_prices

# Schemas (for type hinting in python)
Ticker = TypeVar("Ticker")
EodPrice = TypeVar("EodPrice")
# Vendor-specific
AlphavantageEodPrice = TypeVar("AlphavantageEodPrice")
MarketstackTicker = TypeVar("MarketstackTicker")

module = SnapflowModule(
    "stocks",
    py_module_path=__file__,
    py_module_name=__name__,
    schemas=[
        "schemas/ticker.yml",
        "schemas/eod_price.yml",
        "alphavantage/schemas/alphavantage_eod_price.yml",
        "marketstack/schemas/marketstack_ticker.yml",
    ],
    snaps=[
        alphavantage_extract_eod_prices,
        marketstack_extract_eod_prices,
        marketstack_extract_tickers,
    ],
)
module.export()
