from snapflow_stocks.alphavantage.pipes.extract import (
    alphavantage_extract_company_overview,
    alphavantage_extract_eod_prices,
)
from typing import TypeVar

from snapflow import SnapflowModule
from snapflow_stocks.marketstack.pipes.conform import marketstack_conform_tickers
from snapflow_stocks.marketstack.pipes.extract import (
    marketstack_extract_eod_prices,
    marketstack_extract_tickers,
)
from snapflow_stocks.alphavantage.pipes.extract import alphavantage_extract_eod_prices

# Schemas (for type hinting in python)
Ticker = TypeVar("Ticker")
EodPrice = TypeVar("EodPrice")
# Vendor-specific
AlphavantageEodPrice = TypeVar("AlphavantageEodPrice")
AlphavantageCompanyOverview = TypeVar("AlphavantageCompanyOverview")
MarketstackTicker = TypeVar("MarketstackTicker")

module = SnapflowModule(
    "stocks",
    py_module_path=__file__,
    py_module_name=__name__,
    schemas=[
        "schemas/ticker.yml",
        "schemas/eod_price.yml",
        "alphavantage/schemas/alphavantage_eod_price.yml",
        "alphavantage/schemas/alphavantage_company_overview.yml",
        "marketstack/schemas/marketstack_ticker.yml",
    ],
    pipes=[
        alphavantage_extract_eod_prices,
        alphavantage_extract_company_overview,
        marketstack_extract_eod_prices,
        marketstack_extract_tickers,
        marketstack_conform_tickers,
    ],
)
module.export()
