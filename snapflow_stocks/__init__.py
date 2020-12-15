from typing import TypeVar

from snapflow import SnapflowModule
from snapflow_stocks.marketstack.pipes.extract import (
    marketstack_extract_eod_prices,
    marketstack_extract_tickers,
)

# Schemas (for type hinting in python)
Ticker = TypeVar("Ticker")
EodPrice = TypeVar("EodPrice")
MarketstackTicker = TypeVar("MarketstackTicker")

module = SnapflowModule(
    "stocks",
    py_module_path=__file__,
    py_module_name=__name__,
    schemas=[
        "schemas/ticker.yml",
        "schemas/eod_price.yml",
        "marketstack/schemas/marketstack_ticker.yml",
    ],
    pipes=[marketstack_extract_eod_prices, marketstack_extract_tickers],
)
module.export()
