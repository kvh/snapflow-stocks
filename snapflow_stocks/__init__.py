from typing import TypeVar
from snapflow import SnapflowModule

from snapflow_stocks.marketstack.pipes.extract_ import marketstack_extract_eod_prices

# Schemas (for type hinting in python)
Ticker = TypeVar("Ticker")
EodPrice = TypeVar("EodPrice")

module = SnapflowModule(
    "stocks",
    py_module_path=__file__,
    py_module_name=__name__,
    schemas=["schemas/ticker.yml"],
    pipes=[marketstack_extract_eod_prices],
)
module.export()
