from typing import TypeVar

from snapflow import SnapflowModule
from snapflow_stocks.alphavantage.functions.importers import (
    alphavantage_import_company_overview,
    alphavantage_import_eod_prices,
)
from snapflow_stocks.marketstack.functions.conformers import marketstack_conform_tickers
from snapflow_stocks.marketstack.functions.importers import (
    marketstack_import_eod_prices,
    marketstack_import_tickers,
)

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
    schema_paths=["schemas", "alphavantage/schemas", "marketstack/schemas",],
)
module.add_function(alphavantage_import_eod_prices)
module.add_function(alphavantage_import_company_overview)
module.add_function(marketstack_import_eod_prices)
module.add_function(marketstack_import_tickers)
module.add_function(marketstack_conform_tickers)
