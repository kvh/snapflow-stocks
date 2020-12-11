from typing import TypeVar
from snapflow import SnapflowModule

from snapflow_stocks.marketstack.pipes.extract_ import marketstack_extract_eod_prices

ExampleSchema = TypeVar("ExampleSchema")

module = SnapflowModule(
    "stocks",
    py_module_path=__file__,
    py_module_name=__name__,
    # schemas=["schemas/schema.yml"],
    pipes=[marketstack_extract_eod_prices],
)
module.export()
