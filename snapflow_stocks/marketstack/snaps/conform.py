from __future__ import annotations

from typing import TYPE_CHECKING

from snapflow import DataBlock, Snap
from snapflow.storage.data_formats.data_frame import DataFrameIterator

if TYPE_CHECKING:
    from snapflow_stocks import Ticker, MarketstackTicker


@Snap(
    "marketstack_conform_tickers",
    module="stocks",
)
def marketstack_conform_tickers(
    tickers: DataBlock[MarketstackTicker],
) -> DataFrameIterator[Ticker]:
    for df in tickers.as_dataframe_iterator():
        df["exchange"] = df["exchange_acronym"]
        yield df[["symbol", "name", "exchange"]]
