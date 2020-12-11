from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from snapflow import DataBlock, pipe, PipeContext


@dataclass
class ExampleConfig:
    config_val: str


@dataclass
class ExampleState:
    state_val: str


@pipe(
    "{example_pipe}",
    module="{module name}",
    config_class=ExampleConfig,
    state_class=ExampleState,
)
def example_pipe(
    ctx: PipeContext, block: DataBlock
) -> pd.DataFrame[Any]:
    return block.as_dataframe()