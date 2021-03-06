Stocks module for the [snapflow](https://github.com/kvh/snapflow) framework.

### Install

`pip install snapflow-stocks` or `poetry add snapflow-stocks`

### Components

- Schemas
  - `Ticker`
  - `EodPrice`
- Vendors
  - Marketstack
    - Pipes
      - `marketstack_import_tickers`
      - `marketstack_import_eod_prices`
    - Schemas
      - `MarketstackTicker`

#### Example

```python
import snapflow_stocks


g = graph()

# Initial graph
tickers = g.create_node(
    snapflow_stocks.functions.marketstack_import_tickers,
    params={"access_key": "xxxxx", "exchanges": ["XNAS", "XNYS"]},
)
prices = g.create_node(
    snapflow_stocks.functions.marketstack_import_eod_prices,
    params={"access_key": api_key},
    upstream={"tickers": tickers}
)
blocks = produce(prices, execution_timelimit_seconds=5, modules=[snapflow_stocks])
records = blocks[0].as_records()
print(records)
```
