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
      - `marketstack_extract_tickers`
      - `marketstack_extract_eod_prices`
    - Schemas
      - `MarketstackTicker`

#### Example

```python
import snapflow_stocks


g = graph()

# Initial graph
tickers = g.create_node(
    snapflow_stocks.pipes.marketstack_extract_tickers,
    config={"access_key": "xxxxx", "exchanges": ["XNAS", "XNYS"]},
)
prices = g.create_node(
    snapflow_stocks.pipes.marketstack_extract_eod_prices,
    config={"access_key": api_key},
    upstream={"tickers": tickers}
)
output = produce(prices, node_timelimit_seconds=5, modules=[snapflow_stocks])
records = output.as_records_list()
print(records)
```
