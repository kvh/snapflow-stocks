import os

from snapflow import graph, produce


def ensure_api_key() -> str:
    api_key = os.environ.get("MARKETSTACK_ACCESS_KEY")
    if api_key is not None:
        return api_key
    api_key = input("Enter Marketstack access key: ")
    return api_key


def test_eod():
    from snapflow_stocks import module as stocks

    api_key = ensure_api_key()

    g = graph()

    # Initial graph
    prices = g.create_node(
        stocks.functions.marketstack_import_eod_prices,
        params={"access_key": api_key, "tickers": ["AAPL"]},
    )
    blocks = produce(prices, execution_timelimit_seconds=1, modules=[stocks])
    records = blocks[0].as_records()
    assert len(records) >= 100


def test_tickers():
    from snapflow_stocks import module as stocks

    api_key = ensure_api_key()

    g = graph()

    # Initial graph
    tickers = g.create_node(
        stocks.functions.marketstack_import_tickers,
        params={"access_key": api_key, "exchanges": ["XNAS"]},
    )
    blocks = produce(tickers, execution_timelimit_seconds=1, modules=[stocks])
    records = blocks[0].as_records()
    assert len(records) >= 100


def test_tickers_into_eod():
    from snapflow_stocks import module as stocks

    api_key = ensure_api_key()

    g = graph()

    # Initial graph
    tickers = g.create_node(
        stocks.functions.marketstack_import_tickers,
        params={"access_key": api_key, "exchanges": ["XNAS"]},
    )
    prices = g.create_node(
        stocks.functions.marketstack_import_eod_prices,
        params={"access_key": api_key},
        inputs={"tickers_input": tickers},
    )
    blocks = produce(prices, execution_timelimit_seconds=1, modules=[stocks])
    records = blocks[0].as_records()
    assert len(records) >= 100


if __name__ == "__main__":
    test_tickers_into_eod()
