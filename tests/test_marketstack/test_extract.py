import os

from snapflow import graph, produce


def ensure_api_key() -> str:
    api_key = os.environ.get("MARKETSTACK_ACCESS_KEY")
    if api_key is not None:
        return api_key
    api_key = input("Enter Marketstack access key: ")
    return api_key


def test_eod():
    import snapflow_stocks

    api_key = ensure_api_key()

    g = graph()

    # Initial graph
    prices = g.create_node(
        snapflow_stocks.pipes.marketstack_extract_eod_prices,
        config={"access_key": api_key, "tickers": ["AAPL"]},
    )
    output = produce(prices, node_timelimit_seconds=1, modules=[snapflow_stocks])
    records = output.as_records()
    assert len(records) >= 100


def test_tickers():
    import snapflow_stocks

    api_key = ensure_api_key()

    g = graph()

    # Initial graph
    tickers = g.create_node(
        snapflow_stocks.pipes.marketstack_extract_tickers,
        config={"access_key": api_key, "exchanges": ["XNAS"]},
    )
    output = produce(tickers, node_timelimit_seconds=1, modules=[snapflow_stocks])
    records = output.as_records()
    assert len(records) >= 100


def test_tickers_into_eod():
    import snapflow_stocks

    api_key = ensure_api_key()

    g = graph()

    # Initial graph
    tickers = g.create_node(
        snapflow_stocks.pipes.marketstack_extract_tickers,
        config={"access_key": api_key, "exchanges": ["XNAS"]},
    )
    prices = g.create_node(
        snapflow_stocks.pipes.marketstack_extract_eod_prices,
        config={"access_key": api_key},
        upstream={"tickers": tickers},
    )
    output = produce(prices, node_timelimit_seconds=1, modules=[snapflow_stocks])
    records = output.as_records()
    assert len(records) >= 100


if __name__ == "__main__":
    test_tickers_into_eod()
