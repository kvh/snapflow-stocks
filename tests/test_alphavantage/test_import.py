import os

from snapflow import graph, produce


def ensure_api_key() -> str:
    api_key = os.environ.get("ALPHAVANTAGE_API_KEY")
    if api_key is not None:
        return api_key
    api_key = input("Enter Alphavantage api key: ")
    return api_key


def test_eod():
    import snapflow_stocks

    api_key = ensure_api_key()

    g = graph()

    # Initial graph
    prices = g.node(
        snapflow_stocks.snaps.alphavantage_import_eod_prices,
        params={"api_key": api_key, "tickers": ["AAPL"]},
    )
    output = produce(prices, node_timelimit_seconds=1, modules=[snapflow_stocks])
    records = output.as_records()
    assert len(records) > 0


def test_overview():
    import snapflow_stocks

    api_key = ensure_api_key()

    g = graph()

    # Initial graph
    overview = g.node(
        snapflow_stocks.snaps.alphavantage_import_company_overview,
        params={"api_key": api_key, "tickers": ["AAPL"]},
    )
    output = produce(overview, node_timelimit_seconds=1, modules=[snapflow_stocks])
    records = output.as_records()
    assert len(records) == 1


if __name__ == "__main__":
    test_eod()
    test_overview()
