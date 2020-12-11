from snapflow import graph, produce


def ensure_api_key() -> str:
    api_key = os.environ.get("MARKETSTACK_ACCESS_KEY")
    if api_key is not None:
        return api_key
    api_key = input("Enter Marketstack access key: ")
    return api_key


def test():
    import snapflow_stocks

    api_key = ensure_api_key()

    g = graph()

    # Initial graph
    orders = g.create_node(
        snapflow_stocks.pipes.marketstack_extract_eod_prices,
        config={"access_key": api_key, "tickers": ["AAPL"]},
    )
    output = produce(orders, modules=[snapflow_stocks])
    records = output.as_records_list()
    print(records)
    assert len(records) > 0


if __name__ == "__main__":
    test()
