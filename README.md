# SGTRS
# Amalgam — minimal skeleton


## Quickstart
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env && $EDITOR .env


# fetch 7 days for 5 tickers
python -m ingest.tiingo_prices_eod --from 2025-09-22 --to 2025-09-29 --tickers AAPL,MSFT,AMZN,GOOGL,TSLA


# run tests
pytest