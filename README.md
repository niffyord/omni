# Omni Trading Toolkit

Omni is a collection of Python scripts used to collect market data and run an automated trading loop on Bybit derivatives. It stores historical metrics in TimescaleDB and relies on a small agents framework for LLM based decision making.

## Components

- **ai_trade_agent.py** – main trading loop that runs every 60 seconds and executes orders through Bybit. It implements the `OmniTrader` agent pipeline.
- **orderbook_collector.py** – collects order book and trade data for `ETH/USDT` and streams features to Redis.
- **derivatives_metrics_scheduler.py** – scheduler that stores funding, open interest and other derivative metrics in TimescaleDB.
- **etl_indicators.py** – ETL worker that computes multi‑timeframe technical indicators and saves them to the database.
- **etl_btc_context.py** – calculates BTC momentum and cross‑asset metrics.
- **etl_history.py** – utility for backfilling historical volatility and Bollinger band width metrics.
- **fetch_history.py** – helper to read stored indicator history from TimescaleDB.
- **timescaledb_tools.py** – functions to fetch the latest indicators or historical series for use by the trading agent.
- **tools.py / trade_utils.py** – assorted helpers for live Bybit metrics and TA‑Lib indicator calculations.

## Setup

Install the Python packages listed in `requirements.txt` and provide a `.env` file with credentials such as `BYBIT_API_KEY`, `BYBIT_API_SECRET` and `TIMESCALEDB_URL`. The trading scripts assume TimescaleDB and Redis are available.

## Usage

- Start the trading loop: `python ai_trade_agent.py`
- Collect indicators: `python etl_indicators.py`
- Run the order book collector: `python orderbook_collector.py`
- Capture derivative metrics: `python derivatives_metrics_scheduler.py`

Sample outputs of the trading agent are written to `last_signal.json` and open position snapshots to `position_summary.json`.

## Example

The loop interval for the agent is defined by `INTERVAL = 60` seconds:
```python
INTERVAL          = 60          # seconds between cycles
```
The derivatives scheduler collects metrics such as funding rate, open interest and long/short ratio:
```text
Derivatives-Metrics Collector  • ETH/USDT
— stores 1-min snapshots in TimescaleDB
```
Database access uses the `TIMESCALEDB_URL` environment variable:
```python
def get_timescaledb_conn():
    db_url = os.getenv("TIMESCALEDB_URL")
    if db_url is None:
        raise ValueError("TIMESCALEDB_URL not set in environment.")
    return psycopg2.connect(db_url)
```
Dependencies include ccxt, redis and TA‑Lib:
```
ccxt
ccxtpro
redis
psycopg2-binary
pandas
```
A recent signal example can be found in `last_signal.json`:
```json
{"signal": "BUY", "entry_price": 0.7117, ... }
```
