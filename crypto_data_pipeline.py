import json
import time
import logging
import traceback
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import threading

import requests
import websocket
import pandas as pd
from scipy import stats
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
COINBASE_REST_URL = "https://api.exchange.coinbase.com/products"
COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"
DB_PATH = "crypto_data.db"


logger = logging.getLogger(__name__)


class CryptoDataPipeline:
    def __init__(
        self,
        products: Optional[list[str]] = None,
        db_url: str = f"sqlite:///{DB_PATH}",
    ):
        self.db_url = db_url
        self.engine = create_engine(db_url)
        self.setup_database()
        self.products = products if products else ["BTC-USD", "ETH-USD"]

    def on_ws_message(self, ws, message):
        """Handle WebSocket messages."""
        try:
            data = json.loads(message)
            if data.get("channel") == "market_trades":
                for trade in data.get("events", []):
                    trade_data = trade.get("trades", [])[0]
                    if trade_data:
                        processed_trade = {
                            "trade_id": str(trade_data["trade_id"]),
                            "product_id": trade_data["product_id"],
                            "price": float(trade_data["price"]),
                            "size": float(trade_data["size"]),
                            "time": pd.to_datetime(trade_data["time"])
                            .replace(tzinfo=None)
                            .to_pydatetime(),  # Convert to Python datetime
                            "side": trade_data["side"],
                        }
                        self.process_trade(processed_trade)
        except Exception as e:
            logger.error(f"Error processing WebSocket message: {e}")

    def on_ws_error(self, ws, error):
        """Handle WebSocket errors."""
        logger.error(f"WebSocket error: {error}")

    def on_ws_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close."""
        logger.info("WebSocket connection closed")

    def on_ws_open(self, ws):
        """Handle WebSocket connection open."""
        subscribe_message = {
            "type": "subscribe",
            "product_ids": self.products,
            "channel": "market_trades",
        }
        ws.send(json.dumps(subscribe_message))
        logger.info(
            f"Subscribed to market_trades channel for {', '.join(self.products)}"
        )

    def setup_database(self):
        """Create necessary database tables if they don't exist."""
        with self.engine.begin() as conn:
            # Raw trades table
            conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS raw_trades (
                    trade_id TEXT PRIMARY KEY,
                    product_id TEXT,
                    price REAL,
                    size REAL,
                    time TIMESTAMP,
                    side TEXT,
                    is_spike BOOLEAN,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
                )
            )

            # Cleaned trades table
            conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS clean_trades (
                    trade_id TEXT PRIMARY KEY,
                    product_id TEXT,
                    price REAL,
                    size REAL,
                    time TIMESTAMP,
                    side TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
                )
            )

            # Candlesticks table
            conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS candlesticks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id TEXT,
                    timestamp TIMESTAMP,
                    interval TEXT,
                    open_price REAL,
                    high_price REAL,
                    low_price REAL,
                    close_price REAL,
                    volume REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
                )
            )

    def start_ws_client(self):
        """Start WebSocket client in a separate thread."""
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(
            COINBASE_WS_URL,
            on_message=self.on_ws_message,
            on_error=self.on_ws_error,
            on_close=self.on_ws_close,
            on_open=self.on_ws_open,
        )

        ws_thread = threading.Thread(target=ws.run_forever)
        ws_thread.daemon = True
        ws_thread.start()

        return ws_thread

    def fetch_trades_rest(self, product_id: str):
        """Fetch trades via REST API with pagination using cursor-based pagination."""
        url = f"{COINBASE_REST_URL}/{product_id}/trades"
        # Get trades from the last 5 minutes
        start_time = datetime.utcnow() - timedelta(minutes=5)
        params = {
            "limit": 100,  # Maximum allowed by the API
        }

        try:
            trade_count = 0
            while True:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                trades = response.json()

                if not trades:  # No more trades
                    break

                for trade in trades:
                    trade_time = (
                        pd.to_datetime(trade["time"])
                        .replace(tzinfo=None)
                        .to_pydatetime()
                    )
                    # Skip trades older than our window
                    if trade_time < start_time:
                        logger.info(f"Fetched {trade_count} trades for {product_id}!")
                        return

                    processed_trade = {
                        "trade_id": str(trade["trade_id"]),
                        "product_id": product_id,
                        "price": float(trade["price"]),
                        "size": float(trade["size"]),
                        "time": trade_time,
                        "side": trade["side"],
                    }
                    self.process_trade(processed_trade)
                    trade_count += 1

                # Get cursor for next page
                cursor = response.headers.get("cb-after")
                if not cursor:
                    logger.info(f"Fetched {trade_count} trades for {product_id}!")
                    break

                params["after"] = cursor
                logger.info(f"Fetched {trade_count} trades so far for {product_id}!")

        except Exception as e:
            logger.error(f"Error fetching trades via REST for {product_id}: {e}")

    def process_trade(self, trade_data: Dict[str, Any]):
        """Process and store raw trade data."""
        try:
            # Extract relevant fields
            trade = {
                "trade_id": trade_data["trade_id"],
                "product_id": trade_data["product_id"],
                "price": float(trade_data["price"]),
                "size": float(trade_data["size"]),
                "time": pd.to_datetime(trade_data["time"]),
                "side": trade_data["side"],
            }

            # Check for price spike
            trade["is_spike"] = self.detect_price_spike(trade)

            # Store raw trade
            self.store_raw_trade(trade)

            # Clean and store if valid
            if self.validate_trade(trade):
                self.store_clean_trade(trade)

        except Exception as e:
            logger.error(f"Error processing trade: {e}")

    def detect_price_spike(self, trade: Dict[str, Any]) -> bool:
        """Detect if a trade price is a spike using z-score."""
        query = text(
            """
            SELECT price FROM raw_trades
            WHERE product_id = :product_id
            ORDER BY time DESC LIMIT 100
        """
        )

        with self.engine.connect() as conn:
            result = conn.execute(query, {"product_id": trade["product_id"]})
            prices = [row[0] for row in result]

        if len(prices) < 10:  # Not enough data
            return False

        z_score = abs(stats.zscore([trade["price"]] + prices)[0])
        return z_score > 3  # Consider it a spike if z-score > 3

    def validate_trade(self, trade: Dict[str, Any]) -> bool:
        """Validate trade data quality."""
        return (
            trade["price"] is not None
            and trade["price"] > 0
            and trade["size"] is not None
            and trade["size"] > 0
            and not trade["is_spike"]
        )

    def store_raw_trade(self, trade: Dict[str, Any]):
        """Store raw trade data."""
        # Ensure time is a Python datetime object
        if isinstance(trade["time"], pd.Timestamp):
            trade["time"] = trade["time"].to_pydatetime()

        query = text(
            """
            INSERT OR REPLACE INTO raw_trades
            (trade_id, product_id, price, size, time, side, is_spike)
            VALUES (:trade_id, :product_id, :price, :size, :time, :side, :is_spike)
        """
        )

        with self.engine.begin() as conn:
            conn.execute(query, trade)

    def store_clean_trade(self, trade: Dict[str, Any]):
        """Store cleaned trade data."""
        # Ensure time is a Python datetime object
        clean_trade = {k: v for k, v in trade.items() if k != "is_spike"}
        if isinstance(clean_trade["time"], pd.Timestamp):
            clean_trade["time"] = clean_trade["time"].to_pydatetime()

        query = text(
            """
            INSERT OR REPLACE INTO clean_trades
            (trade_id, product_id, price, size, time, side)
            VALUES (:trade_id, :product_id, :price, :size, :time, :side)
        """
        )

        with self.engine.begin() as conn:
            conn.execute(query, clean_trade)

    def calculate_candlesticks(self, interval: str = "5min"):
        """Calculate and store candlestick data aligned with clock intervals."""
        current_time = datetime.utcnow()

        # Align start time with clock intervals
        if interval == "5min":
            # Round down to nearest 5 minute mark
            minutes_rounded = (current_time.minute // 5) * 5
            end_time = current_time.replace(
                minute=minutes_rounded, second=0, microsecond=0
            )
            start_time = end_time - timedelta(minutes=5)
        else:  # hourly
            # Round down to the start of the hour
            end_time = current_time.replace(minute=0, second=0, microsecond=0)
            start_time = end_time - timedelta(hours=1)

        query = text(
            """
            SELECT * FROM clean_trades
            WHERE time >= :start_time AND time < :end_time
            ORDER BY time ASC
        """
        )

        with self.engine.connect() as conn:
            df = pd.read_sql(
                query, conn, params={"start_time": start_time, "end_time": end_time}
            )

        if df.empty:
            logger.warning(f"No trades found for {interval} candlestick calculation")
            return

        for product_id in self.products:
            product_df = df[df["product_id"] == product_id]
            if product_df.empty:
                continue

            candlestick = {
                "product_id": product_id,
                "timestamp": start_time,
                "interval": interval,
                "open_price": product_df.iloc[0]["price"],
                "high_price": product_df["price"].max(),
                "low_price": product_df["price"].min(),
                "close_price": product_df.iloc[-1]["price"],
                "volume": product_df["size"].sum(),
            }

            self.store_candlestick(candlestick)

    def store_candlestick(self, candlestick: Dict[str, Any]):
        """Store candlestick data."""
        query = text(
            """
            INSERT INTO candlesticks
            (product_id, timestamp, interval, open_price,
                high_price, low_price, close_price, volume)
            VALUES
            (:product_id, :timestamp, :interval, :open_price,
            :high_price, :low_price, :close_price, :volume)
        """
        )

        with self.engine.begin() as conn:
            conn.execute(query, candlestick)

    def generate_quality_report(self, start_time: datetime, end_time: datetime) -> str:
        """Generate a data quality report."""
        with self.engine.connect() as conn:
            # Calculate trade statistics
            raw_trades_stats = pd.read_sql(
                text(
                    """
                SELECT
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN price IS NULL OR size IS NULL THEN 1 ELSE 0 END)
                                                                as missing_data_count,
                    SUM(CASE WHEN side = 'buy' THEN 1 ELSE 0 END) as buy_trades,
                    SUM(CASE WHEN side = 'sell' THEN 1 ELSE 0 END) as sell_trades,
                    SUM(CASE WHEN is_spike THEN 1 ELSE 0 END) as price_spikes
                FROM raw_trades
                WHERE time BETWEEN :start_time AND :end_time
            """
                ),
                conn,
                params={"start_time": start_time, "end_time": end_time},
            ).iloc[0]

            # Calculate candlestick statistics
            candlestick_stats = pd.read_sql(
                text(
                    """
                SELECT
                    interval,
                    COUNT(*) as total_candles,
                    SUM(CASE WHEN close_price > open_price THEN 1 ELSE 0 END) as up_candles,
                    SUM(CASE WHEN close_price < open_price THEN 1 ELSE 0 END) as down_candles
                FROM candlesticks
                WHERE timestamp BETWEEN :start_time AND :end_time
                GROUP BY interval
            """
                ),
                conn,
                params={"start_time": start_time, "end_time": end_time},
            )

        # Format report
        report = [
            "Data Quality Report",
            "==================",
            f"Time Range: {start_time} to {end_time}",
            "\nTrade Statistics:",
            f"Total Trades: {raw_trades_stats['total_trades']}",
            f"Missing Data Percentage: {(raw_trades_stats['missing_data_count'] / raw_trades_stats['total_trades'] * 100):.2f}%",
            f"Buy Orders: {(raw_trades_stats['buy_trades'] / raw_trades_stats['total_trades'] * 100):.2f}%",
            f"Sell Orders: {(raw_trades_stats['sell_trades'] / raw_trades_stats['total_trades'] * 100):.2f}%",
            f"Price Spikes: {raw_trades_stats['price_spikes']}",
            "\nCandlestick Statistics:",
        ]

        for _, row in candlestick_stats.iterrows():
            report.extend(
                [
                    f"\n{row['interval']} Candlesticks:",
                    f"Total Candles: {row['total_candles']}",
                    f"Up Candles: {row['up_candles']}",
                    f"Down Candles: {row['down_candles']}",
                ]
            )

        return "\n".join(report)


def main():
    # Command line arguments
    parser = argparse.ArgumentParser(description="Crypto Market Data Pipeline")
    parser.add_argument(
        "--mode",
        choices=["ws", "rest", "both"],
        default="ws",
        help="Data collection mode: ws (WebSocket), rest (REST API), or both",
    )
    parser.add_argument(
        "--report-minute",
        type=int,
        default=0,
        help="Minute of each hour to generate report (0-59)",
    )
    parser.add_argument(
        "--products",
        nargs="*",
        help="Space-separated list of product IDs (e.g., BTC-USD ETH-USD). "
        "If not provided, defaults will be used.",
    )
    args = parser.parse_args()

    # Initialize pipeline with products from command line
    pipeline = CryptoDataPipeline(products=args.products)
    logger.info(f"Monitoring products: {pipeline.products}")

    ws_thread = None

    # Start WebSocket client
    if args.mode in ["ws", "both"]:
        ws_thread = pipeline.start_ws_client()
        logger.info("WebSocket client started")

    try:
        while True:
            current_time = datetime.utcnow()

            # Fetch trades via REST
            if args.mode in ["rest", "both"]:
                if current_time.minute % 5 == 0:
                    logger.info("Fetching trades via REST...")
                    for product in pipeline.products:
                        pipeline.fetch_trades_rest(product)
                        logger.info(f"Finished fetching trades for {product} via REST!")

            # Generate 5-minute candlesticks at clock intervals
            if current_time.minute % 5 == 0:
                pipeline.calculate_candlesticks("5min")
                logger.info(f"Generated 5-minute candlesticks at {current_time}")

            # Generate hourly candlesticks at the start of each hour
            if current_time.minute == 0:
                pipeline.calculate_candlesticks("1hour")
                logger.info(f"Generated hourly candlesticks at {current_time}")

            # Generate quality report every hour
            if current_time.minute == args.report_minute:
                report = pipeline.generate_quality_report(
                    start_time=current_time - timedelta(hours=1), end_time=current_time
                )
                logger.info(f"Hourly Quality Report:\n{report}")

            # Sleep for 1 minute
            time.sleep(60)

    except Exception as e:
        logger.error(f"Error in main loop: {e}")
        logger.error(traceback.format_exc())
    except KeyboardInterrupt:
        logger.info("Shutting down...")

    # Clean shutdown
    if ws_thread and ws_thread.is_alive():
        logger.info("Stopping WebSocket client...")


if __name__ == "__main__":
    main()
