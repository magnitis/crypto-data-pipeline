# Crypto Market Data Pipeline

This project implements a modern data pipeline for crypto trading data, focusing on data quality, derived datasets, and simplicity. It ingests trade data from Coinbase for BTC-USD and ETH-USD pairs, processes it, and generates various reports.

## Features

- Real-time trade data ingestion via WebSocket API
- Scheduled trade data fetching via REST API (every 5 minutes)
- Data cleaning and spike detection
- Candlestick generation (5min and 1hour intervals)
- Data quality checks and reporting
- SQLite database storage (easily adaptable to other SQL databases)

## Requirements

- Python 3.8+
- Dependencies listed in `requirements.txt`

## Installation

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Unix/macOS
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Local Execution

Run the pipeline with different modes:

```bash
# Run with WebSocket only (default)
python crypto_data_pipeline.py --mode ws

# Run with REST API only
python crypto_data_pipeline.py --mode rest

# Run with both WebSocket and REST API
python crypto_data_pipeline.py --mode both

# Specify when to generate the hourly report (e.g., at minute 30 of each hour)
python crypto_data_pipeline.py --mode ws --report-minute 30
```

Command-line Arguments:
- `--mode`: Choose the data collection mode
  - `ws`: WebSocket only (real-time updates)
  - `rest`: REST API only (polling every 5 minutes)
  - `both`: Both WebSocket and REST API
- `--report-minute`: Minute of each hour to generate the quality report (0-59, default: 0)
- `--products`: Space-separated list of product IDs to monitor (default: BTC-USD ETH-USD)
  ```bash
  # Example: Monitor specific trading pairs
  python crypto_data_pipeline.py --products BTC-USD ETH-USD SOL-USD
  ```

### Airflow Deployment

The pipeline can be deployed using [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/2.6.0/) for scheduled execution. NOTE: The following steps assume [Docker](https://docs.docker.com/engine/install/ubuntu/) is installed.

1. Configure Environment:
   - Add or replace necessary environment variables in the .env file.
   - Pipeline should be runnable with just the existing variables.

2. Start Airflow Services:
   ```bash
   # Start all services
   docker compose up -d --build

   # Check service status
   docker ps

   # Stop services
   docker compose down
   ```

3. Access Airflow:   
   - Open http://localhost:8080 in your browser
   - Login with credentials from .env file (default: airflow/airflow)
   - The DAG 'crypto_market_data_pipeline' should be visible

4. Monitor Execution:
   - The pipeline will run continuously and fetch data every 5 minutes, create `5min` and `1h` candlesticks and generate a quality report at the specified minute every hour.
   - View logs and status in the Airflow UI
   - Data is stored in SQLite database within the container

The script will:
1. Set up the SQLite database
2. Start WebSocket connection for real-time trade data or REST API
3. Calculate candlesticks every 5 minutes and hourly
4. Store all data in the local SQLite database

## Data Quality Considerations

The pipeline implements several data quality checks:
1. Missing data detection (price, size)
2. Price spike detection using z-score analysis
3. Data freshness monitoring
4. Trade side analysis (buy/sell distribution)
5. Candlestick completeness checks

## Production Considerations

For production deployment, consider:

1. **Reliability**:
   - Implement proper error handling and retry mechanisms
   - Add monitoring and alerting
   - Use message queues for better resilience

2. **Scaling**:
   - Replace SQLite with a production-grade database (e.g., PostgreSQL)
   - Implement proper connection pooling
   - Consider using Apache Kafka for stream processing
   - Implement sharding/parallelization for multi-asset support

3. **Quality**:
   - Add more comprehensive data validation
   - Implement backfilling for missing data
   - Implement data reconciliation

4. **Development Experience**:
   - Add comprehensive testing
   - Implement CI/CD pipelines
   - Add proper logging and monitoring
   - Use infrastructure as code

5. **Governance**:
   - Implement proper data access controls
   - Implement data retention policies
   - Add data documentation and metadata management

## Decoupling Fetching and Candlestick Generation

To enhance scalability and maintainability in production deployments, the fetching process and candlestick generation/reporting have been decoupled. This separation allows for independent scaling of each component and simplifies the workflow.

### Rationale
- **Scalability**: By decoupling these processes, you can scale them independently based on the load. For example, if fetching data becomes a bottleneck, you can increase the resources allocated to that process without affecting candlestick generation.
- **Maintainability**: Separating concerns makes the codebase easier to understand and maintain. Changes in one process will have minimal impact on the other.

This architecture will provide a more robust solution for handling real-time market data and generating insights.

## Extending the Pipeline

The code is structured to be easily extensible:
- Add new data sources by implementing new fetcher functions
- Add new quality checks by extending the validation logic
- Add new derived data sets by implementing new calculation functions
- Switch to different databases by changing the SQLAlchemy connection string
