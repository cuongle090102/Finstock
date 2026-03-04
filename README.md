# Finstock - Financial Data Engineering in Vietnamese Stock Market

**Status**: Simulated Paper Trading
**Architecture**: Microservices on Docker 

---
https://res.cloudinary.com/dt65gnluq/video/upload/v1772608603/recording_xpy8qm.webm
## Quick Start

```bash
# 1. Copy environment template
cp .env.template .env
# Edit .env with your credentials (generate all secrets!)

# 2. Start all services
docker compose up -d

# 3. Check status
docker compose ps

# 4. Access UIs
# - Grafana: http://localhost:3000
# - Airflow: http://localhost:8082
# - Kafka UI: http://localhost:8083
# - MinIO: http://localhost:9001
```

---

## Project Structure

```
finstock/
├── config/                    # All configuration
│   ├── dashboards/           # Grafana JSON dashboards (7)
│   ├── grafana_*.yaml        # Grafana provisioning
│   ├── init-timescaledb.sql  # TimescaleDB schema (5 hypertables)
│   └── postgres_init.sql     # Database Airflow initialization
│
├── src/                      # Python source code
│   ├── strategies/           # Trading strategies 
│   ├── regime/               # Market regime detection
│   ├── backtesting/          # Backtesting framework
│   ├── kafka_utils/          # Kafka producers/consumers
│   ├── paper_trading/        # Paper trading broker
│   ├── technical_analysis/   # Indicators library
│   ├── optimization/         # Bayesian/Grid/Genetic optimization
│   └── utils/                # Shared utilities (market config, circuit breaker)
│
├── services/                 # Microservices
│   ├── vnstock_producer/     # Market data ingestion
│   ├── timescale_consumer/   # Data persistence
│   └── paper_trading/        # Trading execution
│
├── airflow/                  # Workflow orchestration
│   └── dags/                 # 3 production DAGs
│       └── functions/        # DAG task functions
│
├── tests/                    # Test suite 
│   ├── integration/          # Integration tests
│   └── test_*.py             # Unit tests
│
├── docker-compose.yml        # Service orchestration
├── .env.template             # Environment template
└── FLOW.md                   # System architecture diagram
```

---

## Technology Stack

### Data Pipeline
- **Ingestion**: VnStock API (batch price_board, 30 symbols)
- **Streaming**: Apache Kafka (Confluent 7.4, Snappy compression, 3 partitions)
- **Storage**: TimescaleDB / PostgreSQL 16 (5 hypertables, compression, continuous aggregates)
- **Archive**: MinIO S3 ( Parquet)

### Execution
- **Language**: Python 3.11
- **Strategies**: Adaptive regime-based (4 sub-strategies)
- **Orchestration**: Apache Airflow (3 DAGs)
- **Trading**: Simulated Paper trading with Vietnamese market rules

### Monitoring
- **Dashboards**: Grafana (7 provisioned dashboards)
- **Kafka**: Kafka UI
- **Logs**: Structured logging (structlog)

---

## Vietnamese Market Features

### Market Coverage
- **Exchanges**: HOSE, HNX, UPCOM
- **Symbols**: VN30 index (30 symbols)
- **Data**: Real-time OHLCV, volume, derived indicators

### Market Rules
- **Trading Hours**: 9:00-15:00 Vietnam time (UTC+7)
- **Sessions**: Morning (9:00-11:30), Afternoon (13:00-15:00)
- **Commission**: 0.15% per trade
- **Tax**: 0.10% securities tax on sells
- **Lot Size**: 100 shares minimum
- **Price Limits**: HOSE ±7%, HNX ±10%, UPCOM ±15%

### Trading Strategies
1. **MA Crossover** (SMA 10/30) — Neutral regime
2. **Breakout** (20-period, 1.5x threshold) — Trending regime
3. **Mean Reversion** (Bollinger Bands 20/2.0 + RSI 14) — Ranging regime
4. **Momentum** (ADX 14 + MACD 12/26/9) — Strong trend
5. **Adaptive** — Regime-based strategy switching with 10-min transition delay

---

## Airflow DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `daily_backtest_dag` | 06:00 VN (23:00 UTC) | Backtest strategies, optimize parameters |
| `daily_pnl_report_dag` | 15:30 VN (08:30 UTC) | Generate daily P&L report |
| `monthly_archive_dag` | 1st of month | Archive old data to MinIO |

---

## Services 

### Infrastructure 
- **PostgreSQL/TimescaleDB** — Time-series market data (port 5432)
- **Kafka** — Message streaming (port 9092)
- **Zookeeper** — Kafka coordination (port 2181)
- **MinIO** — Object storage for archival (ports 9000/9001)

### Application 
- **VnStock Producer** — Market data ingestion from VnStock API
- **TimescaleDB Consumer** — Kafka → TimescaleDB persistence
- **Paper Trading** — Strategy execution and trade simulation

### Orchestration 
- **Airflow Webserver** — Workflow UI (port 8082)
- **Airflow Scheduler** — DAG execution

### Monitoring 
- **Grafana** — 7 dashboards (port 3000)
- **Kafka UI** — Topic/consumer monitoring (port 8083)

---





