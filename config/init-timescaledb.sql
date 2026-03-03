-- ============================================================================
-- Finstock TimescaleDB Initialization Script
-- Purpose: Create TimescaleDB extension, database, hypertables, and indexes
-- Compatible with: PostgreSQL 16 + TimescaleDB latest
-- ============================================================================

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create dedicated database for market data
CREATE DATABASE finstock_market_data;

-- Connect to the market data database
\c finstock_market_data;

-- Re-enable TimescaleDB extension in the new database
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ============================================================================
-- Table 1: market_data_hot (6 month hot storage)
-- ============================================================================
CREATE TABLE IF NOT EXISTS market_data_hot (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open NUMERIC(15, 2) NOT NULL,
    high NUMERIC(15, 2) NOT NULL,
    low NUMERIC(15, 2) NOT NULL,
    close NUMERIC(15, 2) NOT NULL,
    volume BIGINT NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    is_vn30 BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (timestamp, symbol)
);

-- Create hypertable with 1-day chunks
SELECT create_hypertable('market_data_hot', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Add retention policy (6 months)
SELECT add_retention_policy('market_data_hot', INTERVAL '6 months', if_not_exists => TRUE);

-- Create indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_market_data_hot_symbol_time ON market_data_hot (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_market_data_hot_vn30_time ON market_data_hot (is_vn30, timestamp DESC) WHERE is_vn30 = TRUE;
CREATE INDEX IF NOT EXISTS idx_market_data_hot_exchange ON market_data_hot (exchange, timestamp DESC);

-- ============================================================================
-- Table 2: paper_trades (unlimited retention)
-- ============================================================================
CREATE TABLE IF NOT EXISTS paper_trades (
    timestamp TIMESTAMPTZ NOT NULL,
    trade_id VARCHAR(50) NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL CHECK (side IN ('BUY', 'SELL')),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    price NUMERIC(15, 2) NOT NULL CHECK (price > 0),
    commission NUMERIC(15, 2) DEFAULT 0,
    tax NUMERIC(15, 2) DEFAULT 0,
    total_cost NUMERIC(15, 2) NOT NULL,
    pnl NUMERIC(15, 2) DEFAULT 0,
    strategy VARCHAR(50) NOT NULL,
    regime VARCHAR(50),
    exchange VARCHAR(10) NOT NULL,
    is_vn30 BOOLEAN DEFAULT FALSE,
    metadata JSONB,
    PRIMARY KEY (timestamp, trade_id)
);

-- Create hypertable with 7-day chunks
SELECT create_hypertable('paper_trades', 'timestamp',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Create indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_paper_trades_trade_id ON paper_trades (trade_id);
CREATE INDEX IF NOT EXISTS idx_paper_trades_order_id ON paper_trades (order_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_paper_trades_symbol ON paper_trades (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_paper_trades_strategy ON paper_trades (strategy, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_paper_trades_regime ON paper_trades (regime, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_paper_trades_side ON paper_trades (side, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_paper_trades_pnl ON paper_trades (pnl DESC);
CREATE INDEX IF NOT EXISTS idx_paper_trades_metadata_gin ON paper_trades USING GIN (metadata);

-- ============================================================================
-- Table 3: backtest_results (unlimited retention)
-- ============================================================================
CREATE TABLE IF NOT EXISTS backtest_results (
    timestamp TIMESTAMPTZ NOT NULL,
    run_id VARCHAR(50) NOT NULL,
    strategy VARCHAR(50) NOT NULL,
    regime VARCHAR(50),
    parameters JSONB NOT NULL,
    metrics JSONB NOT NULL,
    sharpe_ratio NUMERIC(10, 4),
    total_return NUMERIC(10, 4),
    max_drawdown NUMERIC(10, 4),
    win_rate NUMERIC(5, 4) CHECK (win_rate >= 0 AND win_rate <= 1),
    total_trades INTEGER DEFAULT 0,
    PRIMARY KEY (timestamp, run_id)
);

-- Create hypertable with 7-day chunks
SELECT create_hypertable('backtest_results', 'timestamp',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Create indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_backtest_results_run_id ON backtest_results (run_id);
CREATE INDEX IF NOT EXISTS idx_backtest_results_strategy ON backtest_results (strategy, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_backtest_results_regime ON backtest_results (regime, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_backtest_results_sharpe ON backtest_results (sharpe_ratio DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_backtest_results_return ON backtest_results (total_return DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_backtest_results_win_rate ON backtest_results (win_rate DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_backtest_results_parameters_gin ON backtest_results USING GIN (parameters);
CREATE INDEX IF NOT EXISTS idx_backtest_results_metrics_gin ON backtest_results USING GIN (metrics);

-- ============================================================================
-- Table 4: optimized_parameters (unlimited retention)
-- ============================================================================
CREATE TABLE IF NOT EXISTS optimized_parameters (
    timestamp TIMESTAMPTZ NOT NULL,
    param_id VARCHAR(50) NOT NULL,
    strategy VARCHAR(50) NOT NULL,
    regime VARCHAR(50),
    parameters JSONB NOT NULL,
    optimization_method VARCHAR(50) NOT NULL,
    objective_score NUMERIC(10, 4),
    confidence NUMERIC(5, 4) CHECK (confidence >= 0 AND confidence <= 1),
    is_active BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (timestamp, param_id)
);

-- Create hypertable with 30-day chunks
SELECT create_hypertable('optimized_parameters', 'timestamp',
    chunk_time_interval => INTERVAL '30 days',
    if_not_exists => TRUE
);

-- Create indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_optimized_parameters_param_id ON optimized_parameters (param_id);
CREATE INDEX IF NOT EXISTS idx_optimized_parameters_strategy ON optimized_parameters (strategy, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_optimized_parameters_regime ON optimized_parameters (regime, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_optimized_parameters_active ON optimized_parameters (is_active, timestamp DESC) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_optimized_parameters_score ON optimized_parameters (objective_score DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_optimized_parameters_method ON optimized_parameters (optimization_method, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_optimized_parameters_parameters_gin ON optimized_parameters USING GIN (parameters);

-- ============================================================================
-- Table 5: daily_pnl (unlimited retention)
-- ============================================================================
CREATE TABLE IF NOT EXISTS daily_pnl (
    date DATE NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    total_pnl NUMERIC(15, 2) DEFAULT 0,
    realized_pnl NUMERIC(15, 2) DEFAULT 0,
    unrealized_pnl NUMERIC(15, 2) DEFAULT 0,
    total_trades INTEGER DEFAULT 0,
    winning_trades INTEGER DEFAULT 0,
    losing_trades INTEGER DEFAULT 0,
    win_rate NUMERIC(5, 4) CHECK (win_rate >= 0 AND win_rate <= 1),
    best_trade NUMERIC(15, 2),
    worst_trade NUMERIC(15, 2),
    strategy_breakdown JSONB,
    regime_breakdown JSONB,
    portfolio_value NUMERIC(15, 2),
    cash_balance NUMERIC(15, 2),
    PRIMARY KEY (timestamp, date)
);

-- Create hypertable with 30-day chunks
SELECT create_hypertable('daily_pnl', 'timestamp',
    chunk_time_interval => INTERVAL '30 days',
    if_not_exists => TRUE
);

-- Create indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_daily_pnl_date ON daily_pnl (date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_pnl_total_pnl ON daily_pnl (total_pnl DESC);
CREATE INDEX IF NOT EXISTS idx_daily_pnl_win_rate ON daily_pnl (win_rate DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_daily_pnl_portfolio_value ON daily_pnl (portfolio_value DESC);
CREATE INDEX IF NOT EXISTS idx_daily_pnl_strategy_breakdown_gin ON daily_pnl USING GIN (strategy_breakdown);
CREATE INDEX IF NOT EXISTS idx_daily_pnl_regime_breakdown_gin ON daily_pnl USING GIN (regime_breakdown);

-- ============================================================================
-- Continuous Aggregate: market_data_1hour
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS market_data_1hour
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', timestamp) AS bucket,
    symbol,
    exchange,
    is_vn30,
    FIRST(open, timestamp) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close, timestamp) AS close,
    SUM(volume) AS volume,
    COUNT(*) AS num_ticks
FROM market_data_hot
GROUP BY bucket, symbol, exchange, is_vn30
WITH NO DATA;

-- Create index on continuous aggregate
CREATE INDEX IF NOT EXISTS idx_market_data_1hour_symbol_bucket ON market_data_1hour (symbol, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_market_data_1hour_vn30_bucket ON market_data_1hour (is_vn30, bucket DESC) WHERE is_vn30 = TRUE;

-- Add refresh policy (refresh every 1 hour, covering last 2 hours of data)
SELECT add_continuous_aggregate_policy('market_data_1hour',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- ============================================================================
-- Grant Permissions to finstock_user
-- ============================================================================

-- Grant database connection
GRANT CONNECT ON DATABASE finstock_market_data TO finstock_user;

-- Grant schema usage
GRANT USAGE ON SCHEMA public TO finstock_user;

-- Grant all privileges on all tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO finstock_user;

-- Grant all privileges on all sequences
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO finstock_user;

-- Grant execute on all functions
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO finstock_user;

-- Set default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO finstock_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO finstock_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO finstock_user;

-- ============================================================================
-- Create helpful views for monitoring
-- ============================================================================

-- View: Recent market data activity
CREATE OR REPLACE VIEW v_recent_market_data AS
SELECT
    symbol,
    exchange,
    MAX(timestamp) AS last_update,
    COUNT(*) AS tick_count,
    AVG(volume) AS avg_volume
FROM market_data_hot
WHERE timestamp > NOW() - INTERVAL '1 day'
GROUP BY symbol, exchange
ORDER BY last_update DESC;

-- View: Trading summary
CREATE OR REPLACE VIEW v_trading_summary AS
SELECT
    strategy,
    regime,
    COUNT(*) AS total_trades,
    SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) AS buy_trades,
    SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) AS sell_trades,
    SUM(pnl) AS total_pnl,
    AVG(pnl) AS avg_pnl,
    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0) AS win_rate
FROM paper_trades
WHERE timestamp > NOW() - INTERVAL '30 days'
GROUP BY strategy, regime
ORDER BY total_pnl DESC;

-- View: Best performing strategies
CREATE OR REPLACE VIEW v_best_strategies AS
SELECT
    strategy,
    regime,
    AVG(sharpe_ratio) AS avg_sharpe,
    AVG(total_return) AS avg_return,
    AVG(max_drawdown) AS avg_drawdown,
    AVG(win_rate) AS avg_win_rate,
    COUNT(*) AS num_backtests,
    MAX(timestamp) AS last_backtest
FROM backtest_results
WHERE timestamp > NOW() - INTERVAL '90 days'
GROUP BY strategy, regime
HAVING COUNT(*) >= 3
ORDER BY avg_sharpe DESC NULLS LAST;

-- View: Active optimized parameters
CREATE OR REPLACE VIEW v_active_parameters AS
SELECT
    strategy,
    regime,
    parameters,
    optimization_method,
    objective_score,
    confidence,
    timestamp AS activated_at
FROM optimized_parameters
WHERE is_active = TRUE
ORDER BY strategy, regime;

-- Grant permissions on views
GRANT SELECT ON v_recent_market_data TO finstock_user;
GRANT SELECT ON v_trading_summary TO finstock_user;
GRANT SELECT ON v_best_strategies TO finstock_user;
GRANT SELECT ON v_active_parameters TO finstock_user;

-- ============================================================================
-- Compression policies for older data
-- ============================================================================

-- Enable compression on market_data_hot (compress data older than 7 days)
ALTER TABLE market_data_hot SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol,exchange',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('market_data_hot', INTERVAL '7 days', if_not_exists => TRUE);

-- Enable compression on paper_trades (compress data older than 30 days)
ALTER TABLE paper_trades SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'strategy,symbol',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('paper_trades', INTERVAL '30 days', if_not_exists => TRUE);

-- Enable compression on backtest_results (compress data older than 30 days)
ALTER TABLE backtest_results SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'strategy,regime',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('backtest_results', INTERVAL '30 days', if_not_exists => TRUE);

-- ============================================================================
-- Statistics and Maintenance
-- ============================================================================

-- Analyze all tables for query optimization
ANALYZE market_data_hot;
ANALYZE paper_trades;
ANALYZE backtest_results;
ANALYZE optimized_parameters;
ANALYZE daily_pnl;

-- ============================================================================
-- Completion Message
-- ============================================================================
\echo '========================================='
\echo 'TimescaleDB Initialization Complete!'
\echo '========================================='
\echo 'Database: finstock_market_data'
\echo 'Hypertables created: 5'
\echo '  - market_data_hot (1-day chunks, 6-month retention)'
\echo '  - paper_trades (7-day chunks, unlimited retention)'
\echo '  - backtest_results (7-day chunks, unlimited retention)'
\echo '  - optimized_parameters (30-day chunks, unlimited retention)'
\echo '  - daily_pnl (30-day chunks, unlimited retention)'
\echo 'Continuous aggregates: 1'
\echo '  - market_data_1hour (1-hour buckets)'
\echo 'Views created: 4 monitoring views'
\echo 'Compression policies: Enabled for hot data'
\echo 'Permissions: Granted to finstock_user'
\echo '========================================='
