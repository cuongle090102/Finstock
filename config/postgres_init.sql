-- PostgreSQL initialization script for Finstock metadata - Task 1.6

-- Create Airflow database and user
-- NOTE: Password is set via AIRFLOW_DB_PASSWORD env var in docker-compose
-- The password below is a placeholder; override with ALTER ROLE in entrypoint if needed
CREATE DATABASE airflow_db;
CREATE USER airflow_user WITH PASSWORD 'changeme_use_env_var';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;

-- Connect to airflow_db and grant schema permissions
\c airflow_db
GRANT ALL ON SCHEMA public TO airflow_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow_user;

-- Switch back to default database
\c finstock_metadata

-- Create schemas for different components
CREATE SCHEMA IF NOT EXISTS etl_metadata;
CREATE SCHEMA IF NOT EXISTS data_quality;
CREATE SCHEMA IF NOT EXISTS job_monitoring;

-- ETL Metadata Tables
CREATE TABLE IF NOT EXISTS etl_metadata.data_sources (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL UNIQUE,
    source_type VARCHAR(50) NOT NULL,
    connection_config JSONB,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_metadata.etl_jobs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(200) NOT NULL UNIQUE,
    job_type VARCHAR(50) NOT NULL,
    source_id INTEGER REFERENCES etl_metadata.data_sources(id),
    target_layer VARCHAR(20) NOT NULL CHECK (target_layer IN ('bronze', 'silver', 'gold')),
    target_table VARCHAR(100) NOT NULL,
    schedule_cron VARCHAR(100),
    is_active BOOLEAN DEFAULT true,
    config JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_metadata.job_runs (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES etl_metadata.etl_jobs(id),
    run_id VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('running', 'success', 'failed', 'cancelled')),
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    records_processed INTEGER,
    records_success INTEGER,
    records_failed INTEGER,
    error_message TEXT,
    execution_metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data Quality Tables
CREATE TABLE IF NOT EXISTS data_quality.quality_checks (
    id SERIAL PRIMARY KEY,
    check_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(50) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    check_config JSONB,
    threshold_config JSONB,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data_quality.quality_results (
    id SERIAL PRIMARY KEY,
    check_id INTEGER REFERENCES data_quality.quality_checks(id),
    job_run_id INTEGER REFERENCES etl_metadata.job_runs(id),
    check_result JSONB NOT NULL,
    quality_score DECIMAL(5,2),
    passed BOOLEAN NOT NULL,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Job Monitoring Tables
CREATE TABLE IF NOT EXISTS job_monitoring.system_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(15,4) NOT NULL,
    metric_unit VARCHAR(20),
    tags JSONB,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS job_monitoring.alert_rules (
    id SERIAL PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL UNIQUE,
    metric_name VARCHAR(100) NOT NULL,
    condition_type VARCHAR(20) NOT NULL CHECK (condition_type IN ('gt', 'lt', 'eq', 'ne', 'gte', 'lte')),
    threshold_value DECIMAL(15,4) NOT NULL,
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    notification_config JSONB,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default data sources
INSERT INTO etl_metadata.data_sources (source_name, source_type, connection_config) VALUES
('vnstock', 'api', '{"base_url": "https://api.vnstock.vn", "timeout": 30}'),
('vnquant', 'library', '{"data_source": "cafe", "cache_enabled": true}'),
('cafef', 'scraper', '{"base_url": "https://cafef.vn", "rate_limit": 10}')
ON CONFLICT (source_name) DO NOTHING;

-- Insert default ETL jobs
INSERT INTO etl_metadata.etl_jobs (job_name, job_type, source_id, target_layer, target_table, schedule_cron) VALUES
('vnstock_daily_ohlc', 'data_ingestion', 1, 'bronze', 'market_data', '0 18 * * 1-5'),
('vnquant_fundamentals', 'data_ingestion', 2, 'bronze', 'fundamentals', '0 20 * * 0'),
('cafef_corporate_actions', 'web_scraping', 3, 'bronze', 'corporate_actions', '0 19 * * 1-5'),
('bronze_to_silver_etl', 'data_transformation', NULL, 'silver', 'market_data_cleaned', '0 21 * * 1-5'),
('silver_to_gold_analytics', 'data_aggregation', NULL, 'gold', 'daily_aggregates', '0 22 * * 1-5')
ON CONFLICT (job_name) DO NOTHING;

-- Insert default quality checks
INSERT INTO data_quality.quality_checks (check_name, check_type, table_name, column_name, check_config) VALUES
('price_non_negative', 'range_check', 'market_data', 'close_price', '{"min_value": 0}'),
('volume_non_negative', 'range_check', 'market_data', 'volume', '{"min_value": 0}'),
('price_relationships', 'business_rule', 'market_data', NULL, '{"rule": "high_price >= low_price"}'),
('data_freshness', 'freshness_check', 'market_data', 'timestamp', '{"max_age_hours": 24}'),
('null_check_symbol', 'null_check', 'market_data', 'symbol', '{"max_null_percentage": 0}')
ON CONFLICT DO NOTHING;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_job_runs_job_id ON etl_metadata.job_runs(job_id);
CREATE INDEX IF NOT EXISTS idx_job_runs_status ON etl_metadata.job_runs(status);
CREATE INDEX IF NOT EXISTS idx_job_runs_started_at ON etl_metadata.job_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_quality_results_check_id ON data_quality.quality_results(check_id);
CREATE INDEX IF NOT EXISTS idx_quality_results_executed_at ON data_quality.quality_results(executed_at);
CREATE INDEX IF NOT EXISTS idx_system_metrics_metric_name ON job_monitoring.system_metrics(metric_name);
CREATE INDEX IF NOT EXISTS idx_system_metrics_recorded_at ON job_monitoring.system_metrics(recorded_at);

-- Create views for monitoring
CREATE OR REPLACE VIEW job_monitoring.job_status_summary AS
SELECT 
    j.job_name,
    j.target_layer,
    j.target_table,
    COUNT(jr.id) as total_runs,
    COUNT(CASE WHEN jr.status = 'success' THEN 1 END) as successful_runs,
    COUNT(CASE WHEN jr.status = 'failed' THEN 1 END) as failed_runs,
    AVG(EXTRACT(EPOCH FROM (jr.ended_at - jr.started_at))) as avg_duration_seconds,
    MAX(jr.started_at) as last_run_time
FROM etl_metadata.etl_jobs j
LEFT JOIN etl_metadata.job_runs jr ON j.id = jr.job_id
WHERE j.is_active = true
GROUP BY j.id, j.job_name, j.target_layer, j.target_table;

CREATE OR REPLACE VIEW data_quality.quality_summary AS
SELECT 
    qc.table_name,
    COUNT(qr.id) as total_checks,
    COUNT(CASE WHEN qr.passed = true THEN 1 END) as passed_checks,
    COUNT(CASE WHEN qr.passed = false THEN 1 END) as failed_checks,
    AVG(qr.quality_score) as avg_quality_score,
    MAX(qr.executed_at) as last_check_time
FROM data_quality.quality_checks qc
LEFT JOIN data_quality.quality_results qr ON qc.id = qr.check_id
WHERE qc.is_active = true
GROUP BY qc.table_name;

-- Grant permissions
GRANT ALL ON ALL TABLES IN SCHEMA etl_metadata TO finstock_user;
GRANT ALL ON ALL TABLES IN SCHEMA data_quality TO finstock_user;
GRANT ALL ON ALL TABLES IN SCHEMA job_monitoring TO finstock_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA etl_metadata TO finstock_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA data_quality TO finstock_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA job_monitoring TO finstock_user;