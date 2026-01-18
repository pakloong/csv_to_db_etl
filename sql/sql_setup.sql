-- ============================================================================
-- DATA ENGINEER: DATABASE SETUP AND OPTIMIZATION QUERIES
-- ============================================================================

-- ============================================================================
-- 1. CREATE DATABASE SCHEMA
-- ============================================================================

-- Create sales_transactions table
CREATE TABLE IF NOT EXISTS sales_transactions (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_category VARCHAR(100),
    price NUMERIC(10, 2) NOT NULL,
    payment_type VARCHAR(50),
    shipping_days INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create products table
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(200),
    product_category VARCHAR(100),
    price NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create order_items table (for detailed order breakdown)
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity INTEGER DEFAULT 1,
    unit_price NUMERIC(10, 2),
    total_price NUMERIC(10, 2),
    FOREIGN KEY (order_id) REFERENCES sales_transactions(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Create data_quality_log table
CREATE TABLE IF NOT EXISTS data_quality_log (
    log_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    check_type VARCHAR(100),
    issue_description TEXT,
    affected_rows INTEGER,
    severity VARCHAR(20),  -- 'WARNING', 'ERROR', 'CRITICAL'
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create etl_execution_log table
CREATE TABLE IF NOT EXISTS etl_execution_log (
    execution_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20),  -- 'SUCCESS', 'FAILED', 'PARTIAL'
    rows_processed INTEGER,
    rows_failed INTEGER,
    error_message TEXT,
    execution_details TEXT
);

-- ============================================================================
-- 2. CREATE INDEXES FOR PERFORMANCE
-- ============================================================================

-- Indexes on sales_transactions table
CREATE INDEX IF NOT EXISTS idx_sales_customer_id ON sales_transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_order_date ON sales_transactions(order_date);
CREATE INDEX IF NOT EXISTS idx_sales_product_id ON sales_transactions(product_id);
CREATE INDEX IF NOT EXISTS idx_sales_product_category ON sales_transactions(product_category);
CREATE INDEX IF NOT EXISTS idx_sales_price ON sales_transactions(price);

-- Composite index for common queries
CREATE INDEX IF NOT EXISTS idx_sales_date_customer ON sales_transactions(order_date, customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_category_date ON sales_transactions(product_category, order_date);

-- Indexes on customers table
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_country ON customers(country);

-- Indexes on products table
CREATE INDEX IF NOT EXISTS idx_products_category ON products(product_category);

-- Indexes on order_items table
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);

-- ============================================================================
-- 3. DATA QUALITY CHECKS
-- ============================================================================

-- Check for duplicate records
SELECT 
    'Duplicate Records Check' as check_name,
    COUNT(*) - COUNT(DISTINCT order_id) as duplicate_count
FROM sales_transactions;

-- Check for null values
SELECT 
    'Null Values Check' as check_name,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_ids,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_ids,
    SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) as null_prices
FROM sales_transactions;

-- Check for invalid price values
SELECT 
    'Invalid Price Check' as check_name,
    COUNT(*) as invalid_price_count
FROM sales_transactions
WHERE price <= 0 OR price IS NULL;

-- Check for future dates
SELECT 
    'Future Date Check' as check_name,
    COUNT(*) as future_date_count
FROM sales_transactions
WHERE order_date > CURRENT_TIMESTAMP;

-- Check for referential integrity
SELECT 
    'Orphaned Customer Records' as check_name,
    COUNT(*) as orphaned_records
FROM sales_transactions st
WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE st.customer_id = c.customer_id);

-- ============================================================================
-- 4. ETL MONITORING AND LOGGING
-- ============================================================================

-- Log data quality check
INSERT INTO data_quality_log (table_name, check_type, issue_description, affected_rows, severity)
SELECT 
    'sales_transactions',
    'Null Values',
    'Missing customer_id values found',
    COUNT(*),
    CASE WHEN COUNT(*) > 100 THEN 'CRITICAL' ELSE 'WARNING' END
FROM sales_transactions
WHERE customer_id IS NULL;

-- Log ETL execution
INSERT INTO etl_execution_log (
    pipeline_name, 
    start_time, 
    end_time, 
    status, 
    rows_processed, 
    rows_failed
) VALUES (
    'csv_to_db_etl',
    NOW() - INTERVAL '5 minutes',
    NOW(),
    'SUCCESS',
    50000,
    0
);

-- ============================================================================
-- 5. QUERY PERFORMANCE ANALYSIS
-- ============================================================================

-- Analyze query execution plan for slow query
EXPLAIN ANALYZE
SELECT 
    product_category,
    COUNT(*) as order_count,
    SUM(price) as total_revenue
FROM sales_transactions
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY product_category
ORDER BY total_revenue DESC;

-- Find slow queries
SELECT 
    query,
    calls,
    ROUND(total_time::numeric, 2) as total_ms,
    ROUND(mean_time::numeric, 2) as avg_ms,
    min_time,
    max_time
FROM pg_stat_statements
ORDER BY total_time DESC
LIMIT 10;

-- Check table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) as index_size
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- ============================================================================
-- 6. INCREMENTAL LOAD SUPPORT (CDC - Change Data Capture)
-- ============================================================================

-- Create change tracking table
CREATE TABLE IF NOT EXISTS sales_transactions_changes (
    change_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    change_type VARCHAR(10),  -- 'INSERT', 'UPDATE', 'DELETE'
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    old_values JSONB,
    new_values JSONB
);

-- Trigger to capture changes (example)
CREATE OR REPLACE FUNCTION track_sales_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO sales_transactions_changes (order_id, change_type, new_values)
        VALUES (NEW.order_id, 'INSERT', row_to_json(NEW));
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO sales_transactions_changes (order_id, change_type, old_values, new_values)
        VALUES (NEW.order_id, 'UPDATE', row_to_json(OLD), row_to_json(NEW));
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO sales_transactions_changes (order_id, change_type, old_values)
        VALUES (OLD.order_id, 'DELETE', row_to_json(OLD));
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 7. DATA VALIDATION PROCEDURES
-- ============================================================================

-- Function to validate data before loading
CREATE OR REPLACE FUNCTION validate_sales_data()
RETURNS TABLE(validation_passed BOOLEAN, error_message VARCHAR) AS $$
BEGIN
    -- Check for required fields
    IF EXISTS (SELECT 1 FROM sales_transactions WHERE order_id IS NULL OR customer_id IS NULL OR price IS NULL) THEN
        RETURN QUERY SELECT FALSE, 'Missing required fields found'::VARCHAR;
        RETURN;
    END IF;
    
    -- Check for valid price range
    IF EXISTS (SELECT 1 FROM sales_transactions WHERE price < 0 OR price > 1000000) THEN
        RETURN QUERY SELECT FALSE, 'Invalid price range detected'::VARCHAR;
        RETURN;
    END IF;
    
    -- Check for duplicate orders
    IF EXISTS (SELECT order_id FROM sales_transactions GROUP BY order_id HAVING COUNT(*) > 1) THEN
        RETURN QUERY SELECT FALSE, 'Duplicate orders found'::VARCHAR;
        RETURN;
    END IF;
    
    -- All validations passed
    RETURN QUERY SELECT TRUE, 'All validations passed'::VARCHAR;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 8. DATA TRANSFORMATION VIEWS
-- ============================================================================

-- Daily aggregated sales view
CREATE OR REPLACE VIEW v_daily_sales AS
SELECT 
    DATE(order_date) as sale_date,
    COUNT(DISTINCT order_id) as order_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(price) as total_revenue,
    AVG(price) as avg_order_value,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM sales_transactions
GROUP BY DATE(order_date);

-- Monthly aggregated sales view
CREATE OR REPLACE VIEW v_monthly_sales AS
SELECT 
    DATE_TRUNC('month', order_date)::DATE as month,
    COUNT(DISTINCT order_id) as order_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(price) as total_revenue,
    AVG(price) as avg_order_value
FROM sales_transactions
GROUP BY DATE_TRUNC('month', order_date);

-- Product performance view
CREATE OR REPLACE VIEW v_product_performance AS
SELECT 
    product_id,
    product_category,
    COUNT(*) as order_count,
    SUM(price) as total_revenue,
    AVG(price) as avg_price,
    COUNT(DISTINCT customer_id) as unique_customers
FROM sales_transactions
GROUP BY product_id, product_category;

-- ============================================================================
-- 9. BACKUP AND RECOVERY
-- ============================================================================

-- Full database backup (via bash/pgdump)
-- pg_dump -h localhost -U postgres data_warehouse > backup_$(date +\%Y\%m\%d).sql

-- List recent backups
-- ls -lah backup_*.sql | sort -k9 | tail -10

-- Restore from backup
-- psql -h localhost -U postgres data_warehouse < backup_20260115.sql

-- ============================================================================
-- 10. MAINTENANCE AND OPTIMIZATION
-- ============================================================================

-- Analyze table statistics for query optimizer
ANALYZE sales_transactions;
ANALYZE customers;
ANALYZE products;

-- Vacuum to reclaim space (full mode locks table)
VACUUM ANALYZE sales_transactions;

-- Reindex to optimize index performance
REINDEX TABLE sales_transactions;

-- Check database bloat
SELECT 
    schemaname,
    tablename,
    ROUND(100.0 * (pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) / pg_total_relation_size(schemaname||'.'||tablename), 2) as index_ratio
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- ============================================================================
-- 11. MONITORING QUERIES
-- ============================================================================

-- Monitor active connections
SELECT 
    pid,
    usename,
    application_name,
    state,
    query_start,
    state_change
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY query_start DESC;

-- Find long-running queries
SELECT 
    pid,
    usename,
    query,
    query_start,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - query_start))::INT as duration_seconds
FROM pg_stat_activity
WHERE state = 'active'
    AND query NOT ILIKE '%pg_stat_activity%'
    AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - query_start)) > 300
ORDER BY query_start DESC;

-- Cache hit ratio (should be > 99%)
SELECT 
    sum(heap_blks_read) as heap_read,
    sum(heap_blks_hit) as heap_hit,
    ROUND(100 * sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)), 2) as ratio
FROM pg_statio_user_tables;

-- ============================================================================
-- 12. DATA EXPORT FOR DOWNSTREAM SYSTEMS
-- ============================================================================

-- Export sales data to CSV for analytics
COPY (
    SELECT * FROM sales_transactions 
    WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
) TO '/tmp/sales_export.csv' WITH CSV HEADER;

-- Export aggregated data for BI tools
COPY (
    SELECT 
        DATE(order_date) as date,
        product_category,
        COUNT(*) as order_count,
        SUM(price) as revenue
    FROM sales_transactions
    WHERE order_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE(order_date), product_category
) TO '/tmp/aggregated_sales.csv' WITH CSV HEADER;
