-- Demonstration of materialized view with index creation
-- This SQL shows the syntax requested in GitHub issue #6094

-- Create a base table
CREATE TABLE trades (
    timestamp TIMESTAMP,
    symbol SYMBOL,
    price DOUBLE
) TIMESTAMP(timestamp) PARTITION BY DAY WAL;

-- Create materialized view with index (this syntax should work)
CREATE MATERIALIZED VIEW trades_hourly_prices AS (
    SELECT 
        timestamp,
        symbol,
        avg(price) AS avg_price
    FROM trades
    SAMPLE BY 1h
), INDEX(symbol) PARTITION BY DAY;

-- Show the CREATE statement - should now include INDEX information
SHOW CREATE MATERIALIZED VIEW trades_hourly_prices;

-- Example with multiple indexes
CREATE MATERIALIZED VIEW complex_view AS (
    SELECT 
        timestamp,
        symbol,
        avg(price) AS avg_price,
        count(*) AS trade_count
    FROM trades
    SAMPLE BY 1h
), INDEX(symbol CAPACITY 1024) PARTITION BY DAY;

SHOW CREATE MATERIALIZED VIEW complex_view;