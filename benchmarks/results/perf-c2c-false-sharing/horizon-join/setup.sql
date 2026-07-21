DROP TABLE IF EXISTS markout_trades;
DROP TABLE IF EXISTS markout_quotes;

CREATE TABLE markout_trades AS (
    SELECT
        timestamp_sequence('2025-01-01T00:00:00.000000Z', 1000L) ts,
        x::double price
    FROM long_sequence(2000000)
) TIMESTAMP(ts) PARTITION BY DAY;

CREATE TABLE markout_quotes AS (
    SELECT
        timestamp_sequence('2025-01-01T00:00:00.000000Z', 1000L) ts,
        x::double mid
    FROM long_sequence(2000000)
) TIMESTAMP(ts) PARTITION BY DAY;

SELECT 'markout_trades' AS table_name, count() AS rows FROM markout_trades
UNION ALL
SELECT 'markout_quotes' AS table_name, count() AS rows FROM markout_quotes;
