-- Standalone repro for HORIZON JOIN projection row loss.
-- Run in a scratch database. These names deliberately differ from the report's tables.
-- Six trades, including exact duplicates and a symbol with no quote.
CREATE TABLE horizon_repro_trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE)
TIMESTAMP(ts) PARTITION BY DAY;
CREATE TABLE horizon_repro_quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE, ask DOUBLE)
TIMESTAMP(ts) PARTITION BY DAY;

INSERT INTO horizon_repro_trades VALUES
    ('2026-01-01T00:00:00Z', 'A', 10),
    ('2026-01-01T00:00:00Z', 'A', 10),
    ('2026-01-01T00:00:02Z', 'A', 10),
    ('2026-01-01T00:00:02Z', 'B', 20),
    ('2026-01-01T00:00:02Z', 'C', 30),
    ('2026-01-01T00:00:02Z', 'C', 30);
INSERT INTO horizon_repro_quotes VALUES
    ('2026-01-01T00:00:00Z', 'A', 10, 12),
    ('2026-01-01T00:00:00Z', 'B', 20, 22),
    ('2026-01-01T00:00:03Z', 'A', 14, 16),
    ('2026-01-01T00:00:03Z', 'B', 22, 24);

-- Oracle/workaround: aggregate at the same query level as the HORIZON JOIN.
SELECT offset / 1_000_000 AS seconds,
       avg((bid + ask) / 2.0 - t.price) AS avgMarkoutAll, count() AS nTotal
FROM (SELECT * FROM horizon_repro_trades WHERE sym IN ('A', 'B', 'C')) t
HORIZON JOIN horizon_repro_quotes ON (sym) LIST (1s, 5s, 10s, 30s, 60s) AS h
GROUP BY seconds
ORDER BY seconds;

-- Repro: moving just the projection into a sub-query must not change the result.
SELECT offset / 1_000_000 AS seconds,
       avg((bid + ask) / 2.0 - price) AS avgMarkoutAll, count() AS nTotal
FROM (
    SELECT price, bid, ask, offset
    FROM (SELECT * FROM horizon_repro_trades WHERE sym IN ('A', 'B', 'C')) t
    HORIZON JOIN horizon_repro_quotes ON (sym) LIST (1s, 5s, 10s, 30s, 60s) AS h
)
GROUP BY seconds
ORDER BY seconds;

-- Both queries must return:
-- seconds  avgMarkoutAll  nTotal
-- 1        2.5           6
-- 5        4.5           6
-- 10       4.5           6
-- 30       4.5           6
-- 60       4.5           6
-- Before the fix, the second query returns (1, 3.0, 4), then (5/10/30/60, 4.0, 3).
