SELECT
    h.offset / 1000 AS horizon_ms,
    avg(q.mid - t.price) AS avg_markout
FROM markout_trades t
HORIZON JOIN markout_quotes q
    LIST (0, 1000U, 2000U, 3000U) AS h
ORDER BY h.offset;
