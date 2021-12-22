# CPU-only queries

For context see
[the accompanying TSBS QuestDB guide](https://github.com/questdb/questdb/blob/master/artifacts/tsbs/README.md).
The dataset used to generate the queries was created with commands (for
`high-cpu-1`) as follows:

```bash
~/tmp/go/bin/tsbs_generate_data --use-case="cpu-only" --seed=123 --scale=4000 \
  --timestamp-start="2016-01-01T00:00:00Z" --timestamp-end="2016-01-02T00:00:00Z" \
  --log-interval="10s" --format="influx" > /tmp/bigcpu

~/tmp/go/bin/tsbs_generate_queries --use-case="cpu-only" --seed=123 --scale=4000 \
  --timestamp-start="2016-01-01T00:00:00Z" --timestamp-end="2016-01-02T00:00:01Z" \
  --queries=1000 --query-type="high-cpu-1" --format="questdb" > /tmp/queries_questdb
```

**Example generated queries:**

```sql
-- high-cpu-1
SELECT *
FROM cpu
WHERE usage_user > 90.0
 AND hostname IN ('host_249')
 AND timestamp >= '2016-01-01T07:47:52Z'
 AND timestamp < '2016-01-01T19:47:52Z

-- high-cpu-all
SELECT *
FROM cpu
WHERE usage_user > 90.0
 AND timestamp >= '2016-01-01T07:47:52Z'
 AND timestamp < '2016-01-01T19:47:52Z'

-- cpu-max-all-1
SELECT
 hour(timestamp) AS hour,
 max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle, max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq, max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest, max(usage_guest_nice) AS max_usage_guest_nice
FROM cpu
WHERE hostname IN ('host_249')
 AND timestamp >= '2016-01-01T08:24:59Z'
 AND timestamp < '2016-01-01T16:24:59Z'
 SAMPLE BY 1h

-- cpu-max-all-8
SELECT
 hour(timestamp) AS hour,
 max(usage_user) AS max_usage_user, max(usage_system) AS max_usage_system, max(usage_idle) AS max_usage_idle, max(usage_nice) AS max_usage_nice, max(usage_iowait) AS max_usage_iowait, max(usage_irq) AS max_usage_irq, max(usage_softirq) AS max_usage_softirq, max(usage_steal) AS max_usage_steal, max(usage_guest) AS max_usage_guest, max(usage_guest_nice) AS max_usage_guest_nice
FROM cpu
WHERE hostname IN ('host_249', 'host_1403', 'host_1435', 'host_3539', 'host_3639', 'host_3075', 'host_815', 'host_2121')
 AND timestamp >= '2016-01-01T08:24:59Z'
 AND timestamp < '2016-01-01T16:24:59Z'
 SAMPLE BY 1h

-- double-groupby-1
SELECT timestamp, hostname,
 avg(usage_user) AS avg_usage_user
FROM cpu
WHERE timestamp >= '2016-01-01T07:47:52Z'
 AND timestamp < '2016-01-01T19:47:52Z'
 SAMPLE BY 1h
 GROUP BY timestamp, hostname

-- double-groupby-5
SELECT timestamp, hostname,
 avg(usage_user) AS avg_usage_user, avg(usage_system) AS avg_usage_system, avg(usage_idle) AS avg_usage_idle, avg(usage_nice) AS avg_usage_nice, avg(usage_iowait) AS avg_usage_iowait
FROM cpu
WHERE timestamp >= '2016-01-01T07:47:52Z'
 AND timestamp < '2016-01-01T19:47:52Z'
 SAMPLE BY 1h
 GROUP BY timestamp, hostname

-- double-groupby-all
SELECT timestamp, hostname,
 avg(usage_user) AS avg_usage_user, avg(usage_system) AS avg_usage_system, avg(usage_idle) AS avg_usage_idle, avg(usage_nice) AS avg_usage_nice, avg(usage_iowait) AS avg_usage_iowait, avg(usage_irq) AS avg_usage_irq, avg(usage_softirq) AS avg_usage_softirq, avg(usage_steal) AS avg_usage_steal, avg(usage_guest) AS avg_usage_guest, avg(usage_guest_nice) AS avg_usage_guest_nice
FROM cpu
WHERE timestamp >= '2016-01-01T10:20:52Z'
 AND timestamp < '2016-01-01T22:20:52Z'
 SAMPLE BY 1h
 GROUP BY timestamp, hostname

-- groupby-orderby-limit
SELECT timestamp AS minute,
 max(usage_user)
FROM cpu
WHERE timestamp < '2016-01-01T03:17:08Z'
 SAMPLE BY 1m
 LIMIT 5

-- lastpoint
SELECT * FROM cpu latest by hostname

-- single-groupby-1-1-12
SELECT timestamp,
 max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ('host_249')
 AND timestamp >= '2016-01-01T07:47:52Z'
 AND timestamp < '2016-01-01T19:47:52Z'
 SAMPLE BY 1m

-- single-groupby-1-8-1
SELECT timestamp,
 max(usage_user) AS max_usage_user
FROM cpu
WHERE hostname IN ('host_249', 'host_1403', 'host_1435', 'host_3539', 'host_3639', 'host_3075', 'host_815', 'host_2121')
 AND timestamp >= '2016-01-01T02:17:08Z'
 AND timestamp < '2016-01-01T03:17:08Z'
 SAMPLE BY 1m
```
