/*+******************************************************************************
 *  Copyright (c) 2019-2026 QuestDB
 *  Licensed under the Apache License, Version 2.0
 ******************************************************************************/

package io.questdb.test.griffin.engine.groupby;

import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SampleByFillTest extends AbstractCairoTest {

    @Test
    public void testExplainFillRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES (1.0, '2024-01-01T01:00:00.000000Z')");
            // FROM+TO drive the range attribute in toPlan().
            assertPlanNoLeakCheck(
                    "SELECT sum(val), ts FROM x " +
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T04:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR",
                    """
                            Sample By Fill
                              range: ('2024-01-01','2024-01-01T04:00:00.000000Z')
                              stride: '1h'
                                Sort
                                  keys: [ts]
                                    Async Group By workers: 1
                                      keys: [ts]
                                      keyFunctions: [timestamp_floor_utc('1h',ts,'2024-01-01T00:00:00.000Z')]
                                      values: [sum(val)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Interval forward scan on: x
                                              intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T03:59:59.999999Z")]
                            """
            );
        });
    }

    @Test
    public void testFillConstDecimal128Value() throws Exception {
        assertMemoryLeak(() -> {
            // Non-keyed FILL with a DECIMAL128 constant covers
            // FillRecord.getDecimal128 FILL_CONSTANT branch.
            execute("CREATE TABLE x (val DECIMAL(19, 0), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(cast('1' AS DECIMAL(19,0)), '2024-01-01T00:00:00.000000Z')," +
                    "(cast('3' AS DECIMAL(19,0)), '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    """
                            first\tts
                            1\t2024-01-01T00:00:00.000000Z
                            42\t2024-01-01T01:00:00.000000Z
                            3\t2024-01-01T02:00:00.000000Z
                            """,
                    "SELECT first(val), ts FROM x SAMPLE BY 1h FILL(cast('42' as DECIMAL(19,0))) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillConstDecimal256Value() throws Exception {
        assertMemoryLeak(() -> {
            // Non-keyed FILL with a DECIMAL256 constant covers
            // FillRecord.getDecimal256 FILL_CONSTANT branch.
            execute("CREATE TABLE x (val DECIMAL(39, 0), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(cast('1' AS DECIMAL(39,0)), '2024-01-01T00:00:00.000000Z')," +
                    "(cast('3' AS DECIMAL(39,0)), '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    """
                            first\tts
                            1\t2024-01-01T00:00:00.000000Z
                            42\t2024-01-01T01:00:00.000000Z
                            3\t2024-01-01T02:00:00.000000Z
                            """,
                    "SELECT first(val), ts FROM x SAMPLE BY 1h FILL(cast('42' as DECIMAL(39,0))) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillConstLong256Value() throws Exception {
        assertMemoryLeak(() -> {
            // Non-keyed FILL with a LONG256 constant covers the FILL_CONSTANT
            // branch in FillRecord.getLong256 / getLong256A / getLong256B.
            execute("CREATE TABLE x (val LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(cast('0x01' AS LONG256), '2024-01-01T00:00:00.000000Z')," +
                    "(cast('0x03' AS LONG256), '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    """
                            first\tts
                            1\t2024-01-01T00:00:00.000000Z
                            66\t2024-01-01T01:00:00.000000Z
                            3\t2024-01-01T02:00:00.000000Z
                            """,
                    "SELECT first(val), ts FROM x SAMPLE BY 1h FILL(cast('0x42' as LONG256)) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillIPv4Keyed() throws Exception {
        assertMemoryLeak(() -> {
            // Keyed SAMPLE BY with an IPv4 key column and FILL(NULL).
            // Covers FillRecord.getIPv4 FILL_KEY branch plus the default null
            // returns used for aggregate columns of absent keys.
            execute("CREATE TABLE traffic (ip IPV4, bytes LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO traffic VALUES " +
                    "('10.0.0.1', 100, '2024-01-01T00:00:00.000000Z')," +
                    "('10.0.0.2', 200, '2024-01-01T00:00:00.000000Z')," +
                    "('10.0.0.2', 210, '2024-01-01T01:00:00.000000Z')," +
                    "('10.0.0.1', 110, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    """
                            ts\tip\tsum
                            2024-01-01T00:00:00.000000Z\t10.0.0.1\t100
                            2024-01-01T00:00:00.000000Z\t10.0.0.2\t200
                            2024-01-01T01:00:00.000000Z\t10.0.0.2\t210
                            2024-01-01T01:00:00.000000Z\t10.0.0.1\tnull
                            2024-01-01T02:00:00.000000Z\t10.0.0.1\t110
                            2024-01-01T02:00:00.000000Z\t10.0.0.2\tnull
                            """,
                    "SELECT ts, ip, sum(bytes) FROM traffic SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullDstFallback() throws Exception {
        assertMemoryLeak(() -> {
            // Dense data: one row every 10 minutes around Europe/Riga DST fall-back 2021-10-31.
            // 100 rows from 00:00Z to 16:30Z. With 1h Riga timezone buckets, every bucket
            // has data, so no fill rows are needed. The key assertion: the query terminates
            // (no infinite loop) and output is monotonically time-ordered.
            execute("CREATE TABLE y AS (" +
                    "SELECT x::DOUBLE AS val, " +
                    "timestamp_sequence(cast('2021-10-31T00:00:00.000000Z' AS TIMESTAMP), 600_000_000) k " +
                    "FROM long_sequence(100)) TIMESTAMP(k) PARTITION BY NONE");
            assertSql(
                    """
                            s\tk
                            6\t2021-10-31T00:00:00.000000Z
                            6\t2021-10-31T01:00:00.000000Z
                            6\t2021-10-31T02:00:00.000000Z
                            6\t2021-10-31T03:00:00.000000Z
                            6\t2021-10-31T04:00:00.000000Z
                            6\t2021-10-31T05:00:00.000000Z
                            6\t2021-10-31T06:00:00.000000Z
                            6\t2021-10-31T07:00:00.000000Z
                            6\t2021-10-31T08:00:00.000000Z
                            6\t2021-10-31T09:00:00.000000Z
                            6\t2021-10-31T10:00:00.000000Z
                            6\t2021-10-31T11:00:00.000000Z
                            6\t2021-10-31T12:00:00.000000Z
                            6\t2021-10-31T13:00:00.000000Z
                            6\t2021-10-31T14:00:00.000000Z
                            6\t2021-10-31T15:00:00.000000Z
                            4\t2021-10-31T16:00:00.000000Z
                            """,
                    "SELECT count() s, k FROM y SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Riga'"
            );
        });
    }

    @Test
    public void testFillNullDstSparseData() throws Exception {
        assertMemoryLeak(() -> {
            // Sparse data around Europe/Riga DST fall-back on 2021-10-31.
            // Riga switches from EEST (UTC+3) to EET (UTC+2) at 04:00 local
            // (01:00 UTC). Data only at 00:00 UTC and 04:00 UTC; the buckets
            // at 01:00, 02:00, 03:00 UTC must be filled with NULL.
            execute("CREATE TABLE z (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO z VALUES " +
                    "(1.0, '2021-10-31T00:00:00.000000Z')," +
                    "(5.0, '2021-10-31T04:00:00.000000Z')");
            assertSql(
                    """
                            s\tts
                            1.0\t2021-10-31T00:00:00.000000Z
                            null\t2021-10-31T01:00:00.000000Z
                            null\t2021-10-31T02:00:00.000000Z
                            null\t2021-10-31T03:00:00.000000Z
                            5.0\t2021-10-31T04:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM z SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Riga'"
            );
        });
    }

    @Test
    public void testFillNullEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertSql(
                    "sum\tts\n" +
                            "null\t2024-01-01T00:00:00.000000Z\n" +
                            "null\t2024-01-01T01:00:00.000000Z\n" +
                            "null\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T03:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullKeyed() throws Exception {
        assertMemoryLeak(() -> {
            // London has data at 00:00 and 02:00.
            // Paris has data at 00:00 and 01:00.
            // With 1h stride, expect cartesian product: 3 buckets x 2 keys = 6 rows.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 21.0, '2024-01-01T01:00:00.000000Z')," +
                    "('London', 11.0, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\t20.0
                            2024-01-01T01:00:00.000000Z\tParis\t21.0
                            2024-01-01T01:00:00.000000Z\tLondon\tnull
                            2024-01-01T02:00:00.000000Z\tLondon\t11.0
                            2024-01-01T02:00:00.000000Z\tParis\tnull
                            """,
                    "SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullKeyedFromTo() throws Exception {
        assertMemoryLeak(() -> {
            // 6 buckets (00:00..05:00) x 2 keys = 12 rows.
            // Leading fill at 00:00, 01:00 (null for both keys).
            // Data at 02:00-04:00 with nulls for missing keys.
            // Trailing fill at 05:00 (null for both).
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T02:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T02:00:00.000000Z')," +
                    "('Paris', 21.0, '2024-01-01T03:00:00.000000Z')," +
                    "('London', 11.0, '2024-01-01T04:00:00.000000Z')");
            assertSql(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\tnull
                            2024-01-01T00:00:00.000000Z\tParis\tnull
                            2024-01-01T01:00:00.000000Z\tLondon\tnull
                            2024-01-01T01:00:00.000000Z\tParis\tnull
                            2024-01-01T02:00:00.000000Z\tLondon\t10.0
                            2024-01-01T02:00:00.000000Z\tParis\t20.0
                            2024-01-01T03:00:00.000000Z\tParis\t21.0
                            2024-01-01T03:00:00.000000Z\tLondon\tnull
                            2024-01-01T04:00:00.000000Z\tLondon\t11.0
                            2024-01-01T04:00:00.000000Z\tParis\tnull
                            2024-01-01T05:00:00.000000Z\tLondon\tnull
                            2024-01-01T05:00:00.000000Z\tParis\tnull
                            """,
                    "SELECT ts, city, avg(temp) FROM weather " +
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T06:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullKeyedFromToAfterData() throws Exception {
        assertMemoryLeak(() -> {
            // FROM is after all data — triggers the SIGSEGV fix.
            // Expect empty result: zero keys discovered means zero rows.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T01:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T01:00:00.000000Z')");
            assertSql(
                    """
                            ts\tcity\tavg
                            """,
                    "SELECT ts, city, avg(temp) FROM weather " +
                            "SAMPLE BY 1h FROM '2024-01-05' TO '2024-01-06' FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullKeyedFromToBeforeDataToWithinData() throws Exception {
        assertMemoryLeak(() -> {
            // FROM before all data, TO within data range.
            // 4 buckets (00:00..03:00) x 2 keys = 8 rows.
            // Leading fill at 00:00, 01:00, 02:00. Data at 03:00.
            // London at 04:00 is outside TO range and NOT included.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T03:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T03:00:00.000000Z')," +
                    "('London', 11.0, '2024-01-01T04:00:00.000000Z')");
            assertSql(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\tnull
                            2024-01-01T00:00:00.000000Z\tParis\tnull
                            2024-01-01T01:00:00.000000Z\tLondon\tnull
                            2024-01-01T01:00:00.000000Z\tParis\tnull
                            2024-01-01T02:00:00.000000Z\tLondon\tnull
                            2024-01-01T02:00:00.000000Z\tParis\tnull
                            2024-01-01T03:00:00.000000Z\tLondon\t10.0
                            2024-01-01T03:00:00.000000Z\tParis\t20.0
                            """,
                    "SELECT ts, city, avg(temp) FROM weather " +
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T04:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullKeyedFromToEmptyRange() throws Exception {
        assertMemoryLeak(() -> {
            // FROM == TO — empty range, expect zero rows.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T01:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T01:00:00.000000Z')");
            assertSql(
                    """
                            ts\tcity\tavg
                            """,
                    "SELECT ts, city, avg(temp) FROM weather " +
                            "SAMPLE BY 1h FROM '2024-01-01T05:00:00.000000Z' TO '2024-01-01T05:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullKeyedFromToKeyAppearsMidRange() throws Exception {
        assertMemoryLeak(() -> {
            // Berlin appears only at 03:00 but is discovered in pass 1.
            // 5 buckets (00:00..04:00) x 3 keys = 15 rows.
            // Berlin gets null fill for buckets 00:00-02:00 and 04:00.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T01:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T01:00:00.000000Z')," +
                    "('Berlin', 5.0, '2024-01-01T03:00:00.000000Z')," +
                    "('London', 12.0, '2024-01-01T03:00:00.000000Z')");
            assertSql(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\tnull
                            2024-01-01T00:00:00.000000Z\tParis\tnull
                            2024-01-01T00:00:00.000000Z\tBerlin\tnull
                            2024-01-01T01:00:00.000000Z\tLondon\t10.0
                            2024-01-01T01:00:00.000000Z\tParis\t20.0
                            2024-01-01T01:00:00.000000Z\tBerlin\tnull
                            2024-01-01T02:00:00.000000Z\tLondon\tnull
                            2024-01-01T02:00:00.000000Z\tParis\tnull
                            2024-01-01T02:00:00.000000Z\tBerlin\tnull
                            2024-01-01T03:00:00.000000Z\tBerlin\t5.0
                            2024-01-01T03:00:00.000000Z\tLondon\t12.0
                            2024-01-01T03:00:00.000000Z\tParis\tnull
                            2024-01-01T04:00:00.000000Z\tLondon\tnull
                            2024-01-01T04:00:00.000000Z\tParis\tnull
                            2024-01-01T04:00:00.000000Z\tBerlin\tnull
                            """,
                    "SELECT ts, city, avg(temp) FROM weather " +
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T05:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullKeyedFromToMultipleAggregates() throws Exception {
        assertMemoryLeak(() -> {
            // Two aggregate columns (sum(val), sum(ival)) with keyed FROM/TO.
            // 4 buckets (00:00..03:00) x 2 keys = 8 rows.
            execute("CREATE TABLE x (key STRING, val DOUBLE, ival INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', 1.0, 10, '2024-01-01T01:00:00.000000Z')," +
                    "('B', 2.0, 20, '2024-01-01T01:00:00.000000Z')," +
                    "('A', 3.0, 30, '2024-01-01T03:00:00.000000Z')");
            assertSql(
                    """
                            ts\tkey\tsum\tsum1
                            2024-01-01T00:00:00.000000Z\tA\tnull\tnull
                            2024-01-01T00:00:00.000000Z\tB\tnull\tnull
                            2024-01-01T01:00:00.000000Z\tA\t1.0\t10
                            2024-01-01T01:00:00.000000Z\tB\t2.0\t20
                            2024-01-01T02:00:00.000000Z\tA\tnull\tnull
                            2024-01-01T02:00:00.000000Z\tB\tnull\tnull
                            2024-01-01T03:00:00.000000Z\tA\t3.0\t30
                            2024-01-01T03:00:00.000000Z\tB\tnull\tnull
                            """,
                    "SELECT ts, key, sum(val), sum(ival) FROM x " +
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T04:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullKeyedSymbol() throws Exception {
        assertMemoryLeak(() -> {
            // Tests SYMBOL key column with setSymbolTableResolver for correct
            // string resolution in fill rows. Also verifies stable key order.
            execute("CREATE TABLE weather (city SYMBOL, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "('London', 11.0, '2024-01-01T01:00:00.000000Z')");
            // 2 buckets x 2 keys = 4 rows. Paris missing at 01:00.
            assertSql(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\t20.0
                            2024-01-01T01:00:00.000000Z\tLondon\t11.0
                            2024-01-01T01:00:00.000000Z\tParis\tnull
                            """,
                    "SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullKeyedThreeKeys() throws Exception {
        assertMemoryLeak(() -> {
            // Three keys where Paris and Berlin appear only in first bucket.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Berlin', 5.0, '2024-01-01T00:00:00.000000Z')," +
                    "('London', 11.0, '2024-01-01T01:00:00.000000Z')");
            // 2 buckets x 3 keys = 6 rows.
            // Paris and Berlin missing at 01:00.
            assertSql(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\t20.0
                            2024-01-01T00:00:00.000000Z\tBerlin\t5.0
                            2024-01-01T01:00:00.000000Z\tLondon\t11.0
                            2024-01-01T01:00:00.000000Z\tParis\tnull
                            2024-01-01T01:00:00.000000Z\tBerlin\tnull
                            """,
                    "SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullKeyedWithCalendarOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (ts TIMESTAMP, city SYMBOL, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO test VALUES " +
                    "('2024-01-01T12:00:00.000000Z', 'NYC', 10.0), " +
                    "('2024-01-01T12:00:00.000000Z', 'LON', 20.0), " +
                    "('2024-01-02T12:00:00.000000Z', 'NYC', 30.0)");
            drainWalQueue();

            // Buckets at 10:00. LON missing from second bucket.
            assertQueryNoLeakCheck(
                    """
                            ts\tcity\tavg
                            2024-01-01T10:00:00.000000Z\tNYC\t10.0
                            2024-01-01T10:00:00.000000Z\tLON\t20.0
                            2024-01-02T10:00:00.000000Z\tNYC\t30.0
                            2024-01-02T10:00:00.000000Z\tLON\tnull
                            """,
                    "SELECT ts, city, avg(value) FROM test SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '10:00'",
                    "ts"
            );
        });
    }

    @Test
    public void testFillNullKeyedWithNullKey() throws Exception {
        assertMemoryLeak(() -> {
            // NULL symbol key forms its own group in the cartesian product.
            // 2 buckets x 2 keys (null + London) = 4 rows.
            execute("CREATE TABLE t (city SYMBOL, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(null, 10.0, '2024-01-01T01:00:00.000000Z')," +
                    "('London', 20.0, '2024-01-01T01:00:00.000000Z')," +
                    "(null, 30.0, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    """
                            ts\tcity\tavg
                            2024-01-01T01:00:00.000000Z\t\t10.0
                            2024-01-01T01:00:00.000000Z\tLondon\t20.0
                            2024-01-01T02:00:00.000000Z\t\t30.0
                            2024-01-01T02:00:00.000000Z\tLondon\tnull
                            """,
                    "SELECT ts, city, avg(temp) FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullMultipleAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT x::DOUBLE AS val, " +
                    "x::INT AS ival, " +
                    "timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
                    "FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
            assertSql(
                    "sum\tavg\tsum1\tts\n" +
                            "1.0\t1.0\t1\t2024-01-01T00:00:00.000000Z\n" +
                            "null\tnull\tnull\t2024-01-01T01:00:00.000000Z\n" +
                            "2.0\t2.0\t2\t2024-01-01T02:00:00.000000Z\n" +
                            "null\tnull\tnull\t2024-01-01T03:00:00.000000Z\n" +
                            "3.0\t3.0\t3\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT sum(val), avg(val), sum(ival), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullNonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
                    "FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
            // Data at 00:00, 02:00, 04:00 -- gaps at 01:00, 03:00
            assertSql(
                    "sum\tts\n" +
                            "1.0\t2024-01-01T00:00:00.000000Z\n" +
                            "null\t2024-01-01T01:00:00.000000Z\n" +
                            "2.0\t2024-01-01T02:00:00.000000Z\n" +
                            "null\t2024-01-01T03:00:00.000000Z\n" +
                            "3.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullNonKeyedFromTo() throws Exception {
        assertMemoryLeak(() -> {
            // Data at 02:00, 03:00. FROM 00:00, TO 05:00.
            // Expect leading fill at 00:00, 01:00; trailing fill at 04:00.
            execute("CREATE TABLE x AS (" +
                    "SELECT x::DOUBLE AS val, " +
                    "timestamp_sequence('2024-01-01T02:00:00.000000Z', 3_600_000_000) AS ts " +
                    "FROM long_sequence(2)) TIMESTAMP(ts) PARTITION BY DAY");
            assertSql(
                    "sum\tts\n" +
                            "null\t2024-01-01T00:00:00.000000Z\n" +
                            "null\t2024-01-01T01:00:00.000000Z\n" +
                            "1.0\t2024-01-01T02:00:00.000000Z\n" +
                            "2.0\t2024-01-01T03:00:00.000000Z\n" +
                            "null\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT sum(val), ts FROM x " +
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T05:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullNonKeyedNoToClause() throws Exception {
        assertMemoryLeak(() -> {
            // Regression test: verifies the infinite loop fix.
            // Data at 00:00, 02:00, 04:00 with 1h stride, no TO clause.
            // Without the fix, the query would hang forever emitting fill
            // rows beyond 04:00 because maxTimestamp=Long.MAX_VALUE.
            execute("CREATE TABLE x AS (" +
                    "SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
                    "FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
            assertSql(
                    "sum\tts\n" +
                            "1.0\t2024-01-01T00:00:00.000000Z\n" +
                            "null\t2024-01-01T01:00:00.000000Z\n" +
                            "2.0\t2024-01-01T02:00:00.000000Z\n" +
                            "null\t2024-01-01T03:00:00.000000Z\n" +
                            "3.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullSparseDataLargeRange() throws Exception {
        assertMemoryLeak(() -> {
            // 2 data points ~1 year apart, 1h stride = ~8760 empty buckets.
            // Before the recursive-to-iterative fix in emitNextFillRow(), this
            // query caused StackOverflowError from O(gap_count) recursive
            // hasNext() calls.
            execute("CREATE TABLE sparse (key SYMBOL, val DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY MONTH");
            execute("INSERT INTO sparse VALUES " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('A', 2.0, '2024-12-31T00:00:00.000000Z')");
            // Verify completion without StackOverflowError and correct row count.
            // 2024 is a leap year (366 days). From Jan 1 00:00 to Dec 31 00:00
            // = 365 days = 8760 hours + the final bucket = 8761 hourly buckets.
            assertSql(
                    "count\n8761\n",
                    "SELECT count() FROM (" +
                            "SELECT ts, key, avg(val) FROM sparse " +
                            "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR)"
            );
        });
    }

    @Test
    public void testFillNullWithCalendarOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO test VALUES " +
                    "('2024-01-01T12:00:00.000000Z', 1.0), " +
                    "('2024-01-03T12:00:00.000000Z', 3.0)");
            drainWalQueue();

            // With offset '10:00', buckets start at 10:00 UTC each day.
            // Data at Jan 1 12:00 falls in bucket [Jan 1 10:00, Jan 2 10:00)
            // Data at Jan 3 12:00 falls in bucket [Jan 3 10:00, Jan 4 10:00)
            // Gap bucket: [Jan 2 10:00, Jan 3 10:00) filled with NULL
            assertQueryNoLeakCheck(
                    """
                            ts\tavg
                            2024-01-01T10:00:00.000000Z\t1.0
                            2024-01-02T10:00:00.000000Z\tnull
                            2024-01-03T10:00:00.000000Z\t3.0
                            """,
                    "SELECT ts, avg(value) FROM test SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '10:00'",
                    "ts"
            );
        });
    }

    @Test
    public void testFillNullWithCalendarOffsetAndFromTo() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO test VALUES ('2024-01-02T12:00:00.000000Z', 5.0)");
            drainWalQueue();

            // FROM '2024-01-01' TO '2024-01-04' with offset '02:00'
            // timestamp_floor_utc computes effectiveOffset = from + offset,
            // so buckets start at 02:00 each day.
            // Data at Jan 2 12:00 falls in [Jan 2 02:00, Jan 3 02:00)
            assertQueryNoLeakCheck(
                    """
                            ts\tavg
                            2024-01-01T02:00:00.000000Z\tnull
                            2024-01-02T02:00:00.000000Z\t5.0
                            2024-01-03T02:00:00.000000Z\tnull
                            """,
                    "SELECT ts, avg(value) FROM test " +
                            "SAMPLE BY 1d FROM '2024-01-01' TO '2024-01-04' " +
                            "FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '02:00'",
                    "ts"
            );
        });
    }

    @Test
    public void testFillNullWithZeroOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO test VALUES " +
                    "('2024-01-01T12:00:00.000000Z', 1.0), " +
                    "('2024-01-03T12:00:00.000000Z', 3.0)");
            drainWalQueue();

            // Zero offset should behave identically to no offset
            assertQueryNoLeakCheck(
                    """
                            ts\tavg
                            2024-01-01T00:00:00.000000Z\t1.0
                            2024-01-02T00:00:00.000000Z\tnull
                            2024-01-03T00:00:00.000000Z\t3.0
                            """,
                    "SELECT ts, avg(value) FROM test SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '00:00'",
                    "ts"
            );
        });
    }

    @Test
    public void testFillPrevAllNumericTypes() throws Exception {
        assertMemoryLeak(() -> {
            // FLOAT, CHAR and DECIMAL8/16/32/64 aggregates with FILL(PREV)
            // cover the remaining readColumnAsLongBits branches. Two rows 2h
            // apart create a 1h gap at 01:00 that must carry forward each
            // column's previous value.
            execute("CREATE TABLE types (" +
                    "f FLOAT, " +
                    "c CHAR, " +
                    "d8 DECIMAL(2, 0), " +
                    "d16 DECIMAL(4, 0), " +
                    "d32 DECIMAL(9, 0), " +
                    "d64 DECIMAL(18, 0), " +
                    "ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO types VALUES " +
                    "(1.5, 'A', cast('11' AS DECIMAL(2,0)), cast('1111' AS DECIMAL(4,0)), " +
                    "cast('111111' AS DECIMAL(9,0)), cast('111111111111' AS DECIMAL(18,0)), " +
                    "'2024-01-01T00:00:00.000000Z')," +
                    "(2.5, 'B', cast('22' AS DECIMAL(2,0)), cast('2222' AS DECIMAL(4,0)), " +
                    "cast('222222' AS DECIMAL(9,0)), cast('222222222222' AS DECIMAL(18,0)), " +
                    "'2024-01-01T02:00:00.000000Z')");
            assertSql(
                    """
                            ts\tfirst\tfirst1\tfirst2\tfirst3\tfirst4\tfirst5
                            2024-01-01T00:00:00.000000Z\t1.5\tA\t11\t1111\t111111\t111111111111
                            2024-01-01T01:00:00.000000Z\t1.5\tA\t11\t1111\t111111\t111111111111
                            2024-01-01T02:00:00.000000Z\t2.5\tB\t22\t2222\t222222\t222222222222
                            """,
                    "SELECT ts, first(f), first(c), first(d8), first(d16), first(d32), first(d64) " +
                            "FROM types SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillPrevCaseOverDecimalFallback() throws Exception {
        assertMemoryLeak(() -> {
            // Aggregate argument is a CASE expression returning DECIMAL256.
            // The optimizer gate passes through expression-argument aggregates,
            // and the retro-fallback at codegen detects the unsupported output
            // type and re-dispatches to the legacy Sample By cursor path.
            // Plan assertion proves retro-fallback fires (legacy "Sample By",
            // not fast-path "Sample By Fill").
            execute("CREATE TABLE t (v DOUBLE, d256_col DECIMAL(39, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, cast('1.0' AS DECIMAL(39,2)), '2024-01-01T00:00:00.000000Z')," +
                    "(2.0, cast('3.0' AS DECIMAL(39,2)), '2024-01-01T02:00:00.000000Z')");
            assertPlanNoLeakCheck(
                    "SELECT ts, first(CASE WHEN v > 0 THEN d256_col ELSE NULL::decimal(39,2) END) f " +
                            "FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    """
                            Encode sort
                              keys: [ts]
                                Sample By
                                  keys: [ts]
                                  values: [first(case([0<v,d256_col,null]))]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testFillPrevCrossColumnBadAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES (1.0, '2024-01-01T00:00:00.000000Z')");
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(val) AS s FROM x SAMPLE BY 1h FILL(PREV(nonexistent)) ALIGN TO CALENDAR",
                    55,
                    "column not found"
            );
        });
    }

    @Test
    public void testFillPrevCrossColumnKeyed() throws Exception {
        assertMemoryLeak(() -> {
            // Two keys (A, B) with gaps at different times.
            // FILL(PREV, PREV(s)) means column `a` fills from column `s`'s per-key prev.
            // Each key independently carries forward its own `s` prev into `a`.
            execute("CREATE TABLE x (key VARCHAR, val DOUBLE, ival INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', 1.0, 10, '2024-01-01T00:00:00.000000Z')," +
                    "('B', 2.0, 20, '2024-01-01T00:00:00.000000Z')," +
                    "('A', 3.0, 30, '2024-01-01T02:00:00.000000Z')");
            // At 01:00: A is a gap (s=prev(1.0), a=prev(s)=1.0), B is a gap (s=prev(2.0), a=prev(s)=2.0)
            // At 02:00: A has data (s=3.0, a=30.0), B is a gap (s=prev(2.0), a=prev(s)=2.0)
            assertSql(
                    """
                            ts\tkey\ts\ta
                            2024-01-01T00:00:00.000000Z\tA\t1.0\t10.0
                            2024-01-01T00:00:00.000000Z\tB\t2.0\t20.0
                            2024-01-01T01:00:00.000000Z\tA\t1.0\t1.0
                            2024-01-01T01:00:00.000000Z\tB\t2.0\t2.0
                            2024-01-01T02:00:00.000000Z\tA\t3.0\t30.0
                            2024-01-01T02:00:00.000000Z\tB\t2.0\t2.0
                            """,
                    "SELECT ts, key, sum(val) AS s, sum(ival::DOUBLE) AS a " +
                            "FROM x SAMPLE BY 1h FILL(PREV, PREV(s)) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillPrevCrossColumnMixedFill() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV(a), NULL) — column `s` fills from column `a`'s prev, column `a` fills with null.
            // This multi-fill spec must reach the GROUP BY fast path (not old cursor path).
            execute("CREATE TABLE x (val DOUBLE, ival INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, 10, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, 30, '2024-01-01T02:00:00.000000Z')");
            // At 01:00 gap: s=prev(a)=10.0, a=null
            assertSql(
                    """
                            ts\ts\ta
                            2024-01-01T00:00:00.000000Z\t1.0\t10.0
                            2024-01-01T01:00:00.000000Z\t10.0\tnull
                            2024-01-01T02:00:00.000000Z\t3.0\t30.0
                            """,
                    "SELECT ts, sum(val) AS s, sum(ival::DOUBLE) AS a " +
                            "FROM x SAMPLE BY 1h FILL(PREV(a), NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillPrevCrossColumnNonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV, PREV(s)) — column `s` fills from self prev, column `a` fills from `s`'s prev.
            execute("CREATE TABLE x (val DOUBLE, ival INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, 10, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, 30, '2024-01-01T02:00:00.000000Z')");
            // At 01:00 gap: s=1.0 (self-prev), a=1.0 (cross-column from s's prev)
            assertSql(
                    """
                            ts\ts\ta
                            2024-01-01T00:00:00.000000Z\t1.0\t10.0
                            2024-01-01T01:00:00.000000Z\t1.0\t1.0
                            2024-01-01T02:00:00.000000Z\t3.0\t30.0
                            """,
                    "SELECT ts, sum(val) AS s, sum(ival::DOUBLE) AS a " +
                            "FROM x SAMPLE BY 1h FILL(PREV, PREV(s)) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillPrevCrossColumnUnsupportedFallback() throws Exception {
        assertMemoryLeak(() -> {
            // PREV(s) targets a STRING column via cross-column reference.
            // The optimizer gate detects the unsupported type and skips the
            // fast-path rewrite; the query falls back to the legacy cursor.
            execute("CREATE TABLE x (s STRING, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('hello', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('world', 2.0, '2024-01-01T02:00:00.000000Z')");
            // Plan should show legacy path (Sample By), not Async Group By
            assertPlanNoLeakCheck(
                    "SELECT ts, first(s), sum(val) FROM x SAMPLE BY 1h FILL(PREV, NULL) ALIGN TO CALENDAR",
                    """
                            Sample By
                              fill: value
                              values: [first(s),sum(val)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """
            );
        });
    }

    @Test
    public void testFillPrevExpressionArgDecimal128Fallback() throws Exception {
        assertMemoryLeak(() -> {
            // Aggregate argument is an expression returning DECIMAL128 (scale promotes
            // DECIMAL(25,2) to DECIMAL128). The optimizer gate cannot resolve expression
            // output types, so the rewrite proceeds; retro-fallback at codegen detects
            // the unsupported type on the fully resolved groupBy metadata and re-dispatches
            // to the legacy cursor. Plan asserts "Sample By" (legacy), not "Sample By Fill".
            execute("CREATE TABLE t (d128_col DECIMAL(25, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(cast('1.0' AS DECIMAL(25,2)), '2024-01-01T00:00:00.000000Z')," +
                    "(cast('3.0' AS DECIMAL(25,2)), '2024-01-01T02:00:00.000000Z')");
            assertPlanNoLeakCheck(
                    "SELECT ts, sum(d128_col * 1::decimal(25,2)) s FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    """
                            Encode sort
                              keys: [ts]
                                Sample By
                                  keys: [ts]
                                  values: [sum(d128_col*1.00)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testFillPrevExpressionArgStringFallback() throws Exception {
        assertMemoryLeak(() -> {
            // Aggregate argument is an expression returning STRING (concat). The output
            // type STRING is not fast-path-supported, so retro-fallback routes to the
            // legacy cursor path after the rewrite.
            execute("CREATE TABLE t (s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('hello', '2024-01-01T00:00:00.000000Z')," +
                    "('world', '2024-01-01T02:00:00.000000Z')");
            assertPlanNoLeakCheck(
                    "SELECT ts, first(concat(s, 'x')) f FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    """
                            Encode sort
                              keys: [ts]
                                Sample By
                                  keys: [ts]
                                  values: [first(concat([s,'x']))]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testFillPrevGeoHash() throws Exception {
        assertMemoryLeak(() -> {
            // Non-keyed query with geohash columns and FILL(PREV).
            // Two rows 2h apart. 1h stride creates a gap at 01:00.
            // FILL(PREV) must carry forward geo values from 00:00 into the
            // 01:00 fill row, not return zero/null.
            // Covers getGeoByte (3b), getGeoShort (15b), getGeoInt (6c=30b),
            // getGeoLong (8c=40b).
            execute("CREATE TABLE g (g1 GEOHASH(3b), g2 GEOHASH(15b), " +
                    "g4 GEOHASH(6c), g8 GEOHASH(8c), " +
                    "ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO g VALUES " +
                    "(cast('8' AS GEOHASH(3b)), cast('sp0' AS GEOHASH(15b)), " +
                    " cast('sp052n' AS GEOHASH(6c)), cast('sp052n01' AS GEOHASH(8c)), " +
                    " '2024-01-01T00:00:00.000000Z')," +
                    "(cast('s' AS GEOHASH(3b)), cast('u33' AS GEOHASH(15b)), " +
                    " cast('u33d8b' AS GEOHASH(6c)), cast('u33d8b12' AS GEOHASH(8c)), " +
                    " '2024-01-01T02:00:00.000000Z')");
            // 3 buckets: 00:00 (data), 01:00 (PREV fill), 02:00 (data)
            // The 01:00 row must carry forward values from 00:00
            assertSql(
                    """
                            ts\tfirst\tfirst1\tfirst2\tfirst3
                            2024-01-01T00:00:00.000000Z\t010\tsp0\tsp052n\tsp052n01
                            2024-01-01T01:00:00.000000Z\t010\tsp0\tsp052n\tsp052n01
                            2024-01-01T02:00:00.000000Z\t110\tu33\tu33d8b\tu33d8b12
                            """,
                    "SELECT ts, first(g1), first(g2), first(g4), first(g8) " +
                            "FROM g SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillPrevGeoHashKeyed() throws Exception {
        assertMemoryLeak(() -> {
            // Keyed query with geo columns and FILL(PREV).
            // Two symbol keys, each with their own geohash aggregates.
            // Verify per-key geo PREV tracking (London's prev does not bleed
            // into Paris).
            execute("CREATE TABLE geo_weather (city SYMBOL, g GEOHASH(6c), " +
                    "ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO geo_weather VALUES " +
                    "('London', cast('gcpuuz' AS GEOHASH(6c)), '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', cast('u09tvw' AS GEOHASH(6c)), '2024-01-01T00:00:00.000000Z')," +
                    "('London', cast('gcpvn0' AS GEOHASH(6c)), '2024-01-01T02:00:00.000000Z')");
            // At 01:00: London gap -> prev = gcpuuz, Paris gap -> prev = u09tvw
            // At 02:00: London data = gcpvn0, Paris gap -> prev = u09tvw
            assertSql(
                    """
                            ts\tcity\tfirst
                            2024-01-01T00:00:00.000000Z\tLondon\tgcpuuz
                            2024-01-01T00:00:00.000000Z\tParis\tu09tvw
                            2024-01-01T01:00:00.000000Z\tLondon\tgcpuuz
                            2024-01-01T01:00:00.000000Z\tParis\tu09tvw
                            2024-01-01T02:00:00.000000Z\tLondon\tgcpvn0
                            2024-01-01T02:00:00.000000Z\tParis\tu09tvw
                            """,
                    "SELECT ts, city, first(g) FROM geo_weather " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillPrevIntervalFallback() throws Exception {
        assertMemoryLeak(() -> {
            // INTERVAL aggregate expression (first(interval(ts1, ts2))) is auto-cast to
            // STRING, which is not fast-path-supported. The query takes the retro-fallback
            // path and is served by the legacy Sample By cursor. Plan asserts "Sample By"
            // (legacy), not "Sample By Fill" (fast path). The Tier 1 gate addition of
            // ColumnType.INTERVAL (plan 12-01) closes the asymmetry against
            // isFastPathPrevSupportedType.
            execute("CREATE TABLE t (ts1 TIMESTAMP, ts2 TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00.000000Z', '2024-01-01T01:00:00.000000Z', '2024-01-01T00:00:00.000000Z')," +
                    "('2024-01-01T02:00:00.000000Z', '2024-01-01T03:00:00.000000Z', '2024-01-01T02:00:00.000000Z')");
            assertPlanNoLeakCheck(
                    "SELECT ts, first(interval(ts1, ts2)) i FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    """
                            Encode sort
                              keys: [ts]
                                Sample By
                                  keys: [ts]
                                  values: [first(interval(ts1,ts2)::string)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testFillPrevKeyedIndependent() throws Exception {
        assertMemoryLeak(() -> {
            // Tests that per-key prev tracking does not bleed between keys.
            // London: data at 00:00 (temp=10), gap at 01:00, data at 02:00 (temp=12)
            // Paris:  data at 00:00 (temp=20), data at 01:00 (temp=21), gap at 02:00
            // For FILL(PREV):
            //   London at 01:00 -> prev = 10 (London's own prev, NOT Paris's 21)
            //   Paris at 02:00 -> prev = 21 (Paris's own prev, NOT London's 12)
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 21.0, '2024-01-01T01:00:00.000000Z')," +
                    "('London', 12.0, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\t20.0
                            2024-01-01T01:00:00.000000Z\tParis\t21.0
                            2024-01-01T01:00:00.000000Z\tLondon\t10.0
                            2024-01-01T02:00:00.000000Z\tLondon\t12.0
                            2024-01-01T02:00:00.000000Z\tParis\t21.0
                            """,
                    "SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillPrevKeyedFromTo() throws Exception {
        assertMemoryLeak(() -> {
            // Per-key FILL(PREV) with FROM/TO.
            // 5 buckets (00:00..04:00) x 2 keys = 10 rows.
            // At 00:00 both keys have null (no prev yet).
            // At 01:00 data for both. At 02:00 prev. At 03:00 London data,
            // Paris prev. At 04:00 both prev.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T01:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T01:00:00.000000Z')," +
                    "('London', 12.0, '2024-01-01T03:00:00.000000Z')");
            assertSql(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\tnull
                            2024-01-01T00:00:00.000000Z\tParis\tnull
                            2024-01-01T01:00:00.000000Z\tLondon\t10.0
                            2024-01-01T01:00:00.000000Z\tParis\t20.0
                            2024-01-01T02:00:00.000000Z\tLondon\t10.0
                            2024-01-01T02:00:00.000000Z\tParis\t20.0
                            2024-01-01T03:00:00.000000Z\tLondon\t12.0
                            2024-01-01T03:00:00.000000Z\tParis\t20.0
                            2024-01-01T04:00:00.000000Z\tLondon\t12.0
                            2024-01-01T04:00:00.000000Z\tParis\t20.0
                            """,
                    "SELECT ts, city, avg(temp) FROM weather " +
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T05:00:00.000000Z' FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillPrevKeyedCte() throws Exception {
        assertMemoryLeak(() -> {
            // Wrapping SAMPLE BY FILL(PREV) in a CTE verifies the FILL_KEY
            // reclassification works correctly when the factory is reused
            // as a subquery.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 21.0, '2024-01-01T01:00:00.000000Z')," +
                    "('London', 12.0, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    """
                            ts\tcity\ta
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\t20.0
                            2024-01-01T01:00:00.000000Z\tParis\t21.0
                            2024-01-01T01:00:00.000000Z\tLondon\t10.0
                            2024-01-01T02:00:00.000000Z\tLondon\t12.0
                            2024-01-01T02:00:00.000000Z\tParis\t21.0
                            """,
                    "WITH sq AS (SELECT ts, city, avg(temp) AS a FROM weather SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR) " +
                            "SELECT * FROM sq"
            );
        });
    }

    @Test
    public void testFillPrevKeyedNoPrevYet() throws Exception {
        assertMemoryLeak(() -> {
            // London appears at 00:00, Paris first appears at 01:00.
            // At 00:00, Paris is missing and has no prev -> should get null.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T01:00:00.000000Z')");
            assertSql(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\tnull
                            2024-01-01T01:00:00.000000Z\tParis\t20.0
                            2024-01-01T01:00:00.000000Z\tLondon\t10.0
                            """,
                    "SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillPrevLong128Fallback() throws Exception {
        assertMemoryLeak(() -> {
            // LONG128 has no first/last/sum/min/max aggregate function in QuestDB,
            // so SELECT first(long128_col) ... fails at aggregate resolution. This test
            // documents that LONG128 PREV aggregates cannot reach the fast path and
            // are rejected at compile time. The Tier 1 optimizer gate addition of
            // ColumnType.LONG128 to isUnsupportedPrevType (plan 12-01) closes the
            // asymmetry against isFastPathPrevSupportedType and protects against the
            // day LONG128 aggregates ship.
            execute("CREATE TABLE t (val LONG128, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(to_long128(0, 1), '2024-01-01T00:00:00.000000Z')," +
                    "(to_long128(0, 2), '2024-01-01T02:00:00.000000Z')");
            assertExceptionNoLeakCheck(
                    "SELECT ts, first(val) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    11,
                    "there is no matching function `first` with the argument types"
            );
        });
    }

    @Test
    public void testFillPrevMixedWithSymbol() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV, NULL): PREV on sum(val) (DOUBLE, supported) and NULL
            // on first(sym) (SYMBOL, not PREV-targeted). The query stays on the
            // fast path because the PREV column is numeric; the SYMBOL column
            // uses NULL fill and is never snapshotted.
            execute("CREATE TABLE x AS (" +
                    "SELECT x::DOUBLE val, rnd_symbol('A','B') sym, " +
                    "timestamp_sequence(0, 7_200_000_000) ts " +
                    "FROM long_sequence(3)) TIMESTAMP(ts)");
            // Verify the plan shows fast path (Sample By Fill, not Sample By)
            assertPlanNoLeakCheck(
                    "SELECT ts, sum(val), first(sym) FROM x SAMPLE BY 1h FILL(PREV, NULL) ALIGN TO CALENDAR",
                    """
                            Sample By Fill
                              stride: '1h'
                              fill: prev
                                Sort
                                  keys: [ts]
                                    Async Group By workers: 1
                                      keys: [ts]
                                      keyFunctions: [timestamp_floor_utc('1h',ts)]
                                      values: [sum(val),first(sym)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """
            );
        });
    }

    @Test
    public void testFillPrevMixedWithSymbolKeyed() throws Exception {
        assertMemoryLeak(() -> {
            // Same as testFillPrevMixedWithSymbol but with a key column.
            // FILL(PREV, NULL) with a SYMBOL key should not crash.
            execute("CREATE TABLE x (key SYMBOL, val DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('K1', 1.0, 'A', '2024-01-01T00:00:00.000000Z')," +
                    "('K2', 2.0, 'B', '2024-01-01T00:00:00.000000Z')," +
                    "('K1', 3.0, 'C', '2024-01-01T02:00:00.000000Z')");
            // At 01:00: K1 gap -> sum=prev(1.0), sym=null; K2 gap -> sum=prev(2.0), sym=null
            // At 02:00: K1 data -> sum=3.0, sym=C; K2 gap -> sum=prev(2.0), sym=null
            assertSql(
                    """
                            ts\tkey\tsum\tfirst
                            2024-01-01T00:00:00.000000Z\tK1\t1.0\tA
                            2024-01-01T00:00:00.000000Z\tK2\t2.0\tB
                            2024-01-01T01:00:00.000000Z\tK1\t1.0\t
                            2024-01-01T01:00:00.000000Z\tK2\t2.0\t
                            2024-01-01T02:00:00.000000Z\tK1\t3.0\tC
                            2024-01-01T02:00:00.000000Z\tK2\t2.0\t
                            """,
                    "SELECT ts, key, sum(val), first(sym) FROM x SAMPLE BY 1h FILL(PREV, NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillPrevNonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
                    "FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
            assertSql(
                    """
                            sum\tts
                            1.0\t2024-01-01T00:00:00.000000Z
                            1.0\t2024-01-01T01:00:00.000000Z
                            2.0\t2024-01-01T02:00:00.000000Z
                            2.0\t2024-01-01T03:00:00.000000Z
                            3.0\t2024-01-01T04:00:00.000000Z
                            """,
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillPrevNumericWithTimezone() throws Exception {
        assertMemoryLeak(() -> {
            // Numeric PREV with timezone stays on fast path.
            execute("CREATE TABLE x AS (" +
                    "SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
                    "FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
            assertSql(
                    """
                            sum\tts
                            1.0\t2024-01-01T00:00:00.000000Z
                            1.0\t2024-01-01T01:00:00.000000Z
                            2.0\t2024-01-01T02:00:00.000000Z
                            2.0\t2024-01-01T03:00:00.000000Z
                            3.0\t2024-01-01T04:00:00.000000Z
                            """,
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'"
            );
            // Verify fast path plan (Sample By Fill, not Sample By)
            assertPlanNoLeakCheck(
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                    """
                            Sample By Fill
                              stride: '1h'
                              fill: prev
                                Sort
                                  keys: [ts]
                                    Async Group By workers: 1
                                      keys: [ts]
                                      keyFunctions: [timestamp_floor_utc('1h',ts,null,'00:00','Europe/Berlin')]
                                      values: [sum(val)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """
            );
        });
    }

    @Test
    public void testFillPrevOfIntKeyColumn() throws Exception {
        assertMemoryLeak(() -> {
            // Regression test: FILL(PREV(k)) where k is an INT key column.
            // Before the fix, the fill cursor returned NULL for the mirror
            // column because the key column had no per-bucket prev slot.
            execute("CREATE TABLE x (k INT, val DOUBLE, mirror INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1, 10.0, 100, '2024-01-01T00:00:00.000000Z')," +
                    "(2, 20.0, 200, '2024-01-01T00:00:00.000000Z')," +
                    "(1, 30.0, 300, '2024-01-01T02:00:00.000000Z')");
            // At 01:00 gap: both keys emit fill rows with m=k (not null).
            // At 02:00 gap for key=2: m=2 (mirrors the key value).
            assertSql(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t1\t10.0\t100
                            2024-01-01T00:00:00.000000Z\t2\t20.0\t200
                            2024-01-01T01:00:00.000000Z\t1\t10.0\t1
                            2024-01-01T01:00:00.000000Z\t2\t20.0\t2
                            2024-01-01T02:00:00.000000Z\t1\t30.0\t300
                            2024-01-01T02:00:00.000000Z\t2\t20.0\t2
                            """,
                    "SELECT ts, k, sum(val) AS s, last(mirror) AS m " +
                            "FROM x SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillPrevOfSymbolKeyColumn() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV(k)) where k is a SYMBOL key column.
            // Covers the getSymA key-mirror path.
            execute("CREATE TABLE x (k SYMBOL, val DOUBLE, mirror SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', 10.0, 'foo', '2024-01-01T00:00:00.000000Z')," +
                    "('B', 20.0, 'bar', '2024-01-01T00:00:00.000000Z')," +
                    "('A', 30.0, 'baz', '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\tA\t10.0\tfoo
                            2024-01-01T00:00:00.000000Z\tB\t20.0\tbar
                            2024-01-01T01:00:00.000000Z\tA\t10.0\tA
                            2024-01-01T01:00:00.000000Z\tB\t20.0\tB
                            2024-01-01T02:00:00.000000Z\tA\t30.0\tbaz
                            2024-01-01T02:00:00.000000Z\tB\t20.0\tB
                            """,
                    "SELECT ts, k, sum(val) AS s, last(mirror) AS m " +
                            "FROM x SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillPrevOfVarcharKeyColumn() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV(k)) where k is a VARCHAR key column — no symbol table
            // indirection, exercises the getVarcharA key-mirror path.
            execute("CREATE TABLE x (k VARCHAR, val DOUBLE, mirror VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', 10.0, 'foo', '2024-01-01T00:00:00.000000Z')," +
                    "('B', 20.0, 'bar', '2024-01-01T00:00:00.000000Z')," +
                    "('A', 30.0, 'baz', '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\tA\t10.0\tfoo
                            2024-01-01T00:00:00.000000Z\tB\t20.0\tbar
                            2024-01-01T01:00:00.000000Z\tA\t10.0\tA
                            2024-01-01T01:00:00.000000Z\tB\t20.0\tB
                            2024-01-01T02:00:00.000000Z\tA\t30.0\tbaz
                            2024-01-01T02:00:00.000000Z\tB\t20.0\tB
                            """,
                    "SELECT ts, k, sum(val) AS s, last(mirror) AS m " +
                            "FROM x SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillPrevSymbolLegacyFallback() throws Exception {
        assertMemoryLeak(() -> {
            // PREV on first(s) where s is STRING — unsupported type triggers
            // legacy fallback. The query plan shows Sample By (not Async Group By).
            execute("CREATE TABLE x AS (" +
                    "SELECT rnd_str('hello','world') s, x::DOUBLE val, " +
                    "timestamp_sequence(0, 3_600_000_000) ts " +
                    "FROM long_sequence(3)) TIMESTAMP(ts)");
            assertPlanNoLeakCheck(
                    "SELECT ts, first(s), sum(val) FROM x SAMPLE BY 1h FILL(PREV, NULL) ALIGN TO CALENDAR",
                    """
                            Sample By
                              fill: value
                              values: [first(s),sum(val)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """
            );
        });
    }

    @Test
    public void testFillPrevWithCalendarOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO test VALUES " +
                    "('2024-01-01T12:00:00.000000Z', 1.0), " +
                    "('2024-01-03T12:00:00.000000Z', 3.0)");
            drainWalQueue();

            // PREV fill with offset: gap bucket carries forward 1.0
            assertQueryNoLeakCheck(
                    """
                            ts\tavg
                            2024-01-01T10:00:00.000000Z\t1.0
                            2024-01-02T10:00:00.000000Z\t1.0
                            2024-01-03T10:00:00.000000Z\t3.0
                            """,
                    "SELECT ts, avg(value) FROM test SAMPLE BY 1d FILL(PREV) ALIGN TO CALENDAR WITH OFFSET '10:00'",
                    "ts"
            );
        });
    }

    @Test
    public void testFillToNullTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // A bind variable bound to null timestamp evaluates to LONG_NULL at
            // runtime. The optimizer rewrites FROM/TO into an interval filter,
            // which folds to empty rows when the upper bound is LONG_NULL, so the
            // fill cursor sees zero base rows. Critically, the fill cursor still
            // calls initialize() with toFunc != TimestampConstantNull (no
            // object-identity match) and toFunc.getTimestamp() == LONG_NULL.
            // Without the hasExplicitTo LONG_NULL guard (plan 12-02 task 1),
            // maxTimestamp would be promoted to Long.MAX_VALUE while
            // hasExplicitTo stays true; the zero-base-row path at line 660-668
            // would NOT take the short-circuit (maxTimestamp == LONG_NULL check
            // at 661 would be false), leaving the cursor in an inconsistent state
            // and risking Long.MAX_VALUE-bounded emission in worst cases. With
            // the guard, maxTimestamp stays LONG_NULL, the short-circuit fires,
            // and emission terminates cleanly with zero rows. The test asserts
            // termination (no hang) and bounded empty result.
            execute("CREATE TABLE t (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 1.0)," +
                    "('2024-01-01T02:00:00.000000Z', 2.0)");
            bindVariableService.clear();
            bindVariableService.setTimestamp("upperBound", Numbers.LONG_NULL);
            assertQueryNoLeakCheck(
                    "ts\tavg\n",
                    "SELECT ts, avg(v) FROM t SAMPLE BY 1h FROM '2024-01-01' TO :upperBound FILL(NULL)",
                    "ts", false, false);
        });
    }

    @Test
    public void testFillValueKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 21.0, '2024-01-01T01:00:00.000000Z')," +
                    "('London', 11.0, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\t20.0
                            2024-01-01T01:00:00.000000Z\tParis\t21.0
                            2024-01-01T01:00:00.000000Z\tLondon\t0.0
                            2024-01-01T02:00:00.000000Z\tLondon\t11.0
                            2024-01-01T02:00:00.000000Z\tParis\t0.0
                            """,
                    "SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(0) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillValueKeyedFromTo() throws Exception {
        assertMemoryLeak(() -> {
            // Same data as testFillNullKeyedFromTo but with FILL(0).
            // 6 buckets (00:00..05:00) x 2 keys = 12 rows.
            // Fill values are 0.0 instead of null.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T02:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T02:00:00.000000Z')," +
                    "('Paris', 21.0, '2024-01-01T03:00:00.000000Z')," +
                    "('London', 11.0, '2024-01-01T04:00:00.000000Z')");
            assertSql(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t0.0
                            2024-01-01T00:00:00.000000Z\tParis\t0.0
                            2024-01-01T01:00:00.000000Z\tLondon\t0.0
                            2024-01-01T01:00:00.000000Z\tParis\t0.0
                            2024-01-01T02:00:00.000000Z\tLondon\t10.0
                            2024-01-01T02:00:00.000000Z\tParis\t20.0
                            2024-01-01T03:00:00.000000Z\tParis\t21.0
                            2024-01-01T03:00:00.000000Z\tLondon\t0.0
                            2024-01-01T04:00:00.000000Z\tLondon\t11.0
                            2024-01-01T04:00:00.000000Z\tParis\t0.0
                            2024-01-01T05:00:00.000000Z\tLondon\t0.0
                            2024-01-01T05:00:00.000000Z\tParis\t0.0
                            """,
                    "SELECT ts, city, avg(temp) FROM weather " +
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T06:00:00.000000Z' FILL(0) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillValueNonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
                    "FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
            assertSql(
                    "sum\tts\n" +
                            "1.0\t2024-01-01T00:00:00.000000Z\n" +
                            "0.0\t2024-01-01T01:00:00.000000Z\n" +
                            "2.0\t2024-01-01T02:00:00.000000Z\n" +
                            "0.0\t2024-01-01T03:00:00.000000Z\n" +
                            "3.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(0) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillValueNonKeyedFromTo() throws Exception {
        assertMemoryLeak(() -> {
            // Data at 02:00, 03:00. FROM 00:00, TO 05:00.
            // Fill rows get 0.0 instead of null.
            execute("CREATE TABLE x AS (" +
                    "SELECT x::DOUBLE AS val, " +
                    "timestamp_sequence('2024-01-01T02:00:00.000000Z', 3_600_000_000) AS ts " +
                    "FROM long_sequence(2)) TIMESTAMP(ts) PARTITION BY DAY");
            assertSql(
                    "sum\tts\n" +
                            "0.0\t2024-01-01T00:00:00.000000Z\n" +
                            "0.0\t2024-01-01T01:00:00.000000Z\n" +
                            "1.0\t2024-01-01T02:00:00.000000Z\n" +
                            "2.0\t2024-01-01T03:00:00.000000Z\n" +
                            "0.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT sum(val), ts FROM x " +
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T05:00:00.000000Z' FILL(0) ALIGN TO CALENDAR"
            );
        });
    }
}
