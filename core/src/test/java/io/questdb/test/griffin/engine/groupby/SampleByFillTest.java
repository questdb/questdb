/*+******************************************************************************
 *  Copyright (c) 2019-2026 QuestDB
 *  Licensed under the Apache License, Version 2.0
 ******************************************************************************/

package io.questdb.test.griffin.engine.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SampleByFillTest extends AbstractCairoTest {

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
            assertQueryNoLeakCheck(
                    "s\tk\n" +
                    "6\t2021-10-31T00:00:00.000000Z\n" +
                    "6\t2021-10-31T01:00:00.000000Z\n" +
                    "6\t2021-10-31T02:00:00.000000Z\n" +
                    "6\t2021-10-31T03:00:00.000000Z\n" +
                    "6\t2021-10-31T04:00:00.000000Z\n" +
                    "6\t2021-10-31T05:00:00.000000Z\n" +
                    "6\t2021-10-31T06:00:00.000000Z\n" +
                    "6\t2021-10-31T07:00:00.000000Z\n" +
                    "6\t2021-10-31T08:00:00.000000Z\n" +
                    "6\t2021-10-31T09:00:00.000000Z\n" +
                    "6\t2021-10-31T10:00:00.000000Z\n" +
                    "6\t2021-10-31T11:00:00.000000Z\n" +
                    "6\t2021-10-31T12:00:00.000000Z\n" +
                    "6\t2021-10-31T13:00:00.000000Z\n" +
                    "6\t2021-10-31T14:00:00.000000Z\n" +
                    "6\t2021-10-31T15:00:00.000000Z\n" +
                    "4\t2021-10-31T16:00:00.000000Z\n",
                    "SELECT count() s, k FROM y SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Riga'",
                    null, false, false
            );
        });
    }

    @Test
    public void testFillNullEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertQueryNoLeakCheck(
                    "sum\tts\n" +
                    "null\t2024-01-01T00:00:00.000000Z\n" +
                    "null\t2024-01-01T01:00:00.000000Z\n" +
                    "null\t2024-01-01T02:00:00.000000Z\n",
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T03:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR",
                    null, false, false
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
    public void testFillNullMultipleAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT x::DOUBLE AS val, " +
                    "x::INT AS ival, " +
                    "timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
                    "FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
            assertQueryNoLeakCheck(
                    "sum\tavg\tsum1\tts\n" +
                    "1.0\t1.0\t1\t2024-01-01T00:00:00.000000Z\n" +
                    "null\tnull\tnull\t2024-01-01T01:00:00.000000Z\n" +
                    "2.0\t2.0\t2\t2024-01-01T02:00:00.000000Z\n" +
                    "null\tnull\tnull\t2024-01-01T03:00:00.000000Z\n" +
                    "3.0\t3.0\t3\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT sum(val), avg(val), sum(ival), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    null, false, false
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
            assertQueryNoLeakCheck(
                    "sum\tts\n" +
                    "1.0\t2024-01-01T00:00:00.000000Z\n" +
                    "null\t2024-01-01T01:00:00.000000Z\n" +
                    "2.0\t2024-01-01T02:00:00.000000Z\n" +
                    "null\t2024-01-01T03:00:00.000000Z\n" +
                    "3.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    null, false, false
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
            assertQueryNoLeakCheck(
                    "sum\tts\n" +
                    "null\t2024-01-01T00:00:00.000000Z\n" +
                    "null\t2024-01-01T01:00:00.000000Z\n" +
                    "1.0\t2024-01-01T02:00:00.000000Z\n" +
                    "2.0\t2024-01-01T03:00:00.000000Z\n" +
                    "null\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT sum(val), ts FROM x " +
                    "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T05:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR",
                    null, false, false
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
            assertQueryNoLeakCheck(
                    "sum\tts\n" +
                    "1.0\t2024-01-01T00:00:00.000000Z\n" +
                    "null\t2024-01-01T01:00:00.000000Z\n" +
                    "2.0\t2024-01-01T02:00:00.000000Z\n" +
                    "null\t2024-01-01T03:00:00.000000Z\n" +
                    "3.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    null, false, false
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
    public void testFillValueNonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
                    "FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
            assertQueryNoLeakCheck(
                    "sum\tts\n" +
                    "1.0\t2024-01-01T00:00:00.000000Z\n" +
                    "0.0\t2024-01-01T01:00:00.000000Z\n" +
                    "2.0\t2024-01-01T02:00:00.000000Z\n" +
                    "0.0\t2024-01-01T03:00:00.000000Z\n" +
                    "3.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(0) ALIGN TO CALENDAR",
                    null, false, false
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
            assertQueryNoLeakCheck(
                    "sum\tts\n" +
                    "0.0\t2024-01-01T00:00:00.000000Z\n" +
                    "0.0\t2024-01-01T01:00:00.000000Z\n" +
                    "1.0\t2024-01-01T02:00:00.000000Z\n" +
                    "2.0\t2024-01-01T03:00:00.000000Z\n" +
                    "0.0\t2024-01-01T04:00:00.000000Z\n",
                    "SELECT sum(val), ts FROM x " +
                    "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T05:00:00.000000Z' FILL(0) ALIGN TO CALENDAR",
                    null, false, false
            );
        });
    }
}
