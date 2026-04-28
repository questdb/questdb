/*+******************************************************************************
 *  Copyright (c) 2019-2026 QuestDB
 *  Licensed under the Apache License, Version 2.0
 ******************************************************************************/

package io.questdb.test.griffin.engine.groupby;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.fail;

/**
 * Keyed FILL tests in this class assert exact row order within each bucket
 * (e.g. London before Paris at 00:00, Paris before London at 01:00). That
 * order comes from AsyncGroupByRecordCursorFactory's hash-merge interleave
 * and is NOT a first-class SAMPLE BY contract. See PR #6946 trade-offs.
 * <p>
 * If AsyncGroupBy's hash sizing, merge strategy, or sharedQueryWorkerCount
 * changes and a keyed FILL test starts failing on row ordering alone, do
 * not re-lock the new order -- wrap the query in {@code ORDER BY ts, <key>}
 * so the assertion depends on a stable contract instead. Tests that already
 * do this are tagged // ORDER BY-safe.
 */
public class SampleByFillTest extends AbstractCairoTest {

    @Test
    public void testExplainOuterOrderByAggregate() throws Exception {
        // Outer ORDER BY on an aggregate column must survive the SAMPLE BY rewrite:
        // the fill cursor only guarantees ts order, so ordering by a non-ts column
        // must emit a sort node (radix-based "Encode sort" for DOUBLE) on top of
        // Sample By Fill.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES ('London', 10.0, '2024-01-01T00:00:00.000000Z')");
            assertPlanNoLeakCheck(
                    "SELECT ts, city, avg(temp) AS avg FROM weather " +
                            "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR ORDER BY avg",
                    """
                            Encode sort
                              keys: [avg]
                                Sample By Fill
                                  stride: '1h'
                                  fill: null
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [ts,city]
                                          keyFunctions: [timestamp_floor_utc('1h',ts)]
                                          values: [avg(temp)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: weather
                            """
            );
        });
    }

    @Test
    public void testExplainOuterOrderByKey() throws Exception {
        // Outer ORDER BY on a key column must survive the SAMPLE BY rewrite --
        // the fill cursor only guarantees ts order. Regression guard: an earlier
        // optimiser pass could drop the outer ORDER BY if it mistook ts ordering
        // for full ordering.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES ('London', 10.0, '2024-01-01T00:00:00.000000Z')");
            assertPlanNoLeakCheck(
                    "SELECT ts, city, avg(temp) FROM weather " +
                            "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR ORDER BY city",
                    """
                            Sort
                              keys: [city]
                                Sample By Fill
                                  stride: '1h'
                                  fill: null
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [ts,city]
                                          keyFunctions: [timestamp_floor_utc('1h',ts)]
                                          values: [avg(temp)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: weather
                            """
            );
        });
    }

    @Test
    public void testExplainOuterOrderByTs() throws Exception {
        // Outer ORDER BY ts is redundant with the fill cursor's intrinsic ts
        // ordering and the optimiser correctly elides the outer Sort. The plan
        // has Sample By Fill at the top with no wrapping sort.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES ('London', 10.0, '2024-01-01T00:00:00.000000Z')");
            assertPlanNoLeakCheck(
                    "SELECT ts, city, avg(temp) FROM weather " +
                            "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR ORDER BY ts",
                    """
                            Sample By Fill
                              stride: '1h'
                              fill: null
                                Encode sort light
                                  keys: [ts]
                                    Async Group By workers: 1
                                      keys: [ts,city]
                                      keyFunctions: [timestamp_floor_utc('1h',ts)]
                                      values: [avg(temp)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: weather
                            """
            );
        });
    }

    @Test
    public void testExplainOuterOrderByTsCity() throws Exception {
        // Outer ORDER BY ts, city must emit a Sort on top of Sample By Fill
        // because only the leading ts prefix is already satisfied; the trailing
        // city key is not. Regression guard against the optimiser eliding the
        // whole sort based only on the leading key match.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES ('London', 10.0, '2024-01-01T00:00:00.000000Z')");
            assertPlanNoLeakCheck(
                    "SELECT ts, city, avg(temp) FROM weather " +
                            "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR ORDER BY ts, city",
                    """
                            Sort
                              keys: [ts, city]
                                Sample By Fill
                                  stride: '1h'
                                  fill: null
                                    Encode sort light
                                      keys: [ts]
                                        Async Group By workers: 1
                                          keys: [ts,city]
                                          keyFunctions: [timestamp_floor_utc('1h',ts)]
                                          values: [avg(temp)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: weather
                            """
            );
        });
    }

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
                              fill: null
                                Encode sort light
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
            assertQueryNoLeakCheck(
                    """
                            first\tts
                            1\t2024-01-01T00:00:00.000000Z
                            42\t2024-01-01T01:00:00.000000Z
                            3\t2024-01-01T02:00:00.000000Z
                            """,
                    "SELECT first(val), ts FROM x SAMPLE BY 1h FILL(cast('42' as DECIMAL(19,0))) ALIGN TO CALENDAR",
                    "ts", false, false
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
            assertQueryNoLeakCheck(
                    """
                            first\tts
                            1\t2024-01-01T00:00:00.000000Z
                            42\t2024-01-01T01:00:00.000000Z
                            3\t2024-01-01T02:00:00.000000Z
                            """,
                    "SELECT first(val), ts FROM x SAMPLE BY 1h FILL(cast('42' as DECIMAL(39,0))) ALIGN TO CALENDAR",
                    "ts", false, false
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
            assertQueryNoLeakCheck(
                    """
                            first\tts
                            1\t2024-01-01T00:00:00.000000Z
                            66\t2024-01-01T01:00:00.000000Z
                            3\t2024-01-01T02:00:00.000000Z
                            """,
                    "SELECT first(val), ts FROM x SAMPLE BY 1h FILL(cast('0x42' as LONG256)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillFromNegativeOffsetAtFromBoundary() throws Exception {
        // Regression for PR #6946 C1. With FROM + negative OFFSET, the
        // floor grid is anchored at effectiveOffset = FROM + OFFSET, which
        // lies strictly below FROM. Data at/near FROM floors to a bucket
        // below FROM, making firstTs < fromTs on the first hasNext() call.
        // Before the fix, initialize() anchored the sampler at
        // firstTs + calendarOffset (double-shift) and the next hasNext()
        // tripped the "data row timestamp precedes next bucket" guard.
        //
        // With stride=1h, effectiveOffset=04:30, data rows at 05:00 (floors
        // to 04:30) and 05:30 (floors to 05:30) map to two distinct buckets.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, '2024-01-01T05:00:00.000000Z')," +
                    "(2.0, '2024-01-01T05:30:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tsum
                            2024-01-01T04:30:00.000000Z\t1.0
                            2024-01-01T05:30:00.000000Z\t2.0
                            """,
                    "SELECT ts, sum(val) FROM t " +
                            "SAMPLE BY 1h FROM '2024-01-01T05:00:00.000000Z' TO '2024-01-01T06:30:00.000000Z' " +
                            "FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '-00:30'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillFromNegativeOffsetKeyed() throws Exception {
        // Keyed variant of testFillFromNegativeOffsetAtFromBoundary -- exercises
        // the keyed pass-2 emission path with the corrected grid anchor.
        // Two keys ('A', 'B'); at 05:30 A has no data so FILL(PREV) carries its
        // 04:30 value (1.0) forward. Wrapped in ORDER BY ts, k for stable
        // within-bucket ordering independent of AsyncGroupBy's hash-merge
        // interleave (see class javadoc).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('A', 1.0, '2024-01-01T05:00:00.000000Z')," +
                    "('B', 2.0, '2024-01-01T05:00:00.000000Z')," +
                    "('B', 3.0, '2024-01-01T05:30:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tsum
                            2024-01-01T04:30:00.000000Z\tA\t1.0
                            2024-01-01T04:30:00.000000Z\tB\t2.0
                            2024-01-01T05:30:00.000000Z\tA\t1.0
                            2024-01-01T05:30:00.000000Z\tB\t3.0
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) FROM t " +
                            "SAMPLE BY 1h FROM '2024-01-01T05:00:00.000000Z' TO '2024-01-01T06:30:00.000000Z' " +
                            "FILL(PREV) ALIGN TO CALENDAR WITH OFFSET '-00:30'" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testFillFromNegativeOffsetNoTo() throws Exception {
        // FROM + negative OFFSET without TO -- covers the hasExplicitTo=false
        // termination path with the corrected grid anchor. Data at 05:00
        // floors to 04:30; data at 05:30 floors to 05:30. Without TO, no
        // trailing fills beyond the last data row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, '2024-01-01T05:00:00.000000Z')," +
                    "(2.0, '2024-01-01T05:30:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tsum
                            2024-01-01T04:30:00.000000Z\t1.0
                            2024-01-01T05:30:00.000000Z\t2.0
                            """,
                    "SELECT ts, sum(val) FROM t " +
                            "SAMPLE BY 1h FROM '2024-01-01T05:00:00.000000Z' " +
                            "FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '-00:30'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillFromPositiveOffsetAtFromBoundary() throws Exception {
        // Positive OFFSET sibling of testFillFromNegativeOffsetAtFromBoundary.
        // effectiveOffset = FROM + OFFSET = 05:30 sits ABOVE FROM; data below
        // effectiveOffset clamps up via the `if (micros < offset) return offset`
        // branch in Micros.floor*. So firstTs = 05:30 >= fromTs = 05:00,
        // the C1-fixed branch is not taken, and this path hits the standard
        // else branch. Test pins the positive-offset behavior alongside the
        // negative-offset regression.
        //
        // Data at 05:00 clamps up to 05:30. Data at 05:30 floors to 05:30.
        // Data at 06:00 floors to 05:30 (within [05:30, 06:30)).
        // All three rows aggregate into the 05:30 bucket.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, '2024-01-01T05:00:00.000000Z')," +
                    "(2.0, '2024-01-01T05:30:00.000000Z')," +
                    "(3.0, '2024-01-01T06:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tsum
                            2024-01-01T05:30:00.000000Z\t6.0
                            """,
                    "SELECT ts, sum(val) FROM t " +
                            "SAMPLE BY 1h FROM '2024-01-01T05:00:00.000000Z' TO '2024-01-01T06:30:00.000000Z' " +
                            "FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '+00:30'",
                    "ts", false, false
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
            assertQueryNoLeakCheck(
                    """
                            ts\tip\tsum
                            2024-01-01T00:00:00.000000Z\t10.0.0.1\t100
                            2024-01-01T00:00:00.000000Z\t10.0.0.2\t200
                            2024-01-01T01:00:00.000000Z\t10.0.0.2\t210
                            2024-01-01T01:00:00.000000Z\t10.0.0.1\tnull
                            2024-01-01T02:00:00.000000Z\t10.0.0.1\t110
                            2024-01-01T02:00:00.000000Z\t10.0.0.2\tnull
                            """,
                    "SELECT ts, ip, sum(bytes) FROM traffic SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillInsufficientFillValues() throws Exception {
        assertMemoryLeak(() -> {
            // With 7 aggregates and only 5 fill specs, the query must be rejected
            // rather than silently padding the missing slots with null. The
            // single-element broadcast form (bare FILL(PREV) or FILL(NULL)) is
            // exempt and covered elsewhere. Error position points at the first
            // fill expression.
            execute("CREATE TABLE t (ts TIMESTAMP, a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, e DOUBLE, f DOUBLE, g DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1, 2, 3, 4, 5, 6, 7)");
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(a), sum(b), sum(c), sum(d), sum(e), sum(f), sum(g) FROM t SAMPLE BY 1h FILL(PREV, PREV, PREV, PREV, 0) ALIGN TO CALENDAR",
                    91,
                    "not enough fill values"
            );
        });
    }

    @Test
    public void testFillInsufficientFillValuesSingleConstant() throws Exception {
        assertMemoryLeak(() -> {
            // A single non-null constant (FILL(0)) must not broadcast across
            // multiple aggregates. Only bare FILL(PREV) and FILL(NULL) broadcast;
            // a lone constant with multi-aggregate is under-specified and must
            // raise "not enough fill values".
            execute("CREATE TABLE t (ts TIMESTAMP, a DOUBLE, b DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1, 2)");
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(a), sum(b) FROM t SAMPLE BY 1h FILL(0) ALIGN TO CALENDAR",
                    51,
                    "not enough fill values"
            );
        });
    }

    @Test
    public void testFillKeyedDecimal128() throws Exception {
        assertMemoryLeak(() -> {
            // Keyed SAMPLE BY with a DECIMAL128 key column (DECIMAL(25,2) encodes
            // as DECIMAL128 at the physical level). Covers FillRecord.getDecimal128
            // FILL_KEY dispatch. At 01:00 each key is missing and must emit a fill
            // row carrying the DECIMAL128 key value, not null.
            execute("CREATE TABLE t (k DECIMAL(25, 2), v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(cast('1.00' AS DECIMAL(25,2)), 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "(cast('2.00' AS DECIMAL(25,2)), 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "(cast('1.00' AS DECIMAL(25,2)), 11.0, '2024-01-01T02:00:00.000000Z')," +
                    "(cast('2.00' AS DECIMAL(25,2)), 21.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tsum
                            2024-01-01T00:00:00.000000Z\t1.00\t10.0
                            2024-01-01T00:00:00.000000Z\t2.00\t20.0
                            2024-01-01T01:00:00.000000Z\t1.00\tnull
                            2024-01-01T01:00:00.000000Z\t2.00\tnull
                            2024-01-01T02:00:00.000000Z\t1.00\t11.0
                            2024-01-01T02:00:00.000000Z\t2.00\t21.0
                            """,
                    "SELECT ts, k, sum(v) FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillKeyedDecimal256() throws Exception {
        assertMemoryLeak(() -> {
            // Keyed SAMPLE BY with a DECIMAL256 key column (DECIMAL(39,2) encodes
            // as DECIMAL256 at the physical level). Covers FillRecord.getDecimal256
            // FILL_KEY dispatch.
            execute("CREATE TABLE t (k DECIMAL(39, 2), v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(cast('1.00' AS DECIMAL(39,2)), 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "(cast('2.00' AS DECIMAL(39,2)), 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "(cast('1.00' AS DECIMAL(39,2)), 11.0, '2024-01-01T02:00:00.000000Z')," +
                    "(cast('2.00' AS DECIMAL(39,2)), 21.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tsum
                            2024-01-01T00:00:00.000000Z\t1.00\t10.0
                            2024-01-01T00:00:00.000000Z\t2.00\t20.0
                            2024-01-01T01:00:00.000000Z\t1.00\tnull
                            2024-01-01T01:00:00.000000Z\t2.00\tnull
                            2024-01-01T02:00:00.000000Z\t1.00\t11.0
                            2024-01-01T02:00:00.000000Z\t2.00\t21.0
                            """,
                    "SELECT ts, k, sum(v) FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillKeyedDecimalNarrow() throws Exception {
        assertMemoryLeak(() -> {
            // Keyed SAMPLE BY across the four narrow decimal widths. DECIMAL(2,0),
            // (4,0), (9,0), and (18,0) encode as DECIMAL8, DECIMAL16, DECIMAL32,
            // and DECIMAL64 respectively, so each block exercises a distinct
            // FillRecord.getDecimal{8,16,32,64} FILL_KEY dispatch. The fill row at
            // 01:00 for each key must carry the decimal key value, not its null
            // sentinel.
            String expected = """
                    ts\tk\tsum
                    2024-01-01T00:00:00.000000Z\t1\t10.0
                    2024-01-01T00:00:00.000000Z\t2\t20.0
                    2024-01-01T01:00:00.000000Z\t1\tnull
                    2024-01-01T01:00:00.000000Z\t2\tnull
                    2024-01-01T02:00:00.000000Z\t1\t11.0
                    2024-01-01T02:00:00.000000Z\t2\t21.0
                    """;
            int[] precisions = {2, 4, 9, 18};
            for (int precision : precisions) {
                execute("DROP TABLE IF EXISTS t");
                execute("CREATE TABLE t (k DECIMAL(" + precision + ", 0), v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
                execute("INSERT INTO t VALUES " +
                        "(1::DECIMAL(" + precision + ",0), 10.0, '2024-01-01T00:00:00.000000Z')," +
                        "(2::DECIMAL(" + precision + ",0), 20.0, '2024-01-01T00:00:00.000000Z')," +
                        "(1::DECIMAL(" + precision + ",0), 11.0, '2024-01-01T02:00:00.000000Z')," +
                        "(2::DECIMAL(" + precision + ",0), 21.0, '2024-01-01T02:00:00.000000Z')");
                assertQueryNoLeakCheck(
                        expected,
                        "SELECT ts, k, sum(v) FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                        "ts", false, false
                );
            }
        });
    }

    @Test
    public void testFillKeyedLong256() throws Exception {
        assertMemoryLeak(() -> {
            // Keyed SAMPLE BY with a LONG256 key column. Covers FillRecord.getLong256A,
            // getLong256B, and getLong256(col, sink) FILL_KEY dispatch. The fill row at
            // 01:00 for each key emits the LONG256 key value (not a zero sentinel).
            execute("CREATE TABLE t (k LONG256, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(cast('0x01' AS LONG256), 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "(cast('0x02' AS LONG256), 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "(cast('0x01' AS LONG256), 11.0, '2024-01-01T02:00:00.000000Z')," +
                    "(cast('0x02' AS LONG256), 21.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tsum
                            2024-01-01T00:00:00.000000Z\t0x01\t10.0
                            2024-01-01T00:00:00.000000Z\t0x02\t20.0
                            2024-01-01T01:00:00.000000Z\t0x01\tnull
                            2024-01-01T01:00:00.000000Z\t0x02\tnull
                            2024-01-01T02:00:00.000000Z\t0x01\t11.0
                            2024-01-01T02:00:00.000000Z\t0x02\t21.0
                            """,
                    "SELECT ts, k, sum(v) FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillKeyedPrimitiveTypes() throws Exception {
        assertMemoryLeak(() -> {
            // Multi-key SAMPLE BY using BOOLEAN, BYTE, CHAR, FLOAT, and SHORT key
            // columns simultaneously. Covers the FILL_KEY dispatch in
            // FillRecord.getBool, getByte, getChar, getFloat, getShort. The fill
            // row at 01:00 for each (b, by, c, f, s) tuple must carry the typed
            // key values, not the type's zero/false sentinel.
            execute("CREATE TABLE t (" +
                    "b BOOLEAN, by BYTE, c CHAR, f FLOAT, s SHORT, " +
                    "v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(true,  1::BYTE, 'a', 1.5::FLOAT, 100::SHORT, 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "(false, 2::BYTE, 'b', 2.5::FLOAT, 200::SHORT, 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "(true,  1::BYTE, 'a', 1.5::FLOAT, 100::SHORT, 11.0, '2024-01-01T02:00:00.000000Z')," +
                    "(false, 2::BYTE, 'b', 2.5::FLOAT, 200::SHORT, 21.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tb\tby\tc\tf\ts\tsum
                            2024-01-01T00:00:00.000000Z\ttrue\t1\ta\t1.5\t100\t10.0
                            2024-01-01T00:00:00.000000Z\tfalse\t2\tb\t2.5\t200\t20.0
                            2024-01-01T01:00:00.000000Z\ttrue\t1\ta\t1.5\t100\tnull
                            2024-01-01T01:00:00.000000Z\tfalse\t2\tb\t2.5\t200\tnull
                            2024-01-01T02:00:00.000000Z\ttrue\t1\ta\t1.5\t100\t11.0
                            2024-01-01T02:00:00.000000Z\tfalse\t2\tb\t2.5\t200\t21.0
                            """,
                    "SELECT ts, b, by, c, f, s, sum(v) " +
                            "FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillKeyedRespectsCircuitBreaker() throws Exception {
        // C-3 regression: a keyed SAMPLE BY 1s FROM '1970' TO '2100' FILL(NULL)
        // query iterates billions of buckets without a circuit-breaker check
        // inside the fill-emission outer loop. Phase 15 Plan 02 added two CB
        // check sites (hasNext head + emitNextFillRow outer-loop top) so the
        // query now honors cancellation within bounded wall-clock. The
        // tick-counting MillisecondClock returns 0 until tripWhenTicks ticks
        // have accumulated, then flips to Long.MAX_VALUE so the CB's 1ms query
        // timeout trips deterministically. Pre-fix, the cursor iterates until
        // OOM or the JUnit wall-clock timeout. Post-fix, CairoException fires
        // with the canonical "timeout, query aborted" message inside the fill
        // emission loop.
        final long tripWhenTicks = 100;
        assertMemoryLeak(() -> {
            try {
                circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                    private final AtomicLong ticks = new AtomicLong();

                    @Override
                    @NotNull
                    public MillisecondClock getClock() {
                        return () -> {
                            if (ticks.incrementAndGet() < tripWhenTicks) {
                                return 0;
                            }
                            return Long.MAX_VALUE;
                        };
                    }

                    @Override
                    public long getQueryTimeout() {
                        return 1;
                    }
                };

                final WorkerPool pool = new WorkerPool(() -> 4);
                TestUtils.execute(
                        pool,
                        (engine, compiler, sqlExecutionContext) -> {
                            final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
                            final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                                    engine,
                                    circuitBreakerConfiguration,
                                    MemoryTag.NATIVE_DEFAULT
                            );
                            try {
                                engine.execute(
                                        "CREATE TABLE t (" +
                                                "  ts TIMESTAMP," +
                                                "  k SYMBOL," +
                                                "  x DOUBLE" +
                                                ") TIMESTAMP(ts) PARTITION BY DAY",
                                        sqlExecutionContext
                                );
                                engine.execute(
                                        "INSERT INTO t VALUES" +
                                                " ('2024-01-01T00:00:00.000000Z', 'a', 1.0)," +
                                                " ('2024-01-01T00:00:00.000000Z', 'b', 2.0)",
                                        sqlExecutionContext
                                );
                                context.with(
                                        context.getSecurityContext(),
                                        context.getBindVariableService(),
                                        context.getRandom(),
                                        context.getRequestFd(),
                                        circuitBreaker
                                );
                                TestUtils.assertSql(
                                        compiler,
                                        context,
                                        "SELECT ts, k, first(x) FROM t " +
                                                "SAMPLE BY 1s FROM '1970-01-01' TO '2100-01-01' FILL(NULL)",
                                        sink,
                                        ""
                                );
                                Assert.fail("expected CB-tripped exception");
                            } catch (CairoException ex) {
                                TestUtils.assertContains(ex.getFlyweightMessage(), "timeout, query aborted");
                            } finally {
                                Misc.free(circuitBreaker);
                            }
                        },
                        configuration,
                        LOG
                );
            } finally {
                // D-29 finding 4.6: null the inherited AbstractCairoTest static
                // field so the tick-counting mock clock does not bleed into any
                // future test in this class that constructs a
                // NetworkSqlExecutionCircuitBreaker against circuitBreakerConfiguration.
                circuitBreakerConfiguration = null;
            }
        });
    }

    @Test
    public void testFillKeyedSingleRowFromTo() throws Exception {
        assertMemoryLeak(() -> {
            // m8b single-row keyed + FROM/TO: one data row for one key, with FROM
            // strictly before and TO strictly after the row's bucket. The fill
            // cursor emits leading NULL fill rows (hasKeyPrev()==false for every
            // bucket before the data row), then the real row, then trailing
            // forward-fill rows (hasKeyPrev()==true after the data row lands).
            // This exercises the hasKeyPrev() false->true transition exactly
            // once per key: it flips from false to true at the bucket holding
            // the real data and stays true for the rest of the emission.
            execute("CREATE TABLE t (key VARCHAR, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('A', 42.0, '2024-01-01T03:00:00.000000Z')");
            final String sql = "SELECT ts, key, first(val) val FROM t " +
                    "SAMPLE BY 1h FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-01T06:00:00.000000Z' " +
                    "FILL(PREV) ALIGN TO CALENDAR";
            // 3 leading null rows (no PREV available yet) + the real 42.0 row at
            // 03:00 + 2 trailing PREV rows carrying 42.0. Key column carries 'A'
            // on every row including the leading-null rows (FILL_KEY dispatch).
            assertQueryNoLeakCheck(
                    "ts\tkey\tval\n" +
                            "2024-01-01T00:00:00.000000Z\tA\tnull\n" +
                            "2024-01-01T01:00:00.000000Z\tA\tnull\n" +
                            "2024-01-01T02:00:00.000000Z\tA\tnull\n" +
                            "2024-01-01T03:00:00.000000Z\tA\t42.0\n" +
                            "2024-01-01T04:00:00.000000Z\tA\t42.0\n" +
                            "2024-01-01T05:00:00.000000Z\tA\t42.0\n",
                    sql,
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testFillKeyedUuid() throws Exception {
        assertMemoryLeak(() -> {
            // Keyed SAMPLE BY with a UUID key column. Covers FillRecord.getLong128Hi
            // and getLong128Lo FILL_KEY dispatch (UUID is physically 128 bits). The
            // fill row at 01:00 for each key emits the UUID key value (not a zero
            // sentinel).
            execute("CREATE TABLE t (k UUID, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('00000000-0000-0000-0000-000000000001', 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('00000000-0000-0000-0000-000000000002', 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "('00000000-0000-0000-0000-000000000001', 11.0, '2024-01-01T02:00:00.000000Z')," +
                    "('00000000-0000-0000-0000-000000000002', 21.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tsum
                            2024-01-01T00:00:00.000000Z\t00000000-0000-0000-0000-000000000001\t10.0
                            2024-01-01T00:00:00.000000Z\t00000000-0000-0000-0000-000000000002\t20.0
                            2024-01-01T01:00:00.000000Z\t00000000-0000-0000-0000-000000000001\tnull
                            2024-01-01T01:00:00.000000Z\t00000000-0000-0000-0000-000000000002\tnull
                            2024-01-01T02:00:00.000000Z\t00000000-0000-0000-0000-000000000001\t11.0
                            2024-01-01T02:00:00.000000Z\t00000000-0000-0000-0000-000000000002\t21.0
                            """,
                    "SELECT ts, k, sum(v) FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullCastMultiKey() throws Exception {
        // FILL(NULL) variant of the multi-key inline-function classifier regression (D-04 representative).
        // Captured via probe-and-freeze.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1, 10.0, '2024-01-01T00:00:00.000000Z'),
                        (2, 20.0, '2024-01-01T02:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tfirst
                            2024-01-01T00:00:00.000000Z\t1\t10.0
                            2024-01-01T00:00:00.000000Z\t2\tnull
                            2024-01-01T01:00:00.000000Z\t1\tnull
                            2024-01-01T01:00:00.000000Z\t2\tnull
                            2024-01-01T02:00:00.000000Z\t2\t20.0
                            2024-01-01T02:00:00.000000Z\t1\tnull
                            """,
                    "SELECT ts, x::STRING k, first(v) FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
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
                    "timestamp_sequence('2021-10-31T00:00:00.000000Z'::TIMESTAMP, 600_000_000) k " +
                    "FROM long_sequence(100)) TIMESTAMP(k) PARTITION BY NONE");
            assertQueryNoLeakCheck(
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
                    "SELECT count() s, k FROM y SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Riga'",
                    "k", false, false
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
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2021-10-31T00:00:00.000000Z
                            null\t2021-10-31T01:00:00.000000Z
                            null\t2021-10-31T02:00:00.000000Z
                            null\t2021-10-31T03:00:00.000000Z
                            5.0\t2021-10-31T04:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM z SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Riga'",
                    "ts", false, false
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
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullKeyed() throws Exception {
        assertMemoryLeak(() -> {
            // London has data at 00:00 and 02:00.
            // Paris has data at 00:00 and 01:00.
            // With 1h stride, expect cartesian product: 3 buckets x 2 keys = 6 rows.
            // Row order within a bucket is AsyncGroupBy hash-merge order -- see class
            // Javadoc. If this test starts failing on ordering, wrap in ORDER BY ts, city.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 21.0, '2024-01-01T01:00:00.000000Z')," +
                    "('London', 11.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\t20.0
                            2024-01-01T01:00:00.000000Z\tParis\t21.0
                            2024-01-01T01:00:00.000000Z\tLondon\tnull
                            2024-01-01T02:00:00.000000Z\tLondon\t11.0
                            2024-01-01T02:00:00.000000Z\tParis\tnull
                            """,
                    "SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
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
            // Row order within a bucket is AsyncGroupBy hash-merge order -- see class
            // Javadoc. If this test starts failing on ordering, wrap in ORDER BY ts, city.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T02:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T02:00:00.000000Z')," +
                    "('Paris', 21.0, '2024-01-01T03:00:00.000000Z')," +
                    "('London', 11.0, '2024-01-01T04:00:00.000000Z')");
            assertQueryNoLeakCheck(
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
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T06:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
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
            assertQueryNoLeakCheck(
                    """
                            ts\tcity\tavg
                            """,
                    "SELECT ts, city, avg(temp) FROM weather " +
                            "SAMPLE BY 1h FROM '2024-01-05' TO '2024-01-06' FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
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
            // Row order within a bucket is AsyncGroupBy hash-merge order -- see class
            // Javadoc. If this test starts failing on ordering, wrap in ORDER BY ts, city.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T03:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T03:00:00.000000Z')," +
                    "('London', 11.0, '2024-01-01T04:00:00.000000Z')");
            assertQueryNoLeakCheck(
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
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T04:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
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
            assertQueryNoLeakCheck(
                    """
                            ts\tcity\tavg
                            """,
                    "SELECT ts, city, avg(temp) FROM weather " +
                            "SAMPLE BY 1h FROM '2024-01-01T05:00:00.000000Z' TO '2024-01-01T05:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullKeyedFromToKeyAppearsMidRange() throws Exception {
        assertMemoryLeak(() -> {
            // Berlin appears only at 03:00 but is discovered in pass 1.
            // 5 buckets (00:00..04:00) x 3 keys = 15 rows.
            // Berlin gets null fill for buckets 00:00-02:00 and 04:00.
            // Row order within a bucket is AsyncGroupBy hash-merge order -- see class
            // Javadoc. If this test starts failing on ordering, wrap in ORDER BY ts, city.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T01:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T01:00:00.000000Z')," +
                    "('Berlin', 5.0, '2024-01-01T03:00:00.000000Z')," +
                    "('London', 12.0, '2024-01-01T03:00:00.000000Z')");
            assertQueryNoLeakCheck(
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
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T05:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullKeyedFromToMultipleAggregates() throws Exception {
        assertMemoryLeak(() -> {
            // Two aggregate columns (sum(val), sum(ival)) with keyed FROM/TO.
            // 4 buckets (00:00..03:00) x 2 keys = 8 rows.
            // Row order within a bucket is AsyncGroupBy hash-merge order -- see class
            // Javadoc. If this test starts failing on ordering, wrap in ORDER BY ts, key.
            execute("CREATE TABLE x (key STRING, val DOUBLE, ival INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', 1.0, 10, '2024-01-01T01:00:00.000000Z')," +
                    "('B', 2.0, 20, '2024-01-01T01:00:00.000000Z')," +
                    "('A', 3.0, 30, '2024-01-01T03:00:00.000000Z')");
            assertQueryNoLeakCheck(
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
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T04:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
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
            assertQueryNoLeakCheck(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\t20.0
                            2024-01-01T01:00:00.000000Z\tLondon\t11.0
                            2024-01-01T01:00:00.000000Z\tParis\tnull
                            """,
                    "SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullKeyedThreeKeys() throws Exception {
        assertMemoryLeak(() -> {
            // Three keys where Paris and Berlin appear only in first bucket.
            // Row order within a bucket is AsyncGroupBy hash-merge order -- see class
            // Javadoc. If this test starts failing on ordering, wrap in ORDER BY ts, city.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Berlin', 5.0, '2024-01-01T00:00:00.000000Z')," +
                    "('London', 11.0, '2024-01-01T01:00:00.000000Z')");
            // 2 buckets x 3 keys = 6 rows.
            // Paris and Berlin missing at 01:00.
            assertQueryNoLeakCheck(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\t20.0
                            2024-01-01T00:00:00.000000Z\tBerlin\t5.0
                            2024-01-01T01:00:00.000000Z\tLondon\t11.0
                            2024-01-01T01:00:00.000000Z\tParis\tnull
                            2024-01-01T01:00:00.000000Z\tBerlin\tnull
                            """,
                    "SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullKeyedWithCalendarOffset() throws Exception {
        // Row order within a bucket is AsyncGroupBy hash-merge order -- see class
        // Javadoc. If this test starts failing on ordering, wrap in ORDER BY ts, city.
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
    public void testFillKeyedWithParallelWorkers() throws Exception {
        // The class Javadoc notes that keyed FILL tests at sharedQueryWorkerCount=1
        // lock AsyncGroupBy's intra-bucket hash-merge order. This test runs the
        // same query shape under a 4-worker pool with ORDER BY ts, city so the
        // assertion depends on stable semantics rather than hash interleave --
        // it catches correctness regressions in worker>1 configurations that
        // the single-worker tests cannot see.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE weather (city SYMBOL, temp DOUBLE, ts TIMESTAMP) " +
                                        "TIMESTAMP(ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO weather VALUES " +
                                        "('London', 10.0, '2024-01-01T00:00:00.000000Z')," +
                                        "('Paris', 20.0, '2024-01-01T00:00:00.000000Z')," +
                                        "('Berlin', 5.0, '2024-01-01T00:00:00.000000Z')," +
                                        "('Paris', 21.0, '2024-01-01T01:00:00.000000Z')," +
                                        "('London', 11.0, '2024-01-01T02:00:00.000000Z')," +
                                        "('Berlin', 6.0, '2024-01-01T02:00:00.000000Z')",
                                sqlExecutionContext
                        );
                        TestUtils.assertSql(
                                compiler,
                                sqlExecutionContext,
                                "SELECT ts, city, avg(temp) FROM weather " +
                                        "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR " +
                                        "ORDER BY ts, city",
                                sink,
                                """
                                        ts\tcity\tavg
                                        2024-01-01T00:00:00.000000Z\tBerlin\t5.0
                                        2024-01-01T00:00:00.000000Z\tLondon\t10.0
                                        2024-01-01T00:00:00.000000Z\tParis\t20.0
                                        2024-01-01T01:00:00.000000Z\tBerlin\tnull
                                        2024-01-01T01:00:00.000000Z\tLondon\tnull
                                        2024-01-01T01:00:00.000000Z\tParis\t21.0
                                        2024-01-01T02:00:00.000000Z\tBerlin\t6.0
                                        2024-01-01T02:00:00.000000Z\tLondon\t11.0
                                        2024-01-01T02:00:00.000000Z\tParis\tnull
                                        """
                        );
                        TestUtils.assertSql(
                                compiler,
                                sqlExecutionContext,
                                "SELECT ts, city, last(temp) AS last FROM weather " +
                                        "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR " +
                                        "ORDER BY ts, city",
                                sink,
                                """
                                        ts\tcity\tlast
                                        2024-01-01T00:00:00.000000Z\tBerlin\t5.0
                                        2024-01-01T00:00:00.000000Z\tLondon\t10.0
                                        2024-01-01T00:00:00.000000Z\tParis\t20.0
                                        2024-01-01T01:00:00.000000Z\tBerlin\t5.0
                                        2024-01-01T01:00:00.000000Z\tLondon\t10.0
                                        2024-01-01T01:00:00.000000Z\tParis\t21.0
                                        2024-01-01T02:00:00.000000Z\tBerlin\t6.0
                                        2024-01-01T02:00:00.000000Z\tLondon\t11.0
                                        2024-01-01T02:00:00.000000Z\tParis\t21.0
                                        """
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testFillNullDuplicateAlias() throws Exception {
        // Guard against factoryColToUserFillIdx mis-mapping when two SELECT columns
        // share an alias: groupByMetadata.getColumnIndexQuiet() returns the first
        // match, so a non-rejection here would silently route one aggregate onto
        // the wrong fill slot. The SqlOptimiser rejects duplicate aliases up front
        // (SqlOptimiser#Duplicate column), and this test locks that rejection in
        // place for FILL-bearing SAMPLE BY queries.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1.0)");
            // Position 25 points at the AS token of the second alias -- the
            // optimiser raises the duplicate before the alias literal is
            // positioned, so "AS k" is reported rather than the k token itself.
            assertExceptionNoLeakCheck(
                    "SELECT ts AS k, count(*) AS k FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    25,
                    "Duplicate column [name=k]"
            );
        });
    }

    @Test
    public void testFillNullKeyedWithNullKey() throws Exception {
        assertMemoryLeak(() -> {
            // NULL symbol key forms its own group in the cartesian product.
            // 2 buckets x 2 keys (null + London) = 4 rows.
            // Row order within a bucket is AsyncGroupBy hash-merge order -- see class
            // Javadoc. If this test starts failing on ordering, wrap in ORDER BY ts, city.
            execute("CREATE TABLE t (city SYMBOL, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(null, 10.0, '2024-01-01T01:00:00.000000Z')," +
                    "('London', 20.0, '2024-01-01T01:00:00.000000Z')," +
                    "(null, 30.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tcity\tavg
                            2024-01-01T01:00:00.000000Z\t\t10.0
                            2024-01-01T01:00:00.000000Z\tLondon\t20.0
                            2024-01-01T02:00:00.000000Z\t\t30.0
                            2024-01-01T02:00:00.000000Z\tLondon\tnull
                            """,
                    "SELECT ts, city, avg(temp) FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
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
                    "ts", false, false
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
                    "ts", false, false
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
                    "ts", false, false
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
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullPushdownEliminatesFilteredKeyFills() throws Exception {
        assertMemoryLeak(() -> {
            // Predicate pushdown past SAMPLE BY FILL: a WHERE clause on the key column
            // applied to a FILL(NULL) sub-query pushes into the inner Async Group By's
            // filter slot. The per-key-domain cartesian is computed only over keys
            // matching the filter, so keys eliminated by the predicate never emit
            // leading or trailing NULL-fill buckets outside their observed data range.
            //
            // This differs from master's cursor path, where FILL(NULL) materializes the
            // full bucket grid for every key present in the base data and the outer
            // filter only trims the already-emitted cartesian. See "Predicate pushdown
            // past SAMPLE BY" Trade-off in PR #6946.
            execute("CREATE TABLE t (s SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('s1', 1.0, '2024-01-01T00:00:00.000000Z'), " +
                    "('s1', 2.0, '2024-01-01T00:01:00.000000Z'), " +
                    "('s1', 3.0, '2024-01-01T00:02:00.000000Z'), " +
                    "('s2', 10.0, '2024-01-01T00:01:00.000000Z')");
            final String sql = "SELECT * FROM (" +
                    "SELECT ts, s, first(v) v FROM t " +
                    "SAMPLE BY 1m FILL(NULL) ALIGN TO CALENDAR) " +
                    "WHERE s = 's2'";

            // s2 only has real data in the 00:01 bucket. No leading NULL-fill at 00:00
            // and no trailing NULL-fill at 00:02 for s2 -- both would be present on the
            // cursor path because s1 populates those buckets in the base cartesian.
            assertQueryNoLeakCheck(
                    "ts\ts\tv\n" +
                            "2024-01-01T00:01:00.000000Z\ts2\t10.0\n",
                    sql,
                    "ts",
                    false,
                    false
            );

            // The key invariant: filter: s='s2' appears inside the inner Async Group By,
            // nested below the outer Sample By Fill. This is the mechanism -- the inner
            // cartesian only sees s2 rows, so no s1-driven buckets enter the fill grid.
            assertPlanNoLeakCheck(
                    sql,
                    """
                            Sample By Fill
                              stride: '1m'
                              fill: null
                                Encode sort light
                                  keys: [ts]
                                    Async JIT Group By workers: 1
                                      keys: [ts,s]
                                      keyFunctions: [timestamp_floor_utc('1m',ts)]
                                      values: [first(v)]
                                      filter: s='s2'
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                            """
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
            assertQueryNoLeakCheck(
                    "count\n8761\n",
                    "SELECT count() FROM (" +
                            "SELECT ts, key, avg(val) FROM sparse " +
                            "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR)",
                    null,
                    false,
                    true
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
    public void testFillPrevAcceptPrevToConstant() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV(b), NULL) and FILL(PREV(b), 42.0): the source `b` resolves
            // to FILL_CONSTANT at codegen. The chain-rejection rule rejects only
            // when the source is itself cross-column PREV, so a FILL_CONSTANT
            // source is accepted. Both NULL and a literal constant are exercised.
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, 30.0, '2024-01-01T02:00:00.000000Z')");
            // PREV(b) with b = NULL at gap: a carries forward from b's prev (10.0).
            assertQueryNoLeakCheck(
                    """
                            ts\ta\tb
                            2024-01-01T00:00:00.000000Z\t1.0\t10.0
                            2024-01-01T01:00:00.000000Z\t10.0\tnull
                            2024-01-01T02:00:00.000000Z\t3.0\t30.0
                            """,
                    "SELECT ts, sum(a) AS a, sum(b) AS b FROM t SAMPLE BY 1h FILL(PREV(b), NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
            // PREV(b) with b = 42.0 at gap: a carries forward from b's prev (10.0),
            // b's own fill uses the literal 42.
            assertQueryNoLeakCheck(
                    """
                            ts\ta\tb
                            2024-01-01T00:00:00.000000Z\t1.0\t10.0
                            2024-01-01T01:00:00.000000Z\t10.0\t42.0
                            2024-01-01T02:00:00.000000Z\t3.0\t30.0
                            """,
                    "SELECT ts, sum(a) AS a, sum(b) AS b FROM t SAMPLE BY 1h FILL(PREV(b), 42.0) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevAcceptPrevToSelfPrev() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV(b), PREV): a references b, and b is bare PREV
            // (FILL_PREV_SELF). The chain-rejection rule rejects only when the
            // source is itself cross-column PREV, so self-prev is accepted.
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, 30.0, '2024-01-01T02:00:00.000000Z')");
            // At gap 01:00: b carries self-prev (10.0); a reads from b's snapshot (10.0).
            assertQueryNoLeakCheck(
                    """
                            ts\ta\tb
                            2024-01-01T00:00:00.000000Z\t1.0\t10.0
                            2024-01-01T01:00:00.000000Z\t10.0\t10.0
                            2024-01-01T02:00:00.000000Z\t3.0\t30.0
                            """,
                    "SELECT ts, sum(a) AS a, sum(b) AS b FROM t SAMPLE BY 1h FILL(PREV(b), PREV) ALIGN TO CALENDAR",
                    "ts", false, false
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
            assertQueryNoLeakCheck(
                    """
                            ts\tfirst\tfirst1\tfirst2\tfirst3\tfirst4\tfirst5
                            2024-01-01T00:00:00.000000Z\t1.5\tA\t11\t1111\t111111\t111111111111
                            2024-01-01T01:00:00.000000Z\t1.5\tA\t11\t1111\t111111\t111111111111
                            2024-01-01T02:00:00.000000Z\t2.5\tB\t22\t2222\t222222\t222222222222
                            """,
                    "SELECT ts, first(f), first(c), first(d8), first(d16), first(d32), first(d64) " +
                            "FROM types SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevArrayDouble1D() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a DOUBLE[], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(ARRAY[1.0, 2.0, 3.0], '2024-01-01T00:00:00.000000Z')," +
                    "(NULL, '2024-01-01T02:00:00.000000Z')," +
                    "(ARRAY[4.0, 5.0], '2024-01-01T04:00:00.000000Z')");
            final String query = "SELECT ts, first(a) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR";
            assertQueryNoLeakCheck(
                    """
                            ts\tfirst
                            2024-01-01T00:00:00.000000Z\t[1.0,2.0,3.0]
                            2024-01-01T01:00:00.000000Z\t[1.0,2.0,3.0]
                            2024-01-01T02:00:00.000000Z\tnull
                            2024-01-01T03:00:00.000000Z\tnull
                            2024-01-01T04:00:00.000000Z\t[4.0,5.0]
                            """,
                    query,
                    "ts",
                    false,
                    false
            );
            Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(), "Sample By Fill"));
        });
    }

    @Test
    public void testFillPrevBoolean() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (true, '2024-01-01T00:00:00.000000Z'),
                        (false, '2024-01-01T03:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tv
                            2024-01-01T00:00:00.000000Z\ttrue
                            2024-01-01T01:00:00.000000Z\ttrue
                            2024-01-01T02:00:00.000000Z\ttrue
                            2024-01-01T03:00:00.000000Z\tfalse
                            """,
                    "SELECT ts, first(v) v FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false);
        });
    }

    @Test
    public void testFillPrevByte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (7::BYTE, '2024-01-01T00:00:00.000000Z'),
                        (42::BYTE, '2024-01-01T03:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t7
                            2024-01-01T01:00:00.000000Z\t7
                            2024-01-01T02:00:00.000000Z\t7
                            2024-01-01T03:00:00.000000Z\t42
                            """,
                    "SELECT ts, first(v) v FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false);
        });
    }

    @Test
    public void testFillPrevCastMultiKey() throws Exception {
        // Two distinct x::STRING keys across three buckets produce
        // 2 x 3 = 6 cartesian rows. Captured via probe-and-freeze.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1, 10.0, '2024-01-01T00:00:00.000000Z'),
                        (2, 20.0, '2024-01-01T02:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tfirst
                            2024-01-01T00:00:00.000000Z\t1\t10.0
                            2024-01-01T00:00:00.000000Z\t2\tnull
                            2024-01-01T01:00:00.000000Z\t1\t10.0
                            2024-01-01T01:00:00.000000Z\t2\tnull
                            2024-01-01T02:00:00.000000Z\t2\t20.0
                            2024-01-01T02:00:00.000000Z\t1\t10.0
                            """,
                    "SELECT ts, x::STRING k, first(v) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevConcatMultiKey() throws Exception {
        // Two distinct concat(a, b) (FUNCTION form) keys across three buckets
        // produce 2 x 3 = 6 cartesian rows. Captured via probe-and-freeze.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a STRING, b STRING, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('Lon', 'don', 10.0, '2024-01-01T00:00:00.000000Z'),
                        ('Par', 'is', 20.0, '2024-01-01T02:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tfirst
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\tnull
                            2024-01-01T01:00:00.000000Z\tLondon\t10.0
                            2024-01-01T01:00:00.000000Z\tParis\tnull
                            2024-01-01T02:00:00.000000Z\tParis\t20.0
                            2024-01-01T02:00:00.000000Z\tLondon\t10.0
                            """,
                    "SELECT ts, concat(a, b) k, first(v) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevConcatOperatorMultiKey() throws Exception {
        // Two distinct a || b (OPERATION form) keys across three buckets produce
        // 2 x 3 = 6 cartesian rows. Captured via probe-and-freeze. Pins the
        // OPERATION branch of the classifier predicate.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a STRING, b STRING, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('Lon', 'don', 10.0, '2024-01-01T00:00:00.000000Z'),
                        ('Par', 'is', 20.0, '2024-01-01T02:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tfirst
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\tnull
                            2024-01-01T01:00:00.000000Z\tLondon\t10.0
                            2024-01-01T01:00:00.000000Z\tParis\tnull
                            2024-01-01T02:00:00.000000Z\tParis\t20.0
                            2024-01-01T02:00:00.000000Z\tLondon\t10.0
                            """,
                    "SELECT ts, a || b k, first(v) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevCrossColumnArrayDimsMismatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        ts TIMESTAMP,
                        a DOUBLE[],
                        b DOUBLE[][]
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T00:00:00.000000Z', ARRAY[1.0, 2.0], ARRAY[[1.0, 2.0], [3.0, 4.0]]),
                        ('2024-01-01T02:00:00.000000Z', ARRAY[5.0, 6.0], ARRAY[[5.0, 6.0], [7.0, 8.0]])""");
            // Column layout: ts(0), a(1 DOUBLE[]), b(2 DOUBLE[][]).
            // FILL(PREV, PREV(a)): col=1 (a) is self-prev; col=2 (b DOUBLE[][])
            // is filled from col=1 (a DOUBLE[]). ARRAY target requires exact
            // type equality; dimensionality differs, so reject.
            String sql = "SELECT ts, first(a) a, first(b) b FROM t SAMPLE BY 1h FILL(PREV, PREV(a)) ALIGN TO CALENDAR";
            int prevArgPos = sql.indexOf("a", sql.indexOf("PREV(a)") + 5);
            assertExceptionNoLeakCheck(sql, prevArgPos, "cannot fill target column of type DOUBLE[][]");
            assertExceptionNoLeakCheck(sql, prevArgPos, "source type DOUBLE[]");
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
    public void testFillPrevCrossColumnBroadcastRejection() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        ts TIMESTAMP,
                        x DOUBLE,
                        y DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T00:00:00.000000Z', 1.0, 2.0),
                        ('2024-01-01T02:00:00.000000Z', 3.0, 4.0)""");
            // Two aggregates, single FILL(PREV(a)). PREV(colX) is FUNCTION-typed
            // and does not broadcast across aggregates; codegen emits a targeted
            // error at the position of the `P` of PREV inside FILL(...) rather
            // than falling through to the generic count-mismatch message.
            String sql = "SELECT ts, sum(x) a, sum(y) b FROM t SAMPLE BY 1h FILL(PREV(a)) ALIGN TO CALENDAR";
            int prevLiteralPos = sql.indexOf("PREV(", sql.indexOf("FILL("));
            assertExceptionNoLeakCheck(sql, prevLiteralPos,
                    "FILL(PREV(a)) cannot be broadcast across aggregates");
        });
    }

    @Test
    public void testFillPrevCrossColumnDecimalPrecisionMismatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        ts TIMESTAMP,
                        a DECIMAL(10, 2),
                        b DECIMAL(18, 6)
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T00:00:00.000000Z', 12.34::DECIMAL(10,2), 987654.321000::DECIMAL(18,6))""");
            // Column layout: ts(0), a(1 DECIMAL(10,2)), b(2 DECIMAL(18,6)).
            // FILL(PREV, PREV(a)): col=2 (b) is filled from a. DECIMAL targets
            // require exact type equality; precisions differ, so reject.
            String sql = "SELECT ts, first(a) a, first(b) b FROM t SAMPLE BY 1h FILL(PREV, PREV(a)) ALIGN TO CALENDAR";
            int prevArgPos = sql.indexOf("a", sql.indexOf("PREV(a)") + 5);
            assertExceptionNoLeakCheck(sql, prevArgPos, "cannot fill target column of type DECIMAL(18,6)");
            assertExceptionNoLeakCheck(sql, prevArgPos, "source type DECIMAL(10,2)");
        });
    }

    @Test
    public void testFillPrevCrossColumnGeoHashWidthMismatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        ts TIMESTAMP,
                        a GEOHASH(17b),
                        b GEOHASH(18b)
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T00:00:00.000000Z', rnd_geohash(17), rnd_geohash(18))""");
            // Column layout: ts(0), a(1 GEOHASH(17b)), b(2 GEOHASH(18b)).
            // FILL(PREV, PREV(a)): col=2 (b) is filled from source=a. Widths
            // differ, so needsExactTypeMatch fires and rejects. Widths 17 and
            // 18 are both non-multiples of 5 so they render as GEOHASH(Nb)
            // rather than the GEOHASH(Nc) alias form.
            String sql = "SELECT ts, first(a) a, first(b) b FROM t SAMPLE BY 1h FILL(PREV, PREV(a)) ALIGN TO CALENDAR";
            int prevArgPos = sql.indexOf("a", sql.indexOf("PREV(a)") + 5);
            assertExceptionNoLeakCheck(sql, prevArgPos, "cannot fill target column of type GEOHASH(18b)");
            assertExceptionNoLeakCheck(sql, prevArgPos, "source type GEOHASH(17b)");
        });
    }

    @Test
    public void testFillPrevCrossColumnTimestampUnitMismatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        ts TIMESTAMP,
                        a TIMESTAMP,
                        b TIMESTAMP_NS
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000000Z'::TIMESTAMP_NS)""");
            // Column layout: ts(0), a(1 TIMESTAMP_MICRO), b(2 TIMESTAMP_NANO).
            // FILL(PREV, PREV(a)): col=2 (b) fills from source=a. Both columns
            // share the TIMESTAMP tag (ColumnType.tagOf = type & 0xFF collapses
            // MICRO/NANO to the same tag), so the old tag-equality compatibility
            // path accepted the mix silently and silently fed MICRO longs into
            // a NANO target. The widened needsExactTypeMatch predicate lifts
            // TIMESTAMP into the exact-match set and rejects the cross-unit
            // assignment. 9df205bac5 landed the sibling fix on the
            // FILL_CONSTANT path for the same unit-drift class.
            String sql = "SELECT ts, first(a) a, first(b) b FROM t SAMPLE BY 1h FILL(PREV, PREV(a)) ALIGN TO CALENDAR";
            int prevArgPos = sql.indexOf("a", sql.indexOf("PREV(a)") + 5);
            assertExceptionNoLeakCheck(
                    sql,
                    prevArgPos,
                    "source type TIMESTAMP cannot fill target column of type TIMESTAMP_NS"
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
            assertQueryNoLeakCheck(
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
                            "FROM x SAMPLE BY 1h FILL(PREV, PREV(s)) ALIGN TO CALENDAR",
                    "ts", false, false
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
            assertQueryNoLeakCheck(
                    """
                            ts\ts\ta
                            2024-01-01T00:00:00.000000Z\t1.0\t10.0
                            2024-01-01T01:00:00.000000Z\t10.0\tnull
                            2024-01-01T02:00:00.000000Z\t3.0\t30.0
                            """,
                    "SELECT ts, sum(val) AS s, sum(ival::DOUBLE) AS a " +
                            "FROM x SAMPLE BY 1h FILL(PREV(a), NULL) ALIGN TO CALENDAR",
                    "ts", false, false
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
            assertQueryNoLeakCheck(
                    """
                            ts\ts\ta
                            2024-01-01T00:00:00.000000Z\t1.0\t10.0
                            2024-01-01T01:00:00.000000Z\t1.0\t1.0
                            2024-01-01T02:00:00.000000Z\t3.0\t30.0
                            """,
                    "SELECT ts, sum(val) AS s, sum(ival::DOUBLE) AS a " +
                            "FROM x SAMPLE BY 1h FILL(PREV, PREV(s)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevCrossColumnNoPrevYet() throws Exception {
        assertMemoryLeak(() -> {
            // FROM '2024-01-01T20:00' TO '2024-01-02T00:00' with data only at 22:00.
            // FILL(PREV, PREV(s)) — column `s` is self-prev, column `a` is
            // cross-column-from-s. The leading 20:00 and 21:00 fill rows must
            // emit NULL for both columns: hasKeyPrev() returns false because
            // no prior data row has set simplePrevRowId, so each FillRecord
            // getter falls through past the (mode >= 0 && hasKeyPrev()) branch
            // into the default null sentinel. Ensures the cross-column fall-through
            // path returns the correct null for non-key sources, mirroring
            // testFillPrevGeoNoPrevYet's coverage of the self-prev case.
            execute("CREATE TABLE x (val DOUBLE, ival INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES (1.0, 10, '2024-01-01T22:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\ts\ta
                            2024-01-01T20:00:00.000000Z\tnull\tnull
                            2024-01-01T21:00:00.000000Z\tnull\tnull
                            2024-01-01T22:00:00.000000Z\t1.0\t10.0
                            2024-01-01T23:00:00.000000Z\t1.0\t1.0
                            """,
                    "SELECT ts, sum(val) AS s, sum(ival::DOUBLE) AS a " +
                            "FROM x " +
                            "SAMPLE BY 1h FROM '2024-01-01T20:00:00.000000Z' TO '2024-01-02T00:00:00.000000Z' " +
                            "FILL(PREV, PREV(s)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevDate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2020-01-01'::DATE, '2024-01-01T00:00:00.000000Z'),
                        ('2020-06-01'::DATE, '2024-01-01T03:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000Z
                            2024-01-01T01:00:00.000000Z\t2020-01-01T00:00:00.000Z
                            2024-01-01T02:00:00.000000Z\t2020-01-01T00:00:00.000Z
                            2024-01-01T03:00:00.000000Z\t2020-06-01T00:00:00.000Z
                            """,
                    "SELECT ts, first(v) v FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false);
        });
    }

    @Test
    public void testFillPrevDecimal128() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (d DECIMAL(20, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(12.34::DECIMAL(20, 2), '2024-01-01T00:00:00.000000Z')," +
                    "(NULL, '2024-01-01T02:00:00.000000Z')," +
                    "(56.78::DECIMAL(20, 2), '2024-01-01T04:00:00.000000Z')");
            final String query = "SELECT ts, first(d) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR";
            assertQueryNoLeakCheck(
                    """
                            ts\tfirst
                            2024-01-01T00:00:00.000000Z\t12.34
                            2024-01-01T01:00:00.000000Z\t12.34
                            2024-01-01T02:00:00.000000Z\t
                            2024-01-01T03:00:00.000000Z\t
                            2024-01-01T04:00:00.000000Z\t56.78
                            """,
                    query,
                    "ts",
                    false
            );
            Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(), "Sample By Fill"));
        });
    }

    @Test
    public void testFillPrevDecimal256() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (d DECIMAL(40, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(100.25::DECIMAL(40, 2), '2024-01-01T00:00:00.000000Z')," +
                    "(NULL, '2024-01-01T02:00:00.000000Z')," +
                    "(200.50::DECIMAL(40, 2), '2024-01-01T04:00:00.000000Z')");
            final String query = "SELECT ts, first(d) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR";
            assertQueryNoLeakCheck(
                    """
                            ts\tfirst
                            2024-01-01T00:00:00.000000Z\t100.25
                            2024-01-01T01:00:00.000000Z\t100.25
                            2024-01-01T02:00:00.000000Z\t
                            2024-01-01T03:00:00.000000Z\t
                            2024-01-01T04:00:00.000000Z\t200.50
                            """,
                    query,
                    "ts",
                    false
            );
            Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(), "Sample By Fill"));
        });
    }

    @Test
    public void testFillPrevDecimalZeroVsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        ts TIMESTAMP,
                        d DECIMAL(10, 2)
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            // Insert a legitimate 0.00 at 01:00 and a data value at 03:00.
            // Query spans 00:00..04:00 via FROM/TO so bucket at 00:00 predates
            // any data (produces NULL fill), bucket at 02:00 is between data
            // rows (produces PREV fill carrying the prior 0.00), bucket at
            // 03:00 is a data row (12.34), bucket at 04:00 carries the prior
            // 12.34. The Decimal null sentinel (MIN_VALUE) is preserved
            // through the fill pipeline and renders distinctly from a
            // legitimate 0.00.
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T01:00:00.000000Z', 0.00::DECIMAL(10,2)),
                        ('2024-01-01T03:00:00.000000Z', 12.34::DECIMAL(10,2))""");
            assertQueryNoLeakCheck(
                    """
                            ts\td
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T01:00:00.000000Z\t0.00
                            2024-01-01T02:00:00.000000Z\t0.00
                            2024-01-01T03:00:00.000000Z\t12.34
                            2024-01-01T04:00:00.000000Z\t12.34
                            """,
                    """
                            SELECT ts, first(d) d
                            FROM t
                            SAMPLE BY 1h FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-01T05:00:00.000000Z' FILL(PREV)""",
                    "ts",
                    false,
                    false
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
            assertQueryNoLeakCheck(
                    """
                            ts\tfirst\tfirst1\tfirst2\tfirst3
                            2024-01-01T00:00:00.000000Z\t010\tsp0\tsp052n\tsp052n01
                            2024-01-01T01:00:00.000000Z\t010\tsp0\tsp052n\tsp052n01
                            2024-01-01T02:00:00.000000Z\t110\tu33\tu33d8b\tu33d8b12
                            """,
                    "SELECT ts, first(g1), first(g2), first(g4), first(g8) " +
                            "FROM g SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
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
            assertQueryNoLeakCheck(
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
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevGeoNoPrevYet() throws Exception {
        assertMemoryLeak(() -> {
            // Keyed SAMPLE BY FROM '2024-01-01' TO '2024-01-02' FILL(PREV) with data
            // arriving only in later buckets. The leading fill rows must emit the
            // GEOHASH NULL sentinels (GeoHashes.BYTE_NULL / SHORT_NULL / INT_NULL /
            // GeoHashes.NULL) because no prior data bucket has set a value, NOT
            // zero and NOT Numbers.*_NULL. Covers the four null-sentinel branches
            // in FillRecord at lines ~933 (getGeoByte), ~945 (getGeoShort), ~957
            // (getGeoInt), ~969 (getGeoLong).
            execute("CREATE TABLE t (city SYMBOL, " +
                    "g1 GEOHASH(3b), g2 GEOHASH(15b), g4 GEOHASH(6c), g8 GEOHASH(8c), " +
                    "ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('A', cast('8' AS GEOHASH(3b)), cast('sp0' AS GEOHASH(15b)), " +
                    "cast('sp05ds' AS GEOHASH(6c)), cast('sp05ds00' AS GEOHASH(8c)), " +
                    "'2024-01-01T22:00:00.000000Z')");
            // Buckets from 2024-01-01T20:00 through T23:00 with data only at 22:00.
            // At 20:00 and 21:00 the key 'A' has no prev yet: all four geo columns
            // must emit their width-specific NULL sentinel (rendered as empty
            // string in text output), not "0" or "null".
            assertQueryNoLeakCheck(
                    """
                            ts\tcity\tfirst\tfirst1\tfirst2\tfirst3
                            2024-01-01T20:00:00.000000Z\tA\t\t\t\t
                            2024-01-01T21:00:00.000000Z\tA\t\t\t\t
                            2024-01-01T22:00:00.000000Z\tA\t010\tsp0\tsp05ds\tsp05ds00
                            2024-01-01T23:00:00.000000Z\tA\t010\tsp0\tsp05ds\tsp05ds00
                            """,
                    "SELECT ts, city, first(g1), first(g2), first(g4), first(g8) " +
                            "FROM t " +
                            "SAMPLE BY 1h FROM '2024-01-01T20:00:00.000000Z' TO '2024-01-02T00:00:00.000000Z' " +
                            "FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v IPv4, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('192.168.0.1'::IPv4, '2024-01-01T00:00:00.000000Z'),
                        ('10.0.0.1'::IPv4, '2024-01-01T03:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t192.168.0.1
                            2024-01-01T01:00:00.000000Z\t192.168.0.1
                            2024-01-01T02:00:00.000000Z\t192.168.0.1
                            2024-01-01T03:00:00.000000Z\t10.0.0.1
                            """,
                    "SELECT ts, first(v) v FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false);
        });
    }

    @Test
    public void testFillPrevInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1_234, '2024-01-01T00:00:00.000000Z'),
                        (5_678, '2024-01-01T03:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t1234
                            2024-01-01T01:00:00.000000Z\t1234
                            2024-01-01T02:00:00.000000Z\t1234
                            2024-01-01T03:00:00.000000Z\t5678
                            """,
                    "SELECT ts, first(v) v FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false);
        });
    }

    @Test
    public void testFillPrevInterval() throws Exception {
        // INTERVAL is not a persistable column type and QuestDB has no
        // first(INTERVAL) aggregate, so the only route to an INTERVAL in a
        // SAMPLE BY output column is an inline interval(lo, hi) expression
        // used as the GROUP BY key. Without FillRecord.getInterval, gap rows
        // would fall through to Record.getInterval's default and throw
        // UnsupportedOperationException — this test pins the contract.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        lo TIMESTAMP,
                        hi TIMESTAMP,
                        v DOUBLE,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("""
                    INSERT INTO t VALUES
                        ('2020-01-01T00:00:00.000Z'::TIMESTAMP, '2020-02-01T00:00:00.000Z'::TIMESTAMP, 10.0, '2024-01-01T00:00:00.000000Z'),
                        ('2020-01-01T00:00:00.000Z'::TIMESTAMP, '2020-02-01T00:00:00.000Z'::TIMESTAMP, 30.0, '2024-01-01T03:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tfirst
                            2024-01-01T00:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0
                            2024-01-01T01:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0
                            2024-01-01T02:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0
                            2024-01-01T03:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t30.0
                            """,
                    "SELECT ts, interval(lo, hi) k, first(v) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false
            );
        });
    }

    @Test
    public void testFillPrevIntervalMultiKey() throws Exception {
        // Two distinct interval(lo, hi) keys across three buckets should
        // produce 2 x 3 = 6 cartesian rows. Pre-fix classifier treated the
        // function-valued key as an aggregate, dropped it from keyColIndices,
        // and collapsed output to 3 rows.
        // Row order and Interval.NULL rendering captured from probe-and-freeze run.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        lo TIMESTAMP,
                        hi TIMESTAMP,
                        v DOUBLE,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("""
                    INSERT INTO t VALUES
                        ('2020-01-01T00:00:00.000Z'::TIMESTAMP, '2020-02-01T00:00:00.000Z'::TIMESTAMP, 10.0, '2024-01-01T00:00:00.000000Z'),
                        ('2021-01-01T00:00:00.000Z'::TIMESTAMP, '2021-02-01T00:00:00.000Z'::TIMESTAMP, 20.0, '2024-01-01T02:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tfirst
                            2024-01-01T00:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0
                            2024-01-01T00:00:00.000000Z\t('2021-01-01T00:00:00.000Z', '2021-02-01T00:00:00.000Z')\tnull
                            2024-01-01T01:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0
                            2024-01-01T01:00:00.000000Z\t('2021-01-01T00:00:00.000Z', '2021-02-01T00:00:00.000Z')\tnull
                            2024-01-01T02:00:00.000000Z\t('2021-01-01T00:00:00.000Z', '2021-02-01T00:00:00.000Z')\t20.0
                            2024-01-01T02:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0
                            """,
                    "SELECT ts, interval(lo, hi) k, first(v) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevKeyedArray() throws Exception {
        assertMemoryLeak(() -> {
            // ARRAY as key column — the FILL_KEY branch of FillRecord.getArray
            // must carry the key through gap rows via keysMapRecord.getArray.
            execute("""
                    CREATE TABLE x (
                        k DOUBLE[],
                        v DOUBLE,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("""
                    INSERT INTO x VALUES
                        (ARRAY[1.0, 2.0], 10.0, '2024-01-01T00:00:00.000000Z'),
                        (ARRAY[1.0, 2.0], 30.0, '2024-01-01T03:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tfirst
                            2024-01-01T00:00:00.000000Z\t[1.0,2.0]\t10.0
                            2024-01-01T01:00:00.000000Z\t[1.0,2.0]\t10.0
                            2024-01-01T02:00:00.000000Z\t[1.0,2.0]\t10.0
                            2024-01-01T03:00:00.000000Z\t[1.0,2.0]\t30.0
                            """,
                    "SELECT ts, k, first(v) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testFillPrevKeyedBinary() throws Exception {
        // BINARY as key column — the FILL_KEY branch of FillRecord.getBin and
        // getBinLen must carry the key bytes through gap rows via keysMapRecord.
        //
        // Rendering strategy: rnd_bin() output is seeded-Rnd deterministic but
        // subtle harness wiring makes it fragile to pin exact hex. Instead this
        // test asserts the structural property: every row (data or fill) emits
        // getBin != null and getBinLen > 0.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE x AS (
                        SELECT
                            rnd_bin(4, 8, 0) AS k,
                            x AS v,
                            timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) AS ts
                        FROM long_sequence(1)
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            // Second row with the SAME key bytes as the first row and a data
            // value at 03:00. SAMPLE BY 1h FILL(PREV) then produces 4 rows:
            //   00:00 data (v=1), 01:00 fill (v=1), 02:00 fill (v=1),
            //   03:00 data (v=30).
            // Pre-fix: rows at 01:00 and 02:00 emit k=null.
            // Post-fix: rows at 01:00 and 02:00 emit k = the carried key.
            execute("""
                    INSERT INTO x
                    SELECT k, 30L, '2024-01-01T03:00:00.000000Z'::TIMESTAMP FROM x LIMIT 1""");
            try (RecordCursorFactory factory = select("""
                    SELECT ts, k, first(v) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR""")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Record record = cursor.getRecord();
                    int rows = 0;
                    while (cursor.hasNext()) {
                        rows++;
                        BinarySequence key = record.getBin(1);
                        long keyLen = record.getBinLen(1);
                        Assert.assertNotNull("row " + rows + " BINARY key must be non-null", key);
                        Assert.assertTrue("row " + rows + " BINARY key length must be > 0", keyLen > 0);
                    }
                    Assert.assertEquals("expected 4 rows (2 data + 2 fill)", 4, rows);
                }
            }
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
            assertQueryNoLeakCheck(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\t20.0
                            2024-01-01T01:00:00.000000Z\tParis\t21.0
                            2024-01-01T01:00:00.000000Z\tLondon\t10.0
                            2024-01-01T02:00:00.000000Z\tLondon\t12.0
                            2024-01-01T02:00:00.000000Z\tParis\t21.0
                            """,
                    "SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
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
            // Row order within a bucket is AsyncGroupBy hash-merge order -- see class
            // Javadoc. If this test starts failing on ordering, wrap in ORDER BY ts, city.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T01:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T01:00:00.000000Z')," +
                    "('London', 12.0, '2024-01-01T03:00:00.000000Z')");
            assertQueryNoLeakCheck(
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
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T05:00:00.000000Z' FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevKeyedCte() throws Exception {
        assertMemoryLeak(() -> {
            // Wrapping SAMPLE BY FILL(PREV) in a CTE verifies the FILL_KEY
            // reclassification works correctly when the factory is reused
            // as a subquery.
            // Row order within a bucket is AsyncGroupBy hash-merge order -- see class
            // Javadoc. If this test starts failing on ordering, wrap in ORDER BY ts, city.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 21.0, '2024-01-01T01:00:00.000000Z')," +
                    "('London', 12.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
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
                            "SELECT * FROM sq",
                    "ts",
                    false
            );
        });
    }

    @Test
    public void testFillPrevKeyedNoPrevYet() throws Exception {
        assertMemoryLeak(() -> {
            // London appears at 00:00, Paris first appears at 01:00.
            // At 00:00, Paris is missing and has no prev -> should get null.
            // Row order within a bucket is AsyncGroupBy hash-merge order -- see class
            // Javadoc. If this test starts failing on ordering, wrap in ORDER BY ts, city.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T01:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\tnull
                            2024-01-01T01:00:00.000000Z\tParis\t20.0
                            2024-01-01T01:00:00.000000Z\tLondon\t10.0
                            """,
                    "SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (10_000_000L, '2024-01-01T00:00:00.000000Z'),
                        (20_000_000L, '2024-01-01T03:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t10000000
                            2024-01-01T01:00:00.000000Z\t10000000
                            2024-01-01T02:00:00.000000Z\t10000000
                            2024-01-01T03:00:00.000000Z\t20000000
                            """,
                    "SELECT ts, first(v) v FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false);
        });
    }

    @Test
    public void testFillPrevLong128Fallback() throws Exception {
        assertMemoryLeak(() -> {
            // LONG128 has no first/last/sum/min/max aggregate function in QuestDB,
            // so SELECT first(long128_col) ... fails at aggregate resolution. This
            // test documents that LONG128 PREV aggregates are rejected at compile
            // time regardless of the fill cursor routing.
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
    public void testFillPrevLong256NoPrevYet() throws Exception {
        // M-4 regression: FillRecord.getLong256(int, CharSink<?>) must render
        // null on leading FILL(PREV) rows where hasKeyPrev() == false. Unlike
        // the Decimal128/Decimal256 siblings that call sink.ofRawNull() on the
        // terminal fall-through, CharSink<?> has no ofRawNull() method, so the
        // null-render contract here is "leave the caller-owned sink untouched"
        // (matching NullMemoryCMR.getLong256(offset, CharSink) at :194).
        //
        // FROM '2024-01-01' TO '2024-01-01T03:00:00Z' with data only at 02:00.
        // The 00:00 and 01:00 leading fill rows must emit empty text for
        // sum(h) because no prior data row has set simplePrevRowId; the
        // FillRecord getter falls through past the (mode == FILL_PREV_SELF &&
        // hasKeyPrev()) branch and exits without writing to the sink, matching
        // the empty-string rendering convention for null LONG256.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (h LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES ('0x01', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tsum
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T01:00:00.000000Z\t
                            2024-01-01T02:00:00.000000Z\t0x01
                            """,
                    "SELECT ts, sum(h) FROM x " +
                            "SAMPLE BY 1h FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-01T03:00:00.000000Z' " +
                            "FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testFillPrevLong256NonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (h LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('0x10', '2024-01-01T00:00:00.000000Z')," +
                    "('0xabcdef', '2024-01-01T04:00:00.000000Z')");
            final String query = "SELECT ts, sum(h) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR";
            assertQueryNoLeakCheck(
                    """
                            ts\tsum
                            2024-01-01T00:00:00.000000Z\t0x10
                            2024-01-01T01:00:00.000000Z\t0x10
                            2024-01-01T02:00:00.000000Z\t0x10
                            2024-01-01T03:00:00.000000Z\t0x10
                            2024-01-01T04:00:00.000000Z\t0xabcdef
                            """,
                    query,
                    "ts",
                    false
            );
            Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(), "Sample By Fill"));
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
                              fill: mixed
                                Encode sort light
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
    public void testFillPrevMultiUnitStride() throws Exception {
        // Existing FILL tests only exercise 1-unit strides (1h, 1d, 1m, 1s).
        // A non-unit stride like 3h pushes TimestampSampler.nextTimestamp
        // through a multi-hour advance and catches arithmetic bugs that
        // wouldn't surface at stride=1. Data at 00:00 and 09:00 -- two buckets
        // of data, two 3h gaps in between (03:00 and 06:00).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(4.0, '2024-01-01T09:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfv
                            2024-01-01T00:00:00.000000Z\t1.0
                            2024-01-01T03:00:00.000000Z\t1.0
                            2024-01-01T06:00:00.000000Z\t1.0
                            2024-01-01T09:00:00.000000Z\t4.0
                            """,
                    "SELECT ts, first(val) fv FROM t SAMPLE BY 3h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
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
            assertQueryNoLeakCheck(
                    """
                            ts\tkey\tsum\tfirst
                            2024-01-01T00:00:00.000000Z\tK1\t1.0\tA
                            2024-01-01T00:00:00.000000Z\tK2\t2.0\tB
                            2024-01-01T01:00:00.000000Z\tK1\t1.0\t
                            2024-01-01T01:00:00.000000Z\tK2\t2.0\t
                            2024-01-01T02:00:00.000000Z\tK1\t3.0\tC
                            2024-01-01T02:00:00.000000Z\tK2\t2.0\t
                            """,
                    "SELECT ts, key, sum(val), first(sym) FROM x SAMPLE BY 1h FILL(PREV, NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevNonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
                    "FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
            assertQueryNoLeakCheck(
                    """
                            sum\tts
                            1.0\t2024-01-01T00:00:00.000000Z
                            1.0\t2024-01-01T01:00:00.000000Z
                            2.0\t2024-01-01T02:00:00.000000Z
                            2.0\t2024-01-01T03:00:00.000000Z
                            3.0\t2024-01-01T04:00:00.000000Z
                            """,
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
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
            assertQueryNoLeakCheck(
                    """
                            sum\tts
                            1.0\t2024-01-01T00:00:00.000000Z
                            1.0\t2024-01-01T01:00:00.000000Z
                            2.0\t2024-01-01T02:00:00.000000Z
                            2.0\t2024-01-01T03:00:00.000000Z
                            3.0\t2024-01-01T04:00:00.000000Z
                            """,
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                    "ts", false, false
            );
            // Verify fast path plan (Sample By Fill, not Sample By)
            assertPlanNoLeakCheck(
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                    """
                            Sample By Fill
                              stride: '1h'
                              fill: prev
                                Encode sort light
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
            assertQueryNoLeakCheck(
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
                            "FROM x SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevCrossColumnRejectsSymbolSource() throws Exception {
        assertMemoryLeak(() -> {
            // Cross-column FILL(PREV(k)) where both the SYMBOL key column k and
            // the SYMBOL mirror target column carry per-column symbol tables.
            // Reading a fill row via getInt(target) + getSymbolTable(target) would
            // mix the source column's symbol id with the target column's table.
            // The compiler rejects this at generateFill time; bare FILL(PREV)
            // still works and preserves self-prev semantics per column.
            execute("CREATE TABLE x (k SYMBOL, val DOUBLE, mirror SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', 10.0, 'foo', '2024-01-01T00:00:00.000000Z')," +
                    "('B', 20.0, 'bar', '2024-01-01T00:00:00.000000Z')," +
                    "('A', 30.0, 'baz', '2024-01-01T02:00:00.000000Z')");
            assertExceptionNoLeakCheck(
                    "SELECT ts, k, sum(val) AS s, last(mirror) AS m " +
                            "FROM x SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR",
                    83,
                    "FILL(PREV(k)) is not supported on SYMBOL columns"
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
            assertQueryNoLeakCheck(
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
                            "FROM x SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevOuterProjectionReorder() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        ts TIMESTAMP,
                        k SYMBOL,
                        x DOUBLE,
                        y DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            // Two distinct keys, with a data gap between 00:00 and 04:00 so
            // the intermediate buckets must carry forward key + a+b via
            // FILL(PREV). The outer expression `a + b` over the inner
            // SAMPLE BY triggers propagateTopDownColumns0 in SqlOptimiser,
            // reordering the inner model's columns. Pre-fix: the bare
            // FILL(PREV) branch called isKeyColumn(factoryIdx, bottomUpCols,
            // timestampIndex) with a factory-indexed `i` against a
            // user-ordered bottomUpCols, misclassifying columns. Post-fix:
            // factoryColToUserFillIdx is authoritative and key `k` is
            // carried forward while aggregates self-prev.
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T00:00:00.000000Z', 'A', 1.0, 10.0),
                        ('2024-01-01T00:00:00.000000Z', 'B', 2.0, 20.0),
                        ('2024-01-01T04:00:00.000000Z', 'A', 5.0, 50.0),
                        ('2024-01-01T04:00:00.000000Z', 'B', 6.0, 60.0)""");
            assertQueryNoLeakCheck(
                    """
                            column\tk
                            11.0\tA
                            22.0\tB
                            11.0\tA
                            22.0\tB
                            11.0\tA
                            22.0\tB
                            11.0\tA
                            22.0\tB
                            55.0\tA
                            66.0\tB
                            """,
                    """
                            SELECT a + b, k FROM (
                                SELECT k, sum(x) a, sum(y) b FROM t SAMPLE BY 1h FILL(PREV)
                            )""",
                    null,
                    false,
                    false
            );
        });
    }

    @Test
    public void testFillPrevRejectBindVar() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV($1)) — bind variable is not a LITERAL column name and
            // must be rejected with "PREV argument must be a single column name".
            execute("CREATE TABLE t (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, '2024-01-01T02:00:00.000000Z')");
            bindVariableService.clear();
            bindVariableService.setStr(0, "v");
            assertExceptionNoLeakCheck(
                    "SELECT ts, avg(v) FROM t SAMPLE BY 1h FILL(PREV($1)) ALIGN TO CALENDAR",
                    43,
                    "PREV argument must be a single column name"
            );
        });
    }

    @Test
    public void testFillPrevRejectFuncArg() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV(abs(a))) — PREV argument is a FUNCTION, not a LITERAL.
            // Must be rejected with "PREV argument must be a single column name"
            // at the position of the expression.
            execute("CREATE TABLE t (a DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, '2024-01-01T02:00:00.000000Z')");
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(a) FROM t SAMPLE BY 1h FILL(PREV(abs(a))) ALIGN TO CALENDAR",
                    43,
                    "PREV argument must be a single column name"
            );
        });
    }

    @Test
    public void testFillPrevRejectMultiArg() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV(a, b)) — paramCount > 1. Must be rejected with
            // "PREV argument must be a single column name".
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, 30.0, '2024-01-01T02:00:00.000000Z')");
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(a) a, sum(b) b FROM t SAMPLE BY 1h FILL(PREV(a, b)) ALIGN TO CALENDAR",
                    55,
                    "PREV argument must be a single column name"
            );
        });
    }

    @Test
    public void testFillPrevRejectMutualChain() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV(b), PREV(a)) — mutual cycle: a -> b and b -> a. Must be
            // rejected as a chain. Error at the position of the first PREV.
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, 30.0, '2024-01-01T02:00:00.000000Z')");
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(a) a, sum(b) b FROM t SAMPLE BY 1h FILL(PREV(b), PREV(a)) ALIGN TO CALENDAR",
                    55,
                    "FILL(PREV) chains are not supported"
            );
        });
    }

    @Test
    public void testFillPrevRejectNoArg() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV()) -- zero-argument function call. The PREV grammar
            // rule in SqlCodeGenerator.generateFill rejects this at codegen
            // time with "PREV argument must be a single column name" thrown
            // at the PREV token's position (43 = sql.indexOf("PREV(")). This
            // pins both the exact wording and the token position so a future
            // refactor of the grammar rule cannot silently loosen the
            // rejection (e.g. let PREV() pass through as if it were bare
            // FILL(PREV) with no argument).
            //
            // Note: this message is the canonical PREV-grammar rejection
            // shared with testFillPrevRejectFuncArg, testFillPrevRejectMultiArg
            // and testFillPrevRejectBindVar. The grammar rule fires before the
            // parser can raise "too few arguments" because PREV is classified
            // as a fill-spec keyword whose argument shape is validated by
            // generateFill, not by ExpressionParser's arity check.
            execute("CREATE TABLE t (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final String sql = "SELECT ts, avg(v) FROM t SAMPLE BY 1h FILL(PREV()) ALIGN TO CALENDAR";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("PREV("),
                    "PREV argument must be a single column name"
            );
        });
    }

    @Test
    public void testFillPrevRejectThreeHopChain() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV(b), PREV(c), PREV): a -> b, b -> c, c -> self-prev.
            // Column a forms a chain through b to c, so chain rejection fires
            // for column a even though column c breaks the chain.
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, c DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, 10.0, 100.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, 30.0, 300.0, '2024-01-01T02:00:00.000000Z')");
            assertExceptionNoLeakCheck(
                    "SELECT ts, sum(a) a, sum(b) b, sum(c) c FROM t SAMPLE BY 1h FILL(PREV(b), PREV(c), PREV) ALIGN TO CALENDAR",
                    65,
                    "FILL(PREV) chains are not supported"
            );
        });
    }

    @Test
    public void testFillPrevRejectTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV(ts)) where ts is the designated timestamp column must
            // be rejected at compile time with "PREV cannot reference the
            // designated timestamp column" at the position of the timestamp
            // token inside PREV(ts).
            execute("CREATE TABLE t (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, '2024-01-01T02:00:00.000000Z')");
            assertExceptionNoLeakCheck(
                    "SELECT ts, avg(v) FROM t SAMPLE BY 1h FILL(PREV(ts)) ALIGN TO CALENDAR",
                    48,
                    "PREV cannot reference the designated timestamp column"
            );
        });
    }

    @Test
    public void testFillPrevRejectTypeMismatch() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV(d), PREV) on a query projecting (first(i) INT, sum(d) DOUBLE).
            // For column `i`, the cross-column source `d` has tag DOUBLE; the
            // target tag is INT, so the type check must reject.
            execute("CREATE TABLE t (d DOUBLE, i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, 10, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, 30, '2024-01-01T02:00:00.000000Z')");
            assertExceptionNoLeakCheck(
                    "SELECT ts, first(i) AS i, sum(d) AS d FROM t SAMPLE BY 1h FILL(PREV(d), PREV) ALIGN TO CALENDAR",
                    68,
                    "cannot fill target column of type"
            );
        });
    }

    @Test
    public void testFillPrevSelfAlias() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV(a)) where `a` is the target column name — normalized
            // internally to self-prev so output matches bare FILL(PREV).
            execute("CREATE TABLE t (a DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, '2024-01-01T02:00:00.000000Z')");
            // The plan still shows fill=prev (no column-specific annotation)
            // because FILL_PREV_SELF is the internal mode.
            assertQueryNoLeakCheck(
                    """
                            ts\ta
                            2024-01-01T00:00:00.000000Z\t1.0
                            2024-01-01T01:00:00.000000Z\t1.0
                            2024-01-01T02:00:00.000000Z\t3.0
                            """,
                    "SELECT ts, sum(a) AS a FROM t SAMPLE BY 1h FILL(PREV(a)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (100::SHORT, '2024-01-01T00:00:00.000000Z'),
                        (200::SHORT, '2024-01-01T03:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t100
                            2024-01-01T01:00:00.000000Z\t100
                            2024-01-01T02:00:00.000000Z\t100
                            2024-01-01T03:00:00.000000Z\t200
                            """,
                    "SELECT ts, first(v) v FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false);
        });
    }

    @Test
    public void testFillPrevStringKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (key SYMBOL, s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('K1', 'alpha', '2024-01-01T00:00:00.000000Z')," +
                    "('K2', 'beta', '2024-01-01T00:00:00.000000Z')," +
                    "('K1', 'gamma', '2024-01-01T02:00:00.000000Z')");
            final String query = "SELECT ts, key, first(s) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR";
            assertQueryNoLeakCheck(
                    """
                            ts\tkey\tfirst
                            2024-01-01T00:00:00.000000Z\tK1\talpha
                            2024-01-01T00:00:00.000000Z\tK2\tbeta
                            2024-01-01T01:00:00.000000Z\tK1\talpha
                            2024-01-01T01:00:00.000000Z\tK2\tbeta
                            2024-01-01T02:00:00.000000Z\tK1\tgamma
                            2024-01-01T02:00:00.000000Z\tK2\tbeta
                            """,
                    query,
                    "ts",
                    false
            );
            Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(), "Sample By Fill"));
        });
    }

    @Test
    public void testFillPrevStringNonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('alpha', '2024-01-01T00:00:00.000000Z')," +
                    "(NULL, '2024-01-01T02:00:00.000000Z')," +
                    "('', '2024-01-01T04:00:00.000000Z')," +
                    "('longer string that requires reallocation', '2024-01-01T06:00:00.000000Z')");
            final String query = "SELECT ts, first(s) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR";
            assertQueryNoLeakCheck(
                    """
                            ts\tfirst
                            2024-01-01T00:00:00.000000Z\talpha
                            2024-01-01T01:00:00.000000Z\talpha
                            2024-01-01T02:00:00.000000Z\t
                            2024-01-01T03:00:00.000000Z\t
                            2024-01-01T04:00:00.000000Z\t
                            2024-01-01T05:00:00.000000Z\t
                            2024-01-01T06:00:00.000000Z\tlonger string that requires reallocation
                            """,
                    query,
                    "ts",
                    false
            );
            Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(), "Sample By Fill"));
        });
    }

    @Test
    public void testFillPrevSymbolKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (key SYMBOL, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('K1', 'A', '2024-01-01T00:00:00.000000Z')," +
                    "('K2', 'B', '2024-01-01T00:00:00.000000Z')," +
                    "('K1', 'C', '2024-01-01T02:00:00.000000Z')");
            final String query = "SELECT ts, key, first(sym) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR";
            assertQueryNoLeakCheck(
                    """
                            ts\tkey\tfirst
                            2024-01-01T00:00:00.000000Z\tK1\tA
                            2024-01-01T00:00:00.000000Z\tK2\tB
                            2024-01-01T01:00:00.000000Z\tK1\tA
                            2024-01-01T01:00:00.000000Z\tK2\tB
                            2024-01-01T02:00:00.000000Z\tK1\tC
                            2024-01-01T02:00:00.000000Z\tK2\tB
                            """,
                    query,
                    "ts",
                    false
            );
            Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(), "Sample By Fill"));
        });
    }

    @Test
    public void testFillPrevSymbolNonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', '2024-01-01T00:00:00.000000Z')," +
                    "('B', '2024-01-01T02:00:00.000000Z')," +
                    "('C', '2024-01-01T06:00:00.000000Z')");
            final String query = "SELECT ts, first(sym) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR";
            assertQueryNoLeakCheck(
                    """
                            ts\tfirst
                            2024-01-01T00:00:00.000000Z\tA
                            2024-01-01T01:00:00.000000Z\tA
                            2024-01-01T02:00:00.000000Z\tB
                            2024-01-01T03:00:00.000000Z\tB
                            2024-01-01T04:00:00.000000Z\tB
                            2024-01-01T05:00:00.000000Z\tB
                            2024-01-01T06:00:00.000000Z\tC
                            """,
                    query,
                    "ts",
                    false
            );
            Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(), "Sample By Fill"));
        });
    }

    @Test
    public void testFillPrevSymbolNull() throws Exception {
        assertMemoryLeak(() -> {
            // First snapshotted symbol is NULL — the PREV branch must emit the null
            // symbol, not a CharSequence resolved from the INT_NULL sentinel.
            execute("CREATE TABLE x (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(NULL, '2024-01-01T00:00:00.000000Z')," +
                    "('A', '2024-01-01T02:00:00.000000Z')," +
                    "(NULL, '2024-01-01T04:00:00.000000Z')");
            final String query = "SELECT ts, first(sym) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR";
            assertQueryNoLeakCheck(
                    """
                            ts\tfirst
                            2024-01-01T00:00:00.000000Z\t
                            2024-01-01T01:00:00.000000Z\t
                            2024-01-01T02:00:00.000000Z\tA
                            2024-01-01T03:00:00.000000Z\tA
                            2024-01-01T04:00:00.000000Z\t
                            """,
                    query,
                    "ts",
                    false
            );
            Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(), "Sample By Fill"));
        });
    }

    @Test
    public void testFillPrevTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2020-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                        ('2020-06-01T00:00:00.000000Z', '2024-01-01T03:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tv
                            2024-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z
                            2024-01-01T01:00:00.000000Z\t2020-01-01T00:00:00.000000Z
                            2024-01-01T02:00:00.000000Z\t2020-01-01T00:00:00.000000Z
                            2024-01-01T03:00:00.000000Z\t2020-06-01T00:00:00.000000Z
                            """,
                    "SELECT ts, first(v) v FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false);
        });
    }

    @Test
    public void testFillPrevUuidKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (key SYMBOL, u UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('K1', '11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:00.000000Z')," +
                    "('K2', '22222222-2222-2222-2222-222222222222', '2024-01-01T00:00:00.000000Z')," +
                    "('K1', '33333333-3333-3333-3333-333333333333', '2024-01-01T02:00:00.000000Z')");
            final String query = "SELECT ts, key, first(u) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR";
            assertQueryNoLeakCheck(
                    """
                            ts\tkey\tfirst
                            2024-01-01T00:00:00.000000Z\tK1\t11111111-1111-1111-1111-111111111111
                            2024-01-01T00:00:00.000000Z\tK2\t22222222-2222-2222-2222-222222222222
                            2024-01-01T01:00:00.000000Z\tK1\t11111111-1111-1111-1111-111111111111
                            2024-01-01T01:00:00.000000Z\tK2\t22222222-2222-2222-2222-222222222222
                            2024-01-01T02:00:00.000000Z\tK1\t33333333-3333-3333-3333-333333333333
                            2024-01-01T02:00:00.000000Z\tK2\t22222222-2222-2222-2222-222222222222
                            """,
                    query,
                    "ts",
                    false
            );
            Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(), "Sample By Fill"));
        });
    }

    @Test
    public void testFillPrevUuidNonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (u UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '2024-01-01T00:00:00.000000Z')," +
                    "(NULL, '2024-01-01T02:00:00.000000Z')," +
                    "('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', '2024-01-01T04:00:00.000000Z')");
            final String query = "SELECT ts, first(u) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR";
            assertQueryNoLeakCheck(
                    """
                            ts\tfirst
                            2024-01-01T00:00:00.000000Z\taaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                            2024-01-01T01:00:00.000000Z\taaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
                            2024-01-01T02:00:00.000000Z\t
                            2024-01-01T03:00:00.000000Z\t
                            2024-01-01T04:00:00.000000Z\tbbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb
                            """,
                    query,
                    "ts",
                    false
            );
            Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(), "Sample By Fill"));
        });
    }

    @Test
    public void testFillPrevVarcharKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (key SYMBOL, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('K1', 'alpha', '2024-01-01T00:00:00.000000Z')," +
                    "('K2', 'beta', '2024-01-01T00:00:00.000000Z')," +
                    "('K1', 'gamma', '2024-01-01T02:00:00.000000Z')");
            final String query = "SELECT ts, key, first(v) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR";
            assertQueryNoLeakCheck(
                    """
                            ts\tkey\tfirst
                            2024-01-01T00:00:00.000000Z\tK1\talpha
                            2024-01-01T00:00:00.000000Z\tK2\tbeta
                            2024-01-01T01:00:00.000000Z\tK1\talpha
                            2024-01-01T01:00:00.000000Z\tK2\tbeta
                            2024-01-01T02:00:00.000000Z\tK1\tgamma
                            2024-01-01T02:00:00.000000Z\tK2\tbeta
                            """,
                    query,
                    "ts",
                    false
            );
            Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(), "Sample By Fill"));
        });
    }

    @Test
    public void testFillPrevVarcharNonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('ascii-only', '2024-01-01T00:00:00.000000Z')," +
                    "(NULL, '2024-01-01T02:00:00.000000Z')," +
                    "('unicode: \u20ac \u00f1', '2024-01-01T06:00:00.000000Z')");
            final String query = "SELECT ts, first(v) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR";
            assertQueryNoLeakCheck(
                    """
                            ts\tfirst
                            2024-01-01T00:00:00.000000Z\tascii-only
                            2024-01-01T01:00:00.000000Z\tascii-only
                            2024-01-01T02:00:00.000000Z\t
                            2024-01-01T03:00:00.000000Z\t
                            2024-01-01T04:00:00.000000Z\t
                            2024-01-01T05:00:00.000000Z\t
                            2024-01-01T06:00:00.000000Z\tunicode: \u20ac \u00f1
                            """,
                    query,
                    "ts",
                    false
            );
            Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(), "Sample By Fill"));
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
    public void testFillReExecutionKeyed() throws Exception {
        // Re-execute the same keyed FILL factory twice and assert the second
        // cursor produces identical output. Exercises SampleByFillCursor.toTop()
        // and the getCursor() path: every state field reset in toTop() (keysMap
        // clear, hasSimplePrev, simplePrevRowId, hasPendingRow,
        // isBaseCursorExhausted, hasExplicitTo, hasDataForCurrentBucket,
        // isEmittingFills, isInitialized) must be restored so pass-1 runs cleanly
        // against the already-built SortedRecordCursor chain. A regression that
        // forgets to reset one of these fields would only surface here.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('B', 2.0, '2024-01-01T00:00:00.000000Z')," +
                    "('A', 3.0, '2024-01-01T02:00:00.000000Z')");
            final String expected = "ts\tk\tsum\n" +
                    "2024-01-01T00:00:00.000000Z\tA\t1.0\n" +
                    "2024-01-01T00:00:00.000000Z\tB\t2.0\n" +
                    "2024-01-01T01:00:00.000000Z\tA\t1.0\n" +
                    "2024-01-01T01:00:00.000000Z\tB\t2.0\n" +
                    "2024-01-01T02:00:00.000000Z\tA\t3.0\n" +
                    "2024-01-01T02:00:00.000000Z\tB\t2.0\n";
            try (RecordCursorFactory factory = select(
                    "SELECT * FROM (SELECT ts, k, sum(val) FROM t " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR) ORDER BY ts, k")) {
                assertCursor(expected, factory, true, false, false, sqlExecutionContext);
                assertCursor(expected, factory, true, false, false, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testFillReExecutionNonKeyed() throws Exception {
        // Non-keyed sibling of testFillReExecutionKeyed. No keysMap, so
        // toTop() only needs to reset the non-keyed state fields. Regression
        // guard for simplePrevRowId / hasSimplePrev leakage between runs.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, '2024-01-01T02:00:00.000000Z')");
            final String expected = "ts\tfv\n" +
                    "2024-01-01T00:00:00.000000Z\t1.0\n" +
                    "2024-01-01T01:00:00.000000Z\t1.0\n" +
                    "2024-01-01T02:00:00.000000Z\t3.0\n";
            try (RecordCursorFactory factory = select(
                    "SELECT ts, first(val) fv FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR")) {
                assertCursor(expected, factory, false, false, false, sqlExecutionContext);
                assertCursor(expected, factory, false, false, false, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testFillSubDayTimezoneFromDense() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            // Dense data - four rows at London local 00:00, 01:00, 02:00, 03:00
            // (UTC 23:00Z previous day, 00:00Z, 01:00Z, 02:00Z). No gaps, no
            // leading fill. Case C locks in the dense-path behavior that was
            // already correct pre-fix because the firstTs anchor masked the
            // grid divergence. With the fill-range to_utc wrap in place, this
            // test continues to pass and documents the intended invariant.
            execute("""
                    INSERT INTO t VALUES
                        ('2024-05-31T23:00:00.000000Z', 1.0),
                        ('2024-06-01T00:00:00.000000Z', 2.0),
                        ('2024-06-01T01:00:00.000000Z', 3.0),
                        ('2024-06-01T02:00:00.000000Z', 4.0)""");
            assertQueryNoLeakCheck(
                    """
                            ts\tx
                            2024-05-31T23:00:00.000000Z\t1.0
                            2024-06-01T00:00:00.000000Z\t2.0
                            2024-06-01T01:00:00.000000Z\t3.0
                            """,
                    """
                            SELECT ts, sum(x) x FROM t
                            SAMPLE BY 1h FROM '2024-06-01' TO '2024-06-01T03:00:00.000000Z'
                            FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/London'""",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillSubDayTimezoneFromEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        x DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            // No data rows matching the WHERE predicate - empty base cursor.
            // The fill cursor alone emits the 24 hourly buckets at UTC
            // positions that correspond to London 00:00..23:00 local time.
            // During June 2024, London is on BST (UTC+1), so local 00:00 maps
            // to UTC 23:00 of the previous day. Pre-fix the buckets landed at
            // UTC 00:00..23:00 on 2024-06-01 (one-timezone-offset shifted).
            execute("""
                    INSERT INTO t VALUES
                        ('2024-06-01T00:00:00.000000Z', 'other', 1.0)""");
            assertQueryNoLeakCheck(
                    """
                            ts\tx
                            2024-05-31T23:00:00.000000Z\tnull
                            2024-06-01T00:00:00.000000Z\tnull
                            2024-06-01T01:00:00.000000Z\tnull
                            2024-06-01T02:00:00.000000Z\tnull
                            2024-06-01T03:00:00.000000Z\tnull
                            2024-06-01T04:00:00.000000Z\tnull
                            2024-06-01T05:00:00.000000Z\tnull
                            2024-06-01T06:00:00.000000Z\tnull
                            2024-06-01T07:00:00.000000Z\tnull
                            2024-06-01T08:00:00.000000Z\tnull
                            2024-06-01T09:00:00.000000Z\tnull
                            2024-06-01T10:00:00.000000Z\tnull
                            2024-06-01T11:00:00.000000Z\tnull
                            2024-06-01T12:00:00.000000Z\tnull
                            2024-06-01T13:00:00.000000Z\tnull
                            2024-06-01T14:00:00.000000Z\tnull
                            2024-06-01T15:00:00.000000Z\tnull
                            2024-06-01T16:00:00.000000Z\tnull
                            2024-06-01T17:00:00.000000Z\tnull
                            2024-06-01T18:00:00.000000Z\tnull
                            2024-06-01T19:00:00.000000Z\tnull
                            2024-06-01T20:00:00.000000Z\tnull
                            2024-06-01T21:00:00.000000Z\tnull
                            2024-06-01T22:00:00.000000Z\tnull
                            """,
                    """
                            SELECT ts, sum(x) x FROM t
                            WHERE sym = 'never_matches'
                            SAMPLE BY 1h FROM '2024-06-01' TO '2024-06-02'
                            FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/London'""",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillSubDayTimezoneFromSparse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            // Single data row at London 2024-06-01T03:00 local (UTC 02:00Z).
            // Query spans London 00:00..04:00 local via FROM/TO; expected 3
            // leading NULL fills at London 00:00/01:00/02:00 (UTC 23:00Z /
            // 00:00Z / 01:00Z) then the data row at UTC 02:00Z. Pre-fix this
            // missed the London 00:00 bucket entirely and emitted only 2
            // leading NULL fills at shifted UTC positions.
            execute("""
                    INSERT INTO t VALUES
                        ('2024-06-01T02:00:00.000000Z', 42.0)""");
            assertQueryNoLeakCheck(
                    """
                            ts\tx
                            2024-05-31T23:00:00.000000Z\tnull
                            2024-06-01T00:00:00.000000Z\tnull
                            2024-06-01T01:00:00.000000Z\tnull
                            2024-06-01T02:00:00.000000Z\t42.0
                            """,
                    """
                            SELECT ts, sum(x) x FROM t
                            SAMPLE BY 1h FROM '2024-06-01' TO '2024-06-01T04:00:00.000000Z'
                            FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/London'""",
                    "ts", false, false
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
            assertQueryNoLeakCheck(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\t20.0
                            2024-01-01T01:00:00.000000Z\tParis\t21.0
                            2024-01-01T01:00:00.000000Z\tLondon\t0.0
                            2024-01-01T02:00:00.000000Z\tLondon\t11.0
                            2024-01-01T02:00:00.000000Z\tParis\t0.0
                            """,
                    "SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(0) ALIGN TO CALENDAR",
                    "ts", false, false
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
            assertQueryNoLeakCheck(
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
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T06:00:00.000000Z' FILL(0) ALIGN TO CALENDAR",
                    "ts", false, false
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
                    "ts", false, false
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
                    "ts", false, false
            );
        });
    }

    @Test
    public void testSortedRecordCursorFactoryConstructorThrow() throws Exception {
        // Pin the SortedRecordCursorFactory constructor-throw ownership
        // contract. sqlSortKeyMaxPages = -1 makes MemoryPages fire a
        // LimitOverflowException inside `new RecordTreeChain(...)`, which
        // propagates out of the constructor.
        //
        // The constructor must assign this.base only after RecordTreeChain
        // succeeds, and the catch block must null this.base before cascading
        // close(). That way Misc.free(base) becomes a no-op on a
        // constructor-throw path and the caller retains ownership of base.
        //
        // Most QuestDB factory classes make close() idempotent at the
        // native-memory level, so a latent double-free rarely produces
        // observable imbalance. This test locks in the clean
        // exception-propagation path; assertMemoryLeak guards against any
        // future factory whose close() is not idempotent.
        //
        // AbstractCairoTest.tearDown() restores property overrides via
        // Overrides.reset(), so no try/finally restore is needed here.
        // Force the codegen to pick SortedRecordCursorFactory; the default
        // light_encoded path does not exercise this construct-throw contract.
        setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_FILL_SORT_STRATEGY, "full_recordchain");
        setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_PAGES, -1);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (" +
                    "ts TIMESTAMP, " +
                    "k SYMBOL, " +
                    "x DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 'A', 1.0), " +
                    "('2024-01-01T02:00:00.000000Z', 'B', 2.0)");

            try {
                // A keyed SAMPLE BY FILL(PREV) routes through the keyed fast
                // path whose codegen constructs SortedRecordCursorFactory. The
                // sqlSortKeyMaxPages = -1 override causes RecordTreeChain to
                // throw during SortedRecordCursorFactory construction.
                assertQueryNoLeakCheck("", "SELECT ts, k, sum(x) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR");
                fail("expected LimitOverflowException from pathological sqlSortKeyMaxPages");
            } catch (CairoException ex) {
                // D-30 finding 4.7: catch the LimitOverflowException superclass
                // (CairoException) directly so JVM-level Error subclasses
                // propagate instead of being swallowed, and assert a single
                // canonical substring. Both MemoryPARWImpl.java:1266 and
                // MemoryCARWImpl.java:193 produce the same stable page-limit
                // text for sqlSortKeyMaxPages=-1. If the exception is ever
                // re-wrapped to SqlException on the construct path, the typed
                // catch fails loudly instead of hiding the signal under a
                // 6-way substring disjunction.
                TestUtils.assertContains(ex.getFlyweightMessage(), "Maximum number of pages");
            }
        });
    }

    @Test
    public void testRoutingAlignToFirstObservationStaysOnLegacyPath() throws Exception {
        assertMemoryLeak(() -> {
            // ALIGN TO FIRST OBSERVATION forces the legacy cursor path because
            // the fast-path bucket grid depends on timestamp_floor_utc's
            // calendar alignment, which is incompatible with observation-anchored
            // buckets. The plan must show "Sample By" without the "Fill" suffix.
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertPlanNoLeakCheck(
                    "SELECT first(val) FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO FIRST OBSERVATION",
                    """
                            Sample By
                              fill: null
                              values: [first(val)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """
            );
        });
    }

    @Test
    public void testRoutingFillLinearStaysOnInterpolatePath() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(LINEAR) needs forward-looking interpolation that the streaming
            // fast path cannot provide; SqlOptimiser.hasLinearFill disables the
            // rewrite. The plan must show "Sample By" without the "Fill" suffix.
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertPlanNoLeakCheck(
                    "SELECT first(val) FROM x SAMPLE BY 1h FILL(LINEAR) ALIGN TO CALENDAR",
                    """
                            Sample By
                              fill: linear
                              values: [first(val)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """
            );
        });
    }

    @Test
    public void testRoutingFromBindVariableStaysOnLegacyPath() throws Exception {
        assertMemoryLeak(() -> {
            // A bind variable as the FROM lower bound disables the rewriteSampleBy
            // gate in SqlOptimiser (sampleByFrom.type == BIND_VARIABLE). The query
            // must execute on the legacy cursor path and produce correct rows.
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, '2024-01-01T02:00:00.000000Z')");
            bindVariableService.clear();
            // 2024-01-01T00:00:00Z expressed as microseconds since epoch.
            bindVariableService.setTimestamp("lowerBound", 1_704_067_200_000_000L);
            // The query's correct output is the same regardless of which cursor
            // path executes it. If a future refactor routed bind-var FROM through
            // the fast path and broke it, this test would catch the regression.
            assertQueryNoLeakCheck(
                    """
                            first\tts
                            1.0\t2024-01-01T00:00:00.000000Z
                            null\t2024-01-01T01:00:00.000000Z
                            3.0\t2024-01-01T02:00:00.000000Z
                            """,
                    "SELECT first(val), ts FROM x SAMPLE BY 1h FROM :lowerBound TO '2024-01-01T03:00:00.000000Z' FILL(NULL)",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillKeyedEmptyTableNoFromTo() throws Exception {
        assertMemoryLeak(() -> {
            // Keyed SAMPLE BY FILL on an empty table with neither FROM nor TO
            // emits zero rows; pass 1 discovers no keys and the initialize
            // short-circuit at keyCount==0 must terminate cleanly.
            execute("CREATE TABLE x (sym SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertQueryNoLeakCheck(
                    "sym\tsum\tts\n",
                    "SELECT sym, sum(val), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillThreeWayMixedModes() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(PREV, 42.0, NULL) exercises three distinct fill modes on
            // three aggregates in a single query. Each gap bucket must carry
            // the previous value for agg1, the constant 42.0 for agg2, and
            // NULL for agg3. The plan must report "fill: mixed".
            execute("CREATE TABLE x (a DOUBLE, b DOUBLE, c DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, 10.0, 100.0, '2024-01-01T00:00:00.000000Z')," +
                    "(2.0, 20.0, 200.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            first\tfirst1\tfirst2\tts
                            1.0\t10.0\t100.0\t2024-01-01T00:00:00.000000Z
                            1.0\t42.0\tnull\t2024-01-01T01:00:00.000000Z
                            2.0\t20.0\t200.0\t2024-01-01T02:00:00.000000Z
                            """,
                    "SELECT first(a), first(b), first(c), ts FROM x SAMPLE BY 1h FILL(PREV, 42.0, NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
            assertPlanNoLeakCheck(
                    "SELECT first(a), first(b), first(c), ts FROM x SAMPLE BY 1h FILL(PREV, 42.0, NULL) ALIGN TO CALENDAR",
                    """
                            Sample By Fill
                              stride: '1h'
                              fill: mixed
                                Encode sort light
                                  keys: [ts]
                                    Async Group By workers: 1
                                      keys: [ts]
                                      keyFunctions: [timestamp_floor_utc('1h',ts)]
                                      values: [first(a),first(b),first(c)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """
            );
        });
    }

    @Test
    public void testFillWithOffsetAndTimezoneAcrossDst() throws Exception {
        assertMemoryLeak(() -> {
            // Sub-day stride with ALIGN TO CALENDAR + TIME ZONE + WITH OFFSET
            // across Europe/Riga's DST fall-back (2021-10-31 04:00 local clocks
            // go back to 03:00 local). The offset shifts the bucket grid, the
            // timezone adjusts wall-clock buckets, and the fill cursor must
            // produce the expected fill rows without double-applying the offset.
            execute("CREATE TABLE z (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO z VALUES " +
                    "(1.0, '2021-10-31T00:00:00.000000Z')," +
                    "(5.0, '2021-10-31T04:00:00.000000Z')");
            // Buckets are 1h aligned to Riga calendar with a 30-minute offset.
            // Data exists at 00:00Z and 04:00Z; the gap buckets between them
            // are filled with NULL. The query must terminate (no infinite loop
            // under DST fall-back) and return a monotonic, non-overlapping
            // bucket sequence.
            try (
                    RecordCursorFactory factory = select(
                            "SELECT ts, sum(val) FROM z SAMPLE BY 1h FILL(NULL) " +
                                    "ALIGN TO CALENDAR TIME ZONE 'Europe/Riga' WITH OFFSET '00:30'"
                    );
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                long prevTs = Long.MIN_VALUE;
                int rowCount = 0;
                while (cursor.hasNext()) {
                    final Record record = cursor.getRecord();
                    final long ts = record.getTimestamp(0);
                    Assert.assertTrue("bucket sequence not monotonic at row " + rowCount, ts > prevTs);
                    prevTs = ts;
                    rowCount++;
                    // Bounded sanity: even with DST fall-back the output cannot
                    // exceed a handful of buckets in this narrow data range.
                    Assert.assertTrue("runaway fill emission at row " + rowCount, rowCount < 32);
                }
                // At least one row must be emitted (the real data bucket for
                // 00:00Z) and one NULL fill bucket between the two data points.
                Assert.assertTrue("expected multiple buckets, got " + rowCount, rowCount >= 2);
            }
        });
    }

    @Test
    public void testFillWithOffsetAndTimezoneAcrossDstSpringForward() throws Exception {
        assertMemoryLeak(() -> {
            // m8a spring-forward companion to testFillWithOffsetAndTimezoneAcrossDst.
            // Europe/Riga spring-forward fires on 2021-03-28 at 03:00 local:
            // clocks jump from 03:00 EET to 04:00 EEST, so local times 03:00..03:59
            // do not exist on that date. The UTC grid is linear and continuous --
            // timestamp_floor_utc emits buckets at every UTC hour boundary (plus the
            // 30-minute offset) regardless of the wall-clock discontinuity, so the
            // test invariant is that the bucket sequence is strictly monotonic in
            // UTC with no duplicate or extra bucket at the transition instant. The
            // query must terminate (no infinite loop under DST spring-forward) and
            // fill the gap between the pre- and post-transition data rows with
            // NULLs.
            execute("CREATE TABLE z (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // 00:00Z = 02:00 EET pre-transition; 04:00Z = 07:00 EEST post-transition.
            execute("INSERT INTO z VALUES " +
                    "(1.0, '2021-03-28T00:00:00.000000Z')," +
                    "(5.0, '2021-03-28T04:00:00.000000Z')");
            // Buckets land at UTC :30 boundaries because of the 30-minute offset.
            // Between the pre- and post-transition data rows (UTC 23:30 and 03:30)
            // the cursor emits three NULL fills. No bucket corresponds to 03:xx EET
            // wall-clock time because that hour does not exist; but the UTC
            // grid is unaffected.
            assertQueryNoLeakCheck(
                    "ts\tsum\n" +
                            "2021-03-27T23:30:00.000000Z\t1.0\n" +
                            "2021-03-28T00:30:00.000000Z\tnull\n" +
                            "2021-03-28T01:30:00.000000Z\tnull\n" +
                            "2021-03-28T02:30:00.000000Z\tnull\n" +
                            "2021-03-28T03:30:00.000000Z\t5.0\n",
                    "SELECT ts, sum(val) FROM z SAMPLE BY 1h FILL(NULL) " +
                            "ALIGN TO CALENDAR TIME ZONE 'Europe/Riga' WITH OFFSET '00:30'",
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testFillSubDayTimezoneFromOffset() throws Exception {
        // Sub-day SAMPLE BY + FROM + TIME ZONE + WITH OFFSET + FILL(NULL).
        // The fill bucket grid must match timestamp_floor_utc's grid.
        // With FROM='2024-06-01' and TIME ZONE='Europe/London' (BST = UTC+1 in June),
        // to_utc('2024-06-01', 'Europe/London') = 2024-05-31T23:00:00Z. Adding the
        // 30-minute offset yields the grid anchor 2024-05-31T23:30:00Z, so buckets
        // land at UTC ...:30 boundaries. TO='2024-06-01T04:00:00Z' wraps to
        // 2024-06-01T03:00:00Z, so the last bucket is strictly below 03:00Z.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-06-01T01:45:00.000000Z', 42.0)""");
            assertQueryNoLeakCheck(
                    """
                            ts\tx
                            2024-05-31T23:30:00.000000Z\tnull
                            2024-06-01T00:30:00.000000Z\tnull
                            2024-06-01T01:30:00.000000Z\t42.0
                            2024-06-01T02:30:00.000000Z\tnull
                            """,
                    """
                            SELECT ts, sum(x) x FROM t
                            SAMPLE BY 1h FROM '2024-06-01' TO '2024-06-01T04:00:00.000000Z'
                            FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/London' WITH OFFSET '00:30'""",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillSubDayTimezoneFromOffsetEmpty() throws Exception {
        // Sub-day SAMPLE BY + FROM + TIME ZONE + WITH OFFSET + FILL(NULL),
        // but WHERE filter yields zero base rows. The fill cursor must still
        // generate the pure-fill bucket grid anchored at to_utc(FROM, tz) + offset
        // so that leading/middle gap buckets align with timestamp_floor_utc.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-06-01T00:00:00.000000Z', 'other', 1.0)""");
            assertQueryNoLeakCheck(
                    """
                            ts\tx
                            2024-05-31T23:30:00.000000Z\tnull
                            2024-06-01T00:30:00.000000Z\tnull
                            2024-06-01T01:30:00.000000Z\tnull
                            2024-06-01T02:30:00.000000Z\tnull
                            """,
                    """
                            SELECT ts, sum(x) x FROM t
                            WHERE sym = 'never_matches'
                            SAMPLE BY 1h FROM '2024-06-01' TO '2024-06-01T04:00:00.000000Z'
                            FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/London' WITH OFFSET '00:30'""",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillSubDayTimezoneFromOffsetPlan() throws Exception {
        // Guard the Async Group By + Sample By Fill fast path for the
        // sub-day + FROM + TIME ZONE + WITH OFFSET shape. The key-function
        // receives from=to_utc(FROM, tz) as a pre-resolved UTC instant and
        // offset='00:30' directly; no timezone is re-applied inside floor.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-06-01T01:45:00.000000Z', 42.0)");
            assertPlanNoLeakCheck(
                    """
                            SELECT ts, sum(x) x FROM t
                            SAMPLE BY 1h FROM '2024-06-01' TO '2024-06-01T04:00:00.000000Z'
                            FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/London' WITH OFFSET '00:30'""",
                    """
                            Sample By Fill
                              range: (2024-05-31T23:00:00.000000Z,2024-06-01T03:00:00.000000Z)
                              stride: '1h'
                              fill: null
                                Encode sort light
                                  keys: [ts]
                                    Async Group By workers: 1
                                      keys: [ts]
                                      keyFunctions: [timestamp_floor_utc('1h',ts,'2024-05-31T23:30:00.000Z')]
                                      values: [sum(x)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Interval forward scan on: t
                                              intervals: [("2024-05-31T23:00:00.000000Z","2024-06-01T02:59:59.999999Z")]
                            """
            );
        });
    }

    @Test
    public void testSetOpExceptWithFillNull() throws Exception {
        // Smoke test: SAMPLE BY FILL(NULL) under EXCEPT should not crash and
        // should leave the fill cursor's per-key bucket grid intact on each
        // side of the set operation.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (city STRING, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute(
                    "INSERT INTO t VALUES " +
                            "('London', 10.0, '2024-01-01T00:00:00.000000Z'), " +
                            "('London', 20.0, '2024-01-01T02:00:00.000000Z'), " +
                            "('Paris', 10.0, '2024-01-01T00:00:00.000000Z'), " +
                            "('Paris', 40.0, '2024-01-01T03:00:00.000000Z')"
            );
            // London side: 00:00=10, 01:00=null, 02:00=20.
            // Paris side: 00:00=10, 01:00=null, 02:00=null, 03:00=40.
            // EXCEPT (set difference): rows in London not in Paris.
            // 00:00/10 matches; 01:00/null matches (NULL == NULL in EXCEPT).
            // Remaining: 02:00/20.
            assertSql(
                    "ts\tsum\n" +
                            "2024-01-01T02:00:00.000000Z\t20.0\n",
                    "(SELECT ts, sum(val) FROM t WHERE city='London' " +
                            "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR) " +
                            "EXCEPT " +
                            "(SELECT ts, sum(val) FROM t WHERE city='Paris' " +
                            "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR)"
            );
        });
    }

    @Test
    public void testSetOpIntersectWithFillNull() throws Exception {
        // Smoke test: SAMPLE BY FILL(NULL) under INTERSECT. Common rows
        // include NULL-fill buckets where both sides have a gap at the same
        // timestamp.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (city STRING, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute(
                    "INSERT INTO t VALUES " +
                            "('London', 10.0, '2024-01-01T00:00:00.000000Z'), " +
                            "('London', 20.0, '2024-01-01T02:00:00.000000Z'), " +
                            "('Paris', 10.0, '2024-01-01T00:00:00.000000Z'), " +
                            "('Paris', 30.0, '2024-01-01T02:00:00.000000Z')"
            );
            // London buckets: 00:00=10, 01:00=null, 02:00=20.
            // Paris buckets: 00:00=10, 01:00=null, 02:00=30.
            // INTERSECT (set intersection, dedup): rows present on both sides.
            // 00:00/10 and 01:00/null match; 02:00 differs (20 vs 30).
            assertSql(
                    "ts\tsum\n" +
                            "2024-01-01T00:00:00.000000Z\t10.0\n" +
                            "2024-01-01T01:00:00.000000Z\tnull\n",
                    "(SELECT ts, sum(val) FROM t WHERE city='London' " +
                            "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR) " +
                            "INTERSECT " +
                            "(SELECT ts, sum(val) FROM t WHERE city='Paris' " +
                            "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR)"
            );
        });
    }

    @Test
    public void testSetOpUnionAllWithFillNull() throws Exception {
        // Smoke test: SAMPLE BY FILL(NULL) under UNION ALL emits both sides'
        // bucket grids back-to-back, NULL-fill rows preserved per side.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (city STRING, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute(
                    "INSERT INTO t VALUES " +
                            "('London', 10.0, '2024-01-01T00:00:00.000000Z'), " +
                            "('London', 20.0, '2024-01-01T02:00:00.000000Z'), " +
                            "('Paris', 30.0, '2024-01-01T01:00:00.000000Z'), " +
                            "('Paris', 40.0, '2024-01-01T03:00:00.000000Z')"
            );
            // London: 00:00=10, 01:00=null, 02:00=20.
            // Paris: 01:00=30, 02:00=null, 03:00=40.
            // UNION ALL preserves duplicates and emits side 1, then side 2.
            assertSql(
                    "ts\tsum\n" +
                            "2024-01-01T00:00:00.000000Z\t10.0\n" +
                            "2024-01-01T01:00:00.000000Z\tnull\n" +
                            "2024-01-01T02:00:00.000000Z\t20.0\n" +
                            "2024-01-01T01:00:00.000000Z\t30.0\n" +
                            "2024-01-01T02:00:00.000000Z\tnull\n" +
                            "2024-01-01T03:00:00.000000Z\t40.0\n",
                    "(SELECT ts, sum(val) FROM t WHERE city='London' " +
                            "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR) " +
                            "UNION ALL " +
                            "(SELECT ts, sum(val) FROM t WHERE city='Paris' " +
                            "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR)"
            );
        });
    }

    @Test
    public void testFillKeyedLong() throws Exception {
        // LONG key column. Exercises FillRecord.getLong's FILL_KEY arm with a
        // signed-long key carried through gap rows. Two keys, mid-bucket gap,
        // FILL(NULL) -- gap rows must keep the key value, not zero or LONG_NULL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k LONG, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1_000_001L, 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "(2_000_002L, 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "(1_000_001L, 11.0, '2024-01-01T02:00:00.000000Z')," +
                    "(2_000_002L, 21.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tsum
                            2024-01-01T00:00:00.000000Z\t1000001\t10.0
                            2024-01-01T00:00:00.000000Z\t2000002\t20.0
                            2024-01-01T01:00:00.000000Z\t1000001\tnull
                            2024-01-01T01:00:00.000000Z\t2000002\tnull
                            2024-01-01T02:00:00.000000Z\t1000001\t11.0
                            2024-01-01T02:00:00.000000Z\t2000002\t21.0
                            """,
                    "SELECT ts, k, sum(v) FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevCrossColumnStringSource() throws Exception {
        // Cross-column FILL(PREV(s)) where source is a first(STRING) aggregate.
        // STRING is variable-width, so the codegen routes through DISPATCH_PREV_SLOT
        // (recordAt-based) rather than the fixed-size cache slot. Verifies the
        // string aggregate's per-row value carries into the target column on gap rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s STRING, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('alpha', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('beta',  3.0, '2024-01-01T02:00:00.000000Z')");
            // At 00:00 (data row): s1='alpha', tag='alpha'.
            // At 01:00 (gap): s1=PREV(s1)='alpha', tag=PREV(s1)='alpha'.
            // At 02:00 (data row): s1='beta', tag='beta' (data overrides fill).
            assertQueryNoLeakCheck(
                    """
                            ts\ts1\ttag
                            2024-01-01T00:00:00.000000Z\talpha\talpha
                            2024-01-01T01:00:00.000000Z\talpha\talpha
                            2024-01-01T02:00:00.000000Z\tbeta\tbeta
                            """,
                    "SELECT ts, first(s) AS s1, first(s) AS tag " +
                            "FROM x SAMPLE BY 1h FILL(PREV, PREV(s1)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevCrossColumnVarcharSource() throws Exception {
        // Cross-column FILL(PREV(v)) where source is a first(VARCHAR) aggregate.
        // VARCHAR is variable-width, exercising the DISPATCH_PREV_SLOT path with
        // cross-column targeting -- the source's prior value lands in the target
        // through baseCursor.recordAt + prevRecord.getVarcharA dispatch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v VARCHAR, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('alpha', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('beta',  3.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tv1\ttag
                            2024-01-01T00:00:00.000000Z\talpha\talpha
                            2024-01-01T01:00:00.000000Z\talpha\talpha
                            2024-01-01T02:00:00.000000Z\tbeta\tbeta
                            """,
                    "SELECT ts, first(v) AS v1, first(v) AS tag " +
                            "FROM x SAMPLE BY 1h FILL(PREV, PREV(v1)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevNegativeTimestamps() throws Exception {
        // SAMPLE BY FILL with FROM anchored before 1970. The legacy cursor path
        // rejected this with "cannot FILL for the timestamps before 1970"; the
        // GROUP BY fast path routes through timestamp_floor_utc which handles
        // negative timestamps natively. Designated-timestamp column rules
        // forbid storing pre-1970 rows, so the data lives at 1970-01-02 and
        // FROM/TO drives the pre-1970 fill grid.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(10.0, '1970-01-02T00:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfirst
                            1969-12-31T22:00:00.000000Z\tnull
                            1969-12-31T23:00:00.000000Z\tnull
                            1970-01-01T00:00:00.000000Z\tnull
                            1970-01-01T01:00:00.000000Z\tnull
                            1970-01-01T02:00:00.000000Z\tnull
                            1970-01-01T03:00:00.000000Z\tnull
                            1970-01-01T04:00:00.000000Z\tnull
                            1970-01-01T05:00:00.000000Z\tnull
                            1970-01-01T06:00:00.000000Z\tnull
                            1970-01-01T07:00:00.000000Z\tnull
                            1970-01-01T08:00:00.000000Z\tnull
                            1970-01-01T09:00:00.000000Z\tnull
                            1970-01-01T10:00:00.000000Z\tnull
                            1970-01-01T11:00:00.000000Z\tnull
                            1970-01-01T12:00:00.000000Z\tnull
                            1970-01-01T13:00:00.000000Z\tnull
                            1970-01-01T14:00:00.000000Z\tnull
                            1970-01-01T15:00:00.000000Z\tnull
                            1970-01-01T16:00:00.000000Z\tnull
                            1970-01-01T17:00:00.000000Z\tnull
                            1970-01-01T18:00:00.000000Z\tnull
                            1970-01-01T19:00:00.000000Z\tnull
                            1970-01-01T20:00:00.000000Z\tnull
                            1970-01-01T21:00:00.000000Z\tnull
                            1970-01-01T22:00:00.000000Z\tnull
                            1970-01-01T23:00:00.000000Z\tnull
                            1970-01-02T00:00:00.000000Z\t10.0
                            """,
                    "SELECT ts, first(v) FROM t " +
                            "SAMPLE BY 1h FROM '1969-12-31T22:00:00.000000Z' TO '1970-01-02T01:00:00.000000Z' " +
                            "FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillRejectInvalidQuotedTimestamp() throws Exception {
        // Quoted-but-malformed timestamp literal hits parseQuotedLiteral's
        // NumericException catch in generateFill, which rethrows as
        // SqlException("invalid fill value: ..."). Pins the error path on the
        // new fast path; testTimestampFillValueUnquoted (in SampleByTest)
        // covers the unquoted-literal sibling rejection.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, mark TIMESTAMP, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z'::TIMESTAMP, '2024-01-01T00:00:00.000000Z')," +
                    "(2.0, '2024-01-01T01:00:00.000000Z'::TIMESTAMP, '2024-01-01T01:00:00.000000Z')");
            String sql = "SELECT ts, first(val), first(mark) " +
                    "FROM t SAMPLE BY 1h FILL(NULL, 'not-a-timestamp') ALIGN TO CALENDAR";
            int badLiteralPos = sql.indexOf("'not-a-timestamp'");
            assertExceptionNoLeakCheck(sql, badLiteralPos, "invalid fill value: 'not-a-timestamp'");
        });
    }
}
