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
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVarTuple;
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
    public void testExplainFillRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES (1.0, '2024-01-01T01:00:00.000000Z')");
            // FROM+TO drive the range attribute in toPlan().
            assertQuery("SELECT sum(val), ts FROM x " +
                    "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T04:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .assertsPlan("""
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
                            """);
        });
    }

    @Test
    public void testExplainOuterOrderByAggregate() throws Exception {
        // Outer ORDER BY on an aggregate column must survive the SAMPLE BY rewrite:
        // the fill cursor only guarantees ts order, so ordering by a non-ts column
        // must emit a sort node (radix-based "Encode sort" for DOUBLE) on top of
        // Sample By Fill.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES ('London', 10.0, '2024-01-01T00:00:00.000000Z')");
            assertQuery("SELECT ts, city, avg(temp) AS avg FROM weather " +
                    "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR ORDER BY avg")
                    .noLeakCheck()
                    .assertsPlan("""
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
                            """);
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
            assertQuery("SELECT ts, city, avg(temp) FROM weather " +
                    "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR ORDER BY city")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
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
                            """);
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
            assertQuery("SELECT ts, city, avg(temp) FROM weather " +
                    "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR ORDER BY ts")
                    .noLeakCheck()
                    .assertsPlan("""
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
                            """);
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
            assertQuery("SELECT ts, city, avg(temp) FROM weather " +
                    "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR ORDER BY ts, city")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort
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
                            """);
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
            assertQuery("SELECT first(val), ts FROM x SAMPLE BY 1h FILL(cast('42' as DECIMAL(19,0))) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            first\tts
                            1\t2024-01-01T00:00:00.000000Z
                            42\t2024-01-01T01:00:00.000000Z
                            3\t2024-01-01T02:00:00.000000Z
                            """);
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
            assertQuery("SELECT first(val), ts FROM x SAMPLE BY 1h FILL(cast('42' as DECIMAL(39,0))) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            first\tts
                            1\t2024-01-01T00:00:00.000000Z
                            42\t2024-01-01T01:00:00.000000Z
                            3\t2024-01-01T02:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFillConstLong256ValueRejectedAgainstLongTarget() throws Exception {
        assertMemoryLeak(() -> {
            // first(LONG256) silently resolves to first(L) (no first(H) factory
            // exists), so the aggregate output type is LONG. A LONG256 fill
            // expression is not formally convertible to LONG via
            // ColumnType.isConvertibleFrom even though Long256Function.getLong()
            // happens to return the low 64 bits at runtime. The upfront type
            // check correctly rejects this so the user sees a clean error
            // rather than relying on an undocumented narrowing path.
            execute("CREATE TABLE x (val LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(cast('0x01' AS LONG256), '2024-01-01T00:00:00.000000Z')," +
                    "(cast('0x03' AS LONG256), '2024-01-01T02:00:00.000000Z')");
            String sql = "SELECT first(val), ts FROM x SAMPLE BY 1h FILL(cast('0x42' as LONG256)) ALIGN TO CALENDAR";
            assertQuery(sql)
                    .noLeakCheck()
                    .fails(sql.indexOf("cast('0x42'"), "fill value of type LONG256 cannot fill column of type LONG");
        });
    }

    @Test
    public void testFillFromEqualsToAccepted() throws Exception {
        // Companion of testFillFromGreaterThanToRejected: FROM == TO must NOT
        // raise -- a zero-length range is valid (matches HORIZON JOIN RANGE
        // and REFRESH RANGE precedents). The TO bound is exclusive in
        // SAMPLE BY, so FROM == TO yields zero rows; the test asserts empty
        // result rather than the exception path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1.0, '2024-01-05T12:00:00.000000Z')");
            assertQuery("SELECT ts, sum(val) FROM t " +
                    "SAMPLE BY 1d FROM '2024-01-05' TO '2024-01-05' FILL(NULL) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("ts\tsum\n");
        });
    }

    @Test
    public void testFillFromGreaterThanToRejected() throws Exception {
        // Pin SAMPLE BY ... FILL ... FROM '<later>' TO '<earlier>' to a
        // position-pointing SqlException at cursor.of() time. Aligns SAMPLE BY
        // with the existing time-range constructs:
        //   - HORIZON JOIN RANGE     (SqlCodeGenerator.java:1690-1691)
        //     "FROM must be less than or equal to TO"
        //   - REFRESH ... RANGE      (SqlCompilerImpl.java:3281-3283)
        //     "TO timestamp must not be earlier than FROM timestamp"
        // FROM == TO is allowed (single-point range). The check fires only
        // when both bounds are concrete (LONG_NULL on either side leaves the
        // bound unset and the validation no-ops). Covers every fill mode
        // (NULL, PREV, constant) and both keyed and non-keyed paths.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('A', 2.0, '2024-01-05T00:00:00.000000Z')");
            for (String spec : new String[]{
                    "FILL(NULL)",
                    "FILL(PREV)",
                    "FILL(0.0)"
            }) {
                final String sql = "SELECT ts, sum(val) FROM t " +
                        "SAMPLE BY 1d FROM '2024-01-10' TO '2024-01-05' " + spec + " ALIGN TO CALENDAR";
                assertQuery(sql)
                        .noLeakCheck()
                        .fails(sql.indexOf("'2024-01-05'"), "TO timestamp must not be earlier than FROM timestamp");
            }
            // Keyed: FILL(PREV) takes the keyed pass-1 path; same guard fires.
            final String keyedSql = "SELECT ts, k, sum(val) FROM t " +
                    "SAMPLE BY 1d FROM '2024-01-10' TO '2024-01-05' FILL(PREV) ALIGN TO CALENDAR";
            assertQuery(keyedSql)
                    .noLeakCheck()
                    .fails(keyedSql.indexOf("'2024-01-05'"), "TO timestamp must not be earlier than FROM timestamp");
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
            assertQuery("SELECT ts, sum(val) FROM t " +
                    "SAMPLE BY 1h FROM '2024-01-01T05:00:00.000000Z' TO '2024-01-01T06:30:00.000000Z' " +
                    "FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '-00:30'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tsum
                            2024-01-01T04:30:00.000000Z\t1.0
                            2024-01-01T05:30:00.000000Z\t2.0
                            """);
        });
    }

    @Test
    public void testFillFromNegativeOffsetEqualsStride() throws Exception {
        // Parity regression: FILL output must equal non-FILL output plus
        // fill rows for missing labels in non-FILL's set. With FROM=12:00
        // and OFFSET=-01:00 on a 1h stride, FROM lies on the offset-shifted
        // grid (effectiveOffset=11:00, grid={..., 11:00, 12:00, 13:00, ...}).
        // The shortcut fromTs + calendarOffset = 11:00 lies strictly below
        // floor(fromTs) = 12:00; the bucket [11:00, 12:00) does not overlap
        // [FROM=12:00, TO=15:00) and the WHERE filter ts >= 12:00 rejects
        // any data that could land on the 11:00 label. Non-FILL never
        // produces 11:00 for any data placement, so FILL must not either.
        // The Math.max(effectiveOffset, round(fromTs)) fix picks 12:00.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1.0, '2024-01-01T13:00:00.000000Z')");
            assertQuery("SELECT ts, sum(val) FROM t " +
                    "SAMPLE BY 1h FROM '2024-01-01T12:00:00.000000Z' TO '2024-01-01T15:00:00.000000Z' " +
                    "FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '-01:00'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tsum
                            2024-01-01T12:00:00.000000Z\tnull
                            2024-01-01T13:00:00.000000Z\t1.0
                            2024-01-01T14:00:00.000000Z\tnull
                            """);
        });
    }

    @Test
    public void testFillFromNegativeOffsetEqualsStrideEmptyBase() throws Exception {
        // Same parity rule as testFillFromNegativeOffsetEqualsStride applied
        // to the initialize() empty-base branch. With FROM, TO, and OFFSET
        // set but no rows in the base cursor, FILL still emits one fill row
        // per grid label whose bucket overlaps [FROM, TO). The leading
        // bucket at fromTs + calendarOffset must not appear in the empty
        // case either.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SELECT ts, sum(val) FROM t " +
                    "SAMPLE BY 1h FROM '2024-01-01T12:00:00.000000Z' TO '2024-01-01T15:00:00.000000Z' " +
                    "FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '-01:00'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tsum
                            2024-01-01T12:00:00.000000Z\tnull
                            2024-01-01T13:00:00.000000Z\tnull
                            2024-01-01T14:00:00.000000Z\tnull
                            """);
        });
    }

    @Test
    public void testFillFromNegativeOffsetGreaterThanStride() throws Exception {
        // Parity regression with |OFFSET| > stride. effectiveOffset=10:00
        // and floor(fromTs)=12:00 differ by two strides, not one. The
        // current shortcut fromTs + calendarOffset = 10:00 would emit two
        // leading buckets [10:00, 11:00) and [11:00, 12:00), both below
        // FROM and both unreachable to data via the WHERE filter. The
        // Math.max(effectiveOffset, round(fromTs)) expression must pick
        // round(fromTs) = 12:00 here.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1.0, '2024-01-01T13:00:00.000000Z')");
            assertQuery("SELECT ts, sum(val) FROM t " +
                    "SAMPLE BY 1h FROM '2024-01-01T12:00:00.000000Z' TO '2024-01-01T15:00:00.000000Z' " +
                    "FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '-02:00'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tsum
                            2024-01-01T12:00:00.000000Z\tnull
                            2024-01-01T13:00:00.000000Z\t1.0
                            2024-01-01T14:00:00.000000Z\tnull
                            """);
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
            assertQuery("SELECT * FROM (" +
                    "SELECT ts, k, sum(val) FROM t " +
                    "SAMPLE BY 1h FROM '2024-01-01T05:00:00.000000Z' TO '2024-01-01T06:30:00.000000Z' " +
                    "FILL(PREV) ALIGN TO CALENDAR WITH OFFSET '-00:30'" +
                    ") ORDER BY ts, k")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tk\tsum
                            2024-01-01T04:30:00.000000Z\tA\t1.0
                            2024-01-01T04:30:00.000000Z\tB\t2.0
                            2024-01-01T05:30:00.000000Z\tA\t1.0
                            2024-01-01T05:30:00.000000Z\tB\t3.0
                            """);
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
            assertQuery("SELECT ts, sum(val) FROM t " +
                    "SAMPLE BY 1h FROM '2024-01-01T05:00:00.000000Z' " +
                    "FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '-00:30'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tsum
                            2024-01-01T04:30:00.000000Z\t1.0
                            2024-01-01T05:30:00.000000Z\t2.0
                            """);
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
            assertQuery("SELECT ts, sum(val) FROM t " +
                    "SAMPLE BY 1h FROM '2024-01-01T05:00:00.000000Z' TO '2024-01-01T06:30:00.000000Z' " +
                    "FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '+00:30'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tsum
                            2024-01-01T05:30:00.000000Z\t6.0
                            """);
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
            assertQuery("SELECT ts, ip, sum(bytes) FROM traffic SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tip\tsum
                            2024-01-01T00:00:00.000000Z\t10.0.0.1\t100
                            2024-01-01T00:00:00.000000Z\t10.0.0.2\t200
                            2024-01-01T01:00:00.000000Z\t10.0.0.2\t210
                            2024-01-01T01:00:00.000000Z\t10.0.0.1\tnull
                            2024-01-01T02:00:00.000000Z\t10.0.0.1\t110
                            2024-01-01T02:00:00.000000Z\t10.0.0.2\tnull
                            """);
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
            assertQuery("SELECT ts, sum(a), sum(b), sum(c), sum(d), sum(e), sum(f), sum(g) FROM t SAMPLE BY 1h FILL(PREV, PREV, PREV, PREV, 0) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .fails(91, "not enough fill values");
        });
    }

    @Test
    public void testFillKeyedAsOfJoinConsumesFillCursor() throws Exception {
        // SAMPLE BY ... FILL(PREV) inside an ASOF JOIN. The fill factory now
        // exposes recordCursorSupportsRandomAccess=false, a behaviour change
        // vs master that the plan-only ExplainPlanTest snapshots cannot catch.
        // Asserts that the join consumer reads every fill and data row,
        // including rows produced by FILL_PREV emission, and that the ASOF
        // pairing on (sym, ts) lands the correct rate value per row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE rates (sym SYMBOL, rate DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO prices VALUES " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('B', 2.0, '2024-01-01T00:00:00.000000Z')," +
                    "('A', 3.0, '2024-01-01T02:00:00.000000Z')," +
                    "('B', 4.0, '2024-01-01T02:00:00.000000Z')");
            execute("INSERT INTO rates VALUES " +
                    "('A', 1.1, '2024-01-01T00:00:00.000000Z')," +
                    "('B', 2.2, '2024-01-01T00:00:00.000000Z')");
            assertQuery("SELECT * FROM (" +
                    "SELECT f.ts, f.sym, f.fv, r.rate FROM (" +
                    "SELECT ts, sym, first(price) fv FROM prices " +
                    "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR" +
                    ") f ASOF JOIN rates r ON f.sym = r.sym" +
                    ") ORDER BY ts, sym")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tfv\trate
                            2024-01-01T00:00:00.000000Z\tA\t1.0\t1.1
                            2024-01-01T00:00:00.000000Z\tB\t2.0\t2.2
                            2024-01-01T01:00:00.000000Z\tA\t1.0\t1.1
                            2024-01-01T01:00:00.000000Z\tB\t2.0\t2.2
                            2024-01-01T02:00:00.000000Z\tA\t3.0\t1.1
                            2024-01-01T02:00:00.000000Z\tB\t4.0\t2.2
                            """);
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
            assertQuery("SELECT ts, k, sum(v) FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tk\tsum
                            2024-01-01T00:00:00.000000Z\t1.00\t10.0
                            2024-01-01T00:00:00.000000Z\t2.00\t20.0
                            2024-01-01T01:00:00.000000Z\t1.00\tnull
                            2024-01-01T01:00:00.000000Z\t2.00\tnull
                            2024-01-01T02:00:00.000000Z\t1.00\t11.0
                            2024-01-01T02:00:00.000000Z\t2.00\t21.0
                            """);
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
            assertQuery("SELECT ts, k, sum(v) FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tk\tsum
                            2024-01-01T00:00:00.000000Z\t1.00\t10.0
                            2024-01-01T00:00:00.000000Z\t2.00\t20.0
                            2024-01-01T01:00:00.000000Z\t1.00\tnull
                            2024-01-01T01:00:00.000000Z\t2.00\tnull
                            2024-01-01T02:00:00.000000Z\t1.00\t11.0
                            2024-01-01T02:00:00.000000Z\t2.00\t21.0
                            """);
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
                assertQuery("SELECT ts, k, sum(v) FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR")
                        .noLeakCheck()
                        .timestamp("ts")
                        .noRandomAccess()
                        .returns(expected);
            }
        });
    }

    @Test
    public void testFillKeyedEmptyTableNoFromTo() throws Exception {
        assertMemoryLeak(() -> {
            // Keyed SAMPLE BY FILL on an empty table with neither FROM nor TO
            // emits zero rows; pass 1 discovers no keys and the initialize
            // short-circuit at keyCount==0 must terminate cleanly.
            execute("CREATE TABLE x (sym SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SELECT sym, sum(val), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("sym\tsum\tts\n");
        });
    }

    @Test
    public void testFillKeyedInnerJoinConsumesFillCursor() throws Exception {
        // SAMPLE BY ... FILL(PREV) inside an INNER JOIN against a static
        // lookup table. Verifies the join consumer iterates the non-random-
        // access fill cursor without losing rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE labels (sym SYMBOL, name STRING)");
            execute("INSERT INTO prices VALUES " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('B', 2.0, '2024-01-01T00:00:00.000000Z')," +
                    "('A', 3.0, '2024-01-01T02:00:00.000000Z')," +
                    "('B', 4.0, '2024-01-01T02:00:00.000000Z')");
            execute("INSERT INTO labels VALUES ('A', 'AAA'), ('B', 'BBB')");
            assertQuery("SELECT * FROM (" +
                    "SELECT f.ts, f.sym, f.fv, l.name FROM (" +
                    "SELECT ts, sym, first(price) fv FROM prices " +
                    "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR" +
                    ") f INNER JOIN labels l ON f.sym = l.sym" +
                    ") ORDER BY ts, sym")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tfv\tname
                            2024-01-01T00:00:00.000000Z\tA\t1.0\tAAA
                            2024-01-01T00:00:00.000000Z\tB\t2.0\tBBB
                            2024-01-01T01:00:00.000000Z\tA\t1.0\tAAA
                            2024-01-01T01:00:00.000000Z\tB\t2.0\tBBB
                            2024-01-01T02:00:00.000000Z\tA\t3.0\tAAA
                            2024-01-01T02:00:00.000000Z\tB\t4.0\tBBB
                            """);
        });
    }

    @Test
    public void testFillKeyedLeftJoinConsumesFillCursor() throws Exception {
        // SAMPLE BY ... FILL(NULL) inside a LEFT JOIN. One key in the fill
        // result has no matching lookup row -- the LEFT JOIN must still emit
        // the fill row with NULL on the right side. Differs from INNER JOIN:
        // the fill cursor's reduced semantics must not collapse the LEFT
        // JOIN's null-padding behaviour.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE labels (sym SYMBOL, name STRING)");
            execute("INSERT INTO prices VALUES " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('B', 2.0, '2024-01-01T00:00:00.000000Z')," +
                    "('A', 3.0, '2024-01-01T02:00:00.000000Z')," +
                    "('B', 4.0, '2024-01-01T02:00:00.000000Z')");
            execute("INSERT INTO labels VALUES ('A', 'AAA')");
            assertQuery("SELECT * FROM (" +
                    "SELECT f.ts, f.sym, f.fv, l.name FROM (" +
                    "SELECT ts, sym, first(price) fv FROM prices " +
                    "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR" +
                    ") f LEFT JOIN labels l ON f.sym = l.sym" +
                    ") ORDER BY ts, sym")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tfv\tname
                            2024-01-01T00:00:00.000000Z\tA\t1.0\tAAA
                            2024-01-01T00:00:00.000000Z\tB\t2.0\t
                            2024-01-01T01:00:00.000000Z\tA\tnull\tAAA
                            2024-01-01T01:00:00.000000Z\tB\tnull\t
                            2024-01-01T02:00:00.000000Z\tA\t3.0\tAAA
                            2024-01-01T02:00:00.000000Z\tB\t4.0\t
                            """);
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
            assertQuery("SELECT ts, k, sum(v) FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tk\tsum
                            2024-01-01T00:00:00.000000Z\t1000001\t10.0
                            2024-01-01T00:00:00.000000Z\t2000002\t20.0
                            2024-01-01T01:00:00.000000Z\t1000001\tnull
                            2024-01-01T01:00:00.000000Z\t2000002\tnull
                            2024-01-01T02:00:00.000000Z\t1000001\t11.0
                            2024-01-01T02:00:00.000000Z\t2000002\t21.0
                            """);
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
            assertQuery("SELECT ts, k, sum(v) FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tk\tsum
                            2024-01-01T00:00:00.000000Z\t0x01\t10.0
                            2024-01-01T00:00:00.000000Z\t0x02\t20.0
                            2024-01-01T01:00:00.000000Z\t0x01\tnull
                            2024-01-01T01:00:00.000000Z\t0x02\tnull
                            2024-01-01T02:00:00.000000Z\t0x01\t11.0
                            2024-01-01T02:00:00.000000Z\t0x02\t21.0
                            """);
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
            assertQuery("SELECT ts, b, by, c, f, s, sum(v) " +
                    "FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tb\tby\tc\tf\ts\tsum
                            2024-01-01T00:00:00.000000Z\ttrue\t1\ta\t1.5\t100\t10.0
                            2024-01-01T00:00:00.000000Z\tfalse\t2\tb\t2.5\t200\t20.0
                            2024-01-01T01:00:00.000000Z\ttrue\t1\ta\t1.5\t100\tnull
                            2024-01-01T01:00:00.000000Z\tfalse\t2\tb\t2.5\t200\tnull
                            2024-01-01T02:00:00.000000Z\ttrue\t1\ta\t1.5\t100\t11.0
                            2024-01-01T02:00:00.000000Z\tfalse\t2\tb\t2.5\t200\t21.0
                            """);
        });
    }

    @Test
    public void testFillKeyedRespectsCircuitBreaker() throws Exception {
        // A keyed SAMPLE BY 1s FROM '1970' TO '2100' FILL(NULL) iterates
        // billions of buckets, so the fill cursor must check the circuit
        // breaker on every bucket boundary or the query cannot be cancelled
        // before OOM / JUnit wall-clock timeout. The tick-counting
        // MillisecondClock returns 0 until tripWhenTicks ticks have
        // accumulated, then flips to Long.MAX_VALUE so the CB's 1ms query
        // timeout trips deterministically inside the fill-emission loop with
        // the canonical "timeout, query aborted" message.
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
                                    circuitBreakerConfiguration
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
                                try (
                                        RecordCursorFactory factory = compiler.compile(
                                                "SELECT ts, k, first(x) FROM t " +
                                                        "SAMPLE BY 1s FROM '1970-01-01' TO '2100-01-01' FILL(NULL)",
                                                context
                                        ).getRecordCursorFactory();
                                        RecordCursor cursor = factory.getCursor(context)
                                ) {
                                    while (cursor.hasNext()) {
                                        // Drain until the circuit breaker trips on a bucket boundary.
                                    }
                                }
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
                // Null the inherited AbstractCairoTest static field so the
                // tick-counting mock clock does not bleed into any future test
                // in this class that constructs a
                // NetworkSqlExecutionCircuitBreaker against circuitBreakerConfiguration.
                circuitBreakerConfiguration = null;
            }
        });
    }

    @Test
    public void testFillKeyedSingleRowFromTo() throws Exception {
        assertMemoryLeak(() -> {
            // Single-row keyed + FROM/TO: one data row for one key, with FROM
            // strictly before and TO strictly after the row's bucket. Exercises
            // the hasKeyPrev() false->true transition exactly once per key:
            // leading NULL fill rows (hasKeyPrev()==false), then the real row,
            // then trailing forward-fill rows (hasKeyPrev()==true) for the rest
            // of the emission.
            execute("CREATE TABLE t (key VARCHAR, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('A', 42.0, '2024-01-01T03:00:00.000000Z')");
            final String sql = "SELECT ts, key, first(val) val FROM t " +
                    "SAMPLE BY 1h FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-01T06:00:00.000000Z' " +
                    "FILL(PREV) ALIGN TO CALENDAR";
            // 3 leading null rows (no PREV available yet) + the real 42.0 row at
            // 03:00 + 2 trailing PREV rows carrying 42.0. Key column carries 'A'
            // on every row including the leading-null rows (FILL_KEY dispatch).
            assertQuery(sql)
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tkey\tval
                            2024-01-01T00:00:00.000000Z\tA\tnull
                            2024-01-01T01:00:00.000000Z\tA\tnull
                            2024-01-01T02:00:00.000000Z\tA\tnull
                            2024-01-01T03:00:00.000000Z\tA\t42.0
                            2024-01-01T04:00:00.000000Z\tA\t42.0
                            2024-01-01T05:00:00.000000Z\tA\t42.0
                            """);
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
            assertQuery("SELECT ts, k, sum(v) FROM t SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tk\tsum
                            2024-01-01T00:00:00.000000Z\t00000000-0000-0000-0000-000000000001\t10.0
                            2024-01-01T00:00:00.000000Z\t00000000-0000-0000-0000-000000000002\t20.0
                            2024-01-01T01:00:00.000000Z\t00000000-0000-0000-0000-000000000001\tnull
                            2024-01-01T01:00:00.000000Z\t00000000-0000-0000-0000-000000000002\tnull
                            2024-01-01T02:00:00.000000Z\t00000000-0000-0000-0000-000000000001\t11.0
                            2024-01-01T02:00:00.000000Z\t00000000-0000-0000-0000-000000000002\t21.0
                            """);
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
                        assertQuery("SELECT ts, city, avg(temp) FROM weather " +
                                "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR " +
                                "ORDER BY ts, city")
                                .noLeakCheck()
                                .withCompiler(compiler)
                                .withContext(sqlExecutionContext)
                                .timestamp("ts")
                                .returns("""
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
                                        """);
                        assertQuery("SELECT ts, city, last(temp) AS last FROM weather " +
                                "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR " +
                                "ORDER BY ts, city")
                                .noLeakCheck()
                                .withCompiler(compiler)
                                .withContext(sqlExecutionContext)
                                .timestamp("ts")
                                .returns("""
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
                                        """);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testFillKeyedWithParallelWorkersConstantValue() throws Exception {
        // Parallel-workers companion to testFillKeyedWithParallelWorkers for the
        // FILL(<value>) constant-fill path. Each gap row routes through
        // DISPATCH_CONSTANT, which dispatches to the per-column constant fill
        // function. AsyncGroupBy hash-merge order is independent of fill mode,
        // so an outer ORDER BY ts, city pins the assertion to stable
        // semantics under sharedQueryWorkerCount > 1.
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
                        assertQuery("SELECT ts, city, avg(temp) FROM weather " +
                                "SAMPLE BY 1h FILL(-1.0) ALIGN TO CALENDAR " +
                                "ORDER BY ts, city")
                                .noLeakCheck()
                                .withCompiler(compiler)
                                .withContext(sqlExecutionContext)
                                .timestamp("ts")
                                .returns("""
                                        ts\tcity\tavg
                                        2024-01-01T00:00:00.000000Z\tBerlin\t5.0
                                        2024-01-01T00:00:00.000000Z\tLondon\t10.0
                                        2024-01-01T00:00:00.000000Z\tParis\t20.0
                                        2024-01-01T01:00:00.000000Z\tBerlin\t-1.0
                                        2024-01-01T01:00:00.000000Z\tLondon\t-1.0
                                        2024-01-01T01:00:00.000000Z\tParis\t21.0
                                        2024-01-01T02:00:00.000000Z\tBerlin\t6.0
                                        2024-01-01T02:00:00.000000Z\tLondon\t11.0
                                        2024-01-01T02:00:00.000000Z\tParis\t-1.0
                                        """);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testFillKeyedWithParallelWorkersCrossColumnPrev() throws Exception {
        // Parallel-workers companion to testFillKeyedWithParallelWorkers for the
        // FILL(PREV(col)) cross-column path. Each gap row's mirror column
        // routes via DISPATCH_KEY_SLOT (when source is a key) or
        // DISPATCH_PREV_CACHE_SLOT / DISPATCH_PREV_SLOT (when source is an
        // aggregate). The per-key MapValue snapshot underpinning these arms
        // must produce identical results regardless of how AsyncGroupBy
        // distributes data rows across workers; a regression in the
        // worker>1 hash-merge or per-key snapshot ordering would surface here.
        // ORDER BY ts, city makes the assertion independent of intra-bucket
        // emission order.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE weather (city SYMBOL, temp DOUBLE, humidity DOUBLE, ts TIMESTAMP) " +
                                        "TIMESTAMP(ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO weather VALUES " +
                                        "('London', 10.0, 60.0, '2024-01-01T00:00:00.000000Z')," +
                                        "('Paris',  20.0, 65.0, '2024-01-01T00:00:00.000000Z')," +
                                        "('Berlin', 5.0,  70.0, '2024-01-01T00:00:00.000000Z')," +
                                        "('Paris',  21.0, 66.0, '2024-01-01T01:00:00.000000Z')," +
                                        "('London', 11.0, 61.0, '2024-01-01T02:00:00.000000Z')," +
                                        "('Berlin', 6.0,  71.0, '2024-01-01T02:00:00.000000Z')",
                                sqlExecutionContext
                        );
                        // FILL(PREV, PREV(t)) -- gap rows of `h` mirror `t`'s
                        // most recent value per key. At 01:00 London/Berlin
                        // gap rows pull t and h values from 00:00; at 02:00
                        // Paris's gap row pulls from 01:00.
                        assertQuery("SELECT ts, city, last(temp) t, last(humidity) h FROM weather " +
                                "SAMPLE BY 1h FILL(PREV, PREV(t)) ALIGN TO CALENDAR " +
                                "ORDER BY ts, city")
                                .noLeakCheck()
                                .withCompiler(compiler)
                                .withContext(sqlExecutionContext)
                                .timestamp("ts")
                                .returns("""
                                        ts\tcity\tt\th
                                        2024-01-01T00:00:00.000000Z\tBerlin\t5.0\t70.0
                                        2024-01-01T00:00:00.000000Z\tLondon\t10.0\t60.0
                                        2024-01-01T00:00:00.000000Z\tParis\t20.0\t65.0
                                        2024-01-01T01:00:00.000000Z\tBerlin\t5.0\t5.0
                                        2024-01-01T01:00:00.000000Z\tLondon\t10.0\t10.0
                                        2024-01-01T01:00:00.000000Z\tParis\t21.0\t66.0
                                        2024-01-01T02:00:00.000000Z\tBerlin\t6.0\t71.0
                                        2024-01-01T02:00:00.000000Z\tLondon\t11.0\t61.0
                                        2024-01-01T02:00:00.000000Z\tParis\t21.0\t21.0
                                        """);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testFillOffsetInvalidString() throws Exception {
        // Bind variable holding an unparseable offset value: SqlException at
        // cursor.of() time (runtime), pointing at the offset bind-variable
        // position so operators can locate the offending token in the SQL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO test VALUES ('2024-01-01T12:00:00.000000Z', 1.0)");
            drainWalQueue();
            bindVariableService.clear();
            bindVariableService.setStr(0, "not_an_offset");
            final String sql = "SELECT ts, avg(value) FROM test SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR WITH OFFSET $1";
            assertQuery(sql)
                    .noLeakCheck()
                    .fails(sql.indexOf("$1"), "invalid offset: not_an_offset");
        });
    }

    @Test
    public void testFillOffsetZeroLiteralNormalisation() throws Exception {
        // SqlParser.isZeroOffsetToken normalises '00:00', '+00:00' and '-00:00'
        // to the canonical ZERO_OFFSET singleton so downstream identity checks
        // (e.g., the FILL+FROM+OFFSET wrap in SqlOptimiser) treat all three
        // forms as a no-op offset. Regression guard: these three queries must
        // produce byte-identical results.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, '2024-01-01T02:00:00.000000Z')");
            final String expected = """
                    ts\tfv
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T01:00:00.000000Z\t1.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """;
            for (String offset : new String[]{"'00:00'", "'+00:00'", "'-00:00'"}) {
                assertQuery("SELECT ts, first(val) fv FROM x " +
                        "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR WITH OFFSET " + offset)
                        .noLeakCheck()
                        .timestamp("ts")
                        .noRandomAccess()
                        .returns(expected);
            }
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
        final String expected = """
                ts\tk\tsum
                2024-01-01T00:00:00.000000Z\tA\t1.0
                2024-01-01T00:00:00.000000Z\tB\t2.0
                2024-01-01T01:00:00.000000Z\tA\t1.0
                2024-01-01T01:00:00.000000Z\tB\t2.0
                2024-01-01T02:00:00.000000Z\tA\t3.0
                2024-01-01T02:00:00.000000Z\tB\t2.0
                """;
        // returns() re-reads the same factory (assertCursor twice + calculateSize + variable columns),
        // covering the toTop()/getCursor() state-reset contract this test guards.
        assertQuery(
                "SELECT * FROM (SELECT ts, k, sum(val) FROM t " +
                        "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR) ORDER BY ts, k")
                .ddl(
                        "CREATE TABLE t (k SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                        "INSERT INTO t VALUES " +
                                "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                                "('B', 2.0, '2024-01-01T00:00:00.000000Z')," +
                                "('A', 3.0, '2024-01-01T02:00:00.000000Z')"
                )
                .timestamp("ts")
                .returns(expected);
    }

    @Test
    public void testFillReExecutionNonKeyed() throws Exception {
        // Non-keyed sibling of testFillReExecutionKeyed. No keysMap, so
        // toTop() only needs to reset the non-keyed state fields. Regression
        // guard for simplePrevRowId / hasSimplePrev leakage between runs.
        final String expected = """
                ts\tfv
                2024-01-01T00:00:00.000000Z\t1.0
                2024-01-01T01:00:00.000000Z\t1.0
                2024-01-01T02:00:00.000000Z\t3.0
                """;
        assertQuery("SELECT ts, first(val) fv FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR")
                .ddl(
                        "CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                        "INSERT INTO t VALUES " +
                                "(1.0, '2024-01-01T00:00:00.000000Z')," +
                                "(3.0, '2024-01-01T02:00:00.000000Z')"
                )
                .timestamp("ts")
                .noRandomAccess()
                .returns(expected);
    }

    @Test
    public void testFillReExecutionOffsetBindFlipMonthStride() throws Exception {
        // Month-stride sibling of testFillReExecutionOffsetBindFlipZeroNonZero.
        // Drives the same Branch-1 vs Branch-3 split in
        // SampleByFillRecordCursor.initialize but with MonthTimestampMicrosSampler
        // instead of SimpleTimestampSampler. The day-stride sibling cannot exercise
        // the multi-field state (startDay, startMonth, dayMod) that Month and
        // Year samplers carry. Even though the GROUP BY layer (timestamp_floor_utc)
        // pre-floors firstTs to canonical month boundaries -- so setStart always
        // anchors at startDay = 1 and the cursor never observes a non-canonical
        // bucket label -- this test locks the re-execution path through the
        // unified factory's setOffset call site at
        // SampleByFillRecordCursorFactory.java:831 with a Month sampler in the
        // chain. Three flips: 00:00 -> 05:00 -> 00:00.
        // OFFSET = 0: GROUP BY floors data to canonical month start at
        // 00:00. Branch-3 fires (calendarOffset == 0): setStart(firstTs)
        // anchors at the floored value, round() returns it unchanged.
        final String expectedZero = """
                ts\tavg
                2024-05-01T00:00:00.000000Z\t1.0
                2024-06-01T00:00:00.000000Z\tnull
                2024-07-01T00:00:00.000000Z\tnull
                2024-08-01T00:00:00.000000Z\t4.0
                """;
        // OFFSET = 5h: GROUP BY floors data to canonical month start at
        // 05:00. Branch-1 fires (calendarOffset != 0, no FROM):
        // setOffset(5h) writes dayMod and -- with the in-flight fix --
        // also clears startDay. round(firstTs) returns the canonical
        // YYYY-MM-01T05:00 boundary that matches the floored data row.
        final String expectedFive = """
                ts\tavg
                2024-05-01T05:00:00.000000Z\t1.0
                2024-06-01T05:00:00.000000Z\tnull
                2024-07-01T05:00:00.000000Z\tnull
                2024-08-01T05:00:00.000000Z\t4.0
                """;
        // Three flips on the same compiled factory: 00:00 -> 05:00 -> 00:00.
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok("OFFSET 00:00 (Branch-3, calendarOffset == 0)", expectedZero,
                bindVariableService -> bindVariableService.setStr(0, "00:00")));
        cases.add(BindVarTuple.ok("OFFSET 05:00 (Branch-1, calendarOffset != 0)", expectedFive,
                bindVariableService -> bindVariableService.setStr(0, "05:00")));
        cases.add(BindVarTuple.ok("back to OFFSET 00:00 (no sticky state)", expectedZero,
                bindVariableService -> bindVariableService.setStr(0, "00:00")));

        assertQuery("SELECT ts, avg(value) FROM test SAMPLE BY 1M FILL(NULL) ALIGN TO CALENDAR WITH OFFSET $1")
                .ddl(
                        "CREATE TABLE test (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY MONTH WAL",
                        "INSERT INTO test VALUES " +
                                "('2024-05-20T12:00:00.000000Z', 1.0)," +
                                "('2024-08-20T12:00:00.000000Z', 4.0)"
                )
                .timestamp("ts")
                .noRandomAccess()
                .assertBinds(cases);
    }

    @Test
    public void testFillReExecutionOffsetBindFlipYearStride() throws Exception {
        // Year-stride sibling of testFillReExecutionOffsetBindFlipMonthStride.
        // Drives YearTimestampMicrosSampler through the same re-execution path.
        // Year sampler carries (startMonth, startDay, dayMod) -- a strictly
        // larger state surface than the Month sampler -- and the GROUP BY
        // pre-floor anchors firstTs at canonical year start so the cursor
        // never observes a non-canonical bucket label. This test confirms the
        // unified factory tolerates OFFSET re-execution against a year sampler
        // and is the y-stride entry point on the matrix that the day-stride
        // sibling cannot cover.
        // OFFSET = 0: GROUP BY floors to canonical year start at 00:00.
        final String expectedZero = """
                ts\tavg
                2024-01-01T00:00:00.000000Z\t1.0
                2025-01-01T00:00:00.000000Z\tnull
                2026-01-01T00:00:00.000000Z\t4.0
                """;
        // OFFSET = 5h: GROUP BY floors to canonical year start at 05:00.
        // Branch-1 fires; setOffset(5h) writes dayMod and -- with the
        // in-flight fix -- also clears startMonth and startDay. round()
        // returns the canonical YYYY-01-01T05:00 boundary.
        final String expectedFive = """
                ts\tavg
                2024-01-01T05:00:00.000000Z\t1.0
                2025-01-01T05:00:00.000000Z\tnull
                2026-01-01T05:00:00.000000Z\t4.0
                """;
        // Three flips on the same compiled factory: 00:00 -> 05:00 -> 00:00.
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok("OFFSET 00:00 (year sampler, calendarOffset == 0)", expectedZero,
                bindVariableService -> bindVariableService.setStr(0, "00:00")));
        cases.add(BindVarTuple.ok("OFFSET 05:00 (year sampler, calendarOffset != 0)", expectedFive,
                bindVariableService -> bindVariableService.setStr(0, "05:00")));
        cases.add(BindVarTuple.ok("back to OFFSET 00:00 (no sticky state)", expectedZero,
                bindVariableService -> bindVariableService.setStr(0, "00:00")));

        assertQuery("SELECT ts, avg(value) FROM test SAMPLE BY 1y FILL(NULL) ALIGN TO CALENDAR WITH OFFSET $1")
                .ddl(
                        "CREATE TABLE test (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY YEAR WAL",
                        "INSERT INTO test VALUES " +
                                "('2024-05-20T12:00:00.000000Z', 1.0)," +
                                "('2026-05-20T12:00:00.000000Z', 4.0)"
                )
                .timestamp("ts")
                .noRandomAccess()
                .assertBinds(cases);
    }

    @Test
    public void testFillReExecutionOffsetBindFlipZeroNonZero() throws Exception {
        // Re-execute the same compiled FILL factory with an OFFSET bind variable
        // flipping zero <-> non-zero across runs. Exercises Branch-1 vs Branch-3
        // in SampleByFillRecordCursor.initialize: when calendarOffset != 0 and
        // fromTs == LONG_NULL, MonthTimestampMicrosSampler.setOffset (and the
        // year/nano variants) leave startDay/startMonth untouched, so a stale
        // setStart from a previous execute would corrupt the bucket grid.
        // The fix: every of() must reach a clean bucket grid regardless of the
        // prior bind value. Three flips lock the regression: 0 -> 10 -> 0.
        final String expectedZero = """
                ts\tavg
                2024-01-01T00:00:00.000000Z\t1.0
                2024-01-02T00:00:00.000000Z\tnull
                2024-01-03T00:00:00.000000Z\t3.0
                """;
        final String expectedTen = """
                ts\tavg
                2024-01-01T10:00:00.000000Z\t1.0
                2024-01-02T10:00:00.000000Z\tnull
                2024-01-03T10:00:00.000000Z\t3.0
                """;
        // Three flips on the same compiled factory: 0 -> 10 -> 0.
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok("OFFSET 00:00 (zero)", expectedZero,
                bindVariableService -> bindVariableService.setStr(0, "00:00")));
        cases.add(BindVarTuple.ok("OFFSET 10:00 (non-zero)", expectedTen,
                bindVariableService -> bindVariableService.setStr(0, "10:00")));
        cases.add(BindVarTuple.ok("back to OFFSET 00:00 (no stale setStart)", expectedZero,
                bindVariableService -> bindVariableService.setStr(0, "00:00")));

        assertQuery("SELECT ts, avg(value) FROM test SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR WITH OFFSET $1")
                .ddl(
                        "CREATE TABLE test (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL",
                        "INSERT INTO test VALUES " +
                                "('2024-01-01T12:00:00.000000Z', 1.0)," +
                                "('2024-01-03T12:00:00.000000Z', 3.0)"
                )
                .timestamp("ts")
                .noRandomAccess()
                .assertBinds(cases);
    }

    @Test
    public void testFillRejectInvalidOffsetAtRuntime() throws Exception {
        // Bind-variable OFFSET with an unparseable runtime value on a
        // keyed FILL(PREV) shape. The rewriteSampleBy path threads the
        // offset through timestamp_floor_utc as an AGB key function; the
        // function's init() validates the offset and surfaces SqlException
        // before SampleByFillCursor.of() gets to evaluate its own offset
        // copy. Diversifies testFillOffsetInvalidString (WAL + 1d stride)
        // to a non-WAL keyed shape so the error continuity guarantee
        // covers both routes through the rewrite.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (key SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('A', 2.0, '2024-01-01T01:00:00.000000Z')");
            bindVariableService.clear();
            bindVariableService.setStr(0, "not_an_offset");
            assertQuery(
                    "SELECT ts, key, sum(val) FROM x " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR WITH OFFSET $1"
            )
                    .noLeakCheck()
                    .failsWith("invalid offset: not_an_offset");
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
            assertQuery(sql)
                    .noLeakCheck()
                    .fails(badLiteralPos, "invalid fill value: 'not-a-timestamp'");
        });
    }

    @Test
    public void testFillRejectInvalidTimezoneAtRuntime() throws Exception {
        // Bind-variable TIME ZONE with an unparseable runtime value on a
        // keyed FILL(PREV) 1d shape. As with testFillRejectInvalidOffsetAtRuntime,
        // the rewriteSampleBy path threads the timezone through
        // timestamp_floor_utc as an AGB key function; that function's
        // init() validates the zone and throws before SampleByFillCursor.of()
        // gets to its own tz-resolution branch. Diversifies
        // testFillNullTimezoneBindVariableInvalidString (FILL(NULL),
        // sum/ts shape) to a keyed FILL(PREV) shape so error continuity
        // covers both routes through the rewrite.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (key SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('A', 2.0, '2024-01-02T00:00:00.000000Z')");
            bindVariableService.clear();
            bindVariableService.setStr(0, "Mars/Olympus_Mons");
            assertQuery(
                    "SELECT ts, key, sum(val) FROM x " +
                            "SAMPLE BY 1d FILL(PREV) ALIGN TO CALENDAR TIME ZONE $1"
            )
                    .noLeakCheck()
                    .failsWith("invalid timezone: Mars/Olympus_Mons");
        });
    }

    @Test
    public void testFillRejectNonDeterministicFunction() throws Exception {
        // rnd_double() reports isNonDeterministic=true. Without the gate in
        // generateFill, the function would be stored verbatim into constantFills
        // and produce a different value on every cursor read for synthesized
        // fill rows -- not a fill value. The check rejects only non-deterministic
        // functions so deterministic non-folded wrappers like cast(literal) keep
        // working as fill values.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(2.0, '2024-01-01T02:00:00.000000Z')");
            String sql = "SELECT ts, first(val) FROM t " +
                    "SAMPLE BY 1h FILL(rnd_double()) ALIGN TO CALENDAR";
            int badPos = sql.indexOf("rnd_double()");
            assertQuery(sql)
                    .noLeakCheck()
                    .fails(badPos, "fill value must be a constant expression");
        });
    }

    @Test
    public void testFillSingleConstantBroadcastFunctionRejected() throws Exception {
        assertMemoryLeak(() -> {
            // FUNCTION-typed expressions (function calls, casts, bind variables)
            // are deliberately not in the broadcast set: per-aggregate type
            // coercion for those would need to run per target. Pin this boundary
            // -- a single rnd_int() against multiple aggregates must keep
            // hitting the "not enough fill values" path, not silently broadcast.
            execute("CREATE TABLE t (ts TIMESTAMP, a DOUBLE, b DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1, 2)");
            String sql = "SELECT ts, sum(a), sum(b) FROM t SAMPLE BY 1h FILL(rnd_int()) ALIGN TO CALENDAR";
            assertQuery(sql)
                    .noLeakCheck()
                    .fails(sql.indexOf("rnd_int"), "not enough fill values");
        });
    }

    @Test
    public void testFillSingleConstantBroadcastNumeric() throws Exception {
        assertMemoryLeak(() -> {
            // A single numeric CONSTANT fills every non-key aggregate. Two
            // DOUBLE-output aggregates broadcast the INT 0 via the standard
            // INT->DOUBLE convertibility path; the gap bucket emits 0.0 in both
            // columns.
            execute("CREATE TABLE x (a DOUBLE, b DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, 30.0, '2024-01-01T02:00:00.000000Z')");
            assertQuery("SELECT first(a), first(b), ts FROM x SAMPLE BY 1h FILL(0) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            first\tfirst1\tts
                            1.0\t10.0\t2024-01-01T00:00:00.000000Z
                            0.0\t0.0\t2024-01-01T01:00:00.000000Z
                            3.0\t30.0\t2024-01-01T02:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFillSingleConstantBroadcastString() throws Exception {
        assertMemoryLeak(() -> {
            // String CONSTANT broadcasts across two VARCHAR-output aggregates.
            // Verifies the new fast-path broadcast path correctly threads the
            // parsed VARCHAR Function through every non-key column.
            execute("CREATE TABLE x (a VARCHAR, b VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('one', 'ten', '2024-01-01T00:00:00.000000Z')," +
                    "('three', 'thirty', '2024-01-01T02:00:00.000000Z')");
            assertQuery("SELECT first(a), first(b), ts FROM x SAMPLE BY 1h FILL('xx') ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            first\tfirst1\tts
                            one\tten\t2024-01-01T00:00:00.000000Z
                            xx\txx\t2024-01-01T01:00:00.000000Z
                            three\tthirty\t2024-01-01T02:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFillSingleConstantBroadcastTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // Quoted-timestamp CONSTANT broadcasts across two TIMESTAMP-output
            // aggregates. The TIMESTAMP coercion path runs once per aggregate,
            // producing a unit-correct TimestampConstant for each target.
            execute("CREATE TABLE x (a TIMESTAMP, b TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('2024-02-01T00:00:00.000000Z', '2024-03-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z')," +
                    "('2024-02-01T05:00:00.000000Z', '2024-03-01T05:00:00.000000Z', '2024-01-01T02:00:00.000000Z')");
            assertQuery("SELECT first(a), first(b), ts FROM x SAMPLE BY 1h FILL('2024-06-15T00:00:00.000000Z') ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            first\tfirst1\tts
                            2024-02-01T00:00:00.000000Z\t2024-03-01T00:00:00.000000Z\t2024-01-01T00:00:00.000000Z
                            2024-06-15T00:00:00.000000Z\t2024-06-15T00:00:00.000000Z\t2024-01-01T01:00:00.000000Z
                            2024-02-01T05:00:00.000000Z\t2024-03-01T05:00:00.000000Z\t2024-01-01T02:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFillSingleConstantBroadcastTypeMismatch() throws Exception {
        assertMemoryLeak(() -> {
            // INT 0 is convertible to DOUBLE (widening) but not to VARCHAR. A
            // multi-aggregate broadcast where one target type is incompatible
            // surfaces a clean upfront error instead of producing garbage at
            // runtime via the Function dispatch.
            execute("CREATE TABLE x (a DOUBLE, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, 'one', '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, 'three', '2024-01-01T02:00:00.000000Z')");
            String sql = "SELECT first(a), first(s), ts FROM x SAMPLE BY 1h FILL(42) ALIGN TO CALENDAR";
            assertQuery(sql)
                    .noLeakCheck()
                    .fails(sql.indexOf("42"), "fill value of type INT cannot fill column of type VARCHAR");
        });
    }

    @Test
    public void testFillSingleConstantBroadcastUnquotedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(0) against a TIMESTAMP-output aggregate must reject in
            // broadcast mode just as it does per-column. The TIMESTAMP coercion
            // path requires a quoted literal so a unit-correct value can be
            // parsed.
            execute("CREATE TABLE x (a TIMESTAMP, b TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('2024-02-01T00:00:00.000000Z', '2024-03-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z')");
            String sql = "SELECT first(a), first(b), ts FROM x SAMPLE BY 1h FILL(0) ALIGN TO CALENDAR";
            assertQuery(sql)
                    .noLeakCheck()
                    .fails(sql.indexOf("FILL(0)") + 5, "Timestamp fill value must be in quotes");
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
            assertQuery("""
                    SELECT ts, sum(x) x FROM t
                    SAMPLE BY 1h FROM '2024-06-01' TO '2024-06-01T03:00:00.000000Z'
                    FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/London'""")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tx
                            2024-05-31T23:00:00.000000Z\t1.0
                            2024-06-01T00:00:00.000000Z\t2.0
                            2024-06-01T01:00:00.000000Z\t3.0
                            """);
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
            assertQuery("""
                    SELECT ts, sum(x) x FROM t
                    WHERE sym = 'never_matches'
                    SAMPLE BY 1h FROM '2024-06-01' TO '2024-06-02'
                    FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/London'""")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
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
                            """);
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
            assertQuery("""
                    SELECT ts, sum(x) x FROM t
                    SAMPLE BY 1h FROM '2024-06-01' TO '2024-06-01T04:00:00.000000Z'
                    FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/London' WITH OFFSET '00:30'""")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tx
                            2024-05-31T23:30:00.000000Z\tnull
                            2024-06-01T00:30:00.000000Z\tnull
                            2024-06-01T01:30:00.000000Z\t42.0
                            2024-06-01T02:30:00.000000Z\tnull
                            """);
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
            assertQuery("""
                    SELECT ts, sum(x) x FROM t
                    WHERE sym = 'never_matches'
                    SAMPLE BY 1h FROM '2024-06-01' TO '2024-06-01T04:00:00.000000Z'
                    FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/London' WITH OFFSET '00:30'""")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tx
                            2024-05-31T23:30:00.000000Z\tnull
                            2024-06-01T00:30:00.000000Z\tnull
                            2024-06-01T01:30:00.000000Z\tnull
                            2024-06-01T02:30:00.000000Z\tnull
                            """);
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
            assertQuery("""
                    SELECT ts, sum(x) x FROM t
                    SAMPLE BY 1h FROM '2024-06-01' TO '2024-06-01T04:00:00.000000Z'
                    FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/London' WITH OFFSET '00:30'""")
                    .noLeakCheck()
                    .assertsPlan("""
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
                            """);
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
            assertQuery("""
                    SELECT ts, sum(x) x FROM t
                    SAMPLE BY 1h FROM '2024-06-01' TO '2024-06-01T04:00:00.000000Z'
                    FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/London'""")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tx
                            2024-05-31T23:00:00.000000Z\tnull
                            2024-06-01T00:00:00.000000Z\tnull
                            2024-06-01T01:00:00.000000Z\tnull
                            2024-06-01T02:00:00.000000Z\t42.0
                            """);
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
            assertQuery("SELECT first(a), first(b), first(c), ts FROM x SAMPLE BY 1h FILL(PREV, 42.0, NULL) ALIGN TO CALENDAR")
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlan("""
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
                            """)
                    .returns("""
                            first\tfirst1\tfirst2\tts
                            1.0\t10.0\t100.0\t2024-01-01T00:00:00.000000Z
                            1.0\t42.0\tnull\t2024-01-01T01:00:00.000000Z
                            2.0\t20.0\t200.0\t2024-01-01T02:00:00.000000Z
                            """);
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
            // Without the hasExplicitTo LONG_NULL guard, maxTimestamp would be
            // promoted to Long.MAX_VALUE while hasExplicitTo stays true; the
            // zero-base-row path would NOT take the maxTimestamp == LONG_NULL
            // short-circuit, leaving the cursor in an inconsistent state and
            // risking Long.MAX_VALUE-bounded emission in worst cases. With the
            // guard, maxTimestamp stays LONG_NULL, the short-circuit fires, and
            // emission terminates cleanly with zero rows. The test asserts
            // termination (no hang) and a bounded empty result.
            execute("CREATE TABLE t (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('2024-01-01T00:00:00.000000Z', 1.0)," +
                    "('2024-01-01T02:00:00.000000Z', 2.0)");
            bindVariableService.clear();
            bindVariableService.setTimestamp("upperBound", Numbers.LONG_NULL);
            assertQuery("SELECT ts, avg(v) FROM t SAMPLE BY 1h FROM '2024-01-01' TO :upperBound FILL(NULL)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("ts\tavg\n");
        });
    }

    @Test
    public void testFillValueIntCastsToDecimalAggregate() throws Exception {
        // IntFunction has no typed DECIMAL accessor, so SampleByFillRecord would crash at runtime
        // with UnsupportedOperationException. The compiler wraps the fill value in an INT -> DECIMAL
        // implicit cast so it reads correctly at runtime.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_fv_dec_i (d DECIMAL(10,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_fv_dec_i VALUES (1.5::DECIMAL(10,2), 0), (2.5::DECIMAL(10,2), 300_000_000)");
            execute("CREATE TABLE t_fv_dec_i_out AS (SELECT ts, avg(d) AS avg FROM t_fv_dec_i SAMPLE BY 1m FILL(0))");
        });
    }

    @Test
    public void testFillValueIntWidensToLongAggregate() throws Exception {
        // Sanity check: INT -> LONG is a built-in widening cast, no wrapper needed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_fv_long (n INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_fv_long VALUES (1, 0), (2, 300_000_000)");
            execute("CREATE TABLE t_fv_long_out AS (SELECT ts, sum(n) AS s FROM t_fv_long SAMPLE BY 1m FILL(0))");
        });
    }

    @Test
    public void testFillValueRejectedForArrayAggregate() throws Exception {
        // first(array) returns DOUBLE[]. FirstArrayGroupByFunction omits
        // SAMPLE_BY_FILL_VALUE from getSampleByFlags(), so the GroupByUtils
        // flag-validation in assembleGroupByFunctions rejects FILL(VALUE) up
        // front - before SqlCodeGenerator's INT->ARRAY type-coercion path
        // would have produced "fill value of type INT cannot fill column of
        // type DOUBLE[]". Both messages reject the same query; the flag-based
        // one is the active rejection point on the array_agg branch because
        // rewriteSelectClause0 now re-exposes the rewritten FILL list onto
        // groupByModel.sampleByFill for validation.
        assertQuery("SELECT ts, first(a) FROM t_fv_arr SAMPLE BY 1m FILL(0)")
                .ddl("CREATE TABLE t_fv_arr (a DOUBLE[], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY")
                .fails(52, "support for VALUE fill is not yet implemented [function=first(a), class=io.questdb.griffin.engine.functions.groupby.FirstArrayGroupByFunction]");
    }

    @Test
    public void testFillValueRejectedForGeoHashAggregate() throws Exception {
        // first(geohash) returns GEOHASH; no INT -> GEOHASH implicit cast exists.
        assertQuery("SELECT ts, first(g) FROM t_fv_geo SAMPLE BY 1m FILL(0)")
                .ddl("CREATE TABLE t_fv_geo (g GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY")
                .fails(52, "fill value of type INT cannot fill column of type GEOHASH(5c)");
    }

    @Test
    public void testFillValueRejectedForIPv4Aggregate() throws Exception {
        // first(ipv4) returns IPv4; no INT -> IPv4 implicit cast exists, and IntFunction.getIPv4
        // throws UnsupportedOperationException at runtime.
        assertQuery("SELECT ts, first(ip) FROM t_fv_ip SAMPLE BY 1m FILL(0)")
                .ddl("CREATE TABLE t_fv_ip (ip IPv4, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY")
                .fails(52, "fill value of type INT cannot fill column of type IPv4");
    }

    @Test
    public void testFillValueRejectedForLong256Aggregate() throws Exception {
        // sum(long256) returns LONG256; no INT -> LONG256 implicit cast exists.
        assertQuery("SELECT ts, sum(l) FROM t_fv_l256 SAMPLE BY 1m FILL(0)")
                .ddl("CREATE TABLE t_fv_l256 (l LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY")
                .fails(51, "fill value of type INT cannot fill column of type LONG256");
    }

    @Test
    public void testFillValueRejectedForStringAggregate() throws Exception {
        // first(string) returns STRING; no INT -> STRING implicit cast exists.
        assertQuery("SELECT ts, first(s) FROM t_fv_str SAMPLE BY 1m FILL(0)")
                .ddl("CREATE TABLE t_fv_str (s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY")
                .fails(52, "fill value of type INT cannot fill column of type STRING");
    }

    @Test
    public void testFillValueRejectedForUuidAggregate() throws Exception {
        // first(uuid) returns UUID; no INT -> UUID implicit cast exists.
        assertQuery("SELECT ts, first(u) FROM t_fv_uuid SAMPLE BY 1m FILL(0)")
                .ddl("CREATE TABLE t_fv_uuid (u UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY")
                .fails(53, "fill value of type INT cannot fill column of type UUID");
    }

    @Test
    public void testFillValueStringCastsToDecimalAggregate() throws Exception {
        // STRING -> DECIMAL implicit cast exists via CastStrToDecimalFunctionFactory.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_fv_dec_s (d DECIMAL(10,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_fv_dec_s VALUES (1.5::DECIMAL(10,2), 0), (2.5::DECIMAL(10,2), 300_000_000)");
            execute("CREATE TABLE t_fv_dec_s_out AS (SELECT ts, avg(d) AS avg FROM t_fv_dec_s SAMPLE BY 1m FILL('1.5'))");
        });
    }

    @Test
    public void testFillValueWithSumMinusConstantOverFill() throws Exception {
        // SqlOptimiser.rewriteAggregate would normally split sum(c - K) into sum(c) -
        // count(*) * K when K is an integer constant. Under SAMPLE BY FILL the rewrite
        // is unsafe: the per-aggregate FILL value would land on both inner aggregates
        // and the outer arithmetic would yield v - v * K instead of the user-visible
        // v. The optimiser therefore suppresses the rewrite when any FILL is set (see
        // testFillValueAppliesAfterAggregateArithmetic for the multiplicative form).
        // This test pins the no-rewrite plan and verifies the data path still produces
        // the expected fill rows when the inner aggregate computes sum(c - 1000)
        // directly. FILL(0) hides the bug because 0 propagates correctly through any
        // arithmetic; the multiplicative companion in the FillNullValue test class
        // catches the case where FILL(42) does not.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_fv_sum_minus (c SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_fv_sum_minus VALUES (10::SHORT, '2024-01-01T00:00:00.000000Z'), (20::SHORT, '2024-01-01T03:00:00.000000Z')");
            assertQuery("SELECT sum(c - 1000) AS s, ts FROM t_fv_sum_minus SAMPLE BY 1h FILL(0) ALIGN TO CALENDAR")
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlan("""
                            Sample By Fill
                              stride: '1h'
                              fill: value
                                Encode sort light
                                  keys: [ts]
                                    Async Group By workers: 1
                                      keys: [ts]
                                      keyFunctions: [timestamp_floor_utc('1h',ts)]
                                      values: [sum(c-1000)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t_fv_sum_minus
                            """)
                    .returns("""
                            s\tts
                            -990\t2024-01-01T00:00:00.000000Z
                            0\t2024-01-01T01:00:00.000000Z
                            0\t2024-01-01T02:00:00.000000Z
                            -980\t2024-01-01T03:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFillValueWithSumPlusConstantOverFill() throws Exception {
        // Companion to testFillValueWithSumMinusConstantOverFill for the '+' branch:
        // sum(c + K) would split to sum(c) + count(*) * K but the rewrite is suppressed
        // under SAMPLE BY FILL for the same reason. See testFillValueAppliesAfter-
        // AggregateArithmetic for the value-propagation case.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_fv_sum_plus (c SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_fv_sum_plus VALUES (10::SHORT, '2024-01-01T00:00:00.000000Z'), (20::SHORT, '2024-01-01T03:00:00.000000Z')");
            assertQuery("SELECT sum(c + 1000) AS s, ts FROM t_fv_sum_plus SAMPLE BY 1h FILL(0) ALIGN TO CALENDAR")
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlan("""
                            Sample By Fill
                              stride: '1h'
                              fill: value
                                Encode sort light
                                  keys: [ts]
                                    Async Group By workers: 1
                                      keys: [ts]
                                      keyFunctions: [timestamp_floor_utc('1h',ts)]
                                      values: [sum(c+1000)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t_fv_sum_plus
                            """)
                    .returns("""
                            s\tts
                            1010\t2024-01-01T00:00:00.000000Z
                            0\t2024-01-01T01:00:00.000000Z
                            0\t2024-01-01T02:00:00.000000Z
                            1020\t2024-01-01T03:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFillValueWithSumTimesConstantOverFill() throws Exception {
        // Companion for the '*' branch: sum(c * K) would lift the multiplier outside
        // as sum(c) * K, but the rewrite is suppressed under SAMPLE BY FILL because
        // the FILL value would multiply through K in empty buckets. The matching
        // value-propagation test in SampleByFillNullValueTest
        // (testFillValueAppliesAfterAggregateArithmetic) pins FILL(42) -> 42 in
        // empty buckets, not 42 * K.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_fv_sum_mul (c SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t_fv_sum_mul VALUES (10::SHORT, '2024-01-01T00:00:00.000000Z'), (20::SHORT, '2024-01-01T03:00:00.000000Z')");
            assertQuery("SELECT sum(c * 1000) AS s, ts FROM t_fv_sum_mul SAMPLE BY 1h FILL(0) ALIGN TO CALENDAR")
                    .timestamp("ts")
                    .noRandomAccess()
                    .withPlan("""
                            Sample By Fill
                              stride: '1h'
                              fill: value
                                Encode sort light
                                  keys: [ts]
                                    Async Group By workers: 1
                                      keys: [ts]
                                      keyFunctions: [timestamp_floor_utc('1h',ts)]
                                      values: [sum(c*1000)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t_fv_sum_mul
                            """)
                    .returns("""
                            s\tts
                            10000\t2024-01-01T00:00:00.000000Z
                            0\t2024-01-01T01:00:00.000000Z
                            0\t2024-01-01T02:00:00.000000Z
                            20000\t2024-01-01T03:00:00.000000Z
                            """);
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
            // Spring-forward companion to testFillWithOffsetAndTimezoneAcrossDst.
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
            assertQuery("SELECT ts, sum(val) FROM z SAMPLE BY 1h FILL(NULL) " +
                    "ALIGN TO CALENDAR TIME ZONE 'Europe/Riga' WITH OFFSET '00:30'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tsum
                            2021-03-27T23:30:00.000000Z\t1.0
                            2021-03-28T00:30:00.000000Z\tnull
                            2021-03-28T01:30:00.000000Z\tnull
                            2021-03-28T02:30:00.000000Z\tnull
                            2021-03-28T03:30:00.000000Z\t5.0
                            """);
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
            assertQuery("SELECT first(val) FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: null
                              values: [first(val)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);
        });
    }

    @Test
    public void testRoutingFillLinearStaysOnInterpolatePath() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(LINEAR) needs forward-looking interpolation that the streaming
            // fast path cannot provide; SqlOptimiser.hasLinearFill disables the
            // rewrite. The plan must show "Sample By" without the "Fill" suffix.
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SELECT first(val) FROM x SAMPLE BY 1h FILL(LINEAR) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .assertsPlan("""
                            Sample By
                              fill: linear
                              values: [first(val)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);
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
            assertQuery("SELECT first(val), ts FROM x SAMPLE BY 1h FROM :lowerBound TO '2024-01-01T03:00:00.000000Z' FILL(NULL)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            first\tts
                            1.0\t2024-01-01T00:00:00.000000Z
                            null\t2024-01-01T01:00:00.000000Z
                            3.0\t2024-01-01T02:00:00.000000Z
                            """);
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
            assertQuery("(SELECT ts, sum(val) FROM t WHERE city='London' " +
                    "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR) " +
                    "EXCEPT " +
                    "(SELECT ts, sum(val) FROM t WHERE city='Paris' " +
                    "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tsum
                            2024-01-01T02:00:00.000000Z\t20.0
                            """);
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
            assertQuery("(SELECT ts, sum(val) FROM t WHERE city='London' " +
                    "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR) " +
                    "INTERSECT " +
                    "(SELECT ts, sum(val) FROM t WHERE city='Paris' " +
                    "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tsum
                            2024-01-01T00:00:00.000000Z\t10.0
                            2024-01-01T01:00:00.000000Z\tnull
                            """);
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
            assertQuery("(SELECT ts, sum(val) FROM t WHERE city='London' " +
                    "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR) " +
                    "UNION ALL " +
                    "(SELECT ts, sum(val) FROM t WHERE city='Paris' " +
                    "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR)")
                    .noLeakCheck()
                    .timestamp(// UNION ALL output has no designated timestamp column.
                            null)
                    .noRandomAccess()
                    .returns("""
                            ts\tsum
                            2024-01-01T00:00:00.000000Z\t10.0
                            2024-01-01T01:00:00.000000Z\tnull
                            2024-01-01T02:00:00.000000Z\t20.0
                            2024-01-01T01:00:00.000000Z\t30.0
                            2024-01-01T02:00:00.000000Z\tnull
                            2024-01-01T03:00:00.000000Z\t40.0
                            """);
        });
    }

    @Test
    public void testSortedRecordCursorFactoryHandlesKeyHeapOverflow() throws Exception {
        // Pin the SortedRecordCursorFactory cleanup contract for the keyed
        // SAMPLE BY FILL(PREV) full_recordchain path. With a tiny page size
        // and a 1-page sort.key budget, MemoryPages fires a
        // LimitOverflowException once the RecordTreeChain accumulates more
        // than one page of tree nodes. The factory's catch block must free
        // chain + rankMaps before the exception propagates; assertMemoryLeak
        // would surface any unbalanced native allocations.
        //
        // AbstractCairoTest.tearDown() restores property overrides via
        // Overrides.reset(), so no try/finally restore is needed here.
        // Force the codegen to pick SortedRecordCursorFactory; the default
        // light_encoded path does not exercise this cleanup contract.
        setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_FILL_SORT_STRATEGY, "full_recordchain");
        setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_PAGE_SIZE, 64);
        setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 64);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (" +
                    "ts TIMESTAMP, " +
                    "k SYMBOL, " +
                    "x DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");
            // Generate enough distinct (bucket, key) groups to overflow the
            // 64-byte sort.key budget. Each RecordTreeChain node is roughly
            // 41 bytes, so a few hundred rows guarantees the cap fires.
            execute("INSERT INTO t SELECT " +
                    "timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts, " +
                    "rnd_symbol(64, 4, 4, 0) k, " +
                    "rnd_double() x " +
                    "FROM long_sequence(512)");

            try {
                // A keyed SAMPLE BY FILL(PREV) routes through the keyed fast
                // path whose codegen wraps the group-by factory in
                // SortedRecordCursorFactory. As the cursor populates the
                // RecordTreeChain, MemoryPages exhausts its 1-page budget
                // and throws LimitOverflowException. The outer
                // SampleByFillRecordCursorFactory cursor is not random-access,
                // so use .noRandomAccess() here.
                assertQuery("SELECT ts, k, sum(x) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR")
                        .noLeakCheck()
                        .timestamp("ts")
                        .noRandomAccess()
                        .returns("");
                fail("expected LimitOverflowException from constrained sort.key budget");
            } catch (CairoException ex) {
                // Catching the LimitOverflowException superclass (CairoException)
                // directly lets JVM-level Error subclasses propagate instead of
                // being swallowed, and the assertion locks on a single canonical
                // substring shared by MemoryPages overflow paths. If the
                // exception is ever re-wrapped to SqlException, the typed catch
                // fails loudly instead of hiding the signal under a substring
                // disjunction.
                TestUtils.assertContains(ex.getFlyweightMessage(), "Maximum number of pages");
                // The (raise X) hint must name the actual config key that drove the cap,
                // so renaming cairo.sql.sort.key.max.bytes would fail this assertion
                // instead of silently breaking the user-facing remediation guidance.
                TestUtils.assertContains(ex.getFlyweightMessage(), "(raise cairo.sql.sort.key.max.bytes)");
            }
        });
    }

    @Test
    public void testSortedRecordCursorFactoryHandlesValueHeapOverflow() throws Exception {
        // Sibling of testSortedRecordCursorFactoryHandlesKeyHeapOverflow targeting the value chain:
        // sort.key is left uncapped while a 1-page sort.value budget makes the RecordTreeChain's
        // value RecordChain overflow in MemoryCARWImpl. Pins the (raise cairo.sql.sort.value.max.bytes)
        // hint so a property rename fails here instead of silently breaking remediation guidance.
        setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_FILL_SORT_STRATEGY, "full_recordchain");
        setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 64);
        setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES, 64);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (" +
                    "ts TIMESTAMP, " +
                    "k SYMBOL, " +
                    "x DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT " +
                    "timestamp_sequence('2024-01-01T00:00:00.000000Z', 3_600_000_000L) ts, " +
                    "rnd_symbol(64, 4, 4, 0) k, " +
                    "rnd_double() x " +
                    "FROM long_sequence(512)");

            try {
                assertQuery("SELECT ts, k, sum(x) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR")
                        .noLeakCheck()
                        .timestamp("ts")
                        .noRandomAccess()
                        .returns("");
                fail("expected LimitOverflowException from constrained sort.value budget");
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "breached in VirtualMemory");
                TestUtils.assertContains(ex.getFlyweightMessage(), "(raise cairo.sql.sort.value.max.bytes)");
            }
        });
    }
}
