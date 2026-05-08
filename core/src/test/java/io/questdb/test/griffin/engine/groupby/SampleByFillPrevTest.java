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
public class SampleByFillPrevTest extends AbstractCairoTest {

    @Test
    public void testFillPrevDstBerlinSpringForwardDayStride() throws Exception {
        // FILL(PREV) over the spring-forward day. The middle bucket has no
        // data and must carry forward the previous value across the 23 h gap
        // without drifting; the trailing bucket's data row sits at
        // Apr 1 00:00 CEST = 2024-03-31T22:00 UTC, exactly on the
        // post-transition local-midnight boundary.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(10.0, '2024-03-29T23:00:00.000000Z')," +
                    "(30.0, '2024-03-31T22:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            10.0\t2024-03-29T23:00:00.000000Z
                            10.0\t2024-03-30T23:00:00.000000Z
                            30.0\t2024-03-31T22:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x " +
                            "SAMPLE BY 1d FILL(PREV) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevDstBerlinSpringForwardDayStrideWithOffset() throws Exception {
        // FILL(PREV) over the spring-forward day with OFFSET '06:00'. The
        // middle bucket has no data and must carry forward the previous value
        // across the 23 h gap without drifting against the GROUP BY's
        // timestamp_floor_utc grid.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(10.0, '2024-03-30T06:00:00.000000Z'),"
                    + "(30.0, '2024-04-01T05:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            10.0\t2024-03-30T05:00:00.000000Z
                            10.0\t2024-03-31T04:00:00.000000Z
                            30.0\t2024-04-01T04:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x "
                            + "SAMPLE BY 1d FILL(PREV) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' WITH OFFSET '06:00'",
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
        // 2 x 3 = 6 cartesian rows.
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
        // produce 2 x 3 = 6 cartesian rows.
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
        // 2 x 3 = 6 cartesian rows. Pins the OPERATION branch of the classifier
        // predicate.
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
    public void testFillPrevCrossColumnDecimalScaleMismatch() throws Exception {
        // Sibling of testFillPrevCrossColumnDecimalPrecisionMismatch: source
        // and target share precision 10 but their scales differ (2 vs 4). Both
        // happen to land on the DECIMAL64 physical width, so a tag-only check
        // (DECIMAL == DECIMAL) would silently accept and feed scale-2 longs
        // into a scale-4 target -- a 100x value drift. needsExactTypeMatch
        // demands targetType == sourceType so the scale carried in the type's
        // high bits is part of the comparison; the rejection fires here.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        ts TIMESTAMP,
                        a DECIMAL(10, 2),
                        b DECIMAL(10, 4)
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("""
                    INSERT INTO t VALUES
                        ('2024-01-01T00:00:00.000000Z', 12.34::DECIMAL(10,2), 56.7890::DECIMAL(10,4))""");
            String sql = "SELECT ts, first(a) a, first(b) b FROM t SAMPLE BY 1h FILL(PREV, PREV(a)) ALIGN TO CALENDAR";
            int prevArgPos = sql.indexOf("a", sql.indexOf("PREV(a)") + 5);
            assertExceptionNoLeakCheck(sql, prevArgPos, "cannot fill target column of type DECIMAL(10,4)");
            assertExceptionNoLeakCheck(sql, prevArgPos, "source type DECIMAL(10,2)");
        });
    }

    @Test
    public void testFillPrevCrossColumnIntervalUnitMismatchUnreachable() throws Exception {
        // Sibling-of-record for testFillPrevCrossColumnTimestampUnitMismatch.
        // generateFill's needsExactTypeMatch arm includes
        // targetTag == ColumnType.INTERVAL alongside TIMESTAMP, so a future
        // INTERVAL_MICRO -> INTERVAL_NANO cross-col PREV would reject up front.
        // The arm is currently defensive: there is no INTERVAL aggregate
        // factory (FirstIntervalGroupByFunction etc.), and INTERVAL outputs
        // produced by interval(lo, hi) live as GROUP BY keys, which auto-route
        // through FILL_KEY without consulting the cross-col rejection check.
        // This test pins down both halves of that architectural state:
        //   1. mixed-unit INTERVAL keys coexist under FILL(PREV) without
        //      rejection, because both columns are FILL_KEY;
        //   2. first(interval(...)) currently produces STRING, not INTERVAL,
        //      so the cross-col PREV rejection arm cannot be reached through
        //      current SQL grammar. If a real INTERVAL aggregate ever lands,
        //      the second assertion will start failing and prompt a paired
        //      cross-unit rejection test mirroring the TIMESTAMP variant.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (loA TIMESTAMP, hiA TIMESTAMP, loB TIMESTAMP_NS, hiB TIMESTAMP_NS, v DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('2020-01-01T00:00:00.000000Z', '2020-02-01T00:00:00.000000Z', " +
                    " '2021-03-01T00:00:00.000000000Z'::TIMESTAMP_NS, '2021-04-01T00:00:00.000000000Z'::TIMESTAMP_NS, " +
                    " 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('2020-01-01T00:00:00.000000Z', '2020-02-01T00:00:00.000000Z', " +
                    " '2021-03-01T00:00:00.000000000Z'::TIMESTAMP_NS, '2021-04-01T00:00:00.000000000Z'::TIMESTAMP_NS, " +
                    " 30.0, '2024-01-01T02:00:00.000000Z')");
            // (1) mixed-unit INTERVAL keys coexist under FILL(PREV).
            assertQueryNoLeakCheck(
                    """
                            ts\tkA\tkB\tfv
                            2024-01-01T00:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t('2021-03-01T00:00:00.000Z', '2021-04-01T00:00:00.000Z')\t10.0
                            2024-01-01T01:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t('2021-03-01T00:00:00.000Z', '2021-04-01T00:00:00.000Z')\t10.0
                            2024-01-01T02:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t('2021-03-01T00:00:00.000Z', '2021-04-01T00:00:00.000Z')\t30.0
                            """,
                    "SELECT ts, interval(loA, hiA) kA, interval(loB, hiB) kB, first(v) fv FROM t " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
            // (2) first(interval(...)) currently produces STRING -- no real
            //     INTERVAL aggregate is registered, so the grammar path that
            //     would reach the INTERVAL needsExactTypeMatch arm is closed.
            try (RecordCursorFactory factory = select(
                    "SELECT ts, first(interval(loA, hiA)) a, first(interval(loB, hiB)) b FROM t " +
                            "SAMPLE BY 1h ALIGN TO CALENDAR")) {
                final io.questdb.cairo.sql.RecordMetadata meta = factory.getMetadata();
                Assert.assertEquals("first(interval(...)) is expected to produce STRING until a real INTERVAL aggregate lands",
                        io.questdb.cairo.ColumnType.STRING, meta.getColumnType(meta.getColumnIndex("a")));
                Assert.assertEquals("first(interval(...)) is expected to produce STRING until a real INTERVAL aggregate lands",
                        io.questdb.cairo.ColumnType.STRING, meta.getColumnType(meta.getColumnIndex("b")));
            }
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
    public void testFillPrevDecimal128Keyed() throws Exception {
        // Keyed FILL(PREV) on DECIMAL128 exercises the prev-cache fast path
        // -- the per-key prev value lives in the keys-map slot directly,
        // read via DISPATCH_PREV_CACHE_SLOT. Sparse keys and a NULL row in
        // the middle stress null-sentinel handling.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL, d DECIMAL(20, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                        ('A', 12.34::DECIMAL(20,2), '2024-01-01T00:00:00.000000Z'),
                        ('B', 99.99::DECIMAL(20,2), '2024-01-01T00:00:00.000000Z'),
                        ('A', NULL, '2024-01-01T02:00:00.000000Z'),
                        ('A', 56.78::DECIMAL(20,2), '2024-01-01T04:00:00.000000Z'),
                        ('B', 11.11::DECIMAL(20,2), '2024-01-01T04:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tsym\tfirst
                            2024-01-01T00:00:00.000000Z\tA\t12.34
                            2024-01-01T00:00:00.000000Z\tB\t99.99
                            2024-01-01T01:00:00.000000Z\tA\t12.34
                            2024-01-01T01:00:00.000000Z\tB\t99.99
                            2024-01-01T02:00:00.000000Z\tA\t
                            2024-01-01T02:00:00.000000Z\tB\t99.99
                            2024-01-01T03:00:00.000000Z\tA\t
                            2024-01-01T03:00:00.000000Z\tB\t99.99
                            2024-01-01T04:00:00.000000Z\tA\t56.78
                            2024-01-01T04:00:00.000000Z\tB\t11.11
                            """,
                    "SELECT ts, sym, first(d) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testFillPrevDecimal128KeyedHighPrecision() throws Exception {
        // High-precision DECIMAL128 (38,9) round-trips through the cache slot.
        // Slot stores 16 bytes; precision/scale travel through srcType in
        // mapValueTypes and resurface in the output metadata.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL, d DECIMAL(38, 9), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                        ('A', 1234567890.123456789::DECIMAL(38,9), '2024-01-01T00:00:00.000000Z'),
                        ('B', 0.000000001::DECIMAL(38,9), '2024-01-01T00:00:00.000000Z'),
                        ('A', 9876543210.987654321::DECIMAL(38,9), '2024-01-01T03:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tsym\tfirst
                            2024-01-01T00:00:00.000000Z\tA\t1234567890.123456789
                            2024-01-01T00:00:00.000000Z\tB\t0.000000001
                            2024-01-01T01:00:00.000000Z\tA\t1234567890.123456789
                            2024-01-01T01:00:00.000000Z\tB\t0.000000001
                            2024-01-01T02:00:00.000000Z\tA\t1234567890.123456789
                            2024-01-01T02:00:00.000000Z\tB\t0.000000001
                            2024-01-01T03:00:00.000000Z\tA\t9876543210.987654321
                            2024-01-01T03:00:00.000000Z\tB\t0.000000001
                            """,
                    "SELECT ts, sym, first(d) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testFillPrevDecimal128KeyedNullOnly() throws Exception {
        // A keyed group with all-NULL source rows: leading and trailing fills
        // must render as NULL (the cached null sentinel from the init switch).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL, d DECIMAL(20, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                        ('A', NULL, '2024-01-01T00:00:00.000000Z'),
                        ('A', NULL, '2024-01-01T02:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tsym\tfirst
                            2024-01-01T00:00:00.000000Z\tA\t
                            2024-01-01T01:00:00.000000Z\tA\t
                            2024-01-01T02:00:00.000000Z\tA\t
                            """,
                    "SELECT ts, sym, first(d) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false
            );
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
    public void testFillPrevDecimal256Keyed() throws Exception {
        // Keyed FILL(PREV) on DECIMAL256 (32-byte slot). Two sparse keys,
        // multiple gaps, mixed-precision values.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL, d DECIMAL(70, 18), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                        ('A', 100.500000000000000001::DECIMAL(70,18), '2024-01-01T00:00:00.000000Z'),
                        ('B', 200.000000000000000002::DECIMAL(70,18), '2024-01-01T00:00:00.000000Z'),
                        ('A', 300.750000000000000003::DECIMAL(70,18), '2024-01-01T03:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tsym\tfirst
                            2024-01-01T00:00:00.000000Z\tA\t100.500000000000000001
                            2024-01-01T00:00:00.000000Z\tB\t200.000000000000000002
                            2024-01-01T01:00:00.000000Z\tA\t100.500000000000000001
                            2024-01-01T01:00:00.000000Z\tB\t200.000000000000000002
                            2024-01-01T02:00:00.000000Z\tA\t100.500000000000000001
                            2024-01-01T02:00:00.000000Z\tB\t200.000000000000000002
                            2024-01-01T03:00:00.000000Z\tA\t300.750000000000000003
                            2024-01-01T03:00:00.000000Z\tB\t200.000000000000000002
                            """,
                    "SELECT ts, sym, first(d) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false
            );
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
        // produce 2 x 3 = 6 cartesian rows. A bug had the classifier treat the
        // function-valued key as an aggregate, drop it from keyColIndices, and
        // collapse output to 3 rows.
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
    public void testFillPrevKeyedDenseBucketsEmitNoFillRows() throws Exception {
        // Pin the dense-bucket fast-path: when every key has a data row in
        // every bucket (toEmitCnt == 0 at the start of bucket emission),
        // SampleByFillRecordCursor short-circuits, advances to the next
        // bucket and resets keyPresent without invoking emitNextFillRow.
        // Visible end-to-end as: row count == data row count, no FILL rows
        // injected. Regression guard for any change to the toEmitCnt
        // bookkeeping (#6, #93) that could leak gap rows into a dense bucket.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "('London', 11.0, '2024-01-01T01:00:00.000000Z')," +
                    "('Paris', 21.0, '2024-01-01T01:00:00.000000Z')," +
                    "('London', 12.0, '2024-01-01T02:00:00.000000Z')," +
                    "('Paris', 22.0, '2024-01-01T02:00:00.000000Z')");
            // 3 buckets x 2 keys, all dense -> exactly 6 rows out, no fills.
            assertQueryNoLeakCheck(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\t20.0
                            2024-01-01T01:00:00.000000Z\tLondon\t11.0
                            2024-01-01T01:00:00.000000Z\tParis\t21.0
                            2024-01-01T02:00:00.000000Z\tLondon\t12.0
                            2024-01-01T02:00:00.000000Z\tParis\t22.0
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, city, avg(temp) FROM weather " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, city",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testFillPrevKeyedHighCardinalityCorrectness() throws Exception {
        // High-cardinality (10_000 distinct keys) keyed FILL(PREV) correctness.
        // Every key has a single data row in bucket 0; FROM..TO covers 5 buckets,
        // so the fill cursor must emit one row per (bucket, key) for buckets
        // 1..4 carrying the prev value forward. Asserts (a) total row count is
        // 10_000 * 5 = 50_000 (no drops or duplicates from toEmitCnt-- under
        // duplicate (bucket, key) handling, no leaks from emitNextFillRow),
        // (b) every fill row carries the right prev value (sum invariant).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t " +
                    "SELECT 'k_' || x, 1.0, '2024-01-01T00:00:00.000000Z'::TIMESTAMP " +
                    "FROM long_sequence(10000)");
            assertQueryNoLeakCheck(
                    "c\ts\tmn\tmx\n50000\t50000.0\t1.0\t1.0\n",
                    "SELECT count(*) c, sum(fv) s, min(fv) mn, max(fv) mx FROM (" +
                            "SELECT ts, k, first(val) fv FROM t " +
                            "SAMPLE BY 1h FROM '2024-01-01T00:00:00.000000Z' " +
                            "TO '2024-01-01T05:00:00.000000Z' FILL(PREV) ALIGN TO CALENDAR" +
                            ")",
                    null, false, true
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
    public void testFillPrevLong256Keyed() throws Exception {
        // Keyed FILL(PREV) on LONG256 (32-byte slot) exercises the cache
        // fast path. Two sparse keys, multiple gaps, plus a NULL row in
        // one key's history to exercise null-sentinel handling.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL, h LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                        ('A', '0x10', '2024-01-01T00:00:00.000000Z'),
                        ('B', '0xabcdef', '2024-01-01T00:00:00.000000Z'),
                        ('A', NULL, '2024-01-01T02:00:00.000000Z'),
                        ('A', '0xff00', '2024-01-01T04:00:00.000000Z'),
                        ('B', '0x123456', '2024-01-01T04:00:00.000000Z')""");
            assertQueryNoLeakCheck(
                    """
                            ts\tsym\tsum
                            2024-01-01T00:00:00.000000Z\tA\t0x10
                            2024-01-01T00:00:00.000000Z\tB\t0xabcdef
                            2024-01-01T01:00:00.000000Z\tA\t0x10
                            2024-01-01T01:00:00.000000Z\tB\t0xabcdef
                            2024-01-01T02:00:00.000000Z\tA\t
                            2024-01-01T02:00:00.000000Z\tB\t0xabcdef
                            2024-01-01T03:00:00.000000Z\tA\t
                            2024-01-01T03:00:00.000000Z\tB\t0xabcdef
                            2024-01-01T04:00:00.000000Z\tA\t0xff00
                            2024-01-01T04:00:00.000000Z\tB\t0x123456
                            """,
                    "SELECT ts, sym, sum(h) FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testFillPrevLong256NoPrevYet() throws Exception {
        // FillRecord.getLong256(int, CharSink<?>) must render null on leading
        // FILL(PREV) rows where hasKeyPrev() == false. Unlike
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
    public void testFillPrevRejectConstantSource() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(0, PREV(a)) -- column 'a' is filled with a constant on gaps,
            // so its user-visible value diverges from the base-cursor read
            // PREV(a) resolves to. Reject by analogy with FILL(PREV) chains
            // until we either materialise synthesised values into the snapshot
            // or commit to "PREV reads last real aggregate" as documented
            // semantics.
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, 30.0, '2024-01-01T02:00:00.000000Z')");
            final String sql = "SELECT ts, sum(a) a, sum(b) b FROM t SAMPLE BY 1h FILL(0, PREV(a)) ALIGN TO CALENDAR";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("PREV(a)"),
                    "FILL(PREV) cannot reference a column that is itself filled with a constant"
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
    public void testFillPrevRejectNullSource() throws Exception {
        assertMemoryLeak(() -> {
            // FILL(NULL, PREV(a)) -- same ambiguity as a literal constant. The
            // user-visible 'a' on a gap is NULL, but PREV(a) reads a's last
            // real aggregate from the base cursor.
            execute("CREATE TABLE t (a DOUBLE, b DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(1.0, 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, 30.0, '2024-01-01T02:00:00.000000Z')");
            final String sql = "SELECT ts, sum(a) a, sum(b) b FROM t SAMPLE BY 1h FILL(NULL, PREV(a)) ALIGN TO CALENDAR";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("PREV(a)"),
                    "FILL(PREV) cannot reference a column that is itself filled with a constant"
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
    public void testFillPrevSortStrategiesReExecuteIdenticalOutput() throws Exception {
        // Re-execute the same compiled FILL factory under each
        // SampleBySortStrategy and assert output is byte-identical across
        // re-executions. The sort wrapper for each strategy is a different
        // cursor: light_encoded -> EncodedSortLightRecordCursor,
        // full_encoded -> EncodedSortRecordCursor,
        // light_recordchain -> SortedLightRecordCursor,
        // full_recordchain -> SortedRecordCursor.
        // Each cursor's of() reuse path (chain.clear vs chain.reopen, plus
        // SortKeyEncoder.buildRankMaps) must leave no state from the prior
        // run. A regression in any sort cursor's reuse path corrupts the
        // FILL output on the second execute -- this test catches it
        // end-to-end without depending on cursor-internal reflection.
        final String[] strategies = {"light_encoded", "full_encoded", "light_recordchain", "full_recordchain"};
        final String expected = """
                ts\tk\tsum
                2024-01-01T00:00:00.000000Z\tA\t1.0
                2024-01-01T00:00:00.000000Z\tB\t2.0
                2024-01-01T01:00:00.000000Z\tA\t1.0
                2024-01-01T01:00:00.000000Z\tB\t2.0
                2024-01-01T02:00:00.000000Z\tA\t3.0
                2024-01-01T02:00:00.000000Z\tB\t2.0
                """;
        try {
            for (String strategy : strategies) {
                setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_FILL_SORT_STRATEGY, strategy);
                assertMemoryLeak(() -> {
                    execute("DROP TABLE IF EXISTS t");
                    execute("CREATE TABLE t (k SYMBOL, val DOUBLE, ts TIMESTAMP) " +
                            "TIMESTAMP(ts) PARTITION BY DAY");
                    execute("INSERT INTO t VALUES " +
                            "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                            "('B', 2.0, '2024-01-01T00:00:00.000000Z')," +
                            "('A', 3.0, '2024-01-01T02:00:00.000000Z')");
                    try (RecordCursorFactory factory = select(
                            "SELECT * FROM (SELECT ts, k, sum(val) FROM t " +
                                    "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR) ORDER BY ts, k")) {
                        assertCursor(expected, factory, true, false, false, sqlExecutionContext);
                        assertCursor(expected, factory, true, false, false, sqlExecutionContext);
                    }
                });
            }
        } finally {
            setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_FILL_SORT_STRATEGY, "light_encoded");
        }
    }

    @Test
    public void testFillPrevSortStrategiesProduceIdenticalOutput() throws Exception {
        // Pin the prevRecord/baseRecord independence contract: the fill cursor
        // captures prevRecord = baseCursor.getRecordB() and interleaves reads of
        // baseRecord (current row) and prevRecord (saved rowId) on every fill
        // emission. Each SampleBySortStrategy wraps the GROUP BY base in a
        // different sort cursor, but recordA and recordB must remain independent
        // for the interleave to be correct. AsyncGroupByRecordCursor satisfies
        // this today via separate VirtualRecord instances (recordA/recordB);
        // a future refactor that collapses them into shared state would break
        // every strategy on this test.
        //
        // Two keys with offset gaps so pass 2 emits fill rows that read both
        // baseRecord (the current data row's columns) and prevRecord (the
        // carried-forward column for the absent key) on the same row.
        final String[] strategies = {"light_encoded", "full_encoded", "light_recordchain", "full_recordchain"};
        final String expected = """
                ts\tk\ta\tb
                2024-01-01T00:00:00.000000Z\tA\t1.0\t10.0
                2024-01-01T00:00:00.000000Z\tB\tnull\tnull
                2024-01-01T01:00:00.000000Z\tA\t1.0\t10.0
                2024-01-01T01:00:00.000000Z\tB\t2.0\t20.0
                2024-01-01T02:00:00.000000Z\tA\t3.0\t30.0
                2024-01-01T02:00:00.000000Z\tB\t2.0\t20.0
                2024-01-01T03:00:00.000000Z\tA\t3.0\t30.0
                2024-01-01T03:00:00.000000Z\tB\t4.0\t40.0
                """;
        try {
            for (String strategy : strategies) {
                setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_FILL_SORT_STRATEGY, strategy);
                assertMemoryLeak(() -> {
                    execute("DROP TABLE IF EXISTS t");
                    execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL, a DOUBLE, b DOUBLE) " +
                            "TIMESTAMP(ts) PARTITION BY DAY");
                    execute("INSERT INTO t VALUES " +
                            "('2024-01-01T00:00:00.000000Z', 'A', 1.0, 10.0), " +
                            "('2024-01-01T01:00:00.000000Z', 'B', 2.0, 20.0), " +
                            "('2024-01-01T02:00:00.000000Z', 'A', 3.0, 30.0), " +
                            "('2024-01-01T03:00:00.000000Z', 'B', 4.0, 40.0)");
                    assertQueryNoLeakCheck(
                            expected,
                            "SELECT ts, k, sum(a) AS a, sum(b) AS b FROM t " +
                                    "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR ORDER BY ts, k",
                            "ts", true, false
                    );
                });
            }
        } finally {
            setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_FILL_SORT_STRATEGY, "light_encoded");
        }
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
    public void testFillPrevOffsetBindVariable() throws Exception {
        // FILL(PREV) with a non-literal OFFSET (bind variable) must evaluate
        // offsetFunc at runtime, matching the legacy cursor path. Pins fast-path
        // parity so the runtime-constant widening cannot regress silently.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO test VALUES " +
                    "('2024-01-01T12:00:00.000000Z', 1.0), " +
                    "('2024-01-03T12:00:00.000000Z', 3.0)");
            drainWalQueue();
            bindVariableService.clear();
            bindVariableService.setStr(0, "10:00");
            assertQueryNoLeakCheck(
                    """
                            ts\tavg
                            2024-01-01T10:00:00.000000Z\t1.0
                            2024-01-02T10:00:00.000000Z\t1.0
                            2024-01-03T10:00:00.000000Z\t3.0
                            """,
                    "SELECT ts, avg(value) FROM test SAMPLE BY 1d FILL(PREV) ALIGN TO CALENDAR WITH OFFSET $1",
                    "ts"
            );
        });
    }

    @Test
    public void testFillPrevCrossColumnStringSource() throws Exception {
        // Cross-column FILL(PREV(s1)) where two first(STRING) aggregates pull
        // from different columns. STRING is variable-width, so the codegen
        // routes through DISPATCH_PREV_SLOT (recordAt-based). The two source
        // columns hold distinct values per row so the gap-row tag value
        // reveals which slot the cross-column dispatch targets:
        //   data row: s1=first(s),   tag=first(s2)   -- different
        //   gap  row: s1=PREV(s1),   tag=PREV(s1)    -- both equal s1's prev
        // A regression that targets s2's slot instead of s1's would surface
        // 'X' (rather than 'alpha') in the gap row's tag column.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s STRING, s2 STRING, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('alpha', 'X', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('beta',  'Y', 3.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\ts1\ttag
                            2024-01-01T00:00:00.000000Z\talpha\tX
                            2024-01-01T01:00:00.000000Z\talpha\talpha
                            2024-01-01T02:00:00.000000Z\tbeta\tY
                            """,
                    "SELECT ts, first(s) AS s1, first(s2) AS tag " +
                            "FROM x SAMPLE BY 1h FILL(PREV, PREV(s1)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevCrossColumnVarcharSource() throws Exception {
        // Same shape as testFillPrevCrossColumnStringSource but with VARCHAR
        // source columns. Exercises the DISPATCH_PREV_SLOT path with
        // cross-column targeting through baseCursor.recordAt +
        // prevRecord.getVarcharA dispatch. The 'alpha' (rather than 'X') in
        // the gap row's tag column proves the dispatch follows s1's slot.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v VARCHAR, v2 VARCHAR, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('alpha', 'X', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('beta',  'Y', 3.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tv1\ttag
                            2024-01-01T00:00:00.000000Z\talpha\tX
                            2024-01-01T01:00:00.000000Z\talpha\talpha
                            2024-01-01T02:00:00.000000Z\tbeta\tY
                            """,
                    "SELECT ts, first(v) AS v1, first(v2) AS tag " +
                            "FROM x SAMPLE BY 1h FILL(PREV, PREV(v1)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillPrevNegativeTimestamps() throws Exception {
        // FILL(PREV) sibling of testFillNullNegativeTimestamps. Pre-1970
        // bucket timestamps flow through DISPATCH_PREV_SLOT /
        // DISPATCH_PREV_CACHE_SLOT during the pre-fill walk. Buckets before
        // the first data row emit null (no prev); once a data row is seen,
        // FILL(PREV) carries it forward across subsequent gap buckets.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "(10.0, '1970-01-01T01:00:00.000000Z')," +
                    "(20.0, '1970-01-01T03:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfirst
                            1969-12-31T22:00:00.000000Z\tnull
                            1969-12-31T23:00:00.000000Z\tnull
                            1970-01-01T00:00:00.000000Z\tnull
                            1970-01-01T01:00:00.000000Z\t10.0
                            1970-01-01T02:00:00.000000Z\t10.0
                            1970-01-01T03:00:00.000000Z\t20.0
                            1970-01-01T04:00:00.000000Z\t20.0
                            """,
                    "SELECT ts, first(v) FROM t " +
                            "SAMPLE BY 1h FROM '1969-12-31T22:00:00.000000Z' TO '1970-01-01T05:00:00.000000Z' " +
                            "FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }
}
