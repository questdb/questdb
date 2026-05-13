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
public class SampleByFillNullValueTest extends AbstractCairoTest {

    @Test
    public void testFillNullCastMultiKey() throws Exception {
        // FILL(NULL) variant of the multi-key inline-function classifier
        // regression: an inline cast x::STRING is a FUNCTION-form key and must
        // produce one row per key per bucket.
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
    public void testFillNullDstBerlinFallBackDayStride() throws Exception {
        // Day-granular SAMPLE BY with FILL(NULL) across Europe/Berlin DST
        // fall-back. The CEST -> CET transition at 2024-10-27 03:00 local
        // stretches Oct 27 to 25 h in UTC, so local-midnight buckets land at:
        //   2024-10-25T22:00:00Z (Oct 26 00:00 CEST)
        //   2024-10-26T22:00:00Z (Oct 27 00:00 CEST, 25 h gap, no data)
        //   2024-10-27T23:00:00Z (Oct 28 00:00 CET)
        // A uniform 24 h sampler would drift one hour after the transition and
        // the fill cursor would throw "data row timestamp ... precedes next
        // bucket". TimezoneFloorTimestampSampler walks the same local-calendar
        // grid as timestamp_floor_utc, so the two grids stay locked. The
        // sparse middle row is missing, exercising the gap-fill emit path on
        // the variable-width Oct 27 bucket.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-10-25T22:00:00.000000Z')," +
                    "(3.0, '2024-10-27T23:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-10-25T22:00:00.000000Z
                            null\t2024-10-26T22:00:00.000000Z
                            3.0\t2024-10-27T23:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x " +
                            "SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstBerlinSpringForwardDayStride() throws Exception {
        // Spring-forward mirror of the fall-back regression. The CET -> CEST
        // transition at 2024-03-31 02:00 local shrinks Mar 31 to 23 h in UTC;
        // local-midnight buckets land at:
        //   2024-03-29T23:00:00Z (Mar 30 00:00 CET)
        //   2024-03-30T23:00:00Z (Mar 31 00:00 CET, 23 h gap, no data)
        //   2024-03-31T22:00:00Z (Apr  1 00:00 CEST)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(10.0, '2024-03-29T23:00:00.000000Z')," +
                    "(30.0, '2024-03-31T22:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            10.0\t2024-03-29T23:00:00.000000Z
                            null\t2024-03-30T23:00:00.000000Z
                            30.0\t2024-03-31T22:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x " +
                            "SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstBerlinFallBackWeekStride() throws Exception {
        // Week-granular variant. ISO weeks anchor on Monday local. The week
        // containing 2024-10-27 (fall-back Sunday) starts Mon Oct 21 00:00
        // local (Oct 20 22:00 UTC, CEST) and ends Mon Oct 28 00:00 local
        // (Oct 27 23:00 UTC, CET). The middle bucket is sparse, so the fill
        // cursor must walk the 169 h week without drifting against the
        // GROUP BY's timestamp_floor_utc grid.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-10-20T22:00:00.000000Z')," +
                    "(3.0, '2024-11-03T23:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-10-20T22:00:00.000000Z
                            null\t2024-10-27T23:00:00.000000Z
                            3.0\t2024-11-03T23:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x " +
                            "SAMPLE BY 1w FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstBerlinFallBackDayStrideWithOffset() throws Exception {
        // Day-granular FILL(NULL) + Berlin + OFFSET '06:00' across CEST -> CET
        // fall-back. OFFSET '06:00' anchors the bucket grid at local 06:00 each
        // day, well clear of the 03:00 CEST -> 02:00 CET transition, so each
        // bucket boundary occurs exactly once. Bucket boundaries:
        //   2024-10-26T04:00:00Z (Oct 26 06:00 CEST)
        //   2024-10-27T05:00:00Z (Oct 27 06:00 CET, after fall-back)
        //   2024-10-28T05:00:00Z (Oct 28 06:00 CET)
        // Middle bucket has no data, exercising the variable-width fall-back
        // gap-fill emit path with a non-zero local-grid offset.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-10-26T05:00:00.000000Z'),"
                    + "(3.0, '2024-10-28T06:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-10-26T04:00:00.000000Z
                            null\t2024-10-27T05:00:00.000000Z
                            3.0\t2024-10-28T05:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x "
                            + "SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' WITH OFFSET '06:00'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstBerlinSpringForwardDayStrideWithOffset() throws Exception {
        // Spring-forward mirror with OFFSET '06:00'. CET -> CEST transition at
        // 2024-03-31 02:00 local shrinks Mar 31 to 23 h in UTC; local 06:00 is
        // safely past the gap, so the bucket grid runs at:
        //   2024-03-30T05:00:00Z (Mar 30 06:00 CET)
        //   2024-03-31T04:00:00Z (Mar 31 06:00 CEST, after spring-forward)
        //   2024-04-01T04:00:00Z (Apr 1  06:00 CEST)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(10.0, '2024-03-30T06:00:00.000000Z'),"
                    + "(30.0, '2024-04-01T05:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            10.0\t2024-03-30T05:00:00.000000Z
                            null\t2024-03-31T04:00:00.000000Z
                            30.0\t2024-04-01T04:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x "
                            + "SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' WITH OFFSET '06:00'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstBerlinFallBackWeekStrideWithOffset() throws Exception {
        // Week-granular variant with OFFSET '06:00'. WeekTimestampMicrosSampler
        // with no FROM clause anchors at epoch (Thursday 1970-01-01 + 06:00)
        // walking 7-day strides, so weekly buckets land on Thursdays at local
        // 06:00. The week spanning the Oct 27 fall-back starts Thu Oct 24
        // 06:00 CEST (UTC Oct 24 04:00) and ends Thu Oct 31 06:00 CET (UTC
        // Oct 31 05:00) -- 169 h. Middle week is sparse; the fill cursor must
        // walk the variable-width bucket without drifting against the GROUP
        // BY's timestamp_floor_utc grid.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-10-21T05:00:00.000000Z'),"
                    + "(3.0, '2024-11-04T06:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-10-17T04:00:00.000000Z
                            null\t2024-10-24T04:00:00.000000Z
                            3.0\t2024-10-31T05:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x "
                            + "SAMPLE BY 1w FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' WITH OFFSET '06:00'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstBerlinDayStrideWithFromOnDstBoundary() throws Exception {
        // Berlin spring-forward day as the FROM anchor. Local 02:00 CET ->
        // 03:00 CEST on 2024-03-31, so the Mar 31 bucket is 23 h wide:
        // [2024-03-30T23:00Z, 2024-03-31T22:00Z). FROM='2024-03-31' lands
        // raw fromTs at 2024-03-31T00:00Z, INSIDE the Mar 31 local bucket.
        // Data at Mar 31 15:00Z (CEST, in Mar 31 bucket) and Apr 2 12:00Z
        // (in Apr 2 bucket) leaves Mar 31 with data, Apr 1 sparse, Apr 2
        // with data. Verifies that an explicit FROM coinciding with a DST
        // boundary still routes through the firstTs path (firstTs <
        // fromTs) and preserves Berlin's bucket alignment.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-03-31T15:00:00.000000Z'),"
                    + "(3.0, '2024-04-02T12:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-03-30T23:00:00.000000Z
                            null\t2024-03-31T22:00:00.000000Z
                            3.0\t2024-04-01T22:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1d FROM '2024-03-31' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstBerlinDayStrideWithFromZeroOffset() throws Exception {
        // Eastward-DST regression guard. Berlin (CET = +1 h in January) +
        // FROM + zero OFFSET. firstTs from GROUP BY = 2024-01-14T23:00Z
        // (Jan 15 00:00 CET) is < fromTs = 2024-01-15T00:00Z, so the
        // cursor takes the firstTs path and uses setStart on the wrap.
        // Locks the eastward path against a regression where setStart
        // could be replaced with setLocalAnchor (which would mistreat
        // firstTs as a local-grid seed and drift one tzOff).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-14T23:00:00.000000Z'),"
                    + "(3.0, '2024-01-16T23:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-01-14T23:00:00.000000Z
                            null\t2024-01-15T23:00:00.000000Z
                            3.0\t2024-01-16T23:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1d FROM '2024-01-15' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstLosAngelesDayStrideWithFrom() throws Exception {
        // Los Angeles in January = PST (UTC-8 h). Local 2024-01-15T00:00
        // PST = 2024-01-15T08:00Z. firstTs > fromTs, fromTs path.
        // Verifies that the westward-TZ fix (C1 shape) is offset-agnostic
        // and not hard-coded to NY's -5 h in the cursor.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-15T08:00:00.000000Z'),"
                    + "(3.0, '2024-01-17T08:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-01-15T08:00:00.000000Z
                            null\t2024-01-16T08:00:00.000000Z
                            3.0\t2024-01-17T08:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1d FROM '2024-01-15' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'America/Los_Angeles'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstNewYorkDayStrideWithFromEmptyBase() throws Exception {
        // C1 mirror -- empty base + FROM/TO + westward DST. The cursor
        // must emit bucket labels at the GROUP BY's TZ-shifted positions
        // (2024-01-15T05:00Z each day in EST) rather than at raw FROM
        // midnight (2024-01-15T00:00Z each day). Pre-fix the empty-base
        // mirror called setStart(fromTs) which anchored at fromTs+tzOff
        // local and produced 2024-01-15T00:00Z labels in UTC.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            null\t2024-01-15T05:00:00.000000Z
                            null\t2024-01-16T05:00:00.000000Z
                            null\t2024-01-17T05:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1d FROM '2024-01-15' TO '2024-01-18' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'America/New_York'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstNewYorkDayStrideWithFromZeroOffset() throws Exception {
        // C1 lock. America/New_York (EST = UTC-5 h in January) + 1d +
        // FROM + zero OFFSET + non-empty base. firstTs from GROUP BY =
        // 2024-01-15T05:00Z (= Jan 15 00:00 EST), fromTs = 2024-01-15T00:00Z,
        // firstTs >= fromTs => fromTs path. Pre-fix setStart(fromTs) on
        // the wrap drifted the local grid by tzOff and threw "data row
        // ... precedes next bucket" on the second emission; post-fix
        // setLocalAnchor(fromTs) keeps the local grid aligned with
        // GROUP BY's TZ-shifted UTC labels.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-15T05:00:00.000000Z'),"
                    + "(3.0, '2024-01-17T05:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-01-15T05:00:00.000000Z
                            null\t2024-01-16T05:00:00.000000Z
                            3.0\t2024-01-17T05:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1d FROM '2024-01-15' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'America/New_York'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstNewYorkFallBackDayStrideWithFrom() throws Exception {
        // NY fall-back 2024-11-03: EDT (-4 h) -> EST (-5 h) at local 02:00.
        // Nov 3 local bucket is 25 h: [2024-11-03T04:00Z, 2024-11-04T05:00Z).
        // fromTs = 2024-11-01T00:00Z, firstTs = 2024-11-01T04:00Z (EDT
        // start of Nov 1) >= fromTs => fromTs path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-11-01T04:00:00.000000Z'),"
                    + "(2.0, '2024-11-02T04:00:00.000000Z'),"
                    + "(3.0, '2024-11-04T05:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-11-01T04:00:00.000000Z
                            2.0\t2024-11-02T04:00:00.000000Z
                            null\t2024-11-03T04:00:00.000000Z
                            3.0\t2024-11-04T05:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1d FROM '2024-11-01' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'America/New_York'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstNewYorkMonthStrideAcrossSpringForward() throws Exception {
        // NY + 1M + FROM='2024-03-01'. Buckets anchor at the first day of
        // each local month. Mar 1 local = 2024-03-01T05:00Z (EST). Apr 1
        // local = 2024-04-01T04:00Z (EDT, after spring-forward). The
        // resultTzOff re-derivation in the wrap's offsetFlooredUtcResult
        // kicks in here: the floor result lands at Apr 1 local 00:00,
        // tzRules at 2024-04-01T00:00Z says EDT so the back-conversion
        // subtracts 4 h instead of 5 h.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-03-01T05:00:00.000000Z'),"
                    + "(3.0, '2024-05-01T04:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-03-01T05:00:00.000000Z
                            null\t2024-04-01T04:00:00.000000Z
                            3.0\t2024-05-01T04:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1M FROM '2024-03-01' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'America/New_York'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstNewYorkMonthStrideWithFrom() throws Exception {
        // NY + 1M + FROM='2024-01-15'. 1M cadence anchored at FROM's
        // day-of-month (15). Jan 15 EST = 2024-01-15T05:00Z. Feb 15 EST =
        // 2024-02-15T05:00Z. Locks super-day non-day stride under
        // westward DST + FROM, which had no prior coverage.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-15T05:00:00.000000Z'),"
                    + "(3.0, '2024-03-15T04:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-01-15T05:00:00.000000Z
                            null\t2024-02-15T05:00:00.000000Z
                            3.0\t2024-03-15T04:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1M FROM '2024-01-15' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'America/New_York'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstNewYorkSpringForwardDayStrideWithFrom() throws Exception {
        // NY spring-forward 2024-03-10: EST (-5 h) -> EDT (-4 h) at local
        // 02:00. Mar 10 local bucket is 23 h:
        // [2024-03-10T05:00Z, 2024-03-11T04:00Z). firstTs from data on
        // Mar 9 = 2024-03-09T05:00Z (Mar 9 EST start) >= fromTs =
        // 2024-03-09T00:00Z => fromTs path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-03-09T05:00:00.000000Z'),"
                    + "(2.0, '2024-03-10T05:00:00.000000Z'),"
                    + "(3.0, '2024-03-12T04:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-03-09T05:00:00.000000Z
                            2.0\t2024-03-10T05:00:00.000000Z
                            null\t2024-03-11T04:00:00.000000Z
                            3.0\t2024-03-12T04:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1d FROM '2024-03-09' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'America/New_York'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstNewYorkWeekStrideAcrossSpringForward() throws Exception {
        // NY + 1w + FROM spanning the spring-forward week. ISO weeks
        // anchor on Monday local. The week containing 2024-03-10 starts
        // Mon Mar 4 local 00:00 EST = 2024-03-04T05:00Z and ends Mon Mar
        // 11 local 00:00 EDT = 2024-03-11T04:00Z (167 h, one short hour
        // from the DST jump). Verifies the wrap walks the variable-width
        // week without drift.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY WEEK");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-03-04T05:00:00.000000Z'),"
                    + "(3.0, '2024-03-18T04:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-03-04T05:00:00.000000Z
                            null\t2024-03-11T04:00:00.000000Z
                            3.0\t2024-03-18T04:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1w FROM '2024-03-04' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'America/New_York'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstNewYorkWeekStrideWithFrom() throws Exception {
        // NY + 1w + FROM in winter (no DST transition in the range).
        // Buckets at Mon 00:00 EST = T05:00Z each week.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY WEEK");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-15T05:00:00.000000Z'),"
                    + "(3.0, '2024-01-29T05:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-01-15T05:00:00.000000Z
                            null\t2024-01-22T05:00:00.000000Z
                            3.0\t2024-01-29T05:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1w FROM '2024-01-15' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'America/New_York'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDstNewYorkYearStrideWithFrom() throws Exception {
        // NY + 1y + FROM='2024-01-15'. Yearly cadence at Jan 15 each
        // year. Both 2024 and 2025 are EST on Jan 15, so labels are
        // 2024-01-15T05:00Z and 2025-01-15T05:00Z.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-15T05:00:00.000000Z'),"
                    + "(3.0, '2026-01-15T05:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-01-15T05:00:00.000000Z
                            null\t2025-01-15T05:00:00.000000Z
                            3.0\t2026-01-15T05:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1y FROM '2024-01-15' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'America/New_York'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullEmptyBaseFromToWithCalendarOffset() throws Exception {
        // Empty base + FROM/TO + non-zero OFFSET, no TIME ZONE. Drives the
        // empty-base branch at SampleByFillRecordCursorFactory.java:873-885,
        // which calls timestampSampler.setLocalAnchor(fromTs + calendarOffset)
        // to align the bucket grid before the hasNext loop emits fills against
        // an exhausted base. Existing empty-base tests
        // (testFillNullDstNewYorkDayStrideWithFromEmptyBase,
        //  testFillNullFixedOffsetDayStrideEmptyBaseFrom,
        //  testFillNullDayStrideWithTimezoneAndOffsetEmptyBase,
        //  testFillFromNegativeOffsetEqualsStrideEmptyBase) cover empty base
        // + FROM/TO with TIME ZONE or zero offset; none combine empty base +
        // non-zero OFFSET literal without TIME ZONE. With OFFSET '05:00',
        // setLocalAnchor anchors the SimpleTimestampSampler grid at
        // fromTs+5h, so the first bucket label is 2024-01-01T05:00 and
        // subsequent buckets land on each day at 05:00. The 2024-01-04T05:00
        // candidate is past maxTimestamp = 2024-01-04T00:00, so only 3
        // buckets emit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            null\t2024-01-01T05:00:00.000000Z
                            null\t2024-01-02T05:00:00.000000Z
                            null\t2024-01-03T05:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1d FROM '2024-01-01' TO '2024-01-04' "
                            + "FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '05:00'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullFixedOffsetDayStrideEmptyBaseFrom() throws Exception {
        // C2 mirror -- empty base + fixed-offset TZ + FROM/TO. Pre-fix
        // the cursor used baseSampler (no wrap) and setStart(fromTs)
        // anchored the UTC grid at fromTs midnight; emitted labels were
        // 2024-01-01T00:00Z, 02T, 03T -- one tzOff ahead of GROUP BY's
        // TZ-shifted grid. Post-fix: routes through the wrap with
        // setLocalAnchor, emitting 22:00Z prev day each day matching
        // timestamp_floor_utc.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            null\t2023-12-31T22:00:00.000000Z
                            null\t2024-01-01T22:00:00.000000Z
                            null\t2024-01-02T22:00:00.000000Z
                            null\t2024-01-03T22:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1d FROM '2024-01-01' TO '2024-01-04' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE '+02:00'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullFixedOffsetDayStrideWithFromAndOffset() throws Exception {
        // Latent bug surfaced by C2 fix: fixed-offset + super-day +
        // FROM + non-zero OFFSET. Pre-fix Branch 3b's setLocalAnchor on
        // baseSampler degenerated to setStart, anchoring the grid one
        // tzOff ahead of timestamp_floor_utc. Post-fix the wrap's
        // setLocalAnchor keeps the local grid at fromTs+offset and
        // produces labels at local 01:00 each day = 23:00Z prev day.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-01T23:00:00.000000Z'),"
                    + "(3.0, '2024-01-03T23:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-01-01T23:00:00.000000Z
                            null\t2024-01-02T23:00:00.000000Z
                            3.0\t2024-01-03T23:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1d FROM '2024-01-02' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE '+02:00' WITH OFFSET '01:00'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullFixedOffsetEastDayStrideWithFrom() throws Exception {
        // C2 lock. Eastward fixed-offset (+02:00) + 1d + FROM + non-empty
        // base. firstTs from GROUP BY = 2024-01-01T22:00Z (= Jan 2
        // 00:00 +02:00) > fromTs = 2024-01-01T00:00Z => fromTs path.
        // Pre-fix the cursor used baseSampler without TZ shift and threw
        // "data row 1704146400000000 precedes next bucket 1704153600000000".
        // Post-fix the wrap is engaged and setLocalAnchor folds tzOff
        // into the grid.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-01T22:00:00.000000Z'),"
                    + "(3.0, '2024-01-03T22:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-01-01T22:00:00.000000Z
                            null\t2024-01-02T22:00:00.000000Z
                            3.0\t2024-01-03T22:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1d FROM '2024-01-02' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE '+02:00'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullFixedOffsetWestDayStrideWithFrom() throws Exception {
        // Westward fixed-offset (-05:00) + 1d + FROM. Local Jan 2 00:00
        // -05:00 = 2024-01-02T05:00Z. Symmetric with the eastward case.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-02T05:00:00.000000Z'),"
                    + "(3.0, '2024-01-04T05:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-01-02T05:00:00.000000Z
                            null\t2024-01-03T05:00:00.000000Z
                            3.0\t2024-01-04T05:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1d FROM '2024-01-02' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE '-05:00'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDayStrideWithTimezoneAndNegativeOffset() throws Exception {
        // Negative OFFSET '-00:30' anchors local-grid at -30 m mod 1 d, so
        // bucket boundaries land at local 23:30 each day. Berlin in October
        // uses CEST throughout the FROM-TO range so no DST transition.
        // Bucket containing fromTs=2024-10-21T00:00Z is Oct 20 23:30 CEST
        // (= UTC Oct 20 21:30), strictly below fromTs -- exercises branch 2
        // of cursor.initialize (firstTs < fromTs path) under the wrap. The TO
        // bound similarly straddles a bucket boundary, producing a trailing
        // NULL fill row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-10-21T22:00:00.000000Z'),"
                    + "(3.0, '2024-10-23T22:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            null\t2024-10-20T21:30:00.000000Z
                            1.0\t2024-10-21T21:30:00.000000Z
                            null\t2024-10-22T21:30:00.000000Z
                            3.0\t2024-10-23T21:30:00.000000Z
                            null\t2024-10-24T21:30:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1d FROM '2024-10-21' TO '2024-10-25' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' WITH OFFSET '-00:30'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullDayStrideWithTimezoneAndOffsetEmptyBase() throws Exception {
        // Empty table + FROM/TO + FILL(NULL) + Berlin + OFFSET '06:00'. The
        // bucket grid runs at local 06:00 each day. Exercises the empty-base
        // branch of cursor.initialize() with calendarOffset propagated
        // through TimezoneFloorTimestampSampler.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            null\t2024-10-26T04:00:00.000000Z
                            null\t2024-10-27T05:00:00.000000Z
                            null\t2024-10-28T05:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x SAMPLE BY 1d FROM '2024-10-26' TO '2024-10-29' "
                            + "FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' WITH OFFSET '06:00'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullMonthStrideWithTimezoneAndOffset() throws Exception {
        // SAMPLE BY 1M FILL(NULL) + Berlin + OFFSET '06:00:00' across the
        // March DST transition. Bucket grid anchors at local 06:00 on the
        // first day of each month. Verifies MonthTimestampMicrosSampler
        // semantics (setStart extracts startDay + dayMod) when handed a
        // local-grid anchor through the wrap.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-02-01T06:00:00.000000Z'),"
                    + "(3.0, '2024-04-01T05:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-02-01T05:00:00.000000Z
                            null\t2024-03-01T05:00:00.000000Z
                            3.0\t2024-04-01T04:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x "
                            + "SAMPLE BY 1M FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' WITH OFFSET '06:00'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullSubDayTimezoneOffsetWithoutFrom() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-15T09:35:00.000000Z')," +
                    "(3.0, '2024-01-15T11:35:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-01-15T09:30:00.000000Z
                            null\t2024-01-15T10:30:00.000000Z
                            3.0\t2024-01-15T11:30:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x "
                            + "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin' WITH OFFSET '00:30'",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertQueryNoLeakCheck(
                    """
                            sum\tts
                            null\t2024-01-01T00:00:00.000000Z
                            null\t2024-01-01T01:00:00.000000Z
                            null\t2024-01-01T02:00:00.000000Z
                            """,
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
    public void testFillNullKeyedCte() throws Exception {
        // Wrapping SAMPLE BY FILL(NULL) in a CTE verifies the FILL_KEY
        // reclassification works correctly when the factory is reused as
        // a subquery, mirroring testFillPrevKeyedCte. The CTE projection
        // path differs from FILL(PREV) because FILL(NULL) routes through
        // DISPATCH_CONSTANT with NullConstant.NULL rather than the prev
        // cache slot; this guards both projection paths through CTE.
        assertMemoryLeak(() -> {
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
                            2024-01-01T01:00:00.000000Z\tLondon\tnull
                            2024-01-01T02:00:00.000000Z\tLondon\t12.0
                            2024-01-01T02:00:00.000000Z\tParis\tnull
                            """,
                    "WITH sq AS (SELECT ts, city, avg(temp) AS a FROM weather SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR) " +
                            "SELECT * FROM sq",
                    "ts",
                    false
            );
        });
    }

    @Test
    public void testFillNullKeyedConstantValueCte() throws Exception {
        // Sibling of testFillNullKeyedCte covering FILL(<numeric const>).
        // FILL(0.0) routes through DISPATCH_CONSTANT with a DoubleConstant;
        // CTE wrapping must preserve constant emission across the projection.
        assertMemoryLeak(() -> {
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
                            2024-01-01T01:00:00.000000Z\tLondon\t-1.0
                            2024-01-01T02:00:00.000000Z\tLondon\t12.0
                            2024-01-01T02:00:00.000000Z\tParis\t-1.0
                            """,
                    "WITH sq AS (SELECT ts, city, avg(temp) AS a FROM weather SAMPLE BY 1h FILL(-1.0) ALIGN TO CALENDAR) " +
                            "SELECT * FROM sq",
                    "ts",
                    false
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
                    """
                            sum\tavg\tsum1\tts
                            1.0\t1.0\t1\t2024-01-01T00:00:00.000000Z
                            null\tnull\tnull\t2024-01-01T01:00:00.000000Z
                            2.0\t2.0\t2\t2024-01-01T02:00:00.000000Z
                            null\tnull\tnull\t2024-01-01T03:00:00.000000Z
                            3.0\t3.0\t3\t2024-01-01T04:00:00.000000Z
                            """,
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
                    """
                            sum\tts
                            1.0\t2024-01-01T00:00:00.000000Z
                            null\t2024-01-01T01:00:00.000000Z
                            2.0\t2024-01-01T02:00:00.000000Z
                            null\t2024-01-01T03:00:00.000000Z
                            3.0\t2024-01-01T04:00:00.000000Z
                            """,
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
                    """
                            sum\tts
                            null\t2024-01-01T00:00:00.000000Z
                            null\t2024-01-01T01:00:00.000000Z
                            1.0\t2024-01-01T02:00:00.000000Z
                            2.0\t2024-01-01T03:00:00.000000Z
                            null\t2024-01-01T04:00:00.000000Z
                            """,
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
                    """
                            sum\tts
                            1.0\t2024-01-01T00:00:00.000000Z
                            null\t2024-01-01T01:00:00.000000Z
                            2.0\t2024-01-01T02:00:00.000000Z
                            null\t2024-01-01T03:00:00.000000Z
                            3.0\t2024-01-01T04:00:00.000000Z
                            """,
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillNullPlanRangeFromOnly() throws Exception {
        // Pin the SampleByFillRecordCursorFactory.toPlan range attribute when
        // only FROM is bound (no TO). The factory emits the "range" line
        // whenever fromFunc OR toFunc is non-null
        // (SampleByFillRecordCursorFactory.java:226-228), then renders both
        // sides via val(fromFunc).val(',').val(toFunc). Existing plan tests
        // cover both-sided (FROM..TO at testExplainFillRange) and bare
        // (no-range tests like testExplainOuterOrderByAggregate); the
        // single-sided variant takes the same code path but is not asserted.
        // A future refactor of the val(',').val(toFunc) rendering could break
        // single-sided plans without the suite catching it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertPlanNoLeakCheck(
                    "SELECT first(val), ts FROM x SAMPLE BY 1h FROM '2024-01-01' FILL(NULL) ALIGN TO CALENDAR",
                    """
                            Sample By Fill
                              range: ('2024-01-01',)
                              stride: '1h'
                              fill: null
                                Encode sort light
                                  keys: [ts]
                                    Async Group By workers: 1
                                      keys: [ts]
                                      keyFunctions: [timestamp_floor_utc('1h',ts,'2024-01-01T00:00:00.000Z')]
                                      values: [first(val)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Interval forward scan on: x
                                              intervals: [("2024-01-01T00:00:00.000000Z","MAX")]
                            """
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
                    """
                            ts\ts\tv
                            2024-01-01T00:01:00.000000Z\ts2\t10.0
                            """,
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
    public void testFillNullSortStrategiesProduceIdenticalOutput() throws Exception {
        // Sibling of testFillPrevSortStrategiesProduceIdenticalOutput for the
        // FILL(NULL) shape. Same 4-row, 2-key dataset; FILL(NULL) routes
        // through DISPATCH_CONSTANT with NullConstant.NULL per
        // SqlCodeGenerator.fillModes population. Pins that the sort wrapper
        // choice is invisible to FILL(NULL) emission across all four
        // strategies.
        final String[] strategies = {"light_encoded", "full_encoded", "light_recordchain", "full_recordchain"};
        final String expected = """
                ts\tk\ta\tb
                2024-01-01T00:00:00.000000Z\tA\t1.0\t10.0
                2024-01-01T00:00:00.000000Z\tB\tnull\tnull
                2024-01-01T01:00:00.000000Z\tA\tnull\tnull
                2024-01-01T01:00:00.000000Z\tB\t2.0\t20.0
                2024-01-01T02:00:00.000000Z\tA\t3.0\t30.0
                2024-01-01T02:00:00.000000Z\tB\tnull\tnull
                2024-01-01T03:00:00.000000Z\tA\tnull\tnull
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
                                    "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR ORDER BY ts, k",
                            "ts", true, false
                    );
                });
            }
        } finally {
            setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_FILL_SORT_STRATEGY, "light_encoded");
        }
    }

    @Test
    public void testFillValueSortStrategiesProduceIdenticalOutput() throws Exception {
        // Sibling of testFillPrevSortStrategiesProduceIdenticalOutput for the
        // FILL(value) shape. Same 4-row, 2-key dataset; non-null constant
        // fills route through DISPATCH_CONSTANT with the supplied DoubleConstant.
        // Pins that the sort wrapper choice is invisible to FILL(value)
        // emission across all four strategies.
        final String[] strategies = {"light_encoded", "full_encoded", "light_recordchain", "full_recordchain"};
        final String expected = """
                ts\tk\ta\tb
                2024-01-01T00:00:00.000000Z\tA\t1.0\t10.0
                2024-01-01T00:00:00.000000Z\tB\t0.0\t0.0
                2024-01-01T01:00:00.000000Z\tA\t0.0\t0.0
                2024-01-01T01:00:00.000000Z\tB\t2.0\t20.0
                2024-01-01T02:00:00.000000Z\tA\t3.0\t30.0
                2024-01-01T02:00:00.000000Z\tB\t0.0\t0.0
                2024-01-01T03:00:00.000000Z\tA\t0.0\t0.0
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
                                    "SAMPLE BY 1h FILL(0.0, 0.0) ALIGN TO CALENDAR ORDER BY ts, k",
                            "ts", true, false
                    );
                });
            }
        } finally {
            setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_FILL_SORT_STRATEGY, "light_encoded");
        }
    }

    @Test
    public void testFillNullOffsetBindVariable() throws Exception {
        // Runtime-constant OFFSET via bind variable. The fill cursor parses the
        // offset Function once per of() call (mirroring tsFloor's runtime
        // evaluation), so bind-variable values land on the same calendarOffset
        // as the equivalent literal '00:30'.
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
                            2024-01-02T10:00:00.000000Z\tnull
                            2024-01-03T10:00:00.000000Z\t3.0
                            """,
                    "SELECT ts, avg(value) FROM test SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR WITH OFFSET $1",
                    "ts"
            );
        });
    }

    @Test
    public void testFillNullOffsetCastExpression() throws Exception {
        // Cast expression as offset (FUNCTION node). Same code path as the
        // bind-variable case -- Function.init evaluates the cast, getStrA
        // returns "10:00", parseOffset succeeds.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO test VALUES " +
                    "('2024-01-01T12:00:00.000000Z', 1.0), " +
                    "('2024-01-03T12:00:00.000000Z', 3.0)");
            drainWalQueue();
            assertQueryNoLeakCheck(
                    """
                            ts\tavg
                            2024-01-01T10:00:00.000000Z\t1.0
                            2024-01-02T10:00:00.000000Z\tnull
                            2024-01-03T10:00:00.000000Z\t3.0
                            """,
                    "SELECT ts, avg(value) FROM test SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR WITH OFFSET cast('10:00' AS STRING)",
                    "ts"
            );
        });
    }

    @Test
    public void testFillNullTimezoneBindVariableInvalidString() throws Exception {
        // Bind variable holding an unparseable timezone token: the cursor
        // raises SqlException at of() time, pointing at the TIME ZONE
        // expression position. Mirrors testFillOffsetInvalidString for the
        // tz axis -- guards that runtime tz validation surfaces clearly
        // instead of being swallowed or producing wrong output.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES (1.0, '2024-06-15T12:00:00.000000Z')");
            bindVariableService.clear();
            bindVariableService.setStr(0, "Not/AReal_Zone");
            final String sql = "SELECT sum(val) s, ts FROM x " +
                    "SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR TIME ZONE $1";
            assertExceptionNoLeakCheck(sql, sql.indexOf("$1"), "invalid timezone: Not/AReal_Zone");
        });
    }

    @Test
    public void testFillNullTimezoneBindVariableReExecute() throws Exception {
        // The TIME ZONE clause is a runtime-constant bind variable. Re-executing
        // the same compiled factory with a different bind value must rebind
        // TimezoneFloorTimestampSampler's tz rules so the cursor's grid follows
        // the current bind. The pre-fix code resolved tz once in
        // SqlCodeGenerator.generateFill and baked the resulting TimeZoneRules
        // into a final field, silently reusing the first-execute rules on
        // every subsequent execution. This test pins the contract: each
        // execute lands its own buckets.
        // testFillNullTimezoneNegativeOffset covers negative-offset zones
        // separately; this one stays on positive offsets to keep the bind
        // re-evaluation contract isolated.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-06-15T12:00:00.000000Z')," +
                    "(2.0, '2024-06-17T12:00:00.000000Z')");
            bindVariableService.clear();
            bindVariableService.setStr(0, "Europe/Berlin");
            try (RecordCursorFactory factory = select(
                    "SELECT sum(val) s, ts FROM x " +
                            "SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR TIME ZONE $1")) {
                // Berlin CEST: local midnight = 22:00 UTC (previous day).
                assertCursor(
                        """
                                s\tts
                                1.0\t2024-06-14T22:00:00.000000Z
                                null\t2024-06-15T22:00:00.000000Z
                                2.0\t2024-06-16T22:00:00.000000Z
                                """,
                        factory, false, false, false, sqlExecutionContext);

                // Same compiled factory, different bind value. The pre-fix
                // version reused Berlin's tzRules and emitted Berlin-anchored
                // buckets here; with tzFunc threaded to of(), the wrap rebinds
                // and the buckets land on Helsinki local midnight (EEST = UTC+3).
                bindVariableService.setStr(0, "Europe/Helsinki");
                assertCursor(
                        """
                                s\tts
                                1.0\t2024-06-14T21:00:00.000000Z
                                null\t2024-06-15T21:00:00.000000Z
                                2.0\t2024-06-16T21:00:00.000000Z
                                """,
                        factory, false, false, false, sqlExecutionContext);

                // And re-rebind back to Berlin to confirm there is no
                // sticky-state leak across the Helsinki execute.
                bindVariableService.setStr(0, "Europe/Berlin");
                assertCursor(
                        """
                                s\tts
                                1.0\t2024-06-14T22:00:00.000000Z
                                null\t2024-06-15T22:00:00.000000Z
                                2.0\t2024-06-16T22:00:00.000000Z
                                """,
                        factory, false, false, false, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testFillNullTimezoneBindVariableSwitchFixedOffsetAndDst() throws Exception {
        // Re-execute across a fixed-offset zone and a real DST zone on the
        // same compiled factory. Fixed-offset zones (e.g. '+02:00') return
        // hasFixedOffset() == true and the cursor must NOT route through
        // TimezoneFloorTimestampSampler -- the uniform-UTC sampler covers
        // them since their bucket widths are constant. DST zones (e.g.
        // 'Europe/Berlin') need the wrap. Switching between the two on the
        // same factory exercises both the drop-the-wrap and
        // allocate/rebind-the-wrap branches in of(); a regression that
        // forgets either one leaves the previous of()'s sampler choice in
        // place and the second execute drifts.
        //
        // June data + Europe/Berlin (CEST = UTC+2) and the fixed offset
        // '+02:00' anchor at the same UTC instants -- so the visible bucket
        // labels happen to match. Helsinki (EEST = UTC+3) shifts by an hour
        // and makes the wrap-vs-base routing observable in the output.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-06-15T12:00:00.000000Z')," +
                    "(2.0, '2024-06-17T12:00:00.000000Z')");
            bindVariableService.clear();
            bindVariableService.setStr(0, "Europe/Helsinki");
            try (RecordCursorFactory factory = select(
                    "SELECT sum(val) s, ts FROM x " +
                            "SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR TIME ZONE $1")) {
                // Helsinki EEST: local midnight = 21:00 UTC. tzWrap is
                // allocated on this first of() and routed through.
                assertCursor(
                        """
                                s\tts
                                1.0\t2024-06-14T21:00:00.000000Z
                                null\t2024-06-15T21:00:00.000000Z
                                2.0\t2024-06-16T21:00:00.000000Z
                                """,
                        factory, false, false, false, sqlExecutionContext);

                // Switch to a fixed-offset zone. The cursor must drop the
                // wrap (point timestampSampler back at baseSampler);
                // otherwise the Helsinki rules leak and the buckets stay
                // anchored at 21:00 UTC instead of moving to 22:00 UTC.
                bindVariableService.setStr(0, "+02:00");
                assertCursor(
                        """
                                s\tts
                                1.0\t2024-06-14T22:00:00.000000Z
                                null\t2024-06-15T22:00:00.000000Z
                                2.0\t2024-06-16T22:00:00.000000Z
                                """,
                        factory, false, false, false, sqlExecutionContext);

                // Switch back to a DST zone. The cursor reuses the tzWrap
                // allocated in the first of() (no fresh allocation) and
                // simply rebinds tzRules. Output shifts back to the
                // Helsinki grid.
                bindVariableService.setStr(0, "Europe/Helsinki");
                assertCursor(
                        """
                                s\tts
                                1.0\t2024-06-14T21:00:00.000000Z
                                null\t2024-06-15T21:00:00.000000Z
                                2.0\t2024-06-16T21:00:00.000000Z
                                """,
                        factory, false, false, false, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testFillNullTimezoneNegativeOffset() throws Exception {
        // Regression for TimezoneFloorTimestampSampler.localAnchorAsUtc
        // misbehaving on negative-offset zones. Pre-fix:
        // SampleByFillCursor.initialize() called localAnchorAsUtc(effectiveOffset)
        // even when calendarOffset == 0 -- where effectiveOffset is a UTC instant
        // (firstTs from the GROUP BY base, already a bucket label) rather than a
        // local-grid value. The function then applied a UTC<-local subtraction
        // and returned firstTs - tzOff. For UTC+N zones (Berlin = +2h) the result
        // landed below firstTs and Math.max(localAnchorAsUtc, round(firstTs))
        // picked round(firstTs) = firstTs by accident. For UTC-N zones
        // (America/New_York EDT = -4h) the result landed 4h ahead of firstTs and
        // Math.max picked the wrong value, tripping the cursor's grid-drift
        // guard with "data row timestamp ... precedes next bucket".
        //
        // The fix only invokes localAnchorAsUtc on the setLocalAnchor branch
        // (calendarOffset != 0). When calendarOffset == 0 effectiveOffset is
        // already UTC and is used directly. America/New_York in June (EDT,
        // UTC-4) makes the regression observable.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-06-15T12:00:00.000000Z')," +
                    "(2.0, '2024-06-17T12:00:00.000000Z')");
            // New York EDT: local midnight = 04:00 UTC. The middle bucket has
            // no source row and FILL(NULL) emits the explicit null, proving
            // the wrap's grid stride is correct (24h spans in EDT, no DST
            // boundary in this window).
            assertQueryNoLeakCheck(
                    """
                            s\tts
                            1.0\t2024-06-15T04:00:00.000000Z
                            null\t2024-06-16T04:00:00.000000Z
                            2.0\t2024-06-17T04:00:00.000000Z
                            """,
                    "SELECT sum(val) s, ts FROM x " +
                            "SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'America/New_York'",
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testFillValueAppliesAfterAggregateArithmetic() throws Exception {
        // FILL(v) over sum(col*K) must show v in empty buckets, not v*K. Earlier
        // SqlOptimiser.rewriteAggregate split sum(x*10) into sum(x)*10, so the fill
        // landed on sum(x) and empty buckets returned 420. Coverage: ALIGN TO CALENDAR
        // and ALIGN TO FIRST OBSERVATION, each in non-keyed and keyed form.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, k SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 5),
                    ('2024-01-01T03:00:00.000000Z', 'a', 7)
                    """);
            final String expectedNonKeyed = "ts\ttotal\n" +
                    "2024-01-01T00:00:00.000000Z\t50\n" +
                    "2024-01-01T01:00:00.000000Z\t42\n" +
                    "2024-01-01T02:00:00.000000Z\t42\n" +
                    "2024-01-01T03:00:00.000000Z\t70\n";
            final String expectedKeyed = "ts\tk\ttotal\n" +
                    "2024-01-01T00:00:00.000000Z\ta\t50\n" +
                    "2024-01-01T01:00:00.000000Z\ta\t42\n" +
                    "2024-01-01T02:00:00.000000Z\ta\t42\n" +
                    "2024-01-01T03:00:00.000000Z\ta\t70\n";

            assertQueryNoLeakCheck(
                    expectedNonKeyed,
                    "SELECT ts, sum(x * 10) total FROM tab SAMPLE BY 1h FILL(42) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false
            );
            assertQueryNoLeakCheck(
                    expectedKeyed,
                    "SELECT ts, k, sum(x * 10) total FROM tab SAMPLE BY 1h FILL(42) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false
            );
            assertQueryNoLeakCheck(
                    expectedNonKeyed,
                    "SELECT ts, sum(x * 10) total FROM tab SAMPLE BY 1h FILL(42) ALIGN TO FIRST OBSERVATION",
                    "ts",
                    false,
                    false
            );
            assertQueryNoLeakCheck(
                    expectedKeyed,
                    "SELECT ts, k, sum(x * 10) total FROM tab SAMPLE BY 1h FILL(42) ALIGN TO FIRST OBSERVATION",
                    "ts",
                    false,
                    false
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
                    """
                            sum\tts
                            1.0\t2024-01-01T00:00:00.000000Z
                            0.0\t2024-01-01T01:00:00.000000Z
                            2.0\t2024-01-01T02:00:00.000000Z
                            0.0\t2024-01-01T03:00:00.000000Z
                            3.0\t2024-01-01T04:00:00.000000Z
                            """,
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
                    """
                            sum\tts
                            0.0\t2024-01-01T00:00:00.000000Z
                            0.0\t2024-01-01T01:00:00.000000Z
                            1.0\t2024-01-01T02:00:00.000000Z
                            2.0\t2024-01-01T03:00:00.000000Z
                            0.0\t2024-01-01T04:00:00.000000Z
                            """,
                    "SELECT sum(val), ts FROM x " +
                            "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T05:00:00.000000Z' FILL(0) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testFillValuePerColumnPreservedAcrossDuplicates() throws Exception {
        // FILL(0, 42) over duplicate aggregates must apply 0 to the first column and
        // 42 to the second. Before fill-list propagation reached groupByModel.sampleByFill
        // on the calendar-align path, SqlOptimiser.detectDuplicateAggregates collapsed
        // sum(x) AS a and sum(x) AS b into a single inner aggregate and codegen mapped
        // the FILL list against the collapsed count, silently dropping the 42.
        // Coverage: ALIGN TO CALENDAR and ALIGN TO FIRST OBSERVATION, each in non-keyed
        // and keyed form.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 5),
                    ('2024-01-01T02:00:00.000000Z', 'a', 7)
                    """);
            final String expectedNonKeyed = "ts\ta\tb\n" +
                    "2024-01-01T00:00:00.000000Z\t5\t5\n" +
                    "2024-01-01T01:00:00.000000Z\t0\t42\n" +
                    "2024-01-01T02:00:00.000000Z\t7\t7\n";
            final String expectedKeyed = "ts\tk\ta\tb\n" +
                    "2024-01-01T00:00:00.000000Z\ta\t5\t5\n" +
                    "2024-01-01T01:00:00.000000Z\ta\t0\t42\n" +
                    "2024-01-01T02:00:00.000000Z\ta\t7\t7\n";

            assertQueryNoLeakCheck(
                    expectedNonKeyed,
                    "SELECT ts, sum(x) AS a, sum(x) AS b FROM t SAMPLE BY 1h FILL(0, 42) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false
            );
            assertQueryNoLeakCheck(
                    expectedKeyed,
                    "SELECT ts, k, sum(x) AS a, sum(x) AS b FROM t SAMPLE BY 1h FILL(0, 42) ALIGN TO CALENDAR",
                    "ts",
                    false,
                    false
            );
            assertQueryNoLeakCheck(
                    expectedNonKeyed,
                    "SELECT ts, sum(x) AS a, sum(x) AS b FROM t SAMPLE BY 1h FILL(0, 42) ALIGN TO FIRST OBSERVATION",
                    "ts",
                    false,
                    false
            );
            assertQueryNoLeakCheck(
                    expectedKeyed,
                    "SELECT ts, k, sum(x) AS a, sum(x) AS b FROM t SAMPLE BY 1h FILL(0, 42) ALIGN TO FIRST OBSERVATION",
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testFillValuePlanShape() throws Exception {
        // Pin the "fill: value" plan attribute. SampleByFillRecordCursorFactory
        // toPlan emits one of "null", "prev", "value", or "mixed"; the "value"
        // arm is reached only when no column carries a PREV fill and at least
        // one column carries a non-null constant fill. The existing "null",
        // "prev", and "mixed" arms are already covered by other plan tests.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertPlanNoLeakCheck(
                    "SELECT first(val), ts FROM x SAMPLE BY 1h FILL(0.0) ALIGN TO CALENDAR",
                    """
                            Sample By Fill
                              stride: '1h'
                              fill: value
                                Encode sort light
                                  keys: [ts]
                                    Async Group By workers: 1
                                      keys: [ts]
                                      keyFunctions: [timestamp_floor_utc('1h',ts)]
                                      values: [first(val)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: x
                            """
            );
        });
    }

    @Test
    public void testFillNullNegativeTimestamps() throws Exception {
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
}
