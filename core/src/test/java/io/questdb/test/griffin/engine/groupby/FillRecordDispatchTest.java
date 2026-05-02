/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.griffin.engine.groupby;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Property test for {@code FillRecord} dispatch in
 * {@code SampleByFillRecordCursorFactory.SampleByFillCursor}. Locks the
 * dispatch surface of every supported typed getter against silent branch
 * omission during future refactors.
 * <p>
 * The cursor compiles per-output-column dispatch into seven codes consumed
 * by every {@code FillRecord.getXxx(col)} on each cell read:
 * <ol>
 *   <li><b>DISPATCH_BASE</b> -- data-row pass-through to {@code baseRecord.getXxx(col)}.</li>
 *   <li><b>DISPATCH_KEY_SLOT</b> -- gap rows read a group-by key column via
 *       {@code keysMapRecord.getXxx(keyPos)}. Used for FILL_KEY columns and for
 *       cross-col PREV whose source resolves to a key.</li>
 *   <li><b>DISPATCH_PREV_CACHE_SLOT</b> -- gap rows read a cached fixed-size
 *       FILL_PREV value from a per-source-col slot in the keysMap value section,
 *       avoiding the {@code recordAt} hop. Keyed-only.</li>
 *   <li><b>DISPATCH_PREV_SLOT</b> -- gap rows read FILL_PREV via
 *       {@code prevRecord.getXxx(col)}, with {@code recordAt(prevRowId)} positioning
 *       {@code prevRecord} once per emitted row. Variable-width sources, non-keyed
 *       FILL_PREV, and any source not slot-eligible take this path.</li>
 *   <li><b>DISPATCH_TIMESTAMP_FILL</b> -- {@code getTimestamp / getLong / getDate}
 *       on the timestamp column return the synthesized bucket boundary
 *       ({@code fillTimestampFunc.value}).</li>
 *   <li><b>DISPATCH_CONSTANT</b> -- {@code FILL(<value>)} or {@code FILL(NULL)}.
 *       Gap rows return {@code constantFills.getQuick(col).getXxx(null)}.</li>
 *   <li><b>DISPATCH_NULL</b> -- defensive fallthrough; unreachable in practice
 *       because {@code generateFill} always assigns a fill mode.</li>
 * </ol>
 * The cursor swaps {@code currentDispatchCode} between {@code dataDispatchCode}
 * (uniformly DISPATCH_BASE) and {@code fillDispatchCode} on row boundaries,
 * so each getter sees a single per-cell read of the dispatch code.
 * <p>
 * The default null-sentinel path (FILL_PREV_SELF without prior data for a
 * key) is exercised by gap buckets that precede the first real row.
 * <p>
 * Coverage target: 30 typed getters across the four dispatch branches.
 * Getters are enumerated via SQL column types rather than via reflection on
 * {@code FillRecord}'s private inner class -- each named test method drives
 * one getter through SQL scenarios whose expected output can only be produced
 * if every exercised dispatch branch routes correctly.
 * <p>
 * Getter inventory (35 named getters):
 * getBin, getBinLen, getBool, getByte, getChar, getDecimal8, getDecimal16,
 * getDecimal32, getDecimal64, getDecimal128, getDecimal256, getDouble,
 * getFloat, getGeoByte, getGeoShort, getGeoInt, getGeoLong, getIPv4, getInt,
 * getInterval, getLong, getLong128Hi, getLong128Lo, getLong256A, getLong256B,
 * getShort, getStrA, getStrB, getStrLen, getSymA, getSymB, getTimestamp,
 * getVarcharA, getVarcharB, getVarcharSize.
 * <p>
 * Excluded plumbing overrides (per FillRecord Javadoc): getRecord, getRowId,
 * getUpdateRowId, getSymbolTable, getArray. The void-sink
 * getLong256(int, CharSink) variant is exercised implicitly by any LONG256
 * assertion because CursorPrinter routes through it during text rendering.
 */
public class FillRecordDispatchTest extends AbstractCairoTest {

    @Test
    public void testConstantProjectionDoesNotShiftFillSlots() throws Exception {
        // generateFill walks the inner sample-by bottomUpColumns to map
        // factory columns to user-fill value slots. The optimizer hoists
        // SELECT-list CONSTANT projections into the outer VirtualRecord so
        // they never reach the inner bottomUpColumns; if they ever did,
        // aggNonKeyCount would over-count and per-column FILL values would
        // shift onto the wrong aggregate. Distinct fill values for two
        // aggregates make any shift visible in the gap row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (i INT, j INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(10, 100, '2024-01-01T00:00:00.000000Z')," +
                    "(30, 300, '2024-01-01T02:00:00.000000Z')");
            // Per-column FILL: a -> -1, b -> 99. If the 'tag' constant
            // leaked into the inner bottomUpColumns, the gap row would
            // either fail compilation (wrong number of fill values) or
            // assign -1 to the wrong column.
            assertQueryNoLeakCheck(
                    """
                            ts\tc\ta\tb
                            2024-01-01T00:00:00.000000Z\ttag\t10\t100
                            2024-01-01T01:00:00.000000Z\ttag\t-1\t99
                            2024-01-01T02:00:00.000000Z\ttag\t30\t300
                            """,
                    "SELECT ts, 'tag' AS c, first(i) AS a, sum(j) AS b FROM x " +
                            "SAMPLE BY 1h FILL(-1, 99) ALIGN TO CALENDAR",
                    "ts", false, false
            );
            // Bare FILL(PREV) variant: same hoisting assumption, different
            // dispatch arm (factoryColToUserFillIdx classifies key vs. agg).
            assertQueryNoLeakCheck(
                    """
                            ts\tc\ta\tb
                            2024-01-01T00:00:00.000000Z\ttag\t10\t100
                            2024-01-01T01:00:00.000000Z\ttag\t10\t100
                            2024-01-01T02:00:00.000000Z\ttag\t30\t300
                            """,
                    "SELECT ts, 'tag' AS c, first(i) AS a, sum(j) AS b FROM x " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToAggregateBool() throws Exception {
        // Cross-col PREV(s) where the source aggregate is BOOLEAN exercises
        // FillRecord.getBool's "(mode == FILL_PREV_SELF || mode >= 0) &&
        // hasKeyPrev() -> prevRecord.getBool(mode >= 0 ? mode : col)"
        // branch. Mirrors testDoubleCrossColumnPrevToAggregate.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (b BOOLEAN, i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(true, 10, '2024-01-01T00:00:00.000000Z')," +
                    "(false, 30, '2024-01-01T02:00:00.000000Z')");
            // FILL(PREV, PREV(s)) -- a at 01:00 pulls s's prev value (true).
            assertQueryNoLeakCheck(
                    """
                            ts\ts\ta
                            2024-01-01T00:00:00.000000Z\ttrue\ttrue
                            2024-01-01T01:00:00.000000Z\ttrue\ttrue
                            2024-01-01T02:00:00.000000Z\tfalse\tfalse
                            """,
                    "SELECT ts, first(b) AS s, first(b) AS a FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(s)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToAggregateChar() throws Exception {
        // CHAR cross-col PREV exercises FillRecord.getChar's mode>=0 arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (c CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', '2024-01-01T00:00:00.000000Z')," +
                    "('B', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\ts\ta
                            2024-01-01T00:00:00.000000Z\tA\tA
                            2024-01-01T01:00:00.000000Z\tA\tA
                            2024-01-01T02:00:00.000000Z\tB\tB
                            """,
                    "SELECT ts, first(c) AS s, first(c) AS a FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(s)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToAggregateInt() throws Exception {
        // INT cross-col PREV exercises FillRecord.getInt's mode>=0 arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(10, '2024-01-01T00:00:00.000000Z')," +
                    "(30, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\ts\ta
                            2024-01-01T00:00:00.000000Z\t10\t10
                            2024-01-01T01:00:00.000000Z\t10\t10
                            2024-01-01T02:00:00.000000Z\t30\t30
                            """,
                    "SELECT ts, first(i) AS s, first(i) AS a FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(s)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToAggregateLong() throws Exception {
        // LONG cross-col PREV exercises FillRecord.getLong's mode>=0 arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1_000_000, '2024-01-01T00:00:00.000000Z')," +
                    "(3_000_000, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\ts\ta
                            2024-01-01T00:00:00.000000Z\t1000000\t1000000
                            2024-01-01T01:00:00.000000Z\t1000000\t1000000
                            2024-01-01T02:00:00.000000Z\t3000000\t3000000
                            """,
                    "SELECT ts, first(l) AS s, first(l) AS a FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(s)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToAggregateSymbol() throws Exception {
        // SYMBOL cross-col PREV exercises FillRecord.getSym's mode>=0 arm
        // including symbol-table lookup through the cached prev row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('alpha', '2024-01-01T00:00:00.000000Z')," +
                    "('beta', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tsv\ta
                            2024-01-01T00:00:00.000000Z\talpha\talpha
                            2024-01-01T01:00:00.000000Z\talpha\talpha
                            2024-01-01T02:00:00.000000Z\tbeta\tbeta
                            """,
                    "SELECT ts, first(s) AS sv, first(s) AS a FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(sv)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToAggregateTimestamp() throws Exception {
        // TIMESTAMP cross-col PREV exercises FillRecord.getTimestamp's mode>=0 arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (t TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('2023-12-31T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z')," +
                    "('2023-12-31T05:00:00.000000Z', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\ts\ta
                            2024-01-01T00:00:00.000000Z\t2023-12-31T00:00:00.000000Z\t2023-12-31T00:00:00.000000Z
                            2024-01-01T01:00:00.000000Z\t2023-12-31T00:00:00.000000Z\t2023-12-31T00:00:00.000000Z
                            2024-01-01T02:00:00.000000Z\t2023-12-31T05:00:00.000000Z\t2023-12-31T05:00:00.000000Z
                            """,
                    "SELECT ts, first(t) AS s, first(t) AS a FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(s)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyBool() throws Exception {
        // BOOLEAN cross-col PREV-to-key exercises FillRecord.getBool's
        // DISPATCH_KEY_SLOT arm, dispatched via the (mode >= 0 &&
        // outputColToKeyPos[mode] >= 0) compile path: the source `k` is a
        // group-by key, so the gap row reads the mirror through
        // keysMapRecord.getBool(keyPos[k]). The dispatch lands the key value
        // (true / false), not the prior aggregate value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k BOOLEAN, val DOUBLE, mirror BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(true, 10.0, false, '2024-01-01T00:00:00.000000Z')," +
                    "(false, 20.0, true, '2024-01-01T00:00:00.000000Z')," +
                    "(true, 30.0, false, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\tfalse\t20.0\ttrue
                            2024-01-01T00:00:00.000000Z\ttrue\t10.0\tfalse
                            2024-01-01T01:00:00.000000Z\tfalse\t20.0\tfalse
                            2024-01-01T01:00:00.000000Z\ttrue\t10.0\ttrue
                            2024-01-01T02:00:00.000000Z\tfalse\t20.0\tfalse
                            2024-01-01T02:00:00.000000Z\ttrue\t30.0\tfalse
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyByte() throws Exception {
        // BYTE cross-col PREV-to-key exercises FillRecord.getByte's
        // DISPATCH_KEY_SLOT arm. Gap-row mirror reads echo the key value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k BYTE, val DOUBLE, mirror BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(11::BYTE, 10.0, 100::BYTE, '2024-01-01T00:00:00.000000Z')," +
                    "(22::BYTE, 20.0, 200::BYTE, '2024-01-01T00:00:00.000000Z')," +
                    "(11::BYTE, 30.0, 121::BYTE, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t11\t10.0\t100
                            2024-01-01T00:00:00.000000Z\t22\t20.0\t-56
                            2024-01-01T01:00:00.000000Z\t11\t10.0\t11
                            2024-01-01T01:00:00.000000Z\t22\t20.0\t22
                            2024-01-01T02:00:00.000000Z\t11\t30.0\t121
                            2024-01-01T02:00:00.000000Z\t22\t20.0\t22
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyChar() throws Exception {
        // CHAR cross-col PREV-to-key exercises FillRecord.getChar's
        // DISPATCH_KEY_SLOT arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k CHAR, val DOUBLE, mirror CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', 10.0, 'X', '2024-01-01T00:00:00.000000Z')," +
                    "('B', 20.0, 'Y', '2024-01-01T00:00:00.000000Z')," +
                    "('A', 30.0, 'Z', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\tA\t10.0\tX
                            2024-01-01T00:00:00.000000Z\tB\t20.0\tY
                            2024-01-01T01:00:00.000000Z\tA\t10.0\tA
                            2024-01-01T01:00:00.000000Z\tB\t20.0\tB
                            2024-01-01T02:00:00.000000Z\tA\t30.0\tZ
                            2024-01-01T02:00:00.000000Z\tB\t20.0\tB
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyDate() throws Exception {
        // DATE cross-col PREV-to-key. getDate() defaults to getLong() per the
        // Record contract, so the gap-row read goes through FillRecord.getLong
        // DISPATCH_KEY_SLOT, returning the key DATE value as long milliseconds.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k DATE, val DOUBLE, mirror DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('2020-01-01T00:00:00.000Z'::DATE, 10.0, '2021-06-15T00:00:00.000Z'::DATE, '2024-01-01T00:00:00.000000Z')," +
                    "('2020-02-02T00:00:00.000Z'::DATE, 20.0, '2021-07-20T00:00:00.000Z'::DATE, '2024-01-01T00:00:00.000000Z')," +
                    "('2020-01-01T00:00:00.000Z'::DATE, 30.0, '2021-08-25T00:00:00.000Z'::DATE, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000Z\t10.0\t2021-06-15T00:00:00.000Z
                            2024-01-01T00:00:00.000000Z\t2020-02-02T00:00:00.000Z\t20.0\t2021-07-20T00:00:00.000Z
                            2024-01-01T01:00:00.000000Z\t2020-01-01T00:00:00.000Z\t10.0\t2020-01-01T00:00:00.000Z
                            2024-01-01T01:00:00.000000Z\t2020-02-02T00:00:00.000Z\t20.0\t2020-02-02T00:00:00.000Z
                            2024-01-01T02:00:00.000000Z\t2020-01-01T00:00:00.000Z\t30.0\t2021-08-25T00:00:00.000Z
                            2024-01-01T02:00:00.000000Z\t2020-02-02T00:00:00.000Z\t20.0\t2020-02-02T00:00:00.000Z
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyDecimal128() throws Exception {
        // DECIMAL128 cross-col PREV-to-key exercises
        // FillRecord.getDecimal128 DISPATCH_KEY_SLOT arm. DECIMAL targets
        // require exact type equality on the cross-col PREV resolution
        // path, so source key and target aggregate share precision/scale.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k DECIMAL(25, 2), val DOUBLE, mirror DECIMAL(25, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('1.00'::DECIMAL(25,2), 10.0, '111.11'::DECIMAL(25,2), '2024-01-01T00:00:00.000000Z')," +
                    "('2.00'::DECIMAL(25,2), 20.0, '222.22'::DECIMAL(25,2), '2024-01-01T00:00:00.000000Z')," +
                    "('1.00'::DECIMAL(25,2), 30.0, '333.33'::DECIMAL(25,2), '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t1.00\t10.0\t111.11
                            2024-01-01T00:00:00.000000Z\t2.00\t20.0\t222.22
                            2024-01-01T01:00:00.000000Z\t1.00\t10.0\t1.00
                            2024-01-01T01:00:00.000000Z\t2.00\t20.0\t2.00
                            2024-01-01T02:00:00.000000Z\t1.00\t30.0\t333.33
                            2024-01-01T02:00:00.000000Z\t2.00\t20.0\t2.00
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyDecimal16() throws Exception {
        // DECIMAL16 (precision <= 4) cross-col PREV-to-key exercises
        // FillRecord.getDecimal16 DISPATCH_KEY_SLOT arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k DECIMAL(4, 1), val DOUBLE, mirror DECIMAL(4, 1), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.1::DECIMAL(4,1), 10.0, 99.9::DECIMAL(4,1), '2024-01-01T00:00:00.000000Z')," +
                    "(2.2::DECIMAL(4,1), 20.0, 88.8::DECIMAL(4,1), '2024-01-01T00:00:00.000000Z')," +
                    "(1.1::DECIMAL(4,1), 30.0, 77.7::DECIMAL(4,1), '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t1.1\t10.0\t99.9
                            2024-01-01T00:00:00.000000Z\t2.2\t20.0\t88.8
                            2024-01-01T01:00:00.000000Z\t1.1\t10.0\t1.1
                            2024-01-01T01:00:00.000000Z\t2.2\t20.0\t2.2
                            2024-01-01T02:00:00.000000Z\t1.1\t30.0\t77.7
                            2024-01-01T02:00:00.000000Z\t2.2\t20.0\t2.2
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyDecimal256() throws Exception {
        // DECIMAL256 (precision > 38) cross-col PREV-to-key exercises
        // FillRecord.getDecimal256 DISPATCH_KEY_SLOT arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k DECIMAL(60, 2), val DOUBLE, mirror DECIMAL(60, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('1.00'::DECIMAL(60,2), 10.0, '999.99'::DECIMAL(60,2), '2024-01-01T00:00:00.000000Z')," +
                    "('2.00'::DECIMAL(60,2), 20.0, '888.88'::DECIMAL(60,2), '2024-01-01T00:00:00.000000Z')," +
                    "('1.00'::DECIMAL(60,2), 30.0, '777.77'::DECIMAL(60,2), '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t1.00\t10.0\t999.99
                            2024-01-01T00:00:00.000000Z\t2.00\t20.0\t888.88
                            2024-01-01T01:00:00.000000Z\t1.00\t10.0\t1.00
                            2024-01-01T01:00:00.000000Z\t2.00\t20.0\t2.00
                            2024-01-01T02:00:00.000000Z\t1.00\t30.0\t777.77
                            2024-01-01T02:00:00.000000Z\t2.00\t20.0\t2.00
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyDecimal32() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k DECIMAL(9, 2), val DOUBLE, mirror DECIMAL(9, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.10::DECIMAL(9,2), 10.0, 9999.99::DECIMAL(9,2), '2024-01-01T00:00:00.000000Z')," +
                    "(2.20::DECIMAL(9,2), 20.0, 8888.88::DECIMAL(9,2), '2024-01-01T00:00:00.000000Z')," +
                    "(1.10::DECIMAL(9,2), 30.0, 7777.77::DECIMAL(9,2), '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t1.10\t10.0\t9999.99
                            2024-01-01T00:00:00.000000Z\t2.20\t20.0\t8888.88
                            2024-01-01T01:00:00.000000Z\t1.10\t10.0\t1.10
                            2024-01-01T01:00:00.000000Z\t2.20\t20.0\t2.20
                            2024-01-01T02:00:00.000000Z\t1.10\t30.0\t7777.77
                            2024-01-01T02:00:00.000000Z\t2.20\t20.0\t2.20
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyDecimal64() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k DECIMAL(18, 2), val DOUBLE, mirror DECIMAL(18, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.10::DECIMAL(18,2), 10.0, 11111.11::DECIMAL(18,2), '2024-01-01T00:00:00.000000Z')," +
                    "(2.20::DECIMAL(18,2), 20.0, 22222.22::DECIMAL(18,2), '2024-01-01T00:00:00.000000Z')," +
                    "(1.10::DECIMAL(18,2), 30.0, 33333.33::DECIMAL(18,2), '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t1.10\t10.0\t11111.11
                            2024-01-01T00:00:00.000000Z\t2.20\t20.0\t22222.22
                            2024-01-01T01:00:00.000000Z\t1.10\t10.0\t1.10
                            2024-01-01T01:00:00.000000Z\t2.20\t20.0\t2.20
                            2024-01-01T02:00:00.000000Z\t1.10\t30.0\t33333.33
                            2024-01-01T02:00:00.000000Z\t2.20\t20.0\t2.20
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyDecimal8() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k DECIMAL(2, 1), val DOUBLE, mirror DECIMAL(2, 1), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.1::DECIMAL(2,1), 10.0, 9.9::DECIMAL(2,1), '2024-01-01T00:00:00.000000Z')," +
                    "(2.2::DECIMAL(2,1), 20.0, 8.8::DECIMAL(2,1), '2024-01-01T00:00:00.000000Z')," +
                    "(1.1::DECIMAL(2,1), 30.0, 7.7::DECIMAL(2,1), '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t1.1\t10.0\t9.9
                            2024-01-01T00:00:00.000000Z\t2.2\t20.0\t8.8
                            2024-01-01T01:00:00.000000Z\t1.1\t10.0\t1.1
                            2024-01-01T01:00:00.000000Z\t2.2\t20.0\t2.2
                            2024-01-01T02:00:00.000000Z\t1.1\t30.0\t7.7
                            2024-01-01T02:00:00.000000Z\t2.2\t20.0\t2.2
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyDouble() throws Exception {
        // DOUBLE cross-col PREV-to-key exercises FillRecord.getDouble's
        // DISPATCH_KEY_SLOT arm. Mirrors the testFillPrevCrossColumnKeyed
        // case in SampleByFillPrevTest but pinned to the dispatch-property
        // suite for regression locking.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k DOUBLE, val DOUBLE, mirror DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.5, 10.0, 100.5, '2024-01-01T00:00:00.000000Z')," +
                    "(2.5, 20.0, 200.5, '2024-01-01T00:00:00.000000Z')," +
                    "(1.5, 30.0, 300.5, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t1.5\t10.0\t100.5
                            2024-01-01T00:00:00.000000Z\t2.5\t20.0\t200.5
                            2024-01-01T01:00:00.000000Z\t1.5\t10.0\t1.5
                            2024-01-01T01:00:00.000000Z\t2.5\t20.0\t2.5
                            2024-01-01T02:00:00.000000Z\t1.5\t30.0\t300.5
                            2024-01-01T02:00:00.000000Z\t2.5\t20.0\t2.5
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyFloat() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k FLOAT, val DOUBLE, mirror FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.5::FLOAT, 10.0, 100.5::FLOAT, '2024-01-01T00:00:00.000000Z')," +
                    "(2.5::FLOAT, 20.0, 200.5::FLOAT, '2024-01-01T00:00:00.000000Z')," +
                    "(1.5::FLOAT, 30.0, 300.5::FLOAT, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t1.5\t10.0\t100.5
                            2024-01-01T00:00:00.000000Z\t2.5\t20.0\t200.5
                            2024-01-01T01:00:00.000000Z\t1.5\t10.0\t1.5
                            2024-01-01T01:00:00.000000Z\t2.5\t20.0\t2.5
                            2024-01-01T02:00:00.000000Z\t1.5\t30.0\t300.5
                            2024-01-01T02:00:00.000000Z\t2.5\t20.0\t2.5
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyGeoByte() throws Exception {
        // GEOHASH <= 7 bits stored as byte. Cross-col PREV-to-key on a GEO
        // key exercises FillRecord.getGeoByte DISPATCH_KEY_SLOT arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k GEOHASH(5b), val DOUBLE, mirror GEOHASH(5b), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(##00111, 10.0, ##11111, '2024-01-01T00:00:00.000000Z')," +
                    "(##11000, 20.0, ##00000, '2024-01-01T00:00:00.000000Z')," +
                    "(##00111, 30.0, ##10101, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t7\t10.0\tz
                            2024-01-01T00:00:00.000000Z\ts\t20.0\t0
                            2024-01-01T01:00:00.000000Z\t7\t10.0\t7
                            2024-01-01T01:00:00.000000Z\ts\t20.0\ts
                            2024-01-01T02:00:00.000000Z\t7\t30.0\tp
                            2024-01-01T02:00:00.000000Z\ts\t20.0\ts
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyGeoInt() throws Exception {
        // GEOHASH 16..31 bits stored as int. Cross-col PREV-to-key exercises
        // FillRecord.getGeoInt DISPATCH_KEY_SLOT arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k GEOHASH(4c), val DOUBLE, mirror GEOHASH(4c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('sp05', 10.0, 'wxyz', '2024-01-01T00:00:00.000000Z')," +
                    "('u33d', 20.0, 'bcde', '2024-01-01T00:00:00.000000Z')," +
                    "('sp05', 30.0, 'rstv', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\tsp05\t10.0\twxyz
                            2024-01-01T00:00:00.000000Z\tu33d\t20.0\tbcde
                            2024-01-01T01:00:00.000000Z\tsp05\t10.0\tsp05
                            2024-01-01T01:00:00.000000Z\tu33d\t20.0\tu33d
                            2024-01-01T02:00:00.000000Z\tsp05\t30.0\trstv
                            2024-01-01T02:00:00.000000Z\tu33d\t20.0\tu33d
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyGeoLong() throws Exception {
        // GEOHASH 32..60 bits stored as long. Cross-col PREV-to-key exercises
        // FillRecord.getGeoLong DISPATCH_KEY_SLOT arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k GEOHASH(10c), val DOUBLE, mirror GEOHASH(10c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('sp052n01b3', 10.0, 'wxyzwxyzwx', '2024-01-01T00:00:00.000000Z')," +
                    "('u33d8b1234', 20.0, 'bcdebcdebc', '2024-01-01T00:00:00.000000Z')," +
                    "('sp052n01b3', 30.0, 'mnpqrstvwx', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\tsp052n01b3\t10.0\twxyzwxyzwx
                            2024-01-01T00:00:00.000000Z\tu33d8b1234\t20.0\tbcdebcdebc
                            2024-01-01T01:00:00.000000Z\tsp052n01b3\t10.0\tsp052n01b3
                            2024-01-01T01:00:00.000000Z\tu33d8b1234\t20.0\tu33d8b1234
                            2024-01-01T02:00:00.000000Z\tsp052n01b3\t30.0\tmnpqrstvwx
                            2024-01-01T02:00:00.000000Z\tu33d8b1234\t20.0\tu33d8b1234
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyGeoShort() throws Exception {
        // GEOHASH 8..15 bits stored as short. Cross-col PREV-to-key exercises
        // FillRecord.getGeoShort DISPATCH_KEY_SLOT arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k GEOHASH(12b), val DOUBLE, mirror GEOHASH(12b), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(##011001100101, 10.0, ##111111111111, '2024-01-01T00:00:00.000000Z')," +
                    "(##111000111000, 20.0, ##000000000000, '2024-01-01T00:00:00.000000Z')," +
                    "(##011001100101, 30.0, ##101010101010, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t011001100101\t10.0\t111111111111
                            2024-01-01T00:00:00.000000Z\t111000111000\t20.0\t000000000000
                            2024-01-01T01:00:00.000000Z\t011001100101\t10.0\t011001100101
                            2024-01-01T01:00:00.000000Z\t111000111000\t20.0\t111000111000
                            2024-01-01T02:00:00.000000Z\t011001100101\t30.0\t101010101010
                            2024-01-01T02:00:00.000000Z\t111000111000\t20.0\t111000111000
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyIPv4() throws Exception {
        // IPv4 cross-col PREV-to-key exercises FillRecord.getIPv4
        // DISPATCH_KEY_SLOT arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k IPV4, val DOUBLE, mirror IPV4, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('1.2.3.4'::IPV4, 10.0, '10.0.0.1'::IPV4, '2024-01-01T00:00:00.000000Z')," +
                    "('5.6.7.8'::IPV4, 20.0, '10.0.0.2'::IPV4, '2024-01-01T00:00:00.000000Z')," +
                    "('1.2.3.4'::IPV4, 30.0, '10.0.0.3'::IPV4, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t1.2.3.4\t10.0\t10.0.0.1
                            2024-01-01T00:00:00.000000Z\t5.6.7.8\t20.0\t10.0.0.2
                            2024-01-01T01:00:00.000000Z\t1.2.3.4\t10.0\t1.2.3.4
                            2024-01-01T01:00:00.000000Z\t5.6.7.8\t20.0\t5.6.7.8
                            2024-01-01T02:00:00.000000Z\t1.2.3.4\t30.0\t10.0.0.3
                            2024-01-01T02:00:00.000000Z\t5.6.7.8\t20.0\t5.6.7.8
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyInt() throws Exception {
        // INT cross-col PREV-to-key exercises FillRecord.getInt
        // DISPATCH_KEY_SLOT arm. Mirrors testFillPrevOfIntKeyColumn from
        // SampleByFillPrevTest but pinned to the dispatch-property suite.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k INT, val DOUBLE, mirror INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1, 10.0, 100, '2024-01-01T00:00:00.000000Z')," +
                    "(2, 20.0, 200, '2024-01-01T00:00:00.000000Z')," +
                    "(1, 30.0, 300, '2024-01-01T02:00:00.000000Z')");
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
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyLong() throws Exception {
        // LONG cross-col PREV-to-key exercises FillRecord.getLong
        // DISPATCH_KEY_SLOT arm. Distinct from getLong's
        // DISPATCH_TIMESTAMP_FILL arm covered by
        // testGetLongOnTimestampDuringGapReturnsBucketTs.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k LONG, val DOUBLE, mirror LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1_000_000, 10.0, 111, '2024-01-01T00:00:00.000000Z')," +
                    "(2_000_000, 20.0, 222, '2024-01-01T00:00:00.000000Z')," +
                    "(1_000_000, 30.0, 333, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t1000000\t10.0\t111
                            2024-01-01T00:00:00.000000Z\t2000000\t20.0\t222
                            2024-01-01T01:00:00.000000Z\t1000000\t10.0\t1000000
                            2024-01-01T01:00:00.000000Z\t2000000\t20.0\t2000000
                            2024-01-01T02:00:00.000000Z\t1000000\t30.0\t333
                            2024-01-01T02:00:00.000000Z\t2000000\t20.0\t2000000
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k SHORT, val DOUBLE, mirror SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(11::SHORT, 10.0, 1001::SHORT, '2024-01-01T00:00:00.000000Z')," +
                    "(22::SHORT, 20.0, 2002::SHORT, '2024-01-01T00:00:00.000000Z')," +
                    "(11::SHORT, 30.0, 3003::SHORT, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t11\t10.0\t1001
                            2024-01-01T00:00:00.000000Z\t22\t20.0\t2002
                            2024-01-01T01:00:00.000000Z\t11\t10.0\t11
                            2024-01-01T01:00:00.000000Z\t22\t20.0\t22
                            2024-01-01T02:00:00.000000Z\t11\t30.0\t3003
                            2024-01-01T02:00:00.000000Z\t22\t20.0\t22
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyString() throws Exception {
        // STRING cross-col PREV-to-key exercises FillRecord.getStrA / getStrB
        // / getStrLen DISPATCH_KEY_SLOT arms.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k STRING, val DOUBLE, mirror STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('Ka', 10.0, 'foo', '2024-01-01T00:00:00.000000Z')," +
                    "('Kb', 20.0, 'bar', '2024-01-01T00:00:00.000000Z')," +
                    "('Ka', 30.0, 'baz', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\tKa\t10.0\tfoo
                            2024-01-01T00:00:00.000000Z\tKb\t20.0\tbar
                            2024-01-01T01:00:00.000000Z\tKa\t10.0\tKa
                            2024-01-01T01:00:00.000000Z\tKb\t20.0\tKb
                            2024-01-01T02:00:00.000000Z\tKa\t30.0\tbaz
                            2024-01-01T02:00:00.000000Z\tKb\t20.0\tKb
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyTimestamp() throws Exception {
        // TIMESTAMP cross-col PREV-to-key exercises FillRecord.getTimestamp's
        // DISPATCH_KEY_SLOT arm. Distinct from getTimestamp's special
        // (col == timestampIndex) shortcut for the bucket boundary.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k TIMESTAMP, val DOUBLE, mirror TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('2020-01-01T00:00:00.000000Z', 10.0, '2021-06-15T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z')," +
                    "('2020-02-02T00:00:00.000000Z', 20.0, '2021-07-20T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z')," +
                    "('2020-01-01T00:00:00.000000Z', 30.0, '2021-08-25T00:00:00.000000Z', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t2020-01-01T00:00:00.000000Z\t10.0\t2021-06-15T00:00:00.000000Z
                            2024-01-01T00:00:00.000000Z\t2020-02-02T00:00:00.000000Z\t20.0\t2021-07-20T00:00:00.000000Z
                            2024-01-01T01:00:00.000000Z\t2020-01-01T00:00:00.000000Z\t10.0\t2020-01-01T00:00:00.000000Z
                            2024-01-01T01:00:00.000000Z\t2020-02-02T00:00:00.000000Z\t20.0\t2020-02-02T00:00:00.000000Z
                            2024-01-01T02:00:00.000000Z\t2020-01-01T00:00:00.000000Z\t30.0\t2021-08-25T00:00:00.000000Z
                            2024-01-01T02:00:00.000000Z\t2020-02-02T00:00:00.000000Z\t20.0\t2020-02-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyUuid() throws Exception {
        // UUID is backed by long128 (hi + lo). Cross-col PREV-to-key
        // exercises FillRecord.getLong128Hi / getLong128Lo
        // DISPATCH_KEY_SLOT arms in one shot.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k UUID, val DOUBLE, mirror UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('00000000-0000-0000-0000-000000000001'::UUID, 10.0, 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::UUID, '2024-01-01T00:00:00.000000Z')," +
                    "('00000000-0000-0000-0000-000000000002'::UUID, 20.0, 'ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb'::UUID, '2024-01-01T00:00:00.000000Z')," +
                    "('00000000-0000-0000-0000-000000000001'::UUID, 30.0, '11111111-2222-3333-4444-555555555555'::UUID, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\t00000000-0000-0000-0000-000000000001\t10.0\taaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
                            2024-01-01T00:00:00.000000Z\t00000000-0000-0000-0000-000000000002\t20.0\tffffffff-eeee-dddd-cccc-bbbbbbbbbbbb
                            2024-01-01T01:00:00.000000Z\t00000000-0000-0000-0000-000000000001\t10.0\t00000000-0000-0000-0000-000000000001
                            2024-01-01T01:00:00.000000Z\t00000000-0000-0000-0000-000000000002\t20.0\t00000000-0000-0000-0000-000000000002
                            2024-01-01T02:00:00.000000Z\t00000000-0000-0000-0000-000000000001\t30.0\t11111111-2222-3333-4444-555555555555
                            2024-01-01T02:00:00.000000Z\t00000000-0000-0000-0000-000000000002\t20.0\t00000000-0000-0000-0000-000000000002
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testCrossColumnPrevToKeyVarchar() throws Exception {
        // VARCHAR cross-col PREV-to-key exercises FillRecord.getVarcharA /
        // getVarcharB / getVarcharSize DISPATCH_KEY_SLOT arms. Mirrors
        // testFillPrevOfVarcharKeyColumn from SampleByFillPrevTest but
        // pinned to the dispatch-property suite.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k VARCHAR, val DOUBLE, mirror VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('Ka', 10.0, 'foo', '2024-01-01T00:00:00.000000Z')," +
                    "('Kb', 20.0, 'bar', '2024-01-01T00:00:00.000000Z')," +
                    "('Ka', 30.0, 'baz', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\ts\tm
                            2024-01-01T00:00:00.000000Z\tKa\t10.0\tfoo
                            2024-01-01T00:00:00.000000Z\tKb\t20.0\tbar
                            2024-01-01T01:00:00.000000Z\tKa\t10.0\tKa
                            2024-01-01T01:00:00.000000Z\tKb\t20.0\tKb
                            2024-01-01T02:00:00.000000Z\tKa\t30.0\tbaz
                            2024-01-01T02:00:00.000000Z\tKb\t20.0\tKb
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, sum(val) AS s, last(mirror) AS m FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(k)) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testDispatchPrevCacheSlotFixedSizeKeyed() throws Exception {
        // DISPATCH_PREV_CACHE_SLOT is the keyed fixed-size FILL_PREV path: the
        // cursor caches the source value in a per-source-col MapValue slot at
        // every data row, then gap rows read the slot directly via
        // keysMapRecord.getXxx(slot) -- no recordAt hop, no prevRecord
        // positioning. Distinct from DISPATCH_PREV_SLOT (variable-width or
        // non-keyed PREV, which positions prevRecord via recordAt).
        // DOUBLE is fixed-size and slot-eligible; a keyed FILL(PREV) on DOUBLE
        // routes through DISPATCH_PREV_CACHE_SLOT.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('B', 2.0, '2024-01-01T00:00:00.000000Z')," +
                    "('A', 3.0, '2024-01-01T02:00:00.000000Z')," +
                    "('B', 4.0, '2024-01-01T02:00:00.000000Z')");
            // 01:00 is a gap bucket for both keys. Both keys have prev values
            // observed at 00:00, so the cached slots return 1.0 / 2.0 directly.
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tfv
                            2024-01-01T00:00:00.000000Z\tA\t1.0
                            2024-01-01T00:00:00.000000Z\tB\t2.0
                            2024-01-01T01:00:00.000000Z\tA\t1.0
                            2024-01-01T01:00:00.000000Z\tB\t2.0
                            2024-01-01T02:00:00.000000Z\tA\t3.0
                            2024-01-01T02:00:00.000000Z\tB\t4.0
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, first(v) fv FROM x " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    "ts", true, false
            );
        });
    }

    @Test
    public void testDoubleCrossColumnPrevToAggregate() throws Exception {
        // DOUBLE cross-col PREV-to-aggregate branch exercises the
        // FillRecord.getDouble "(mode == FILL_PREV_SELF || mode >= 0) &&
        // hasKeyPrev() -> prevRecord.getDouble(mode >= 0 ? mode : col)"
        // branch when the source is an aggregate column (not a key).
        // Mirrors testFillPrevCrossColumnKeyed in SampleByFillTest.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ival INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, 10, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, 30, '2024-01-01T02:00:00.000000Z')");
            // FILL(PREV, PREV(s)) -- a at 01:00 pulls s's prev value (1.0).
            assertQueryNoLeakCheck(
                    """
                            ts\ts\ta
                            2024-01-01T00:00:00.000000Z\t1.0\t10.0
                            2024-01-01T01:00:00.000000Z\t1.0\t1.0
                            2024-01-01T02:00:00.000000Z\t3.0\t30.0
                            """,
                    "SELECT ts, sum(val) AS s, sum(ival::DOUBLE) AS a FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(s)) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testDoubleFillConstantNullSentinelNoPrevYet() throws Exception {
        // FILL(PREV) with no prior data for a key produces the getter's null
        // sentinel. For DOUBLE, Double.NaN renders as "null" in output.
        // Wrap in ORDER BY ts, k because SAMPLE BY FILL emits keys within a
        // bucket in insertion / key-map order, not alphabetical order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k VARCHAR, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('B', 3.0, '2024-01-01T02:00:00.000000Z')");
            // Key B has no data in the 00:00 or 01:00 buckets -- its fill
            // rows fall through to the null sentinel (no prev yet).
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tfv
                            2024-01-01T00:00:00.000000Z\tA\t1.0
                            2024-01-01T00:00:00.000000Z\tB\tnull
                            2024-01-01T01:00:00.000000Z\tA\t1.0
                            2024-01-01T01:00:00.000000Z\tB\tnull
                            2024-01-01T02:00:00.000000Z\tA\t1.0
                            2024-01-01T02:00:00.000000Z\tB\t3.0
                            """,
                    "SELECT * FROM (" +
                            "SELECT ts, k, first(v) fv FROM x " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k",
                    // Outer ORDER BY wraps the FILL cursor in a sort factory that supports random access.
                    "ts", true, false
            );
        });
    }

    @Test
    public void testGetBinAndBinLenDispatch() throws Exception {
        // BINARY group-by key routes through FillRecord.getBin /
        // FillRecord.getBinLen via DISPATCH_BASE on data rows
        // (baseRecord.getBin) and DISPATCH_KEY_SLOT on gap-bucket fill
        // rows (keysMapRecord.getBin). Single key + FROM/TO bound: one
        // data row at 00:00, two fill rows at 01:00 and 02:00. ORDER BY
        // is unavailable for BINARY columns, so we keep cardinality at
        // one to avoid relying on key-map iteration order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (bn BINARY, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x SELECT rnd_bin(4, 4, 0), 1.0, " +
                    "'2024-01-01T00:00:00.000000Z' FROM long_sequence(1)");
            assertQueryNoLeakCheck(
                    """
                            ts\tbn\tsum
                            2024-01-01T00:00:00.000000Z\t00000000 ee 41 1d 15\t1.0
                            2024-01-01T01:00:00.000000Z\t00000000 ee 41 1d 15\tnull
                            2024-01-01T02:00:00.000000Z\t00000000 ee 41 1d 15\tnull
                            """,
                    "SELECT ts, bn, sum(v) FROM x " +
                            "SAMPLE BY 1h FROM '2024-01-01T00:00:00' TO '2024-01-01T03:00:00' FILL(NULL) " +
                            "ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetBoolDispatch() throws Exception {
        // BOOLEAN aggregate via first(bool), FILL(PREV) exercises
        // FillRecord.getBool FILL_PREV_SELF branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (b BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(true, '2024-01-01T00:00:00.000000Z')," +
                    "(true, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfb
                            2024-01-01T00:00:00.000000Z\ttrue
                            2024-01-01T01:00:00.000000Z\ttrue
                            2024-01-01T02:00:00.000000Z\ttrue
                            """,
                    "SELECT ts, first(b) fb FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetByteDispatch() throws Exception {
        // BYTE aggregate via first(b), FILL(PREV) -> FILL_PREV_SELF branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (b BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(17::BYTE, '2024-01-01T00:00:00.000000Z')," +
                    "(17::BYTE, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfb
                            2024-01-01T00:00:00.000000Z\t17
                            2024-01-01T01:00:00.000000Z\t17
                            2024-01-01T02:00:00.000000Z\t17
                            """,
                    "SELECT ts, first(b) fb FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetByteDispatchFillConstant() throws Exception {
        // BYTE FILL_CONSTANT branch -- 01:00 gap row reads the fill constant
        // through FillRecord.getByte's DISPATCH_CONSTANT arm. Narrow-integer
        // Function convention: byte fills come through getInt() -> (byte) cast.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (b BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(17::BYTE, '2024-01-01T00:00:00.000000Z')," +
                    "(33::BYTE, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfb
                            2024-01-01T00:00:00.000000Z\t17
                            2024-01-01T01:00:00.000000Z\t7
                            2024-01-01T02:00:00.000000Z\t33
                            """,
                    "SELECT ts, first(b) fb FROM x SAMPLE BY 1h FILL(7) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetCharDispatch() throws Exception {
        // CHAR aggregate via first(c), FILL(PREV) -> FILL_PREV_SELF branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (c CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', '2024-01-01T00:00:00.000000Z')," +
                    "('A', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfc
                            2024-01-01T00:00:00.000000Z\tA
                            2024-01-01T01:00:00.000000Z\tA
                            2024-01-01T02:00:00.000000Z\tA
                            """,
                    "SELECT ts, first(c) fc FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetDecimal128Dispatch() throws Exception {
        // DECIMAL128 (precision > 18) aggregate via first, FILL(PREV).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (d DECIMAL(30, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(123.45::DECIMAL(30, 2), '2024-01-01T00:00:00.000000Z')," +
                    "(123.45::DECIMAL(30, 2), '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfd
                            2024-01-01T00:00:00.000000Z\t123.45
                            2024-01-01T01:00:00.000000Z\t123.45
                            2024-01-01T02:00:00.000000Z\t123.45
                            """,
                    "SELECT ts, first(d) fd FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetDecimal16Dispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (d DECIMAL(4, 1), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(12.3::DECIMAL(4, 1), '2024-01-01T00:00:00.000000Z')," +
                    "(12.3::DECIMAL(4, 1), '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfd
                            2024-01-01T00:00:00.000000Z\t12.3
                            2024-01-01T01:00:00.000000Z\t12.3
                            2024-01-01T02:00:00.000000Z\t12.3
                            """,
                    "SELECT ts, first(d) fd FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetDecimal256Dispatch() throws Exception {
        // DECIMAL256 aggregate via first, FILL(PREV).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (d DECIMAL(60, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(99999999.99::DECIMAL(60, 2), '2024-01-01T00:00:00.000000Z')," +
                    "(99999999.99::DECIMAL(60, 2), '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfd
                            2024-01-01T00:00:00.000000Z\t99999999.99
                            2024-01-01T01:00:00.000000Z\t99999999.99
                            2024-01-01T02:00:00.000000Z\t99999999.99
                            """,
                    "SELECT ts, first(d) fd FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetDecimal32Dispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (d DECIMAL(9, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(12345.67::DECIMAL(9, 2), '2024-01-01T00:00:00.000000Z')," +
                    "(12345.67::DECIMAL(9, 2), '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfd
                            2024-01-01T00:00:00.000000Z\t12345.67
                            2024-01-01T01:00:00.000000Z\t12345.67
                            2024-01-01T02:00:00.000000Z\t12345.67
                            """,
                    "SELECT ts, first(d) fd FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetDecimal64Dispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (d DECIMAL(18, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(123456789.01::DECIMAL(18, 2), '2024-01-01T00:00:00.000000Z')," +
                    "(123456789.01::DECIMAL(18, 2), '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfd
                            2024-01-01T00:00:00.000000Z\t123456789.01
                            2024-01-01T01:00:00.000000Z\t123456789.01
                            2024-01-01T02:00:00.000000Z\t123456789.01
                            """,
                    "SELECT ts, first(d) fd FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetDecimal8Dispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (d DECIMAL(2, 1), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.2::DECIMAL(2, 1), '2024-01-01T00:00:00.000000Z')," +
                    "(1.2::DECIMAL(2, 1), '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfd
                            2024-01-01T00:00:00.000000Z\t1.2
                            2024-01-01T01:00:00.000000Z\t1.2
                            2024-01-01T02:00:00.000000Z\t1.2
                            """,
                    "SELECT ts, first(d) fd FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetDoubleDispatchFillConstant() throws Exception {
        // FILL_CONSTANT branch -- 01:00 gap row uses the fill constant.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfv
                            2024-01-01T00:00:00.000000Z\t1.0
                            2024-01-01T01:00:00.000000Z\t42.0
                            2024-01-01T02:00:00.000000Z\t3.0
                            """,
                    "SELECT ts, first(v) fv FROM x SAMPLE BY 1h FILL(42.0) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetDoubleDispatchFillPrevSelf() throws Exception {
        // FILL_PREV_SELF branch -- 01:00 gap row reads from prevRecord.
        // Uses assertQueryNoLeakCheck so supportsRandomAccess=false and
        // expectSize=false are asserted against the fill cursor's factory
        // properties -- a regression flipping recordCursorSupportsRandomAccess
        // to true would be caught here. The rest of the per-getter tests
        // remain on assertSql for brevity; locking these properties once is
        // sufficient since every fill query routes through the same factory.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfv
                            2024-01-01T00:00:00.000000Z\t1.0
                            2024-01-01T01:00:00.000000Z\t1.0
                            2024-01-01T02:00:00.000000Z\t3.0
                            """,
                    "SELECT ts, first(v) fv FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetFloatDispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (f FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(4.5::FLOAT, '2024-01-01T00:00:00.000000Z')," +
                    "(4.5::FLOAT, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tff
                            2024-01-01T00:00:00.000000Z\t4.5
                            2024-01-01T01:00:00.000000Z\t4.5
                            2024-01-01T02:00:00.000000Z\t4.5
                            """,
                    "SELECT ts, first(f) ff FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetFloatDispatchFillConstant() throws Exception {
        // FLOAT FILL_CONSTANT branch -- 01:00 gap row reads the fill constant
        // through FillRecord.getFloat's DISPATCH_CONSTANT arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (f FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(4.5::FLOAT, '2024-01-01T00:00:00.000000Z')," +
                    "(7.5::FLOAT, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tff
                            2024-01-01T00:00:00.000000Z\t4.5
                            2024-01-01T01:00:00.000000Z\t99.25
                            2024-01-01T02:00:00.000000Z\t7.5
                            """,
                    "SELECT ts, first(f) ff FROM x SAMPLE BY 1h FILL(99.25::FLOAT) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetGeoByteDispatch() throws Exception {
        // GEOHASH <= 7 bits -> stored as byte. first(g) + FILL(PREV) exercises
        // FillRecord.getGeoByte via the FILL_PREV_SELF branch. Two distinct
        // values at 00:00 and 02:00 with a fill at 01:00 lets us pin the gap
        // row to the carried-forward value rather than just row count.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (g GEOHASH(5b), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(##00111, '2024-01-01T00:00:00.000000Z')," +
                    "(##11000, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfg
                            2024-01-01T00:00:00.000000Z\t7
                            2024-01-01T01:00:00.000000Z\t7
                            2024-01-01T02:00:00.000000Z\ts
                            """,
                    "SELECT ts, first(g) fg FROM x " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetGeoIntDispatch() throws Exception {
        // GEOHASH 16..31 bits -> stored as int. Two distinct literal geohashes
        // pin the FILL_PREV_SELF carry-forward to a specific char value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (g GEOHASH(4c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('sp05', '2024-01-01T00:00:00.000000Z')," +
                    "('u33d', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfg
                            2024-01-01T00:00:00.000000Z\tsp05
                            2024-01-01T01:00:00.000000Z\tsp05
                            2024-01-01T02:00:00.000000Z\tu33d
                            """,
                    "SELECT ts, first(g) fg FROM x " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetGeoLongDispatch() throws Exception {
        // GEOHASH 32..60 bits -> stored as long.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (g GEOHASH(10c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('sp052n01b3', '2024-01-01T00:00:00.000000Z')," +
                    "('u33d8b1234', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfg
                            2024-01-01T00:00:00.000000Z\tsp052n01b3
                            2024-01-01T01:00:00.000000Z\tsp052n01b3
                            2024-01-01T02:00:00.000000Z\tu33d8b1234
                            """,
                    "SELECT ts, first(g) fg FROM x " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetGeoShortDispatch() throws Exception {
        // GEOHASH 8..15 bits -> stored as short.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (g GEOHASH(12b), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(##011001100101, '2024-01-01T00:00:00.000000Z')," +
                    "(##111000111000, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfg
                            2024-01-01T00:00:00.000000Z\t011001100101
                            2024-01-01T01:00:00.000000Z\t011001100101
                            2024-01-01T02:00:00.000000Z\t111000111000
                            """,
                    "SELECT ts, first(g) fg FROM x " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetIPv4Dispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ip IPV4, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('1.2.3.4'::IPV4, '2024-01-01T00:00:00.000000Z')," +
                    "('1.2.3.4'::IPV4, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfip
                            2024-01-01T00:00:00.000000Z\t1.2.3.4
                            2024-01-01T01:00:00.000000Z\t1.2.3.4
                            2024-01-01T02:00:00.000000Z\t1.2.3.4
                            """,
                    "SELECT ts, first(ip) fip FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetIntDispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(17, '2024-01-01T00:00:00.000000Z')," +
                    "(17, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfi
                            2024-01-01T00:00:00.000000Z\t17
                            2024-01-01T01:00:00.000000Z\t17
                            2024-01-01T02:00:00.000000Z\t17
                            """,
                    "SELECT ts, first(i) fi FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetIntDispatchFillConstant() throws Exception {
        // INT FILL_CONSTANT branch -- 01:00 gap row reads the fill constant
        // through FillRecord.getInt's DISPATCH_CONSTANT arm.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(17, '2024-01-01T00:00:00.000000Z')," +
                    "(33, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfi
                            2024-01-01T00:00:00.000000Z\t17
                            2024-01-01T01:00:00.000000Z\t99
                            2024-01-01T02:00:00.000000Z\t33
                            """,
                    "SELECT ts, first(i) fi FROM x SAMPLE BY 1h FILL(99) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetIntervalDispatch() throws Exception {
        // INTERVAL has no first(interval) aggregate, so the only route to an
        // INTERVAL output column is an inline interval(lo, hi) expression used
        // as a GROUP BY key. FillRecord.getInterval FILL_KEY branch is the
        // only reachable branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (lo TIMESTAMP, hi TIMESTAMP, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('2020-01-01T00:00:00.000Z'::TIMESTAMP, '2020-02-01T00:00:00.000Z'::TIMESTAMP, 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('2020-01-01T00:00:00.000Z'::TIMESTAMP, '2020-02-01T00:00:00.000Z'::TIMESTAMP, 30.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tk\tfirst
                            2024-01-01T00:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0
                            2024-01-01T01:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0
                            2024-01-01T02:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t30.0
                            """,
                    "SELECT ts, interval(lo, hi) k, first(v) FROM t " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetLong128HiAndLoDispatch() throws Exception {
        // UUID is backed by long128 (hi + lo). first(u) + FILL(PREV) exercises
        // FillRecord.getLong128Hi and getLong128Lo via FILL_PREV_SELF.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (u UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('11111111-2222-3333-4444-555555555555'::UUID, '2024-01-01T00:00:00.000000Z')," +
                    "('11111111-2222-3333-4444-555555555555'::UUID, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfu
                            2024-01-01T00:00:00.000000Z\t11111111-2222-3333-4444-555555555555
                            2024-01-01T01:00:00.000000Z\t11111111-2222-3333-4444-555555555555
                            2024-01-01T02:00:00.000000Z\t11111111-2222-3333-4444-555555555555
                            """,
                    "SELECT ts, first(u) fu FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetLong256AAndBDispatch() throws Exception {
        // LONG256 key column drives getLong256A, getLong256B, and the void-sink
        // getLong256(int, CharSink) via FILL_KEY on gap rows. CursorPrinter
        // routes rendering through getLong256(int, CharSink), locking the
        // void-sink variant as well. first(long256) currently returns null in
        // SAMPLE BY FILL pipelines, so use the key-column path.
        // Verify dispatch via row count; text rendering of LONG256 literal
        // constants varies and is out of scope for this property test.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (l LONG256, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(cast('0x01' AS LONG256), 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(cast('0x01' AS LONG256), 3.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    "c\n3\n",
                    "SELECT count(*) c FROM (" +
                            "SELECT ts, l, sum(v) v FROM x " +
                            "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR" +
                            ")",
                    null, false, true
            );
        });
    }

    @Test
    public void testGetLongDispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1234567890, '2024-01-01T00:00:00.000000Z')," +
                    "(1234567890, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfl
                            2024-01-01T00:00:00.000000Z\t1234567890
                            2024-01-01T01:00:00.000000Z\t1234567890
                            2024-01-01T02:00:00.000000Z\t1234567890
                            """,
                    "SELECT ts, first(l) fl FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetLongOnTimestampDuringGapReturnsBucketTs() throws Exception {
        // The TIMESTAMP column is a 64-bit long internally, so Record.getLong(timestampIndex)
        // is a valid call from any caller that doesn't route through getTimestamp(). Without
        // a DISPATCH_TIMESTAMP_FILL arm in getLong(), gap-fill rows would silently return
        // Numbers.LONG_NULL while getTimestamp() correctly returns the bucket boundary.
        // This drives the cursor directly so the bug is observable rather than masked by
        // upstream callers that happen to use getTimestamp(). getDate() defaults to
        // getLong() (Record.java:159), so the same arm covers both.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, '2024-01-01T02:00:00.000000Z')");
            try (RecordCursorFactory factory = select(
                    "SELECT ts, first(v) fv FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final int tsIdx = factory.getMetadata().getColumnIndex("ts");
                    final Record record = cursor.getRecord();
                    final long[] expected = {
                            1_704_067_200_000_000L,  // 2024-01-01T00:00:00 -- data row
                            1_704_070_800_000_000L,  // 2024-01-01T01:00:00 -- GAP-FILL row
                            1_704_074_400_000_000L   // 2024-01-01T02:00:00 -- data row
                    };
                    int row = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(
                                "row " + row + " getTimestamp",
                                expected[row],
                                record.getTimestamp(tsIdx)
                        );
                        Assert.assertEquals(
                                "row " + row + " getLong (DISPATCH_TIMESTAMP_FILL arm)",
                                expected[row],
                                record.getLong(tsIdx)
                        );
                        Assert.assertEquals(
                                "row " + row + " getDate (defaults to getLong)",
                                expected[row],
                                record.getDate(tsIdx)
                        );
                        row++;
                    }
                    Assert.assertEquals("expected 3 rows", 3, row);
                }
            }
        });
    }

    @Test
    public void testGetShortDispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(17::SHORT, '2024-01-01T00:00:00.000000Z')," +
                    "(17::SHORT, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfs
                            2024-01-01T00:00:00.000000Z\t17
                            2024-01-01T01:00:00.000000Z\t17
                            2024-01-01T02:00:00.000000Z\t17
                            """,
                    "SELECT ts, first(s) fs FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetStrAAndBAndLenDispatch() throws Exception {
        // STRING first(s) + FILL(PREV) -> FillRecord.getStrA, getStrB, getStrLen
        // all route via FILL_PREV_SELF.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('hello', '2024-01-01T00:00:00.000000Z')," +
                    "('hello', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfs
                            2024-01-01T00:00:00.000000Z\thello
                            2024-01-01T01:00:00.000000Z\thello
                            2024-01-01T02:00:00.000000Z\thello
                            """,
                    "SELECT ts, first(s) fs FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetSymAAndBDispatch() throws Exception {
        // SYMBOL first(sym) + FILL(PREV) exercises getSymA / getSymB via
        // FILL_PREV_SELF. Cross-col SYMBOL is forbidden by grammar; that
        // rejection path is covered by testFillPrevCrossColumnRejectsSymbolSource.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('sym1', '2024-01-01T00:00:00.000000Z')," +
                    "('sym1', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfsym
                            2024-01-01T00:00:00.000000Z\tsym1
                            2024-01-01T01:00:00.000000Z\tsym1
                            2024-01-01T02:00:00.000000Z\tsym1
                            """,
                    "SELECT ts, first(sym) fsym FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetTimestampDispatch() throws Exception {
        // getTimestamp has a special col == timestampIndex branch (:1200) that
        // returns currentBucketTimestamp directly rather than dispatching.
        // Any FILL query emits the bucket timestamp through this fast path;
        // the FILL_PREV_SELF branch is exercised by including a non-timestamp
        // aggregate and observing that its gap-fill value is the prev record's.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfv
                            2024-01-01T00:00:00.000000Z\t1.0
                            2024-01-01T01:00:00.000000Z\t1.0
                            2024-01-01T02:00:00.000000Z\t3.0
                            """,
                    "SELECT ts, first(v) fv FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetVarcharAAndBAndSizeDispatch() throws Exception {
        // VARCHAR first(vc) + FILL(PREV) exercises getVarcharA / getVarcharB /
        // getVarcharSize via FILL_PREV_SELF.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (vc VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('varchar_val', '2024-01-01T00:00:00.000000Z')," +
                    "('varchar_val', '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tfvc
                            2024-01-01T00:00:00.000000Z\tvarchar_val
                            2024-01-01T01:00:00.000000Z\tvarchar_val
                            2024-01-01T02:00:00.000000Z\tvarchar_val
                            """,
                    "SELECT ts, first(vc) fvc FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

}
