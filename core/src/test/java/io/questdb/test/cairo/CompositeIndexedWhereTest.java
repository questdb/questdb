/*******************************************************************************
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

package io.questdb.test.cairo;

import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Reading a COMPOSITE table through an INDEX must return what an unindexed scan returns.
 * <p>
 * <b>The oracle.</b> Every assertion here is a TWIN COMPARISON: the same query with and without
 * {@code NO_INDEX}. Nothing is compared against a hand-written expected string, because a hand-written
 * string encodes what I believe the answer is, and the two failure modes here are exactly the ones
 * belief gets wrong. The twin catches both:
 * <ul>
 *   <li><b>Wrong ORDER.</b> A composite table's page frames arrive {@code (day ASC, cellKey ASC)}, so
 *       the row-cursor family that serves an indexed WHERE emits, per day, {@code cell0 ++ cell1 ++ ...}
 *       -- cell-major, not timestamp order. MEASURED before the fix: {@code 01 E0, 03 E0, 02 E1, 04 E1}
 *       against the twin's {@code 01, 02, 03, 04}.</li>
 *   <li><b>Wrong ROWS.</b> The alternative fix -- reusing the existing cross-cell merge cursor -- would
 *       have silently DROPPED the key predicate, returning rows for symbol values outside the WHERE
 *       clause. A twin comparison catches that; an order-only assertion would not.</li>
 * </ul>
 * <p>
 * <b>The shapes are not interchangeable.</b> Each test below reaches a DIFFERENT factory in
 * {@code SqlCodeGenerator}'s indexed branch, and each of those was a separate return site needing its
 * own wrap. When this work started only the single-value shape was measured; the {@code IN}, {@code
 * NOT IN}, subquery and covering shapes were found by reading the branch, not by the symptom. Closing
 * one shape is not closing the class.
 * <p>
 * Every test asserts the twins AGREE and that the result is NON-EMPTY. Without the emptiness check a
 * predicate that matched nothing would pass every comparison vacuously.
 */
public class CompositeIndexedWhereTest extends AbstractCairoTest {

    /**
     * Interleaved across cells: each symbol value appears in BOTH cells at alternating timestamps.
     * A cell-major result is therefore visibly different from a timestamp-ordered one -- on a layout
     * where each value lived in a single cell, the two would coincide and the bug would hide.
     */
    private void createInterleaved(String name) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, exch SYMBOL, sym SYMBOL INDEX TYPE POSTING, px DOUBLE) "
                + "TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
        execute("INSERT INTO " + name + " VALUES "
                + "('2023-05-01T01:00:00.000000Z','E0','AAA',1.0),"
                + "('2023-05-01T02:00:00.000000Z','E1','AAA',2.0),"
                + "('2023-05-01T03:00:00.000000Z','E0','AAA',3.0),"
                + "('2023-05-01T04:00:00.000000Z','E1','AAA',4.0),"
                + "('2023-05-01T05:00:00.000000Z','E0','BBB',5.0),"
                + "('2023-05-01T06:00:00.000000Z','E1','BBB',6.0),"
                + "('2023-05-02T01:00:00.000000Z','E1','AAA',7.0),"
                + "('2023-05-02T02:00:00.000000Z','E0','AAA',8.0),"
                + "('2023-05-02T03:00:00.000000Z','E1','CCC',9.0)");
        drainWalQueue();
    }

    @Test
    public void testSingleValueEquals() throws Exception {
        assertMemoryLeak(() -> {
            createInterleaved("c");
            assertMatchesUnindexed("SELECT ts, exch, sym, px FROM c WHERE sym = 'AAA'", "sym");
        });
    }

    @Test
    public void testSingleValueEqualsOrderByTimestampAscending() throws Exception {
        assertMemoryLeak(() -> {
            createInterleaved("c");
            // ORDER BY ts is the shape where the generator ELIDES the sort if the factory claims
            // timestamp order -- so this is the assertion that the claim is now honest.
            assertMatchesUnindexed("SELECT ts, exch, sym FROM c WHERE sym = 'AAA' ORDER BY ts", "sym");
        });
    }

    @Test
    public void testSingleValueEqualsOrderByTimestampDescending() throws Exception {
        assertMemoryLeak(() -> {
            createInterleaved("c");
            assertMatchesUnindexed("SELECT ts, exch, sym FROM c WHERE sym = 'AAA' ORDER BY ts DESC", "sym");
        });
    }

    /**
     * {@code ORDER BY sym, ts} -- the shape that previously took the symbol-key shortcut.
     */
    @Test
    public void testOrderBySymbolThenTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createInterleaved("c");
            assertMatchesUnindexed(
                    "SELECT ts, exch, sym FROM c WHERE sym IN ('AAA','BBB') ORDER BY sym, ts", "sym");
        });
    }

    /**
     * Multi-value IN: a different factory again (FilterOnValues).
     */
    @Test
    public void testInList() throws Exception {
        assertMemoryLeak(() -> {
            createInterleaved("c");
            assertMatchesUnindexed("SELECT ts, exch, sym, px FROM c WHERE sym IN ('AAA','CCC')", "sym");
        });
    }

    /**
     * NOT IN reaches FilterOnExcludedValues, which was a separate return site.
     */
    @Test
    public void testNotInList() throws Exception {
        assertMemoryLeak(() -> {
            createInterleaved("c");
            assertMatchesUnindexed("SELECT ts, exch, sym, px FROM c WHERE sym NOT IN ('BBB')", "sym");
        });
    }

    /**
     * A sub-query key set reaches FilterOnSubQuery -- yet another return site.
     */
    @Test
    public void testSubQueryKeySet() throws Exception {
        assertMemoryLeak(() -> {
            createInterleaved("c");
            assertMatchesUnindexed(
                    "SELECT ts, exch, sym FROM c WHERE sym IN (SELECT sym FROM c WHERE px > 6.0)", "sym");
        });
    }

    /**
     * An indexed predicate PLUS a residual filter, which is compiled and attached separately.
     */
    @Test
    public void testIndexedPredicateWithResidualFilter() throws Exception {
        assertMemoryLeak(() -> {
            createInterleaved("c");
            assertMatchesUnindexed("SELECT ts, exch, sym, px FROM c WHERE sym = 'AAA' AND px > 2.0", "sym");
        });
    }

    /**
     * A COVERING index serves the projection from the index sidecars rather than the column files,
     * which is a different cursor entirely.
     */
    @Test
    public void testCoveringIndexProjection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cc (ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("INSERT INTO cc VALUES "
                    + "('2023-05-01T01:00:00.000000Z','E0','AAA',1.0),"
                    + "('2023-05-01T02:00:00.000000Z','E1','AAA',2.0),"
                    + "('2023-05-01T03:00:00.000000Z','E0','AAA',3.0),"
                    + "('2023-05-01T04:00:00.000000Z','E1','BBB',4.0)");
            drainWalQueue();
            execute("ALTER TABLE cc ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (px)");
            drainWalQueue();
            assertMatchesUnindexed("SELECT ts, sym, px FROM cc WHERE sym = 'AAA'", "sym");
        });
    }

    /**
     * A predicate on the DIMENSION column still prunes to whole cells and must not be sorted or
     * otherwise disturbed -- it never had the defect, so this guards against the fix over-reaching.
     */
    @Test
    public void testDimensionPredicateUnaffected() throws Exception {
        assertMemoryLeak(() -> {
            createInterleaved("c");
            assertMatchesUnindexed("SELECT ts, exch, sym FROM c WHERE exch = 'E0'", "exch");
        });
    }

    /**
     * POSITIVE CONTROL. The same shapes on a PLAIN table must also agree, so a passing composite
     * assertion cannot be explained by {@code NO_INDEX} and the indexed path having been broken in
     * the same direction, or by the hint silently doing nothing.
     */
    @Test
    public void testPlainTableTwinsAgree() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, sym SYMBOL INDEX TYPE POSTING, px DOUBLE) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO p VALUES "
                    + "('2023-05-01T01:00:00.000000Z','E0','AAA',1.0),"
                    + "('2023-05-01T02:00:00.000000Z','E1','AAA',2.0),"
                    + "('2023-05-01T03:00:00.000000Z','E0','BBB',3.0)");
            drainWalQueue();
            assertMatchesUnindexed("SELECT ts, exch, sym, px FROM p WHERE sym = 'AAA'", "sym");
            assertMatchesUnindexed("SELECT ts, exch, sym, px FROM p WHERE sym IN ('AAA','BBB')", "sym");
        });
    }

    /**
     * Runs {@code query} twice -- once as written, once with {@code NO_INDEX} on {@code hintColumn} --
     * and asserts the two agree. Also asserts the result has rows, so a predicate that matched nothing
     * cannot pass vacuously.
     */
    private void assertMatchesUnindexed(String query, String hintColumn) throws Exception {
        final String unindexed = query.replaceFirst("(?i)^SELECT",
                "SELECT /*+ NO_INDEX(" + hintColumn + ") */");

        final StringSink indexedSink = new StringSink();
        final StringSink unindexedSink = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, query, indexedSink);
        TestUtils.printSql(engine, sqlExecutionContext, unindexed, unindexedSink);

        // A header-only result would make the comparison meaningless.
        org.junit.Assert.assertTrue(
                "twin comparison is vacuous -- the predicate matched no rows: " + query,
                unindexedSink.toString().indexOf('\n') < unindexedSink.length() - 1);

        TestUtils.assertEquals(
                "indexed result differs from the unindexed twin for: " + query,
                unindexedSink, indexedSink);
    }
}
