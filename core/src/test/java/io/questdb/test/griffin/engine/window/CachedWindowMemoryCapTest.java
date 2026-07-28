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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CachedWindowMemoryCapTest extends AbstractCairoTest {

    @Test
    public void testCacheCapErrorNamesLegacyStorePagesWhenBytesUnset() throws Exception {
        // When the legacy cairo.sql.window.store.max.pages is the only explicit cap, the runtime
        // error must name it so the user can raise the right key. The new bytes key would have no
        // effect here because store.max.pages drives the resolved cap.
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 4096);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_MAX_PAGES, 2);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT" +
                    " ('s' || (x % 8))::SYMBOL AS sym," +
                    " (x * 1_000_000_000L)::TIMESTAMP AS ts" +
                    " FROM long_sequence(50_000)) TIMESTAMP(ts)");

            assertExceptionNoLeakCheck(
                    "SELECT sym, ts, lag(ts, 1) OVER (PARTITION BY sym ORDER BY ts DESC) FROM tab",
                    0,
                    "breached in VirtualMemory (raise cairo.sql.window.store.max.pages)"
            );
        });
    }

    @Test
    public void testCacheCapFires() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 4096);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHE_MAX_BYTES, 8192);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT" +
                    " ('s' || (x % 8))::SYMBOL AS sym," +
                    " (x * 1_000_000_000L)::TIMESTAMP AS ts" +
                    " FROM long_sequence(50_000)) TIMESTAMP(ts)");

            assertExceptionNoLeakCheck(
                    "SELECT sym, ts, lag(ts, 1) OVER (PARTITION BY sym ORDER BY ts DESC) FROM tab",
                    0,
                    "breached in VirtualMemory (raise cairo.sql.window.cache.max.bytes)"
            );
        });
    }

    @Test
    public void testCacheCapRaisedUnblocksQuery() throws Exception {
        // testCacheCapFires uses the same query/dataset and fails at the 8 KiB cap.
        // Raising cairo.sql.window.cache.max.bytes lets the same workload complete.
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHE_MAX_BYTES, 16L * 1024 * 1024);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT" +
                    " ('s' || (x % 8))::SYMBOL AS sym," +
                    " (x * 1_000_000_000L)::TIMESTAMP AS ts" +
                    " FROM long_sequence(50_000)) TIMESTAMP(ts)");

            assertQuery("SELECT sym, ts, lag(ts, 1) OVER (PARTITION BY sym ORDER BY ts DESC) FROM tab LIMIT 3")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            sym\tts\tlag
                            s1\t1970-01-01T00:16:40.000000Z\t1970-01-01T02:30:00.000000Z
                            s2\t1970-01-01T00:33:20.000000Z\t1970-01-01T02:46:40.000000Z
                            s3\t1970-01-01T00:50:00.000000Z\t1970-01-01T03:03:20.000000Z
                            """);
        });
    }

    @Test
    public void testDenseRankStreamingSinkCapNamesDenseRankOwner() throws Exception {
        // DenseRankFunctionFactory reuses RankFunctionFactory.RankFunction, which owns the
        // SingleRecordSink pair, so the budget message is produced by shared code that only sees
        // the dense flag. It used to hard-code the RANK() owner, which told a DENSE_RANK() user to
        // go look at a function their query never mentioned.
        //
        // Same shape as testRankStreamingSinkCapNamesRankOwner: a two-column (SYMBOL, TIMESTAMP)
        // window ORDER BY serializes to 12 bytes and so outgrows the sink's 8-byte initial
        // capacity, which is what takes the query into resize().
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 64);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_MAX_PAGES, 0);

        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tab (sym SYMBOL INDEX, l LONG, ts TIMESTAMP)
                      TIMESTAMP(ts) PARTITION BY DAY""");
            execute("""
                    INSERT INTO tab VALUES
                      ('a', 1, '2024-01-01T00:00:00.000000Z'),
                      ('b', 2, '2024-01-01T00:00:01.000000Z'),
                      ('a', 3, '2024-01-01T00:00:02.000000Z'),
                      ('b', 4, '2024-01-01T00:00:03.000000Z')""");

            assertSinkCapMessage("dense_rank", "DENSE_RANK() window function");
            // The sibling test pins the same path for rank(), so assert here that the two owners
            // stay distinct rather than both drifting onto one name.
            assertSinkCapMessage("rank", "RANK() window function");
        });
    }

    @Test
    public void testEncodedSortCapFires() throws Exception {
        // An encoded-eligible ORDER BY key (the designated timestamp) takes the encoded sort
        // buffer, not the tree. The buffer must still honor the window-specific memory caps, so a
        // user who lowers them to bound window sort memory keeps that bound on the encoded path.
        // The encoded buffer interleaves keys and rowIds, so its budget is the sum of both caps.
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_TREE_MAX_BYTES, 4096);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_ROWID_MAX_BYTES, 4096);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT" +
                    " ('s' || (x % 8))::SYMBOL AS sym," +
                    " (x * 1_000_000_000L)::TIMESTAMP AS ts" +
                    " FROM long_sequence(50_000)) TIMESTAMP(ts)");

            assertExceptionNoLeakCheck(
                    "SELECT sym, ts, lag(ts, 1) OVER (PARTITION BY sym ORDER BY ts DESC) FROM tab",
                    0,
                    "memory exceeded in window encoded sort (raise cairo.sql.window.tree.max.bytes / cairo.sql.window.rowid.max.bytes)"
            );
        });
    }

    @Test
    public void testEncodedSortVarcharCapFires() throws Exception {
        // A VARCHAR ORDER BY key takes the encoded buffer too, spilling its bytes into the key
        // heap; the window memory caps still bound that path.
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_TREE_MAX_BYTES, 4096);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_ROWID_MAX_BYTES, 4096);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT" +
                    " ('s' || (x % 8))::SYMBOL AS sym," +
                    " (x * 1_000_000_000L)::TIMESTAMP AS ts," +
                    " ('v' || x)::VARCHAR AS v" +
                    " FROM long_sequence(50_000)) TIMESTAMP(ts)");

            assertExceptionNoLeakCheck(
                    "SELECT sym, ts, lag(ts, 1) OVER (PARTITION BY sym ORDER BY v) FROM tab",
                    0,
                    "memory exceeded in window encoded sort (raise cairo.sql.window.tree.max.bytes / cairo.sql.window.rowid.max.bytes)"
            );
        });
    }

    @Test
    public void testEncodedSortVarcharResults() throws Exception {
        // A VARCHAR ORDER BY key exercises the encoded buffer's variable sort; verify the ordering.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT" +
                    " ('p' || (x % 2))::SYMBOL AS sym," +
                    " (x * 1_000_000L)::TIMESTAMP AS ts," +
                    " ('k' || (100 - x))::VARCHAR AS k," +
                    " x::LONG AS n" +
                    " FROM long_sequence(6)) TIMESTAMP(ts)");

            assertQuery("SELECT sym, ts, n, lag(n, 1) OVER (PARTITION BY sym ORDER BY k) AS prev_n FROM tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            sym\tts\tn\tprev_n
                            p1\t1970-01-01T00:00:01.000000Z\t1\t3
                            p0\t1970-01-01T00:00:02.000000Z\t2\t4
                            p1\t1970-01-01T00:00:03.000000Z\t3\t5
                            p0\t1970-01-01T00:00:04.000000Z\t4\t6
                            p1\t1970-01-01T00:00:05.000000Z\t5\tnull
                            p0\t1970-01-01T00:00:06.000000Z\t6\tnull
                            """);
        });
    }

    @Test
    public void testHappyPathUnchanged() throws Exception {
        // The default uncapped configuration must not regress small queries.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT" +
                    " ('s' || (x % 4))::SYMBOL AS sym," +
                    " timestamp_sequence(0, 1_000_000L) AS ts," +
                    " x::LONG AS v" +
                    " FROM long_sequence(12)) TIMESTAMP(ts)");

            assertQuery("SELECT sym, ts, v, lag(v, 1) OVER (PARTITION BY sym ORDER BY ts) AS prev_v FROM tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            sym\tts\tv\tprev_v
                            s1\t1970-01-01T00:00:00.000000Z\t1\tnull
                            s2\t1970-01-01T00:00:01.000000Z\t2\tnull
                            s3\t1970-01-01T00:00:02.000000Z\t3\tnull
                            s0\t1970-01-01T00:00:03.000000Z\t4\tnull
                            s1\t1970-01-01T00:00:04.000000Z\t5\t1
                            s2\t1970-01-01T00:00:05.000000Z\t6\t2
                            s3\t1970-01-01T00:00:06.000000Z\t7\t3
                            s0\t1970-01-01T00:00:07.000000Z\t8\t4
                            s1\t1970-01-01T00:00:08.000000Z\t9\t5
                            s2\t1970-01-01T00:00:09.000000Z\t10\t6
                            s3\t1970-01-01T00:00:10.000000Z\t11\t7
                            s0\t1970-01-01T00:00:11.000000Z\t12\t8
                            """);
        });
    }

    @Test
    public void testRankStreamingSinkCapNamesRankOwner() throws Exception {
        // The streaming RANK() path serializes each window ORDER BY key into a SingleRecordSink,
        // whose limit message names the owning feature. RankFunctionFactory threads that name in;
        // the message used to be hard-coded to "ASOF join", so RANK reported an ASOF join error.
        //
        // Reaching the sink's budget needs a key wider than its 8-byte initial capacity. Following
        // order-by advice on an indexed SYMBOL admits a two-column (SYMBOL, TIMESTAMP) window
        // ORDER BY, which serializes to 12 bytes and therefore enters resize().
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 64);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_MAX_PAGES, 0);

        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tab (sym SYMBOL INDEX, l LONG, ts TIMESTAMP)
                      TIMESTAMP(ts) PARTITION BY DAY""");
            execute("""
                    INSERT INTO tab VALUES
                      ('a', 1, '2024-01-01T00:00:00.000000Z'),
                      ('b', 2, '2024-01-01T00:00:01.000000Z'),
                      ('a', 3, '2024-01-01T00:00:02.000000Z'),
                      ('b', 4, '2024-01-01T00:00:03.000000Z')""");

            // Catch explicitly to assert on the typed LimitOverflowException rather than on a
            // message alone. (assertExceptionNoLeakCheck() would also do: TestUtils.assertException
            // ends with Assert.fail("SQL statement should have failed"), and that AssertionError is
            // not a FlyweightMessageContainer, so it is rethrown rather than swallowed.)
            assertSinkCapMessage("rank", "RANK() window function");

            // Negative control: a single 8-byte key fits the initial capacity, so the same budget
            // never reaches resize(). This pins that the assertion above is driven by key width.
            assertQuery("SELECT ts, rank() OVER (ORDER BY ts) FROM tab")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            ts\trank
                            2024-01-01T00:00:00.000000Z\t1
                            2024-01-01T00:00:01.000000Z\t2
                            2024-01-01T00:00:02.000000Z\t3
                            2024-01-01T00:00:03.000000Z\t4
                            """);
        });
    }

    @Test
    public void testRepeatedCursorsStayUnderCap() throws Exception {
        // The cap is enforced per cursor execution. Running the same query twice in a row,
        // with each run staying under the cap, must succeed both times - the second run
        // must not see leftover state from the first.
        final long perCursorBytes = 16L * 1024 * 1024;
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_TREE_MAX_BYTES, perCursorBytes);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_ROWID_MAX_BYTES, perCursorBytes);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHE_MAX_BYTES, perCursorBytes);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT" +
                    " ('s' || (x % 8))::SYMBOL AS sym," +
                    " (x * 1_000_000_000L)::TIMESTAMP AS ts" +
                    " FROM long_sequence(5_000)) TIMESTAMP(ts)");

            final String query = "SELECT sym, ts, lag(ts, 1) OVER (PARTITION BY sym ORDER BY ts DESC) FROM tab LIMIT 3";
            final String expected = """
                    sym\tts\tlag
                    s1\t1970-01-01T00:16:40.000000Z\t1970-01-01T02:30:00.000000Z
                    s2\t1970-01-01T00:33:20.000000Z\t1970-01-01T02:46:40.000000Z
                    s3\t1970-01-01T00:50:00.000000Z\t1970-01-01T03:03:20.000000Z
                    """;

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
        });
    }

    @Test
    public void testRowIdCapFires() throws Exception {
        // Disabling the encoded sort routes the window sort to the LongTreeChain; the rowid
        // cap bounds its value heap.
        node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_ROWID_PAGE_SIZE, 4096);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_ROWID_MAX_BYTES, 8192);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT" +
                    " ('s' || (x % 8))::SYMBOL AS sym," +
                    " (x * 1_000_000_000L)::TIMESTAMP AS ts," +
                    " ('v' || x)::VARCHAR AS v" +
                    " FROM long_sequence(50_000)) TIMESTAMP(ts)");

            assertExceptionNoLeakCheck(
                    "SELECT sym, ts, lag(ts, 1) OVER (PARTITION BY sym ORDER BY v) FROM tab",
                    0,
                    "memory exceeded in LongTreeChain (raise cairo.sql.window.rowid.max.bytes)"
            );
        });
    }

    @Test
    public void testTreeKeyCapFires() throws Exception {
        // Disabling the encoded sort routes the window sort to the LongTreeChain; the tree-key
        // cap bounds its key heap.
        node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_TREE_PAGE_SIZE, 4096);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_TREE_MAX_BYTES, 8192);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT" +
                    " ('s' || (x % 8))::SYMBOL AS sym," +
                    " (x * 1_000_000_000L)::TIMESTAMP AS ts," +
                    " ('v' || x)::VARCHAR AS v" +
                    " FROM long_sequence(50_000)) TIMESTAMP(ts)");

            assertExceptionNoLeakCheck(
                    "SELECT sym, ts, lag(ts, 1) OVER (PARTITION BY sym ORDER BY v) FROM tab",
                    0,
                    "memory exceeded in RedBlackTree (raise cairo.sql.window.tree.max.bytes)"
            );
        });
    }

    // Both sink-cap tests configure window.store.max.pages = 0, so the budget is the product 64 * 0
    // floored at the sink's 8-byte allocation unit. Assert that number rather than just the tail of
    // the message: without it, dropping the floor in SingleRecordSink's constructor still matches
    // the substring and reports "limit of 0". RankFunctionFactory's / 2 stays unpinned here - the
    // product is 0 with or without it - and no reachable configuration prints a halved budget,
    // since at stock settings the limit works out to ~1 PB.
    private void assertSinkCapMessage(String function, String owner) throws Exception {
        try {
            printSql("SELECT sym, ts, " + function + "() OVER (ORDER BY sym, ts) FROM tab" +
                    " WHERE ts IN '2024-01-01' ORDER BY sym, ts");
            Assert.fail("expected LimitOverflowException");
        } catch (LimitOverflowException e) {
            TestUtils.assertContains(e.getFlyweightMessage(),
                    "limit of 8 memory exceeded in " + owner
                            + " (raise cairo.sql.window.store.page.size or cairo.sql.window.store.max.pages)");
        }
    }
}
