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
import io.questdb.test.AbstractCairoTest;
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

            assertQuery("SELECT sym, ts, lag(ts, 1) OVER (PARTITION BY sym ORDER BY ts DESC) FROM tab")
                    .noLeakCheck()
                    .fails(0, "breached in VirtualMemory (raise cairo.sql.window.store.max.pages)");
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

            assertQuery("SELECT sym, ts, lag(ts, 1) OVER (PARTITION BY sym ORDER BY ts DESC) FROM tab")
                    .noLeakCheck()
                    .fails(0, "breached in VirtualMemory (raise cairo.sql.window.cache.max.bytes)");
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
                    .returns("sym\tts\tlag\n" +
                            "s1\t1970-01-01T00:16:40.000000Z\t1970-01-01T02:30:00.000000Z\n" +
                            "s2\t1970-01-01T00:33:20.000000Z\t1970-01-01T02:46:40.000000Z\n" +
                            "s3\t1970-01-01T00:50:00.000000Z\t1970-01-01T03:03:20.000000Z\n");
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
                    .returns("sym\tts\tv\tprev_v\n" +
                            "s1\t1970-01-01T00:00:00.000000Z\t1\tnull\n" +
                            "s2\t1970-01-01T00:00:01.000000Z\t2\tnull\n" +
                            "s3\t1970-01-01T00:00:02.000000Z\t3\tnull\n" +
                            "s0\t1970-01-01T00:00:03.000000Z\t4\tnull\n" +
                            "s1\t1970-01-01T00:00:04.000000Z\t5\t1\n" +
                            "s2\t1970-01-01T00:00:05.000000Z\t6\t2\n" +
                            "s3\t1970-01-01T00:00:06.000000Z\t7\t3\n" +
                            "s0\t1970-01-01T00:00:07.000000Z\t8\t4\n" +
                            "s1\t1970-01-01T00:00:08.000000Z\t9\t5\n" +
                            "s2\t1970-01-01T00:00:09.000000Z\t10\t6\n" +
                            "s3\t1970-01-01T00:00:10.000000Z\t11\t7\n" +
                            "s0\t1970-01-01T00:00:11.000000Z\t12\t8\n");
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
            final String expected = "sym\tts\tlag\n" +
                    "s1\t1970-01-01T00:16:40.000000Z\t1970-01-01T02:30:00.000000Z\n" +
                    "s2\t1970-01-01T00:33:20.000000Z\t1970-01-01T02:46:40.000000Z\n" +
                    "s3\t1970-01-01T00:50:00.000000Z\t1970-01-01T03:03:20.000000Z\n";

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
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_ROWID_PAGE_SIZE, 4096);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_ROWID_MAX_BYTES, 8192);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT" +
                    " ('s' || (x % 8))::SYMBOL AS sym," +
                    " (x * 1_000_000_000L)::TIMESTAMP AS ts" +
                    " FROM long_sequence(50_000)) TIMESTAMP(ts)");

            assertQuery("SELECT sym, ts, lag(ts, 1) OVER (PARTITION BY sym ORDER BY ts DESC) FROM tab")
                    .noLeakCheck()
                    .fails(0, "memory exceeded in LongTreeChain (raise cairo.sql.window.rowid.max.bytes)");
        });
    }

    @Test
    public void testTreeKeyCapFires() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_TREE_PAGE_SIZE, 4096);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_TREE_MAX_BYTES, 8192);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT" +
                    " ('s' || (x % 8))::SYMBOL AS sym," +
                    " (x * 1_000_000_000L)::TIMESTAMP AS ts" +
                    " FROM long_sequence(50_000)) TIMESTAMP(ts)");

            assertQuery("SELECT sym, ts, lag(ts, 1) OVER (PARTITION BY sym ORDER BY ts DESC) FROM tab")
                    .noLeakCheck()
                    .fails(0, "memory exceeded in RedBlackTree (raise cairo.sql.window.tree.max.bytes)");
        });
    }
}
