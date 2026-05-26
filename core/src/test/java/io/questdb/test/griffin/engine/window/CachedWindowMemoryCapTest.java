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
    public void testCacheCapFiresAndCleansUp() throws Exception {
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
                    "Maximum number of pages"
            );
        });
    }

    @Test
    public void testConcurrentCursorsHaveIndependentCaps() throws Exception {
        // Each cached-window cursor allocates its own LongTreeChain/RecordArray. The cap is
        // per-cursor; two sequential runs each under the cap must both succeed even though
        // their combined heap would exceed it.
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

            assertQueryNoLeakCheck(expected, query, "ts", true);
            assertQueryNoLeakCheck(expected, query, "ts", true);
        });
    }

    @Test
    public void testHappyPathUnchanged() throws Exception {
        // The default 4 GiB cap must not regress small queries.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT" +
                    " ('s' || (x % 4))::SYMBOL AS sym," +
                    " timestamp_sequence(0, 1_000_000L) AS ts," +
                    " x::LONG AS v" +
                    " FROM long_sequence(12)) TIMESTAMP(ts)");

            assertQueryNoLeakCheck(
                    "sym\tts\tv\tprev_v\n" +
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
                            "s0\t1970-01-01T00:00:11.000000Z\t12\t8\n",
                    "SELECT sym, ts, v, lag(v, 1) OVER (PARTITION BY sym ORDER BY ts) AS prev_v FROM tab",
                    "ts",
                    false,
                    true
            );
        });
    }

    @Test
    public void testRowIdCapFiresAndCleansUp() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_ROWID_PAGE_SIZE, 4096);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_ROWID_MAX_BYTES, 8192);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT" +
                    " ('s' || (x % 8))::SYMBOL AS sym," +
                    " (x * 1_000_000_000L)::TIMESTAMP AS ts" +
                    " FROM long_sequence(50_000)) TIMESTAMP(ts)");

            assertExceptionNoLeakCheck(
                    "SELECT sym, ts, lag(ts, 1) OVER (PARTITION BY sym ORDER BY ts DESC) FROM tab",
                    0,
                    "memory exceeded in LongTreeChain"
            );
        });
    }

    @Test
    public void testTreeKeyCapFiresAndCleansUp() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_TREE_PAGE_SIZE, 4096);
        node1.setProperty(PropertyKey.CAIRO_SQL_WINDOW_TREE_MAX_BYTES, 8192);

        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (" +
                    "SELECT" +
                    " ('s' || (x % 8))::SYMBOL AS sym," +
                    " (x * 1_000_000_000L)::TIMESTAMP AS ts" +
                    " FROM long_sequence(50_000)) TIMESTAMP(ts)");

            assertExceptionNoLeakCheck(
                    "SELECT sym, ts, lag(ts, 1) OVER (PARTITION BY sym ORDER BY ts DESC) FROM tab",
                    0,
                    "memory exceeded in RedBlackTree"
            );
        });
    }
}
