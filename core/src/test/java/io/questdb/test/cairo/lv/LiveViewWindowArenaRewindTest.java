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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link WindowFunction#onCheckpointRestoreBegin()} must rewind a function's ring
 * arena, not release it.
 * <p>
 * The live-view O3 replay path calls the hook once per function per restore and then
 * refills the arena to roughly the size it just held. Releasing it - which
 * {@code MemoryARW.truncate()} does, by reallocating the region down to a single page -
 * makes every replay re-grow the arena through a chain of doubling reallocations and
 * fault each page back in. On a view whose frame holds millions of rows that costs a
 * large share of the refresh thread, so the hook rewinds with {@code jumpTo(0)} instead.
 * <p>
 * The window arena is the sole user of {@link MemoryTag#NATIVE_CIRCULAR_BUFFER}, so the
 * tag's byte count is a direct read of its size.
 */
public class LiveViewWindowArenaRewindTest extends AbstractCairoTest {

    // Small enough that a modest fixture spans many pages, so a release would be
    // unmissable against the assertions below.
    private static final int WINDOW_STORE_PAGE_SIZE = 4096;

    @Test
    public void testRestoreBeginRewindsArenaWithoutReleasingIt() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, WINDOW_STORE_PAGE_SIZE);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE base (
                              ts TIMESTAMP, sym SYMBOL, v DOUBLE
                            ) TIMESTAMP(ts) PARTITION BY DAY WAL"""
            );
            // 1 ms apart across 40 keys, so the whole 40 s span sits inside one
            // 1-minute frame and every key's ring holds all 1000 of its rows. A
            // sparser fixture leaves the rings at their initial size and the arena
            // too small to tell a rewind from a release.
            execute(
                    """
                            INSERT INTO base
                            SELECT (x * 1000)::timestamp,
                                   'k' || (x % 40),
                                   x::double
                            FROM long_sequence(40_000)"""
            );
            drainWalQueue();

            final String sql = """
                    SELECT ts, sym, avg(v) OVER (
                      PARTITION BY sym ORDER BY ts RANGE 1 minute PRECEDING
                    ) AS v FROM base""";
            sqlExecutionContext.setLiveViewCompile(true);
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)
            ) {
                RecordCursorFactory root = factory;
                while (root instanceof QueryProgress) {
                    root = root.getBaseFactory();
                }
                Assert.assertTrue(root instanceof WindowRecordCursorFactory);
                final ObjList<WindowFunction> functions =
                        ((WindowRecordCursorFactory) root).getWindowFunctions();
                Assert.assertEquals(1, functions.size());
                final WindowFunction function = functions.getQuick(0);

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        record.getDouble(2);
                    }

                    final long grown = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_CIRCULAR_BUFFER);
                    // Guards the guard: a one-page arena could not tell a rewind from a
                    // release, so fail loudly rather than pass vacuously.
                    Assert.assertTrue(
                            "fixture must grow the arena well past one page, was " + grown,
                            grown > 32L * WINDOW_STORE_PAGE_SIZE
                    );

                    function.onCheckpointRestoreBegin();

                    Assert.assertEquals(
                            "onCheckpointRestoreBegin() must not hand the ring arena back to the allocator",
                            grown,
                            Unsafe.getMemUsedByTag(MemoryTag.NATIVE_CIRCULAR_BUFFER)
                    );

                    // toTop() is the from-scratch reset and still releases; the two
                    // must not converge on the same behaviour.
                    function.toTop();
                    Assert.assertTrue(
                            "toTop() must still release the arena",
                            Unsafe.getMemUsedByTag(MemoryTag.NATIVE_CIRCULAR_BUFFER) < grown
                    );
                }
            } finally {
                sqlExecutionContext.setLiveViewCompile(false);
            }
        });
    }
}
