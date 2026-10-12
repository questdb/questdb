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

package io.questdb.test.cairo.sql.async;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Frame groups of the ordered page frame sequence: an Async Filter over many small partitions sends
 * runs of consecutive small native frames to the workers as one queue task each, and collects their
 * frames one by one. Every query runs with the frame dispatch switch on (groups) and off (one frame
 * per task) and must print the same; the on runs must have dispatched groups. Covers the JIT and the
 * Java filter, count-only sizing, LIMIT from both ends, ORDER BY designated timestamp DESC, interval
 * scans, Parquet partitions among native ones (no Parquet frame joins a group), a rewind and a close
 * in the middle of a group, an error in a group's later frame, and real workers.
 */
public class PageFrameGroupTest extends AbstractCairoTest {

    @Test
    public void testErrorInGroupFrameAfterRowsOfEarlierFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            // the cast fails on one row of the 17th hour only, inside a group
            final String query = "select x from t where d > s";
            for (boolean on : new boolean[]{false, true}) {
                setSwitch(on);
                final StringSink sink = new StringSink();
                try (RecordCursorFactory factory = select(query); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    try {
                        CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
                        Assert.fail("expected an error");
                    } catch (Throwable th) {
                        TestUtils.assertContains(th.getMessage(), "inconvertible value: `oops`");
                    }
                }
            }
        });
    }

    @Test
    public void testQueriesSameWithAndWithoutGroups() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            final long groupsBefore = PageFrameSequence.getGroupsDispatched();
            assertQueries(engine, sqlExecutionContext);
            Assert.assertTrue(PageFrameSequence.getGroupsDispatched() > groupsBefore);
        });
    }

    @Test
    public void testRewindAndCloseInsideGroup() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext);
            final String query = "select * from t where x % 3 = 0";
            setSwitch(false);
            final String expected = print(engine, sqlExecutionContext, query);
            setSwitch(true);
            try (RecordCursorFactory factory = select(query)) {
                for (int stop : new int[]{1, 7, 40, 133}) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        for (int i = 0; i < stop && cursor.hasNext(); i++) {
                            // part of a group
                        }
                        cursor.toTop();
                        final StringSink sink = new StringSink();
                        CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
                        TestUtils.assertEquals(expected, sink);
                    }
                    // closed in the middle of a group
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        for (int i = 0; i < stop && cursor.hasNext(); i++) {
                            // part of a group
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSameOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, context) -> {
                final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
                createTable(engine, ctx);
                for (int run = 0; run < 3; run++) {
                    assertQueries(engine, ctx);
                }
            }, configuration, LOG);
        });
    }

    private static void assertQueries(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        final String[] queries = {
                "select * from t where x % 7 = 1",
                "select * from t where d < 0.2 and s != '3'",
                "select x, s from t where s in ('1', '4') and x > 100",
                "select * from t where x > 50 limit 37",
                "select * from t where x > 50 limit -41",
                "select * from t where x % 5 = 0 order by ts desc limit 23",
                "select * from t where x % 2 = 0 and ts in '1970-01-01T05:00;3h'",
                "select count() from t where x % 3 = 1",
                "select * from (select * from t where x > 10) limit 100, 160",
                "select * from t where l = null",
        };
        for (String query : queries) {
            setSwitch(false);
            final String expected = print(engine, ctx, query);
            setSwitch(true);
            TestUtils.assertEquals(query, expected, print(engine, ctx, query));
            // the size of the result, which a count-only pass computes
            try (RecordCursorFactory factory = engine.select(query, ctx); RecordCursor cursor = factory.getCursor(ctx)) {
                final long size = cursor.size();
                long n = 0;
                while (cursor.hasNext()) {
                    n++;
                }
                if (size > -1) {
                    Assert.assertEquals(query, n, size);
                }
            }
        }
    }

    private static void createTable(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        // 300 hourly partitions of 2 to 40 rows: tiny frames, as many small partitions make them
        engine.execute("create table t (x long, d double, s string, l long, ts timestamp) timestamp(ts) partition by HOUR", ctx);
        engine.execute(
                "insert into t select x, rnd_double(), case when x = 371 then 'oops' else (x % 5)::string end, "
                        + "case when x % 11 = 0 then null else x end, "
                        + "((x * 3_600_000_000L) / 21 + (x % 13) * 1_000_000L)::timestamp from long_sequence(6_300)",
                ctx
        );
        // a Parquet partition among the native ones: groups stop at it
        engine.execute("alter table t convert partition to parquet list '1970-01-01T12'", ctx);
    }

    private static String print(CairoEngine engine, SqlExecutionContext ctx, String query) throws Exception {
        final StringSink sink = new StringSink();
        try (RecordCursorFactory factory = engine.select(query, ctx); RecordCursor cursor = factory.getCursor(ctx)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
        }
        return sink.toString();
    }

    private static void setSwitch(boolean on) {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, String.valueOf(on));
    }
}
