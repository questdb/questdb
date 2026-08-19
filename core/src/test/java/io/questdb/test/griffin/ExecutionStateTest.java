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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.ExecutionState;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class ExecutionStateTest extends AbstractCairoTest {
    private static final AtomicInteger STARTS = new AtomicInteger();
    private static final ExecutionState COUNTING_STATE = executionContext -> STARTS.incrementAndGet();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = configuration -> new CairoEngine(configuration) {
            @Override
            public ExecutionState createExecutionState() {
                return COUNTING_STATE;
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testCursorOpenRefreshesOncePerExecution() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x FROM long_sequence(10))");
            try (RecordCursorFactory factory = select("tab")) {
                final int base = STARTS.get();
                try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(base + 1, STARTS.get());
                }
                // a second execution of the same prepared factory re-stamps
                try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertEquals(base + 2, STARTS.get());
                }
            }
        });
    }

    @Test
    public void testInitNowFiresExecutionStartAndReplayStampDoesNot() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine)) {
                Assert.assertSame(COUNTING_STATE, ctx.getExecutionState());
                final int base = STARTS.get();
                ctx.initNow();
                Assert.assertEquals(base + 1, STARTS.get());
                ctx.initNow();
                Assert.assertEquals(base + 2, STARTS.get());
                // WAL-replay deterministic-time channel must never refresh execution state
                ctx.setNowAndFixClock(0, ColumnType.TIMESTAMP_MICRO);
                Assert.assertEquals(base + 2, STARTS.get());
            }
        });
    }

    @Test
    public void testInnerScalarSubqueryDoesNotRefreshMidExecution() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x FROM long_sequence(10))");
            try (RecordCursorFactory factory = select("SELECT (SELECT max(x) FROM tab) = null r FROM long_sequence(2)")) {
                final int base = STARTS.get();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    // inner CursorFunction factories are not QueryProgress-wrapped:
                    // exactly one refresh for the whole statement
                    Assert.assertEquals(base + 1, STARTS.get());
                }
            }
        });
    }

    @Test
    public void testPageFrameCursorOpenRefreshes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x FROM long_sequence(10))");
            try (RecordCursorFactory factory = select("tab")) {
                final int base = STARTS.get();
                try (PageFrameCursor ignore = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                    Assert.assertEquals(base + 1, STARTS.get());
                }
            }
        });
    }
}
