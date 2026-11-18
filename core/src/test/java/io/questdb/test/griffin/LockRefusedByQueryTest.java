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

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A statement that needs every reader of a table closed, such as TRUNCATE or DROP TABLE of a non-WAL table, is
 * refused while a query reads the table. Once the query is done, the statement must succeed from any thread, and the
 * table must stay readable in the meantime.
 */
public class LockRefusedByQueryTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // two readers per segment of the reader pool: a query past the first segment takes three readers
        setProperty(PropertyKey.DEBUG_CAIRO_POOL_SEGMENT_SIZE, 2);
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testDropRefusedByQueryThenDroppedFromAnotherThread() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_drop (x LONG)");
            execute("INSERT INTO t_drop VALUES (1), (2)");
            final TableReader idle = getReader("t_drop");
            final TableReader query = getReader("t_drop");
            idle.close();
            try {
                assertRefusedInThread("DROP TABLE t_drop", "[reason='busyReader']");
            } finally {
                query.close();
            }
            Assert.assertNull(executeInThread("DROP TABLE t_drop"));
            Assert.assertNull(engine.getTableTokenIfExists("t_drop"));
        });
    }

    @Test
    public void testTruncateRefusedByQueryInSecondSegmentKeepsTableReadable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_second (x LONG)");
            execute("INSERT INTO t_second VALUES (1), (2)");
            final TableReader r0 = getReader("t_second");
            final TableReader r1 = getReader("t_second");
            final TableReader query = getReader("t_second");
            r0.close();
            r1.close();
            try {
                assertRefusedInThread("TRUNCATE TABLE t_second", "there is an active query against 't_second'. Try again.");
                // the refused TRUNCATE leaves the readers of the table to other queries
                assertQuery("SELECT count() FROM t_second")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\n2\n");
            } finally {
                query.close();
            }
            Assert.assertNull(executeInThread("TRUNCATE TABLE t_second"));
            assertQuery("SELECT count() FROM t_second")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n0\n");
        });
    }

    @Test
    public void testTruncateRefusedByQueryThenTruncatedFromAnotherThread() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_first (x LONG)");
            execute("INSERT INTO t_first VALUES (1), (2)");
            final TableReader idle = getReader("t_first");
            final TableReader query = getReader("t_first");
            idle.close();
            try {
                assertRefusedInThread("TRUNCATE TABLE t_first", "there is an active query against 't_first'. Try again.");
            } finally {
                query.close();
            }
            Assert.assertNull(executeInThread("TRUNCATE TABLE t_first"));
            assertQuery("SELECT count() FROM t_first")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n0\n");
        });
    }

    private static void assertRefusedInThread(String sql, String message) throws InterruptedException {
        final Throwable thrown = executeInThread(sql);
        Assert.assertNotNull("'" + sql + "' must be refused", thrown);
        TestUtils.assertContains(thrown.getMessage(), message);
    }

    // Runs the statement on a thread of its own, as a statement of another connection would
    private static Throwable executeInThread(String sql) throws InterruptedException {
        final AtomicReference<Throwable> thrown = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try (SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine)) {
                engine.execute(sql, context);
            } catch (Throwable th) {
                thrown.set(th);
            } finally {
                Path.clearThreadLocals();
            }
        });
        thread.start();
        thread.join();
        return thrown.get();
    }
}
