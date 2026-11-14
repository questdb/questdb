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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ConcurrentWalTableRenameTest extends AbstractCairoTest {
    private static final String EXPECTED = """
            ts
            2022-02-24T04:00:00.000000Z
            2022-02-24T04:01:40.000000Z
            2022-02-24T04:03:20.000000Z
            2022-02-24T04:05:00.000000Z
            2022-02-24T04:06:40.000000Z
            """;
    private static final String QUERY = "SELECT t1.ts FROM t1 JOIN t2 ON t1.ts = t2.ts";

    @Before
    @Override
    public void setUp() {
        super.setUp();
        // Compilers capture the retry budget at construction; these tests vary it.
        engine.getSqlCompilerPool().releaseAll();
    }

    @Test
    public void testConcurrentSelectMissingTable() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("RENAME TABLE t1 TO temp");
            assertExceptionNoLeakCheck("t1", 0, "table does not exist [table=t1]");
            assertSelectDuringRename("t1", sqlExecutionContext);
            execute("RENAME TABLE temp TO t1");
            assertSelect(QUERY, sqlExecutionContext);
        });
    }

    @Test
    public void testConcurrentSelectRejectsOtherCairoErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1)) {
                context.with(new DenyAllSecurityContext() {
                    @Override
                    public boolean isTableVisible(TableToken tableToken) {
                        // Reach SELECT authorization rather than reporting an invisible table as missing.
                        return true;
                    }
                }, null, null, -1, null);
                try {
                    assertSelectDuringRename(QUERY, context);
                    Assert.fail("expected the permission denial to fail the racing read");
                } catch (CairoException e) {
                    TestUtils.assertEquals("permission denied", e.getFlyweightMessage());
                }
                context.with(AllowAllSecurityContext.INSTANCE, null, null, -1, null);
                assertSelect(QUERY, context);
            }
        });
    }

    @Test
    public void testConcurrentSelectRejectsOtherSqlErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try {
                assertSelectDuringRename("SELECT t1.missing FROM t1 JOIN t2 ON t1.ts = t2.ts", sqlExecutionContext);
                Assert.fail("expected the invalid column to fail the racing read");
            } catch (SqlException e) {
                Assert.assertEquals(7, e.getPosition());
                TestUtils.assertEquals("Invalid column: t1.missing", e.getFlyweightMessage());
            }
            assertSelect(QUERY, sqlExecutionContext);
        });
    }

    @Test
    public void testConcurrentSelectRename() throws Exception {
        assertMemoryLeak(() -> {
            int threadCount = 3;
            int tableCount = 100;
            AtomicReference<Throwable> ref = new AtomicReference<>();
            CyclicBarrier barrier = new CyclicBarrier(threadCount);

            createTables();

            ObjList<Thread> threads = new ObjList<>(threadCount + 2);
            for (int i = 1; i < threadCount; i++) {
                threads.add(new Thread(() -> {
                    try {
                        barrier.await();
                        try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                            for (int j = 0; j < tableCount; j++) {
                                assertSelectDuringRename(QUERY, executionContext);
                            }
                        }
                    } catch (Throwable e) {
                        ref.set(e);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }));
                threads.getLast().start();
            }

            AtomicBoolean isDone = new AtomicBoolean();
            threads.add(new Thread(() -> {
                try {
                    barrier.await();
                    try (
                            SqlCompiler compiler = engine.getSqlCompiler();
                            SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
                    ) {
                        while (!isDone.get()) {
                            try {
                                compiler.compile("rename table t1 to temp", executionContext);
                                compiler.compile("rename table t2 to t1", executionContext);
                                compiler.compile("rename table temp to t2", executionContext);
                            } catch (SqlException | CairoException e) {
                                if (!Chars.contains(e.getFlyweightMessage(), "table does not exist")) {
                                    throw e;
                                }
                            } catch (TableReferenceOutOfDateException e) {
                                // ignore
                            }
                        }
                    }
                } catch (Throwable e) {
                    ref.set(e);
                } finally {
                    Path.clearThreadLocals();
                }
            }));
            threads.getLast().start();

            for (int i = 0; i < threads.size() - 1; i++) {
                threads.getQuick(i).join();
            }

            isDone.set(true);
            threads.getLast().join();

            if (ref.get() != null) {
                throw new RuntimeException(ref.get());
            }
            // Even if every racing read failed transiently, a settled name must yield all rows.
            assertSelect(QUERY, sqlExecutionContext);
        });
    }

    @Test
    public void testSelectRecoversAfterSingleRename() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (RenamingExecutionContext context = new RenamingExecutionContext(1)) {
                assertSelect(QUERY, context);
                Assert.assertEquals(1, context.renameCount);
                Assert.assertEquals(2, context.tableResolutionCount);
                Assert.assertEquals(0, engine.getBusyReaderCount());
                assertSelect(QUERY, context);
                Assert.assertEquals(1, context.renameCount);
                Assert.assertEquals(3, context.tableResolutionCount);
            }
        });
    }

    @Test
    public void testSelectRetryLimitDefault() throws Exception {
        assertSelectRetryLimit(configuration.getMaxSqlRecompileAttempts());
    }

    @Test
    public void testSelectRetryLimitOne() throws Exception {
        assertSelectRetryLimit(1);
    }

    @Test
    public void testSelectRetryLimitZero() throws Exception {
        assertSelectRetryLimit(0);
    }

    private void assertSelect(String query, SqlExecutionContext executionContext) throws Exception {
        assertQuery(query)
                .withContext(executionContext)
                .timestamp("ts")
                .noRandomAccess()
                .noLeakCheck()
                // Other readers allocate concurrently; the outer leak check covers all threads.
                .noMemoryUsageCheck()
                .returns(EXPECTED);
    }

    private void assertSelectDuringRename(String query, SqlExecutionContext executionContext) throws Exception {
        try {
            assertSelect(query, executionContext);
        } catch (SqlException e) {
            // Continuous renames can exhaust compilation retries before either reader makes progress.
            // The compiler reports that bounded failure as SqlException, not a raw stale-plan exception.
            final boolean isRetryExhausted = e.getPosition() == 0 && Chars.startsWith(
                    e.getFlyweightMessage(),
                    "too many cached query plan cannot be used because table schema has changed [table="
            );
            if (!isRetryExhausted && !Chars.contains(e.getFlyweightMessage(), "table does not exist")) {
                throw e;
            }
        } catch (CairoException e) {
            if (!Chars.contains(e.getFlyweightMessage(), "table does not exist")) {
                throw e;
            }
        } catch (TableReferenceOutOfDateException e) {
            // A rename can invalidate a plan after compilation, while opening its cursor.
        }
    }

    private void assertSelectRetryLimit(int maxRetries) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_MAX_RECOMPILE_ATTEMPTS, maxRetries);
        assertMemoryLeak(() -> {
            createTables();
            final int attempts = maxRetries + 1;
            final int t1Id = engine.verifyTableName("t1").getTableId();
            final int t2Id = engine.verifyTableName("t2").getTableId();
            final int expectedId = (attempts & 1) == 1 ? t1Id : t2Id;
            final int actualId = (attempts & 1) == 1 ? t2Id : t1Id;
            try (RenamingExecutionContext context = new RenamingExecutionContext(attempts)) {
                try {
                    assertSelect(QUERY, context);
                    Assert.fail("expected table renames to exhaust the compilation retry budget");
                } catch (SqlException e) {
                    Assert.assertEquals(0, e.getPosition());
                    TestUtils.assertEquals(
                            "too many cached query plan cannot be used because table schema has changed "
                                    + "[table=t1', expectedTableId=" + expectedId + ", actualTableId=" + actualId
                                    + ", expectedMetadataVersion=0, actualMetadataVersion=0]",
                            e.getFlyweightMessage()
                    );
                }
                Assert.assertEquals(attempts, context.renameCount);
                Assert.assertEquals(attempts, context.tableResolutionCount);
                Assert.assertEquals(0, engine.getBusyReaderCount());
                assertSelect(QUERY, context);
                Assert.assertEquals(attempts + 1, context.tableResolutionCount);

                // Drive the stress test's handler through the same bounded failure deterministically.
                context.remainingRenames = attempts;
                assertSelectDuringRename(QUERY, context);
                Assert.assertEquals(2 * attempts, context.renameCount);
                Assert.assertEquals(2 * attempts + 1, context.tableResolutionCount);
                Assert.assertEquals(0, engine.getBusyReaderCount());
                assertSelect(QUERY, context);
                Assert.assertEquals(2 * attempts + 2, context.tableResolutionCount);
            }
        });
    }

    private void createTables() throws SqlException {
        for (int i = 1; i <= 2; i++) {
            execute("CREATE TABLE t" + i + " AS (SELECT x, rnd_symbol('a', 'b', 'c', null, 'd') s," +
                    "timestamp_sequence('2022-02-24T04', 100_000_000) ts " +
                    "FROM long_sequence(5)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        }
        drainWalQueue();
    }

    private static final class RenamingExecutionContext extends SqlExecutionContextImpl {
        private int remainingRenames;
        private int renameCount;
        private int tableResolutionCount;

        private RenamingExecutionContext(int remainingRenames) {
            super(engine, 1);
            this.remainingRenames = remainingRenames;
            with(AllowAllSecurityContext.INSTANCE, null, null, -1, null);
        }

        @Override
        public TableToken getTableToken(CharSequence tableName) {
            // Optimisation uses getTableTokenIfExists; generation resolves getTableToken.
            // Swap real tables at that boundary rather than depending on thread scheduling.
            if (Chars.equals("t1", tableName)) {
                tableResolutionCount++;
                if (remainingRenames > 0) {
                    remainingRenames--;
                    try {
                        execute("RENAME TABLE t1 TO temp");
                        execute("RENAME TABLE t2 TO t1");
                        execute("RENAME TABLE temp TO t2");
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                    renameCount++;
                }
            }
            return super.getTableToken(tableName);
        }
    }
}
