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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompilerFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Pins the configuration of the parallel hash join and the context flag that switches it:
 * {@code SqlExecutionContextImpl} follows {@code cairo.sql.parallel.hash.join.enabled} only with
 * shared query workers, and the materialized view refresh and WAL apply contexts turn the flag off
 * when their own parallel SQL property is off. The refresh and apply jobs build those contexts
 * themselves, so the tests run the real jobs and read the flag off the context that each compile
 * of the jobs receives.
 */
public class ParallelHashJoinConfigurationTest extends AbstractCairoTest {
    // The context class and flag of every compile since a test armed the list. Null while no test
    // is looking.
    private static ObjList<String> compileContexts;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Every compiler the engine pools, the ones the refresh and apply jobs borrow included, notes
        // the context it compiles under.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public SqlCompilerFactory getSqlCompilerFactory() {
                return ContextRecordingCompiler::new;
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        // The operator is on by default. Turning the shared test context's flag off before
        // AbstractCairoTest.setUp() lets testSharedTestContextFollowsConfiguration() see that setUp()
        // resets the context's flag from the configuration.
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        super.setUp();
    }

    @Test
    public void testContextFollowsConfigurationWithSharedWorkers() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1)) {
                Assert.assertTrue(context.isParallelHashJoinEnabled());
                context.setParallelHashJoinEnabled(false);
                Assert.assertFalse(context.isParallelHashJoinEnabled());
                context.setParallelHashJoinEnabled(true);
                Assert.assertTrue(context.isParallelHashJoinEnabled());
            }
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_ENABLED, "false");
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 4)) {
                Assert.assertFalse(context.isParallelHashJoinEnabled());
            }
        });
    }

    @Test
    public void testContextOffWithoutSharedWorkers() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 0)) {
                Assert.assertFalse(context.isParallelHashJoinEnabled());
            }
        });
    }

    @Test
    public void testDefaultConfiguration() {
        final CairoConfiguration defaultConfiguration = new DefaultCairoConfiguration(root);
        assertDefaults(defaultConfiguration);
        assertDefaults(new CairoConfigurationWrapper(defaultConfiguration));

        // The defaults follow the page frame size, the dispatch limit and the queue capacity of a
        // configuration that overrides them.
        Assert.assertEquals(4_096, new LimitedConfiguration(4_096, 4, 16).getSqlParallelHashJoinTaskOutputMaxRows());
        // 4 frames of 4,096 rows at 48 bytes per row.
        Assert.assertEquals(786_432, new LimitedConfiguration(4_096, 4, 16).getSqlParallelHashJoinTupleRingMaxSize());
        // The queue's 16 frames bound a larger dispatch limit.
        Assert.assertEquals(3_145_728, new LimitedConfiguration(4_096, 64, 16).getSqlParallelHashJoinTupleRingMaxSize());
        // A dispatch limit of 0 or below leaves no frame to size the budget from.
        Assert.assertEquals(0, new LimitedConfiguration(4_096, 0, 16).getSqlParallelHashJoinTupleRingMaxSize());
        Assert.assertEquals(0, new LimitedConfiguration(4_096, -1, 16).getSqlParallelHashJoinTupleRingMaxSize());
        // 2^30 frames of 2^24 rows, the largest frames the server accepts, fit a long.
        Assert.assertEquals(
                864_691_128_455_135_232L,
                new LimitedConfiguration(16_777_216, Integer.MAX_VALUE, 1_073_741_824).getSqlParallelHashJoinTupleRingMaxSize()
        );
    }

    // A view's first refresh compiles its query, and later refreshes reuse the factory it cached,
    // so each of the refresh tests below creates its own view.
    @Test
    public void testMatViewRefreshContextFollowsConfigurationWithParallelSql() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, "true");
        assertMemoryLeak(() -> assertMatViewRefreshContextFlag(true));
    }

    @Test
    public void testMatViewRefreshContextOffWithParallelSqlAndOperatorOff() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_ENABLED, "false");
        assertMemoryLeak(() -> assertMatViewRefreshContextFlag(false));
    }

    @Test
    public void testMatViewRefreshContextOffWithoutParallelSql() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, "false");
        assertMemoryLeak(() -> assertMatViewRefreshContextFlag(false));
    }

    @Test
    public void testSharedTestContextFollowsConfiguration() {
        Assert.assertTrue(sqlExecutionContext.isParallelHashJoinEnabled());
        // The next test's setUp() turns it on again.
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        Assert.assertFalse(sqlExecutionContext.isParallelHashJoinEnabled());
    }

    @Test
    public void testWalApplyContextFollowsConfigurationWithParallelSql() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_PARALLEL_SQL_ENABLED, "true");
        assertMemoryLeak(() -> {
            assertWalApplyContextFlag(true, 2);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_ENABLED, "false");
            assertWalApplyContextFlag(false, 3);
        });
    }

    @Test
    public void testWalApplyContextOffWithoutParallelSql() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_PARALLEL_SQL_ENABLED, "false");
        assertMemoryLeak(() -> assertWalApplyContextFlag(false, 2));
    }

    private static void assertCompileContexts(String expectedContext) {
        Assert.assertTrue("the job compiled nothing", compileContexts.size() > 0);
        for (int i = 0, n = compileContexts.size(); i < n; i++) {
            Assert.assertEquals(expectedContext, compileContexts.getQuick(i));
        }
    }

    private static void assertDefaults(CairoConfiguration configuration) {
        Assert.assertTrue(configuration.isSqlParallelHashJoinEnabled());
        Assert.assertEquals(1_000_000, configuration.getSqlParallelHashJoinTaskOutputMaxRows());
        // The dispatch limit is Integer.MAX_VALUE, so the queue capacity of 32 frames bounds the
        // ring: 32 frames of 1,000,000 rows at 48 bytes per row.
        Assert.assertEquals(1_536_000_000, configuration.getSqlParallelHashJoinTupleRingMaxSize());
    }

    // Creates a view over a base table with one row and refreshes the view with a refresh job that
    // builds its context with one shared query worker.
    private void assertMatViewRefreshContextFlag(boolean expected) throws Exception {
        execute("CREATE TABLE base (k INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE MATERIALIZED VIEW mv AS SELECT ts, k, sum(v) s FROM base SAMPLE BY 1h");
        execute("INSERT INTO base VALUES (7, 1.5, '2024-01-01T00:10:00.000000Z')");
        try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(engine, 1)) {
            compileContexts = new ObjList<>();
            try {
                drainWalAndMatViewQueues(refreshJob, engine);
                assertCompileContexts("MatViewRefreshSqlExecutionContext isParallelHashJoinEnabled=" + expected);
            } finally {
                compileContexts = null;
            }
        }
        assertQuery("SELECT ts, k, s FROM mv")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\tk\ts
                        2024-01-01T00:00:00.000000Z\t7\t1.5
                        """);
    }

    // Updates the table and applies the update with an apply job that builds its context with one
    // shared query worker.
    private void assertWalApplyContextFlag(boolean expected, int value) throws Exception {
        if (engine.getTableTokenIfExists("t") == null) {
            execute("CREATE TABLE t (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES (1, '2024-01-01T00:00:00.000000Z')");
            drainWalQueue();
        }
        execute("UPDATE t SET v = " + value);
        try (ApplyWal2TableJob applyJob = new ApplyWal2TableJob(engine, 1)) {
            compileContexts = new ObjList<>();
            try {
                drainWalQueue(applyJob);
                assertCompileContexts("WalApplySqlExecutionContext isParallelHashJoinEnabled=" + expected);
            } finally {
                compileContexts = null;
            }
        }
        assertQuery("SELECT v FROM t")
                .noLeakCheck()
                .expectSize()
                .returns("v\n" + value + "\n");
    }

    private static class ContextRecordingCompiler extends SqlCompilerImpl {
        private ContextRecordingCompiler(CairoEngine engine) {
            super(engine);
        }

        @Override
        public CompiledQuery compile(@NotNull CharSequence sqlText, @NotNull SqlExecutionContext executionContext) throws SqlException {
            if (compileContexts != null) {
                compileContexts.add(
                        executionContext.getClass().getSimpleName()
                                + " isParallelHashJoinEnabled=" + executionContext.isParallelHashJoinEnabled()
                );
            }
            return super.compile(sqlText, executionContext);
        }
    }

    private static class LimitedConfiguration extends DefaultCairoConfiguration {
        private final int dispatchLimit;
        private final int pageFrameMaxRows;
        private final int queueCapacity;

        private LimitedConfiguration(int pageFrameMaxRows, int dispatchLimit, int queueCapacity) {
            super(root);
            this.pageFrameMaxRows = pageFrameMaxRows;
            this.dispatchLimit = dispatchLimit;
            this.queueCapacity = queueCapacity;
        }

        @Override
        public int getPageFrameReduceQueueCapacity() {
            return queueCapacity;
        }

        @Override
        public int getSqlPageFrameMaxRows() {
            return pageFrameMaxRows;
        }

        @Override
        public int getSqlParallelFilterDispatchLimit() {
            return dispatchLimit;
        }
    }
}
