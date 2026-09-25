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

package io.questdb.test.griffin.engine;

import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.QueryModelWrapper;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cleanup of the table-name-function factories the optimiser parks on query models.
 * <p>
 * A non-null {@code tableNameFunction} slot means the current compiler attempt owns the factory; code
 * generation takes it when ownership moves to the returned cursor factory, and whatever is left at a
 * reset, a failure, or a compiler lifecycle boundary belongs to nobody and has to be closed. The unit
 * tests cover the two cleanup routines directly; the SQL-level tests drive them through the compiler.
 * <p>
 * The SQL-level tests that read through {@code table_partitions()} or {@code SHOW PARTITIONS} keep a
 * native {@link io.questdb.std.str.Path} inside the factory, so a factory that leaks shows up as
 * native memory in {@code assertMemoryLeak}, and a factory closed while the caller still holds it
 * fails on use.
 */
public class SqlCodeGeneratorCleanupTest extends AbstractCairoTest {

    @Test
    public void testAbandonedExecutionModelIsReleasedOnCompilerPoolReturn() throws Exception {
        // A caller can build an execution model and then walk away from it without generating a cursor
        // factory. Nothing ever takes the factory the optimiser attached, so returning the borrowed
        // compiler to the pool has to close it.
        assertMemoryLeak(() -> {
            createPartitionedTable("x");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                Assert.assertNotNull(compiler.generateExecutionModel(
                        "SELECT name FROM table_partitions('x')",
                        sqlExecutionContext
                ));
            }
        });
    }

    @Test
    public void testAbandonedExecutionModelIsReleasedOnDirectCompilerClose() throws Exception {
        // Same abandoned model, but on a compiler the caller owns outright. This one never reaches
        // SqlCompilerPool.C.close(), so SqlCompilerImpl.close() is the only boundary that can release
        // the factory.
        assertMemoryLeak(() -> {
            createPartitionedTable("x");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                Assert.assertNotNull(compiler.generateExecutionModel(
                        "SELECT name FROM table_partitions('x')",
                        sqlExecutionContext
                ));
            }
        });
    }

    @Test
    public void testCapturePreparationFailureClosesUntransferredFactories() throws Exception {
        assertPreparationFailure(0);
    }

    @Test
    public void testCompilerReturnKeepsTheCompiledQueryReadable() throws Exception {
        // Returning a compiler to the pool releases untransferred factories only. A full clear() there
        // would also drop the flyweight CompiledQuery the caller is still reading.
        assertMemoryLeak(() -> {
            createPartitionedTable("x");
            final CompiledQuery compiledQuery;
            final RecordCursorFactory factory;
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiledQuery = compiler.compile("SELECT name FROM table_partitions('x')", sqlExecutionContext);
                factory = compiledQuery.getRecordCursorFactory();
                Assert.assertNotNull(factory);
            }
            Assert.assertEquals(CompiledQuery.SELECT, compiledQuery.getType());
            Assert.assertSame(factory, compiledQuery.getRecordCursorFactory());
            try (RecordCursorFactory f = factory) {
                assertHasRows(f);
            }
        });
    }

    @Test
    public void testDiscardedRetryAttemptReleasesItsFactory() throws Exception {
        // Move the row-expiry policy epoch while the first insert-select attempt is in flight. The
        // compiler discards that attempt and re-parses through the shared reset, which is where the
        // discarded attempt's factory gets closed. The statement still lands its rows.
        assertMemoryLeak(() -> {
            createPartitionedTable("x");
            execute("CREATE TABLE dst (name VARCHAR)");
            final AtomicInteger attempts = new AtomicInteger();
            SqlCompilerImpl.setInsertSelectFactoryGenerationBarrier(() -> {
                if (attempts.getAndIncrement() == 0) {
                    engine.getMetadataCache().publishExpiryPolicyUpdate();
                }
            });
            try {
                execute("INSERT INTO dst SELECT name FROM table_partitions('x')");
            } finally {
                SqlCompilerImpl.setInsertSelectFactoryGenerationBarrier(null);
            }
            Assert.assertEquals("the first attempt must have been discarded", 2, attempts.get());
            assertQuery("SELECT count() FROM dst").noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    @Test
    public void testFreeTableNameFunctionsTraversesSharedGraphAndPreservesPrimaryError() {
        final AtomicInteger closeCount = new AtomicInteger();
        final QueryModel root = QueryModel.FACTORY.newInstance();
        final QueryModel expressionModel = QueryModel.FACTORY.newInstance();
        final QueryModel joinModel = QueryModel.FACTORY.newInstance();
        final QueryModel sharedModel = QueryModel.FACTORY.newInstance();
        final QueryModel unionModel = QueryModel.FACTORY.newInstance();
        final QueryModelWrapper sharedWrapper = new QueryModelWrapper().of(sharedModel, 1);

        root.setTableNameFunction(newFactory(closeCount, false));
        expressionModel.setTableNameFunction(newFactory(closeCount, false));
        joinModel.setTableNameFunction(newFactory(closeCount, false));
        sharedModel.setTableNameFunction(newFactory(closeCount, true));
        unionModel.setTableNameFunction(newFactory(closeCount, false));

        final ExpressionNode expressionNode = ExpressionNode.FACTORY.newInstance();
        expressionNode.queryModel = expressionModel;
        root.addExpressionModel(expressionNode);
        final ExpressionNode sharedExpressionNode = ExpressionNode.FACTORY.newInstance();
        sharedExpressionNode.queryModel = sharedWrapper;
        root.addExpressionModel(sharedExpressionNode);
        root.setNestedModel(sharedWrapper);
        root.addJoinModel(root);
        root.addJoinModel(joinModel);
        root.setUnionModel(unionModel);

        final RuntimeException primaryError = new RuntimeException("primary");
        SqlCodeGenerator.freeTableNameFunctionsForTesting(root, primaryError);

        Assert.assertEquals(5, closeCount.get());
        Assert.assertNull(root.getTableNameFunction());
        Assert.assertNull(expressionModel.getTableNameFunction());
        Assert.assertNull(joinModel.getTableNameFunction());
        Assert.assertNull(sharedModel.getTableNameFunction());
        Assert.assertNull(unionModel.getTableNameFunction());
        Assert.assertEquals(1, primaryError.getSuppressed().length);
        Assert.assertEquals("cleanup", primaryError.getSuppressed()[0].getMessage());
    }

    @Test
    public void testFreeTableNameFunctionsWithoutPrimaryErrorClosesGraphAndPropagatesCloseFailure() {
        // Some callers tidy up when nothing has gone wrong: after a retry, or once code
        // generation has taken the factories over. They have no exception in hand, so they
        // pass null. Everything still closes, and if one of the closes fails, that failure
        // is thrown to the caller instead of being recorded against an exception that
        // does not exist.
        final AtomicInteger closeCount = new AtomicInteger();
        final QueryModel root = QueryModel.FACTORY.newInstance();
        final QueryModel nested = QueryModel.FACTORY.newInstance();
        final QueryModel unionModel = QueryModel.FACTORY.newInstance();

        root.setTableNameFunction(newFactory(closeCount, false));
        nested.setTableNameFunction(newFactory(closeCount, false));
        unionModel.setTableNameFunction(newFactory(closeCount, false));
        root.setNestedModel(nested);
        root.setUnionModel(unionModel);

        SqlCodeGenerator.freeTableNameFunctionsForTesting(root, null);

        Assert.assertEquals(3, closeCount.get());
        Assert.assertNull(root.getTableNameFunction());
        Assert.assertNull(nested.getTableNameFunction());
        Assert.assertNull(unionModel.getTableNameFunction());

        final QueryModel throwingModel = QueryModel.FACTORY.newInstance();
        throwingModel.setTableNameFunction(newFactory(closeCount, true));
        try {
            SqlCodeGenerator.freeTableNameFunctionsForTesting(throwingModel, null);
            Assert.fail("expected the close failure to reach the caller");
        } catch (RuntimeException e) {
            Assert.assertEquals("cleanup", e.getMessage());
        }
        Assert.assertEquals(4, closeCount.get());
        // The field is cleared before the close runs, so even a close that fails leaves
        // nothing else pointing at the factory.
        Assert.assertNull(throwingModel.getTableNameFunction());
    }

    @Test
    public void testGenerationFailureBelowALatestByHoistReleasesItsFactory() throws Exception {
        // The latest-by hoist moves the WHERE clause to a different model, which can leave the model
        // holding the table function unreachable from the graph the failure path walks. The compiler's
        // pool sweep reaches it anyway.
        assertMemoryLeak(() -> {
            createPartitionedTable("x");
            execute("CREATE TABLE y (ts TIMESTAMP, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO y VALUES ('2024-01-01T00:00:00.000000Z', '2024-01-01')");
            execute("CREATE TABLE dst (s SYMBOL)");
            SqlCompilerImpl.setInsertSelectFactoryGenerationBarrier(() -> {
                throw new RuntimeException("generation failed");
            });
            try {
                execute("""
                        INSERT INTO dst
                        SELECT s FROM y
                        WHERE s IN (SELECT name FROM table_partitions('x'))
                        LATEST ON ts PARTITION BY s""");
                Assert.fail("expected the forced generation failure");
            } catch (RuntimeException e) {
                TestUtils.assertContains(e.getMessage(), "generation failed");
            } finally {
                SqlCompilerImpl.setInsertSelectFactoryGenerationBarrier(null);
            }
        });
    }

    @Test
    public void testPoolSweepClosesModelDisconnectedFromTheGraph() {
        // The optimiser can rewrite a model out of the graph the caller ends up holding. The graph walk
        // then cannot reach it, while the pool still can: the sweep runs over every slot the attempt
        // allocated, following no model edges at all.
        final AtomicInteger closeCount = new AtomicInteger();
        final ObjectPool<QueryModel> pool = new ObjectPool<>(QueryModel.FACTORY, 4);
        final QueryModel root = pool.next();
        final QueryModel nested = pool.next();
        final QueryModel disconnected = pool.next();

        root.setTableNameFunction(newFactory(closeCount, false));
        nested.setTableNameFunction(newFactory(closeCount, false));
        disconnected.setTableNameFunction(newFactory(closeCount, false));
        root.setNestedModel(nested);

        SqlCodeGenerator.freeTableNameFunctionsForTesting(root, null);
        Assert.assertEquals(2, closeCount.get());
        Assert.assertNotNull(disconnected.getTableNameFunction());

        // The graph walk ran first and emptied the slots it reached, so the sweep behind it closes the
        // one factory left over rather than closing anything a second time.
        Assert.assertNull(SqlCompilerImpl.freePooledTableNameFunctionsForTesting(pool, null));
        Assert.assertEquals(3, closeCount.get());
        Assert.assertNull(disconnected.getTableNameFunction());

        // A repeated sweep finds every slot empty.
        Assert.assertNull(SqlCompilerImpl.freePooledTableNameFunctionsForTesting(pool, null));
        Assert.assertEquals(3, closeCount.get());
    }

    @Test
    public void testPoolSweepIgnoresSlotsPastThePoolPosition() {
        // ObjectPool.clear() only rewinds the position, so a sweep after it enumerates nothing. This is
        // why the compiler sweeps before it resets the pool.
        final AtomicInteger closeCount = new AtomicInteger();
        final ObjectPool<QueryModel> pool = new ObjectPool<>(QueryModel.FACTORY, 4);
        final QueryModel model = pool.next();
        model.setTableNameFunction(newFactory(closeCount, false));

        pool.clear();
        Assert.assertNull(SqlCompilerImpl.freePooledTableNameFunctionsForTesting(pool, null));
        Assert.assertEquals(0, closeCount.get());
        Assert.assertNotNull(model.getTableNameFunction());

        // Reacquiring the model drops the field without closing it, which is the leak the sweep exists
        // to prevent. Close it here so the test leaves nothing behind.
        Assert.assertSame(model, pool.next());
        Assert.assertNull(model.getTableNameFunction());
        Assert.assertEquals(0, closeCount.get());
    }

    @Test
    public void testPoolSweepIsBestEffortAndPreservesPrimaryError() {
        final AtomicInteger closeCount = new AtomicInteger();
        final ObjectPool<QueryModel> pool = new ObjectPool<>(QueryModel.FACTORY, 4);
        final QueryModel first = pool.next();
        final QueryModel throwing = pool.next();
        final QueryModel last = pool.next();

        first.setTableNameFunction(newFactory(closeCount, false));
        throwing.setTableNameFunction(newFactory(closeCount, true));
        last.setTableNameFunction(newFactory(closeCount, false));

        final RuntimeException primaryError = new RuntimeException("primary");
        Assert.assertSame(primaryError, SqlCompilerImpl.freePooledTableNameFunctionsForTesting(pool, primaryError));

        // The slot after the failing close is still visited, and the primary error is the one that
        // survives, carrying the close failure as a suppressed exception.
        Assert.assertEquals(3, closeCount.get());
        Assert.assertNull(first.getTableNameFunction());
        Assert.assertNull(throwing.getTableNameFunction());
        Assert.assertNull(last.getTableNameFunction());
        Assert.assertEquals(1, primaryError.getSuppressed().length);
        Assert.assertEquals("cleanup", primaryError.getSuppressed()[0].getMessage());
    }

    @Test
    public void testPoolSweepWithoutPrimaryErrorReportsTheFirstCloseFailure() {
        final AtomicInteger closeCount = new AtomicInteger();
        final ObjectPool<QueryModel> pool = new ObjectPool<>(QueryModel.FACTORY, 4);
        final QueryModel firstThrowing = pool.next();
        final QueryModel plain = pool.next();
        final QueryModel secondThrowing = pool.next();

        firstThrowing.setTableNameFunction(newFactory(closeCount, true));
        plain.setTableNameFunction(newFactory(closeCount, false));
        secondThrowing.setTableNameFunction(newFactory(closeCount, true));

        final Throwable cleanupFailure = SqlCompilerImpl.freePooledTableNameFunctionsForTesting(pool, null);

        Assert.assertEquals(3, closeCount.get());
        Assert.assertNotNull(cleanupFailure);
        Assert.assertEquals("cleanup", cleanupFailure.getMessage());
        Assert.assertEquals(1, cleanupFailure.getSuppressed().length);
        Assert.assertEquals("cleanup", cleanupFailure.getSuppressed()[0].getMessage());
    }

    @Test
    public void testRegeneratedBranchPreparationFailureClosesPrimaryExactlyOnce() throws Exception {
        assertPreparationFailure(1);
    }

    @Test
    public void testShowFactorySurvivesCompilerReset() throws Exception {
        // The optimiser builds the SHOW cursor itself and parks it on the model. Code generation takes
        // it, so the reset that runs on the compiler's next borrow finds an empty slot and leaves the
        // caller's factory alone.
        assertMemoryLeak(() -> {
            createPartitionedTable("x");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final RecordCursorFactory factory = compiler
                        .compile("SHOW PARTITIONS FROM x", sqlExecutionContext)
                        .getRecordCursorFactory();
                try (RecordCursorFactory f = factory) {
                    compiler.clear();
                    assertHasRows(f);
                }
            }
        });
    }

    @Test
    public void testTableFunctionFactorySurvivesCompilerReset() throws Exception {
        assertMemoryLeak(() -> {
            createPartitionedTable("x");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final RecordCursorFactory factory = compiler
                        .compile("SELECT name FROM table_partitions('x')", sqlExecutionContext)
                        .getRecordCursorFactory();
                try (RecordCursorFactory f = factory) {
                    compiler.clear();
                    assertHasRows(f);
                }
            }
        });
    }

    @Test
    public void testTakeTableNameFunctionThroughWrapper() {
        // The wrapper rejects setTableNameFunction(), so the take operation has to reach the delegate
        // that owns the field.
        final AtomicInteger closeCount = new AtomicInteger();
        final QueryModel delegate = QueryModel.FACTORY.newInstance();
        final QueryModelWrapper wrapper = new QueryModelWrapper().of(delegate, 1);
        final RecordCursorFactory factory = newFactory(closeCount, false);
        delegate.setTableNameFunction(factory);

        Assert.assertSame(factory, wrapper.takeTableNameFunction());
        Assert.assertNull(delegate.getTableNameFunction());
        Assert.assertNull(wrapper.getTableNameFunction());
        Assert.assertNull(wrapper.takeTableNameFunction());
        Assert.assertEquals(0, closeCount.get());
    }

    @Test
    public void testUnionTailPreparationFailureClosesGroupedHeadExactlyOnce() throws Exception {
        assertPreparationFailure(2);
    }

    private static void assertHasRows(RecordCursorFactory factory) throws Exception {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue("the factory must still be usable", cursor.hasNext());
        }
    }

    private static void createPartitionedTable(CharSequence tableName) throws Exception {
        execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO " + tableName + " VALUES ('2024-01-01T00:00:00.000000Z', 1)");
        final TableToken tableToken = engine.verifyTableName(tableName);
        Assert.assertNotNull(tableToken);
    }

    private static RecordCursorFactory newFactory(AtomicInteger closeCount, boolean isThrowing) {
        return new RecordCursorFactory() {
            private final RecordMetadata metadata = new GenericRecordMetadata();

            @Override
            public void close() {
                closeCount.incrementAndGet();
                if (isThrowing) {
                    throw new RuntimeException("cleanup");
                }
            }

            @Override
            public RecordMetadata getMetadata() {
                return metadata;
            }

            @Override
            public boolean recordCursorSupportsRandomAccess() {
                return false;
            }

            @Override
            public void toPlan(PlanSink sink) {
            }
        };
    }

    private void assertPreparationFailure(int failureMode) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u AS (SELECT x::TIMESTAMP ts FROM long_sequence(50)) TIMESTAMP(ts)");
            String source = failureMode == 2
                    ? "SELECT x::TIMESTAMP ts, max(x) x FROM long_sequence(200) WHERE x IN (1, 101) GROUP BY ts UNION ALL SELECT x::TIMESTAMP ts, x FROM long_sequence(200) WHERE x = 200"
                    : "SELECT x::TIMESTAMP ts, x FROM long_sequence(200) WHERE x IN (101, 200) LIMIT 1";
            String query = "SELECT o.ts, o.x, l.c FROM (" + source + ") o "
                    + "JOIN LATERAL (SELECT count() c FROM u WHERE u.ts <= o.ts) l ON true ORDER BY o.x";
            try (PreparationFailureCompiler compiler = new PreparationFailureCompiler(failureMode)) {
                try (RecordCursorFactory ignored = select(compiler, query, sqlExecutionContext)) {
                    Assert.fail("preparation must fail");
                } catch (OutOfMemoryError e) {
                    Assert.assertSame(compiler.failure, e);
                }
                Assert.assertTrue("hook must reach the requested seam", compiler.hasInjected);
                Assert.assertTrue("must observe an optimizer-owned table function", compiler.factories.size() > 0);
                for (int i = 0; i < compiler.factories.size(); i++) {
                    Assert.assertEquals("each attached factory closes exactly once", 1, compiler.factories.getQuick(i).closeCount);
                }
                Assert.assertEquals(0, compiler.retainedNodes());
                compiler.disarm();
                assertQuery("SELECT x FROM long_sequence(2) WHERE x = 2").withCompiler(compiler).returns("x\n2\n");
                Assert.assertEquals(0, compiler.retainedNodes());
            }
        });
    }

    private static class CountingFactory implements RecordCursorFactory {
        private final RecordCursorFactory delegate;
        private int closeCount;

        private CountingFactory(RecordCursorFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public void close() {
            closeCount++;
            delegate.close();
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext context) throws SqlException {
            return delegate.getCursor(context);
        }

        @Override
        public RecordMetadata getMetadata() {
            return delegate.getMetadata();
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return delegate.recordCursorSupportsRandomAccess();
        }

        @Override
        public void toPlan(PlanSink sink) {
            delegate.toPlan(sink);
        }
    }

    private static class PreparationFailureCompiler extends SqlCompilerImpl {
        private final ObjList<CountingFactory> factories = new ObjList<>();
        private final OutOfMemoryError failure = new OutOfMemoryError("injected generation preparation");
        private final int failureMode;
        private final ObjList<IQueryModel> functionModels = new ObjList<>();
        private final IdentityHashMap<IQueryModel, Boolean> tails = new IdentityHashMap<>();
        private boolean hasInjected;
        private boolean isArmed = true;

        private PreparationFailureCompiler(int failureMode) {
            super(AbstractCairoTest.engine);
            this.failureMode = failureMode;
            codeGenerator.getGenerationStateForTesting().setPreparationHook(model -> {
                if (!isArmed || hasInjected) {
                    return;
                }
                boolean hasTransferred = false;
                for (int i = 0; i < functionModels.size(); i++) {
                    hasTransferred |= functionModels.getQuick(i).getTableNameFunction() == null;
                }
                if (failureMode == 0 || (hasTransferred && (failureMode == 1 || tails.containsKey(model)))) {
                    hasInjected = true;
                    throw failure;
                }
            });
        }

        @Override
        protected RecordCursorFactory generateSelectOneShot(IQueryModel model, SqlExecutionContext context, boolean isProgressLogger) throws SqlException {
            if (isArmed) {
                ObjList<IQueryModel> pending = new ObjList<>();
                IdentityHashMap<IQueryModel, Boolean> visited = new IdentityHashMap<>();
                pending.add(model);
                while (pending.size() > 0) {
                    IQueryModel current = pending.popLast();
                    if (current instanceof QueryModelWrapper wrapper) {
                        current = wrapper.getDelegate();
                    }
                    if (visited.put(current, Boolean.TRUE) != null) {
                        continue;
                    }
                    if (current.getTableNameFunction() != null) {
                        CountingFactory factory = new CountingFactory(current.getTableNameFunction());
                        factories.add(factory);
                        functionModels.add(current);
                        current.setTableNameFunction(factory);
                    }
                    if (current.getNestedModel() != null) {
                        pending.add(current.getNestedModel());
                    }
                    if (current.getUnionModel() != null) {
                        IQueryModel tail = current.getUnionModel();
                        tails.put(tail instanceof QueryModelWrapper wrapper ? wrapper.getDelegate() : tail, Boolean.TRUE);
                        pending.add(tail);
                    }
                    for (int i = 1; i < current.getJoinModels().size(); i++) {
                        pending.add(current.getJoinModels().getQuick(i));
                    }
                }
            }
            return super.generateSelectOneShot(model, context, isProgressLogger);
        }

        private void disarm() {
            isArmed = false;
            codeGenerator.getGenerationStateForTesting().setPreparationHook(null);
        }

        private int retainedNodes() {
            return codeGenerator.getGenerationStateForTesting().getRetainedNodeCount();
        }
    }
}
