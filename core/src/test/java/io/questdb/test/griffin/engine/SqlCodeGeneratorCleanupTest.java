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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.QueryModelWrapper;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;

import org.junit.Assert;
import org.junit.Test;

import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SqlCodeGeneratorCleanupTest extends AbstractCairoTest {

    @Test
    public void testCapturePreparationFailureClosesUntransferredFactories() throws Exception {
        assertPreparationFailure(0);
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
    public void testRegeneratedBranchPreparationFailureClosesPrimaryExactlyOnce() throws Exception {
        assertPreparationFailure(1);
    }

    @Test
    public void testUnionTailPreparationFailureClosesGroupedHeadExactlyOnce() throws Exception {
        assertPreparationFailure(2);
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
}
