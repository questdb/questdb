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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.PostOrderTreeTraversalAlgo;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;

public class SqlCodeGeneratorWorkerFunctionExtractionTest extends AbstractCairoTest {

    @Test
    public void testCloneCompilationFailureClosesOnlyWorkerClones() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFunctionParser parser = new CountingFunctionParser(3);
            final ObjectPool<QueryColumn> queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 4);
            final ObjectPool<ExpressionNode> expressionNodePool = new ObjectPool<>(ExpressionNode.FACTORY, 4);
            final CountingFunction safeOwnerFunction = new CountingFunction(true);
            final CountingFunction unsafeOwnerFunction = new CountingFunction(false);
            final ObjList<Function> ownerFunctions = new ObjList<>();
            ownerFunctions.add(safeOwnerFunction);
            ownerFunctions.add(unsafeOwnerFunction);
            try (SqlCodeGenerator codeGenerator = new SqlCodeGenerator(
                    configuration,
                    parser,
                    new PostOrderTreeTraversalAlgo(),
                    queryColumnPool,
                    expressionNodePool
            )) {
                final ObjList<QueryColumn> queryColumns = new ObjList<>();
                queryColumns.add(new QueryColumn().of("safe", expressionNodePool.next().of(ExpressionNode.LITERAL, "safe", 0, 0)));
                queryColumns.add(new QueryColumn().of("unsafe", expressionNodePool.next().of(ExpressionNode.LITERAL, "unsafe", 0, 0)));

                final IntList flags = new IntList();
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL);
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL);

                final Method method = getCompilePerWorkerMethod();
                try {
                    method.invoke(codeGenerator, null, queryColumns, ownerFunctions, 4, null, flags);
                    Assert.fail();
                } catch (InvocationTargetException e) {
                    Assert.assertSame(parser.failure, e.getCause());
                }
                Assert.assertEquals(3, parser.parseCount);
                Assert.assertEquals(2, parser.functions.size());
                Assert.assertEquals(0, safeOwnerFunction.closeCount);
                Assert.assertEquals(0, unsafeOwnerFunction.closeCount);
                for (int i = 0, n = parser.functions.size(); i < n; i++) {
                    Assert.assertEquals(1, parser.functions.getQuick(i).closeCount);
                }
            } finally {
                Misc.freeObjList(ownerFunctions);
            }
            Assert.assertEquals(1, safeOwnerFunction.closeCount);
            Assert.assertEquals(1, unsafeOwnerFunction.closeCount);
        });
    }

    @Test
    public void testCompilesOnlyThreadUnsafeFunctionsPerWorker() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFunctionParser parser = new CountingFunctionParser();
            final ObjectPool<QueryColumn> queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 4);
            final ObjectPool<ExpressionNode> expressionNodePool = new ObjectPool<>(ExpressionNode.FACTORY, 4);
            try (SqlCodeGenerator codeGenerator = new SqlCodeGenerator(
                    configuration,
                    parser,
                    new PostOrderTreeTraversalAlgo(),
                    queryColumnPool,
                    expressionNodePool
            )) {
                final ObjList<QueryColumn> queryColumns = new ObjList<>();
                queryColumns.add(new QueryColumn().of("safe", expressionNodePool.next().of(ExpressionNode.LITERAL, "safe", 0, 0)));
                queryColumns.add(new QueryColumn().of("unsafe", expressionNodePool.next().of(ExpressionNode.LITERAL, "unsafe", 0, 0)));

                final CountingFunction safeOwnerFunction = new CountingFunction(true);
                final CountingFunction unsafeOwnerFunction = new CountingFunction(false);
                final ObjList<Function> ownerFunctions = new ObjList<>();
                ownerFunctions.add(safeOwnerFunction);
                ownerFunctions.add(unsafeOwnerFunction);

                final IntList flags = new IntList();
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL);
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL);

                final Method method = getCompilePerWorkerMethod();
                final int workerCount = 3;
                @SuppressWarnings("unchecked") final ObjList<ObjList<Function>> workerFunctions = (ObjList<ObjList<Function>>) method.invoke(
                        codeGenerator,
                        null,
                        queryColumns,
                        ownerFunctions,
                        workerCount,
                        null,
                        flags
                );
                try {
                    Assert.assertEquals(workerCount, parser.parseCount);
                    Assert.assertEquals(workerCount, parser.functions.size());
                    for (int i = 0; i < workerCount; i++) {
                        final ObjList<Function> functions = workerFunctions.getQuick(i);
                        Assert.assertSame(ownerFunctions.getQuick(0), functions.getQuick(0));
                        Assert.assertSame(parser.functions.getQuick(i), functions.getQuick(1));
                        PerWorkerFunctionList.init(functions, ownerFunctions, null, null);
                        PerWorkerFunctionList.clear(functions);
                    }
                    Assert.assertEquals(0, safeOwnerFunction.clearCount);
                    Assert.assertEquals(0, safeOwnerFunction.initCount);
                    Assert.assertEquals(0, unsafeOwnerFunction.clearCount);
                    Assert.assertEquals(0, unsafeOwnerFunction.initCount);
                    for (int i = 0; i < workerCount; i++) {
                        final CountingFunction function = parser.functions.getQuick(i);
                        Assert.assertEquals(1, function.clearCount);
                        Assert.assertEquals(1, function.initCount);
                    }
                } finally {
                    if (workerFunctions != null) {
                        for (int i = 0, n = workerFunctions.size(); i < n; i++) {
                            PerWorkerFunctionList.close(workerFunctions.getQuick(i));
                        }
                    }
                    for (int i = 0, n = parser.functions.size(); i < n; i++) {
                        Assert.assertEquals(1, parser.functions.getQuick(i).closeCount);
                    }
                    Misc.freeObjList(ownerFunctions);
                }
                Assert.assertEquals(1, safeOwnerFunction.closeCount);
                Assert.assertEquals(1, unsafeOwnerFunction.closeCount);
            }
        });
    }

    @Test
    public void testEmptyWorkerMatrixPreservesFlaggedEmptyLists() throws Exception {
        final CountingIntList flags = new CountingIntList();
        flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_COLUMN);
        flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_GROUP_BY);
        flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL);

        final Object result = extract(flags, new ObjList<>());

        Assert.assertEquals(flags.size(), flags.getQuickCallCount);
        Assert.assertEquals(0, getGroupByFunctions(result).size());
        Assert.assertEquals(0, getKeyFunctions(result).size());
    }

    @Test
    public void testNullWorkerMatrixReturnsNull() throws Exception {
        final CountingIntList flags = new CountingIntList();
        flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_GROUP_BY);

        Assert.assertNull(extract(flags, null));
        Assert.assertEquals(0, flags.getQuickCallCount);
    }

    @Test
    public void testPartitionsGroupByOnlyWithoutKeyList() throws Exception {
        final CountingIntList flags = new CountingIntList();
        flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_COLUMN);
        flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_GROUP_BY);

        final ObjList<Function> functions = new ObjList<>();
        functions.add(null);
        functions.add(null);
        final ObjList<ObjList<Function>> workerFunctions = new ObjList<>();
        workerFunctions.add(functions);

        final Object result = extract(flags, workerFunctions);

        Assert.assertEquals(flags.size(), flags.getQuickCallCount);
        Assert.assertEquals(1, getGroupByFunctions(result).size());
        Assert.assertNull(getKeyFunctions(result));
    }

    @Test
    public void testPartitionsSparseWideMatrixWithoutProjectionWidthCapacity() throws Exception {
        final int columnCount = 10_000;
        final CountingIntList flags = new CountingIntList();
        for (int i = 0; i < columnCount; i++) {
            flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_COLUMN);
        }
        flags.setQuick(1, GroupByUtils.PROJECTION_FUNCTION_FLAG_GROUP_BY);
        flags.setQuick(columnCount - 2, GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL);

        final int workerCount = 3;
        final ObjList<ObjList<Function>> workerFunctions = new ObjList<>();
        for (int i = 0; i < workerCount; i++) {
            final boolean isGroupByOwned = i % 2 == 0;
            final boolean isKeyOwned = !isGroupByOwned;
            final PerWorkerFunctionList<Function> functions = new PerWorkerFunctionList<>(columnCount);
            for (int j = 0; j < columnCount; j++) {
                functions.add(
                        null,
                        isGroupByOwned && j == 1 || isKeyOwned && j == columnCount - 2
                );
            }
            workerFunctions.add(functions);
        }

        final Object result = extract(flags, workerFunctions);

        Assert.assertEquals(workerCount * columnCount, flags.getQuickCallCount);
        final ObjList<ObjList<GroupByFunction>> groupByFunctions = getGroupByFunctions(result);
        final ObjList<ObjList<Function>> keyFunctions = getKeyFunctions(result);
        for (int i = 0; i < workerCount; i++) {
            final ObjList<GroupByFunction> workerGroupByFunctions = groupByFunctions.getQuick(i);
            final ObjList<Function> workerKeyFunctions = keyFunctions.getQuick(i);
            Assert.assertEquals(1, workerGroupByFunctions.size());
            Assert.assertEquals(1, workerKeyFunctions.size());
            Assert.assertEquals(i % 2 == 0, PerWorkerFunctionList.isOwned(workerGroupByFunctions, 0));
            Assert.assertEquals(i % 2 != 0, PerWorkerFunctionList.isOwned(workerKeyFunctions, 0));
            Assert.assertEquals(16, getBackingCapacity(workerGroupByFunctions));
            Assert.assertEquals(16, getBackingCapacity(workerKeyFunctions));
        }
    }

    @Test
    public void testPartitionsWorkerMatrixInSingleTraversal() throws Exception {
        final CountingIntList flags = new CountingIntList();
        flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_COLUMN);
        flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_GROUP_BY);
        flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL);
        flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_GROUP_BY);
        flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_COLUMN);

        final int workerCount = 3;
        final ObjList<ObjList<Function>> workerFunctions = new ObjList<>();
        for (int i = 0; i < workerCount; i++) {
            final ObjList<Function> functions = new ObjList<>();
            for (int j = 0; j < flags.size(); j++) {
                functions.add(null);
            }
            workerFunctions.add(functions);
        }

        final Object result = extract(flags, workerFunctions);

        Assert.assertEquals(workerCount * flags.size(), flags.getQuickCallCount);
        final ObjList<ObjList<GroupByFunction>> groupByFunctions = getGroupByFunctions(result);
        final ObjList<ObjList<Function>> keyFunctions = getKeyFunctions(result);
        Assert.assertEquals(workerCount, groupByFunctions.size());
        Assert.assertEquals(workerCount, keyFunctions.size());
        for (int i = 0; i < workerCount; i++) {
            Assert.assertEquals(2, groupByFunctions.getQuick(i).size());
            Assert.assertEquals(1, keyFunctions.getQuick(i).size());
        }
    }

    private static int getBackingCapacity(ObjList<?> list) throws Exception {
        final Field field = ObjList.class.getDeclaredField("buffer");
        field.setAccessible(true);
        return ((Object[]) field.get(list)).length;
    }

    private static Method getCompilePerWorkerMethod() throws NoSuchMethodException {
        final Method method = SqlCodeGenerator.class.getDeclaredMethod(
                "compilePerWorkerInnerProjectionFunctions",
                SqlExecutionContext.class,
                ObjList.class,
                ObjList.class,
                int.class,
                io.questdb.cairo.sql.RecordMetadata.class,
                IntList.class
        );
        method.setAccessible(true);
        return method;
    }

    private static Object extract(
            IntList flags,
            ObjList<ObjList<Function>> workerFunctions
    ) throws Exception {
        final Method method = SqlCodeGenerator.class.getDeclaredMethod(
                "extractWorkerFunctions",
                IntList.class,
                ObjList.class
        );
        method.setAccessible(true);
        return method.invoke(null, flags, workerFunctions);
    }

    @SuppressWarnings("unchecked")
    private static ObjList<ObjList<GroupByFunction>> getGroupByFunctions(Object result) throws Exception {
        final Field field = result.getClass().getDeclaredField("groupByFunctions");
        field.setAccessible(true);
        return (ObjList<ObjList<GroupByFunction>>) field.get(result);
    }

    @SuppressWarnings("unchecked")
    private static ObjList<ObjList<Function>> getKeyFunctions(Object result) throws Exception {
        final Field field = result.getClass().getDeclaredField("keyFunctions");
        field.setAccessible(true);
        return (ObjList<ObjList<Function>>) field.get(result);
    }

    private static class CountingFunction extends LongFunction {
        private int clearCount;
        private int closeCount;
        private int initCount;
        private final boolean isThreadSafe;

        private CountingFunction(boolean isThreadSafe) {
            this.isThreadSafe = isThreadSafe;
        }

        @Override
        public void clear() {
            clearCount++;
        }

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            initCount++;
        }

        @Override
        public boolean isThreadSafe() {
            return isThreadSafe;
        }
    }

    private static class CountingFunctionParser extends FunctionParser {
        private final RuntimeException failure = new RuntimeException("expected");
        private final int failAt;
        private final ObjList<CountingFunction> functions = new ObjList<>();
        private int parseCount;

        private CountingFunctionParser() {
            this(Integer.MAX_VALUE);
        }

        private CountingFunctionParser(int failAt) {
            super(configuration, new FunctionFactoryCache(configuration, Collections.emptyList()));
            this.failAt = failAt;
        }

        @Override
        public Function parseFunction(
                ExpressionNode node,
                io.questdb.cairo.sql.RecordMetadata metadata,
                SqlExecutionContext executionContext
        ) {
            parseCount++;
            if (parseCount == failAt) {
                throw failure;
            }
            final CountingFunction function = new CountingFunction("safe".contentEquals(node.token));
            functions.add(function);
            return function;
        }
    }

    private static class CountingIntList extends IntList {
        private int getQuickCallCount;

        @Override
        public int getQuick(int index) {
            getQuickCallCount++;
            return super.getQuick(index);
        }
    }
}
