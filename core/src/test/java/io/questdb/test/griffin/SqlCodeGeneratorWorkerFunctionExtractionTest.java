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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
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
import io.questdb.std.Chars;
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

/**
 * Pins the contract of SqlCodeGenerator.compilePerWorkerInnerProjectionFunctions(): a single
 * traversal of the projection flags compiles only the retained GROUP_BY/VIRTUAL slots directly
 * into compact per-worker views, borrowing thread-safe owners, cloning the rest, and cleaning up
 * only the owned clones on a mid-compile failure.
 */
public class SqlCodeGeneratorWorkerFunctionExtractionTest extends AbstractCairoTest {

    @Test
    public void testAllThreadSafeProjectionReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFunctionParser parser = new CountingFunctionParser();
            final ObjectPool<QueryColumn> queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 4);
            final ObjectPool<ExpressionNode> expressionNodePool = new ObjectPool<>(ExpressionNode.FACTORY, 4);
            final CountingGroupByFunction groupByOwner = new CountingGroupByFunction(true, 7);
            final CountingFunction keyOwner = new CountingFunction(true);
            final ObjList<Function> ownerFunctions = new ObjList<>();
            ownerFunctions.add(null); // COLUMN slot, read natively by the per-worker record sink
            ownerFunctions.add(groupByOwner);
            ownerFunctions.add(keyOwner);
            try (SqlCodeGenerator codeGenerator = new SqlCodeGenerator(
                    configuration,
                    parser,
                    new PostOrderTreeTraversalAlgo(),
                    queryColumnPool,
                    expressionNodePool
            )) {
                final ObjList<QueryColumn> queryColumns = new ObjList<>();
                queryColumns.add(null);
                queryColumns.add(new QueryColumn().of("agg", expressionNodePool.next().of(ExpressionNode.LITERAL, "agg", 0, 0)));
                queryColumns.add(new QueryColumn().of("safe", expressionNodePool.next().of(ExpressionNode.LITERAL, "safe", 0, 0)));

                final CountingIntList flags = new CountingIntList();
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_COLUMN);
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_GROUP_BY);
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL);

                // Workers share the owner functions: no per-worker views, no clone parses,
                // and no per-worker flag traversal beyond the single collection pass.
                Assert.assertNull(compilePerWorkerFunctions(codeGenerator, queryColumns, ownerFunctions, 3, flags));
                Assert.assertEquals(flags.size(), flags.getQuickCallCount);
                Assert.assertEquals(0, parser.parseCount);
            } finally {
                Misc.freeObjList(ownerFunctions);
            }
        });
    }

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

                try {
                    compilePerWorkerFunctions(codeGenerator, queryColumns, ownerFunctions, 4, flags);
                    Assert.fail();
                } catch (RuntimeException e) {
                    Assert.assertSame(parser.failure, e);
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

                final int workerCount = 3;
                final Object result = compilePerWorkerFunctions(codeGenerator, queryColumns, ownerFunctions, workerCount, flags);
                final ObjList<ObjList<Function>> workerFunctions = getKeyFunctions(result);
                try {
                    // No GROUP_BY slot in the projection: no group-by view at all.
                    Assert.assertNull(getGroupByFunctions(result));
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
    public void testCompilesSparseWideProjectionIntoCompactViews() throws Exception {
        assertMemoryLeak(() -> {
            final int columnCount = 10_000;
            final CountingFunctionParser parser = new CountingFunctionParser();
            final ObjectPool<QueryColumn> queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 4);
            final ObjectPool<ExpressionNode> expressionNodePool = new ObjectPool<>(ExpressionNode.FACTORY, 4);
            final CountingGroupByFunction groupByOwner = new CountingGroupByFunction(false, 11);
            final CountingFunction keyOwner = new CountingFunction(true);
            final ObjList<Function> ownerFunctions = new ObjList<>();
            final ObjList<QueryColumn> queryColumns = new ObjList<>();
            for (int i = 0; i < columnCount; i++) {
                ownerFunctions.add(null);
                queryColumns.add(null);
            }
            ownerFunctions.setQuick(1, groupByOwner);
            ownerFunctions.setQuick(columnCount - 2, keyOwner);
            try (SqlCodeGenerator codeGenerator = new SqlCodeGenerator(
                    configuration,
                    parser,
                    new PostOrderTreeTraversalAlgo(),
                    queryColumnPool,
                    expressionNodePool
            )) {
                queryColumns.setQuick(1, new QueryColumn().of("agg", expressionNodePool.next().of(ExpressionNode.LITERAL, "agg", 0, 0)));
                queryColumns.setQuick(columnCount - 2, new QueryColumn().of("safe", expressionNodePool.next().of(ExpressionNode.LITERAL, "safe", 0, 0)));

                final CountingIntList flags = new CountingIntList();
                for (int i = 0; i < columnCount; i++) {
                    flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_COLUMN);
                }
                flags.setQuick(1, GroupByUtils.PROJECTION_FUNCTION_FLAG_GROUP_BY);
                flags.setQuick(columnCount - 2, GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL);

                final int workerCount = 3;
                final Object result = compilePerWorkerFunctions(codeGenerator, queryColumns, ownerFunctions, workerCount, flags);
                final ObjList<ObjList<GroupByFunction>> groupByFunctions = getGroupByFunctions(result);
                final ObjList<ObjList<Function>> keyFunctions = getKeyFunctions(result);
                try {
                    // One collection pass over the projection plus one flag read per retained
                    // slot per worker; no dense workerCount x columnCount rescan.
                    Assert.assertEquals(columnCount + workerCount * 2, flags.getQuickCallCount);
                    // Only the thread-unsafe group-by owner is cloned per worker.
                    Assert.assertEquals(workerCount, parser.parseCount);
                    for (int i = 0; i < workerCount; i++) {
                        final ObjList<GroupByFunction> workerGroupByFunctions = groupByFunctions.getQuick(i);
                        final ObjList<Function> workerKeyFunctions = keyFunctions.getQuick(i);
                        Assert.assertEquals(1, workerGroupByFunctions.size());
                        Assert.assertEquals(1, workerKeyFunctions.size());
                        Assert.assertTrue(PerWorkerFunctionList.isOwned(workerGroupByFunctions, 0));
                        Assert.assertFalse(PerWorkerFunctionList.isOwned(workerKeyFunctions, 0));
                        Assert.assertSame(parser.functions.getQuick(i), workerGroupByFunctions.getQuick(0));
                        Assert.assertSame(keyOwner, workerKeyFunctions.getQuick(0));
                        // Worker clones adopt the owner aggregate's value index.
                        Assert.assertEquals(11, workerGroupByFunctions.getQuick(0).getValueIndex());
                        // View capacity tracks the retained slot count, not the projection width.
                        Assert.assertEquals(16, getBackingCapacity(workerGroupByFunctions));
                        Assert.assertEquals(16, getBackingCapacity(workerKeyFunctions));
                    }
                } finally {
                    if (groupByFunctions != null) {
                        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                            PerWorkerFunctionList.close(groupByFunctions.getQuick(i));
                        }
                    }
                    if (keyFunctions != null) {
                        for (int i = 0, n = keyFunctions.size(); i < n; i++) {
                            PerWorkerFunctionList.close(keyFunctions.getQuick(i));
                        }
                    }
                }
                Assert.assertEquals(0, keyOwner.closeCount);
                for (int i = 0, n = parser.functions.size(); i < n; i++) {
                    Assert.assertEquals(1, parser.functions.getQuick(i).closeCount);
                }
            } finally {
                Misc.freeObjList(ownerFunctions);
            }
        });
    }

    @Test
    public void testGroupByOnlyProjectionSkipsKeyList() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFunctionParser parser = new CountingFunctionParser();
            final ObjectPool<QueryColumn> queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 4);
            final ObjectPool<ExpressionNode> expressionNodePool = new ObjectPool<>(ExpressionNode.FACTORY, 4);
            final CountingGroupByFunction groupByOwner = new CountingGroupByFunction(false, 7);
            final ObjList<Function> ownerFunctions = new ObjList<>();
            ownerFunctions.add(null); // COLUMN slot, read natively by the per-worker record sink
            ownerFunctions.add(groupByOwner);
            try (SqlCodeGenerator codeGenerator = new SqlCodeGenerator(
                    configuration,
                    parser,
                    new PostOrderTreeTraversalAlgo(),
                    queryColumnPool,
                    expressionNodePool
            )) {
                final ObjList<QueryColumn> queryColumns = new ObjList<>();
                queryColumns.add(null);
                queryColumns.add(new QueryColumn().of("agg", expressionNodePool.next().of(ExpressionNode.LITERAL, "agg", 0, 0)));

                final CountingIntList flags = new CountingIntList();
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_COLUMN);
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_GROUP_BY);

                final int workerCount = 2;
                final Object result = compilePerWorkerFunctions(codeGenerator, queryColumns, ownerFunctions, workerCount, flags);
                final ObjList<ObjList<GroupByFunction>> groupByFunctions = getGroupByFunctions(result);
                try {
                    // No VIRTUAL slot in the projection: no key view at all.
                    Assert.assertNull(getKeyFunctions(result));
                    Assert.assertEquals(flags.size() + workerCount, flags.getQuickCallCount);
                    Assert.assertEquals(workerCount, parser.parseCount);
                    Assert.assertEquals(workerCount, groupByFunctions.size());
                    for (int i = 0; i < workerCount; i++) {
                        final ObjList<GroupByFunction> workerGroupByFunctions = groupByFunctions.getQuick(i);
                        Assert.assertEquals(1, workerGroupByFunctions.size());
                        Assert.assertTrue(PerWorkerFunctionList.isOwned(workerGroupByFunctions, 0));
                        Assert.assertSame(parser.functions.getQuick(i), workerGroupByFunctions.getQuick(0));
                        Assert.assertEquals(7, workerGroupByFunctions.getQuick(0).getValueIndex());
                    }
                } finally {
                    if (groupByFunctions != null) {
                        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                            PerWorkerFunctionList.close(groupByFunctions.getQuick(i));
                        }
                    }
                }
                Assert.assertEquals(0, groupByOwner.closeCount);
                for (int i = 0, n = parser.functions.size(); i < n; i++) {
                    Assert.assertEquals(1, parser.functions.getQuick(i).closeCount);
                }
            } finally {
                Misc.freeObjList(ownerFunctions);
            }
        });
    }

    @Test
    public void testPartitionsRetainedFunctionsInProjectionOrder() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFunctionParser parser = new CountingFunctionParser();
            final ObjectPool<QueryColumn> queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 8);
            final ObjectPool<ExpressionNode> expressionNodePool = new ObjectPool<>(ExpressionNode.FACTORY, 8);
            final CountingGroupByFunction unsafeGroupByOwner = new CountingGroupByFunction(false, 3);
            final CountingFunction unsafeKeyOwner = new CountingFunction(false);
            final CountingGroupByFunction safeGroupByOwner = new CountingGroupByFunction(true, 5);
            final ObjList<Function> ownerFunctions = new ObjList<>();
            ownerFunctions.add(null);
            ownerFunctions.add(unsafeGroupByOwner);
            ownerFunctions.add(unsafeKeyOwner);
            ownerFunctions.add(safeGroupByOwner);
            ownerFunctions.add(null);
            try (SqlCodeGenerator codeGenerator = new SqlCodeGenerator(
                    configuration,
                    parser,
                    new PostOrderTreeTraversalAlgo(),
                    queryColumnPool,
                    expressionNodePool
            )) {
                final ObjList<QueryColumn> queryColumns = new ObjList<>();
                queryColumns.add(null);
                queryColumns.add(new QueryColumn().of("agg1", expressionNodePool.next().of(ExpressionNode.LITERAL, "agg1", 0, 0)));
                queryColumns.add(new QueryColumn().of("unsafe", expressionNodePool.next().of(ExpressionNode.LITERAL, "unsafe", 0, 0)));
                queryColumns.add(new QueryColumn().of("agg2", expressionNodePool.next().of(ExpressionNode.LITERAL, "agg2", 0, 0)));
                queryColumns.add(null);

                final CountingIntList flags = new CountingIntList();
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_COLUMN);
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_GROUP_BY);
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL);
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_GROUP_BY);
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_COLUMN);

                final int workerCount = 3;
                final Object result = compilePerWorkerFunctions(codeGenerator, queryColumns, ownerFunctions, workerCount, flags);
                final ObjList<ObjList<GroupByFunction>> groupByFunctions = getGroupByFunctions(result);
                final ObjList<ObjList<Function>> keyFunctions = getKeyFunctions(result);
                try {
                    Assert.assertEquals(flags.size() + workerCount * 3, flags.getQuickCallCount);
                    // Per worker, only the two thread-unsafe slots are cloned, in projection order.
                    Assert.assertEquals(workerCount * 2, parser.parseCount);
                    Assert.assertEquals(workerCount, groupByFunctions.size());
                    Assert.assertEquals(workerCount, keyFunctions.size());
                    for (int i = 0; i < workerCount; i++) {
                        final ObjList<GroupByFunction> workerGroupByFunctions = groupByFunctions.getQuick(i);
                        final ObjList<Function> workerKeyFunctions = keyFunctions.getQuick(i);
                        Assert.assertEquals(2, workerGroupByFunctions.size());
                        Assert.assertEquals(1, workerKeyFunctions.size());
                        // Views stay aligned with the compact owner lists: retained slots appear
                        // in projection order, borrowed entries included.
                        Assert.assertSame(parser.functions.getQuick(2 * i), workerGroupByFunctions.getQuick(0));
                        Assert.assertTrue(PerWorkerFunctionList.isOwned(workerGroupByFunctions, 0));
                        Assert.assertEquals(3, workerGroupByFunctions.getQuick(0).getValueIndex());
                        Assert.assertSame(safeGroupByOwner, workerGroupByFunctions.getQuick(1));
                        Assert.assertFalse(PerWorkerFunctionList.isOwned(workerGroupByFunctions, 1));
                        Assert.assertSame(parser.functions.getQuick(2 * i + 1), workerKeyFunctions.getQuick(0));
                        Assert.assertTrue(PerWorkerFunctionList.isOwned(workerKeyFunctions, 0));
                    }
                    // Borrowed owners keep their own value index.
                    Assert.assertEquals(0, safeGroupByOwner.initValueIndexCount);
                    Assert.assertEquals(5, safeGroupByOwner.getValueIndex());
                } finally {
                    if (groupByFunctions != null) {
                        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                            PerWorkerFunctionList.close(groupByFunctions.getQuick(i));
                        }
                    }
                    if (keyFunctions != null) {
                        for (int i = 0, n = keyFunctions.size(); i < n; i++) {
                            PerWorkerFunctionList.close(keyFunctions.getQuick(i));
                        }
                    }
                }
                Assert.assertEquals(0, unsafeGroupByOwner.closeCount);
                Assert.assertEquals(0, unsafeKeyOwner.closeCount);
                Assert.assertEquals(0, safeGroupByOwner.closeCount);
                for (int i = 0, n = parser.functions.size(); i < n; i++) {
                    Assert.assertEquals(1, parser.functions.getQuick(i).closeCount);
                }
            } finally {
                Misc.freeObjList(ownerFunctions);
            }
        });
    }

    @Test
    public void testZeroWorkerCountPreservesFlaggedEmptyLists() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFunctionParser parser = new CountingFunctionParser();
            final ObjectPool<QueryColumn> queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 4);
            final ObjectPool<ExpressionNode> expressionNodePool = new ObjectPool<>(ExpressionNode.FACTORY, 4);
            final CountingGroupByFunction groupByOwner = new CountingGroupByFunction(false, 7);
            final CountingFunction keyOwner = new CountingFunction(true);
            final ObjList<Function> ownerFunctions = new ObjList<>();
            ownerFunctions.add(null);
            ownerFunctions.add(groupByOwner);
            ownerFunctions.add(keyOwner);
            try (SqlCodeGenerator codeGenerator = new SqlCodeGenerator(
                    configuration,
                    parser,
                    new PostOrderTreeTraversalAlgo(),
                    queryColumnPool,
                    expressionNodePool
            )) {
                final ObjList<QueryColumn> queryColumns = new ObjList<>();
                queryColumns.add(null);
                queryColumns.add(new QueryColumn().of("agg", expressionNodePool.next().of(ExpressionNode.LITERAL, "agg", 0, 0)));
                queryColumns.add(new QueryColumn().of("safe", expressionNodePool.next().of(ExpressionNode.LITERAL, "safe", 0, 0)));

                final CountingIntList flags = new CountingIntList();
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_COLUMN);
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_GROUP_BY);
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL);

                // Thread-unsafe projection with no shared workers: both flagged views exist
                // and stay empty, and no clone is parsed.
                final Object result = compilePerWorkerFunctions(codeGenerator, queryColumns, ownerFunctions, 0, flags);
                Assert.assertNotNull(result);
                Assert.assertEquals(0, getGroupByFunctions(result).size());
                Assert.assertEquals(0, getKeyFunctions(result).size());
                Assert.assertEquals(flags.size(), flags.getQuickCallCount);
                Assert.assertEquals(0, parser.parseCount);
            } finally {
                Misc.freeObjList(ownerFunctions);
            }
        });
    }

    private static Object compilePerWorkerFunctions(
            SqlCodeGenerator codeGenerator,
            ObjList<QueryColumn> queryColumns,
            ObjList<Function> ownerFunctions,
            int workerCount,
            IntList flags
    ) throws Exception {
        final Method method = getCompilePerWorkerMethod();
        try {
            return method.invoke(codeGenerator, null, queryColumns, ownerFunctions, workerCount, null, flags);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw e;
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
                RecordMetadata.class,
                IntList.class
        );
        method.setAccessible(true);
        return method;
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
        int clearCount;
        int closeCount;
        int initCount;
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
                RecordMetadata metadata,
                SqlExecutionContext executionContext
        ) {
            parseCount++;
            if (parseCount == failAt) {
                throw failure;
            }
            final CountingFunction function = Chars.startsWith(node.token, "agg")
                    ? new CountingGroupByFunction(false, -1)
                    : new CountingFunction("safe".contentEquals(node.token));
            functions.add(function);
            return function;
        }
    }

    private static class CountingGroupByFunction extends CountingFunction implements GroupByFunction {
        private int initValueIndexCount;
        private int valueIndex;

        private CountingGroupByFunction(boolean isThreadSafe, int valueIndex) {
            super(isThreadSafe);
            this.valueIndex = valueIndex;
        }

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
        }

        @Override
        public int getValueIndex() {
            return valueIndex;
        }

        @Override
        public void initValueIndex(int valueIndex) {
            initValueIndexCount++;
            this.valueIndex = valueIndex;
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
        }

        @Override
        public void setNull(MapValue mapValue) {
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
