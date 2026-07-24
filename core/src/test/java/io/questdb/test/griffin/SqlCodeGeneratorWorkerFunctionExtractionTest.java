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

import com.sun.management.ThreadMXBean;
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
import io.questdb.griffin.SqlException;
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
import org.junit.Assume;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.Collections;

/**
 * Pins the contract of SqlCodeGenerator.compilePerWorkerInnerProjectionFunctions(): an
 * allocation-free safety scan (breaking at the first thread-unsafe retained slot) settles the
 * common all-thread-safe case, and only the thread-unsafe case takes a second collection pass
 * that compiles the retained GROUP_BY/VIRTUAL slots directly into compact per-worker views,
 * borrowing thread-safe owners, cloning the rest, and cleaning up only the owned clones on a
 * mid-compile failure.
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
                // and no flag traversal beyond the single safety scan (one read per slot at most).
                Assert.assertNull(compilePerWorkerFunctions(codeGenerator, queryColumns, ownerFunctions, 3, flags));
                Assert.assertTrue("flag reads " + flags.getQuickCallCount, flags.getQuickCallCount <= flags.size());
                Assert.assertEquals(0, parser.parseCount);
            } finally {
                Misc.freeObjList(ownerFunctions);
            }
        });
    }

    @Test
    public void testAllThreadSafeWideProjectionAllocatesNoIndexList() throws Exception {
        assertMemoryLeak(() -> {
            final java.lang.management.ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
            Assume.assumeTrue("thread allocation profiling unavailable", mxBean instanceof ThreadMXBean);
            final ThreadMXBean threadMXBean = (ThreadMXBean) mxBean;
            Assume.assumeTrue(threadMXBean.isThreadAllocatedMemorySupported());
            if (!threadMXBean.isThreadAllocatedMemoryEnabled()) {
                threadMXBean.setThreadAllocatedMemoryEnabled(true);
            }

            final int columnCount = 1_000_000;
            final CountingFunctionParser parser = new CountingFunctionParser();
            final ObjectPool<QueryColumn> queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 4);
            final ObjectPool<ExpressionNode> expressionNodePool = new ObjectPool<>(ExpressionNode.FACTORY, 4);
            final CountingFunction safeOwner = new CountingFunction(true);
            final ObjList<Function> ownerFunctions = new ObjList<>(columnCount);
            final ObjList<QueryColumn> queryColumns = new ObjList<>(columnCount);
            final IntList flags = new IntList(columnCount);
            for (int i = 0; i < columnCount; i++) {
                ownerFunctions.add(safeOwner);
                queryColumns.add(null);
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL);
            }
            final ObjList<Function> warmUpFunctions = new ObjList<>();
            warmUpFunctions.add(safeOwner);
            final ObjList<QueryColumn> warmUpQueryColumns = new ObjList<>();
            warmUpQueryColumns.add(null);
            final IntList warmUpFlags = new IntList();
            warmUpFlags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL);
            try (SqlCodeGenerator codeGenerator = new SqlCodeGenerator(
                    configuration,
                    parser,
                    new PostOrderTreeTraversalAlgo(),
                    queryColumnPool,
                    expressionNodePool
            )) {
                // Warm up class loading and the allocation counter so the measured call
                // sees steady-state allocation behavior.
                for (int i = 0; i < 64; i++) {
                    Assert.assertNull(compilePerWorkerFunctions(codeGenerator, warmUpQueryColumns, warmUpFunctions, 3, warmUpFlags));
                    threadMXBean.getCurrentThreadAllocatedBytes();
                }
                final long maxAllowedBytes = 1_048_576;
                final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
                Assert.assertNull(compilePerWorkerFunctions(codeGenerator, queryColumns, ownerFunctions, 3, flags));
                final long allocatedBytes = threadMXBean.getCurrentThreadAllocatedBytes() - allocatedBefore;
                // The all-thread-safe verdict must use constant auxiliary space: an
                // O(columnCount) index list for a 1_000_000-slot projection would allocate
                // at least a 4 MiB int array, while the allocation-free scan stays within
                // measurement noise.
                Assert.assertTrue(
                        "all-thread-safe scan allocated " + allocatedBytes + " bytes",
                        allocatedBytes < maxAllowedBytes
                );
                Assert.assertEquals(0, parser.parseCount);
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
    public void testCloneCompilationFailurePreservesPrimaryAndClosesEveryWorkerList() throws Exception {
        assertMemoryLeak(() -> {
            final CountingFunctionParser parser = new CountingFunctionParser(4, 1);
            final ObjectPool<QueryColumn> queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 4);
            final ObjectPool<ExpressionNode> expressionNodePool = new ObjectPool<>(ExpressionNode.FACTORY, 4);
            final CountingFunction unsafeOwnerFunction = new CountingFunction(false);
            final ObjList<Function> ownerFunctions = new ObjList<>();
            ownerFunctions.add(unsafeOwnerFunction);
            try (SqlCodeGenerator codeGenerator = new SqlCodeGenerator(
                    configuration,
                    parser,
                    new PostOrderTreeTraversalAlgo(),
                    queryColumnPool,
                    expressionNodePool
            )) {
                final ObjList<QueryColumn> queryColumns = new ObjList<>();
                queryColumns.add(new QueryColumn().of("unsafe", expressionNodePool.next().of(ExpressionNode.LITERAL, "unsafe", 0, 0)));

                final IntList flags = new IntList();
                flags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_VIRTUAL);

                try {
                    compilePerWorkerFunctions(codeGenerator, queryColumns, ownerFunctions, 4, flags);
                    Assert.fail();
                } catch (RuntimeException e) {
                    Assert.assertSame(parser.failure, e);
                    Assert.assertArrayEquals(new Throwable[]{parser.closeFailure}, e.getSuppressed());
                }
                Assert.assertEquals(4, parser.parseCount);
                Assert.assertEquals(3, parser.functions.size());
                Assert.assertEquals(0, unsafeOwnerFunction.closeCount);
                for (int i = 0, n = parser.functions.size(); i < n; i++) {
                    Assert.assertEquals("clone " + i, 1, parser.functions.getQuick(i).closeCount);
                }
            } finally {
                Misc.freeObjList(ownerFunctions);
            }
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
                final SqlCodeGenerator.WorkerFunctionLists result = compilePerWorkerFunctions(codeGenerator, queryColumns, ownerFunctions, workerCount, flags);
                final ObjList<ObjList<Function>> workerFunctions = result.getKeyFunctions();
                try {
                    // No GROUP_BY slot in the projection: no group-by view at all.
                    Assert.assertNull(result.getGroupByFunctions());
                    Assert.assertTrue("parse count " + parser.parseCount, parser.parseCount <= workerCount);
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
                final SqlCodeGenerator.WorkerFunctionLists result = compilePerWorkerFunctions(codeGenerator, queryColumns, ownerFunctions, workerCount, flags);
                final ObjList<ObjList<GroupByFunction>> groupByFunctions = result.getGroupByFunctions();
                final ObjList<ObjList<Function>> keyFunctions = result.getKeyFunctions();
                try {
                    // Upper bound on flag reads: the safety scan breaks at slot 1 (the
                    // thread-unsafe group-by owner), then one collection pass over the
                    // projection plus one flag read per retained slot per worker. Staying
                    // within this bound rules out a dense workerCount x columnCount rescan.
                    Assert.assertTrue(
                            "flag reads " + flags.getQuickCallCount,
                            flags.getQuickCallCount <= 2 + columnCount + workerCount * 2
                    );
                    // Only the thread-unsafe group-by owner is cloned per worker.
                    Assert.assertTrue("parse count " + parser.parseCount, parser.parseCount <= workerCount);
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
                final SqlCodeGenerator.WorkerFunctionLists result = compilePerWorkerFunctions(codeGenerator, queryColumns, ownerFunctions, workerCount, flags);
                final ObjList<ObjList<GroupByFunction>> groupByFunctions = result.getGroupByFunctions();
                try {
                    // No VIRTUAL slot in the projection: no key view at all. Upper bound on
                    // flag reads: the safety scan breaks at slot 1 (the thread-unsafe group-by
                    // owner) before the collection pass and the per-worker reads.
                    Assert.assertNull(result.getKeyFunctions());
                    Assert.assertTrue(
                            "flag reads " + flags.getQuickCallCount,
                            flags.getQuickCallCount <= 2 + flags.size() + workerCount
                    );
                    Assert.assertTrue("parse count " + parser.parseCount, parser.parseCount <= workerCount);
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
    public void testNonKeyedMixedSafeUnsafeAggregatesReturnNullCorrectly() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT CASE WHEN x = 1 THEN NULL ELSE 'value' END::STRING s FROM long_sequence(100_000))");

            assertQuery("SELECT count() c0, count() c1, count() c2, count() c3, count(s) c4, count() c5, count() c6, count() c7, first(s) f FROM x")
                    .withPlanContaining("Async Group By")
                    .expectSize()
                    .noRandomAccess()
                    .returns("c0\tc1\tc2\tc3\tc4\tc5\tc6\tc7\tf\n100000\t100000\t100000\t100000\t99999\t100000\t100000\t100000\t\n");
        });
    }

    @Test
    public void testNonKeyedWideProjectionClonesOnlyUnsafeAggregatePerWorker() throws Exception {
        assertMemoryLeak(() -> {
            final int safeAggregateCount = 64;
            final int workerCount = 3;
            final CountingFunctionParser parser = new CountingFunctionParser();
            final ObjectPool<QueryColumn> queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, safeAggregateCount + 1);
            final ObjectPool<ExpressionNode> expressionNodePool = new ObjectPool<>(ExpressionNode.FACTORY, safeAggregateCount + 1);
            final ObjList<QueryColumn> queryColumns = new ObjList<>(safeAggregateCount + 1);
            final ObjList<GroupByFunction> ownerFunctions = new ObjList<>(safeAggregateCount + 1);
            final ObjList<Function> innerProjectionFunctions = new ObjList<>(safeAggregateCount + 1);
            final IntList projectionFunctionFlags = new IntList(safeAggregateCount + 1);
            for (int i = 0; i < safeAggregateCount; i++) {
                queryColumns.add(new QueryColumn().of(
                        "safe" + i,
                        expressionNodePool.next().of(ExpressionNode.FUNCTION, "aggSafe" + i, 0, 0)
                ));
                final CountingGroupByFunction function = new CountingGroupByFunction(true, i);
                ownerFunctions.add(function);
                innerProjectionFunctions.add(function);
                projectionFunctionFlags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_GROUP_BY);
            }
            queryColumns.add(new QueryColumn().of(
                    "unsafe",
                    expressionNodePool.next().of(ExpressionNode.FUNCTION, "aggUnsafe", 0, 0)
            ));
            final CountingGroupByFunction unsafeOwner = new CountingGroupByFunction(false, safeAggregateCount);
            ownerFunctions.add(unsafeOwner);
            innerProjectionFunctions.add(unsafeOwner);
            projectionFunctionFlags.add(GroupByUtils.PROJECTION_FUNCTION_FLAG_GROUP_BY);

            try (SqlCodeGenerator codeGenerator = new SqlCodeGenerator(
                    configuration,
                    parser,
                    new PostOrderTreeTraversalAlgo(),
                    queryColumnPool,
                    expressionNodePool
            )) {
                final ObjList<ObjList<GroupByFunction>> workerFunctions = codeGenerator.compileWorkerGroupByFunctionsConditionallyForTesting(
                        null,
                        queryColumns,
                        innerProjectionFunctions,
                        workerCount,
                        null,
                        projectionFunctionFlags
                );
                try {
                    Assert.assertNotNull(workerFunctions);
                    Assert.assertTrue("parse count " + parser.parseCount, parser.parseCount <= workerCount);
                    Assert.assertEquals(workerCount, parser.functions.size());
                    for (int worker = 0; worker < workerCount; worker++) {
                        final ObjList<GroupByFunction> functions = workerFunctions.getQuick(worker);
                        Assert.assertEquals(ownerFunctions.size(), functions.size());
                        for (int i = 0; i < safeAggregateCount; i++) {
                            Assert.assertSame(ownerFunctions.getQuick(i), functions.getQuick(i));
                            Assert.assertFalse(PerWorkerFunctionList.isOwned(functions, i));
                        }
                        Assert.assertSame(parser.functions.getQuick(worker), functions.getQuick(safeAggregateCount));
                        Assert.assertTrue(PerWorkerFunctionList.isOwned(functions, safeAggregateCount));
                        Assert.assertEquals(safeAggregateCount, functions.getQuick(safeAggregateCount).getValueIndex());
                        PerWorkerFunctionList.init(functions, ownerFunctions, null, null);
                        PerWorkerFunctionList.clear(functions);
                    }
                    for (int i = 0; i < safeAggregateCount; i++) {
                        final CountingGroupByFunction owner = (CountingGroupByFunction) ownerFunctions.getQuick(i);
                        Assert.assertEquals(0, owner.initCount);
                        Assert.assertEquals(0, owner.clearCount);
                    }
                    Assert.assertEquals(0, unsafeOwner.initCount);
                    Assert.assertEquals(0, unsafeOwner.clearCount);
                    Assert.assertEquals(workerCount, unsafeOwner.offerCount);
                    for (int i = 0; i < safeAggregateCount; i++) {
                        Assert.assertEquals(0, ((CountingGroupByFunction) ownerFunctions.getQuick(i)).offerCount);
                    }
                    for (int i = 0; i < workerCount; i++) {
                        Assert.assertEquals(1, parser.functions.getQuick(i).initCount);
                        Assert.assertEquals(1, parser.functions.getQuick(i).clearCount);
                    }
                } finally {
                    for (int i = 0, n = workerFunctions.size(); i < n; i++) {
                        PerWorkerFunctionList.close(workerFunctions.getQuick(i));
                    }
                }
                for (int i = 0, n = parser.functions.size(); i < n; i++) {
                    Assert.assertEquals(1, parser.functions.getQuick(i).closeCount);
                }
                for (int i = 0, n = ownerFunctions.size(); i < n; i++) {
                    Assert.assertEquals(0, ((CountingGroupByFunction) ownerFunctions.getQuick(i)).closeCount);
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
                final SqlCodeGenerator.WorkerFunctionLists result = compilePerWorkerFunctions(codeGenerator, queryColumns, ownerFunctions, workerCount, flags);
                final ObjList<ObjList<GroupByFunction>> groupByFunctions = result.getGroupByFunctions();
                final ObjList<ObjList<Function>> keyFunctions = result.getKeyFunctions();
                try {
                    // Upper bound on flag reads: the safety scan breaks at slot 1 (the
                    // thread-unsafe group-by owner) before the collection pass and the
                    // per-worker reads; staying within it rules out a dense rescan.
                    Assert.assertTrue(
                            "flag reads " + flags.getQuickCallCount,
                            flags.getQuickCallCount <= 2 + flags.size() + workerCount * 3
                    );
                    // Per worker, only the two thread-unsafe slots are cloned, in projection order.
                    Assert.assertTrue("parse count " + parser.parseCount, parser.parseCount <= workerCount * 2);
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
                final SqlCodeGenerator.WorkerFunctionLists result = compilePerWorkerFunctions(codeGenerator, queryColumns, ownerFunctions, 0, flags);
                Assert.assertNotNull(result);
                Assert.assertEquals(0, result.getGroupByFunctions().size());
                Assert.assertEquals(0, result.getKeyFunctions().size());
                // Upper bound on flag reads: the safety scan breaks at slot 1 (the
                // thread-unsafe group-by owner), then the collection pass reads every flag
                // once; no per-worker reads.
                Assert.assertTrue("flag reads " + flags.getQuickCallCount, flags.getQuickCallCount <= 2 + flags.size());
                Assert.assertEquals(0, parser.parseCount);
            } finally {
                Misc.freeObjList(ownerFunctions);
            }
        });
    }

    private static SqlCodeGenerator.WorkerFunctionLists compilePerWorkerFunctions(
            SqlCodeGenerator codeGenerator,
            ObjList<QueryColumn> queryColumns,
            ObjList<Function> ownerFunctions,
            int workerCount,
            IntList flags
    ) throws SqlException {
        return codeGenerator.compilePerWorkerInnerProjectionFunctionsForTesting(
                null,
                queryColumns,
                ownerFunctions,
                workerCount,
                null,
                flags
        );
    }

    private static class CountingFunction extends LongFunction {
        int clearCount;
        int closeCount;
        int initCount;
        int offerCount;
        private final RuntimeException closeFailure;
        private final boolean isThreadSafe;

        private CountingFunction(boolean isThreadSafe) {
            this(isThreadSafe, null);
        }

        private CountingFunction(boolean isThreadSafe, RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
            this.isThreadSafe = isThreadSafe;
        }

        @Override
        public void clear() {
            clearCount++;
        }

        @Override
        public void close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
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

        @Override
        public void offerStateTo(Function that) {
            offerCount++;
        }
    }

    private static class CountingFunctionParser extends FunctionParser {
        private final RuntimeException closeFailure = new RuntimeException("close failure");
        private final RuntimeException failure = new RuntimeException("expected");
        private final int failAt;
        private final int throwingCloseAt;
        private final ObjList<CountingFunction> functions = new ObjList<>();
        private int parseCount;

        private CountingFunctionParser() {
            this(Integer.MAX_VALUE);
        }

        private CountingFunctionParser(int failAt) {
            this(failAt, Integer.MAX_VALUE);
        }

        private CountingFunctionParser(int failAt, int throwingCloseAt) {
            super(configuration, new FunctionFactoryCache(configuration, Collections.emptyList()));
            this.failAt = failAt;
            this.throwingCloseAt = throwingCloseAt;
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
                    : new CountingFunction("safe".contentEquals(node.token), parseCount == throwingCloseAt ? closeFailure : null);
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
