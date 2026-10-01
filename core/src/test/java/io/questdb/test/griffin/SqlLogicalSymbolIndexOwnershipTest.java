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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.FullPartitionFrameCursorFactory;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;

public class SqlLogicalSymbolIndexOwnershipTest extends AbstractCairoTest {
    @Test
    public void testCoveringBackupConstructionFailureClosesSharedFramesOnce() throws Exception {
        assertMemoryLeak(() -> {
            try (ConstructionFailureEngine failing = new ConstructionFailureEngine(temp.newFolder().getAbsolutePath())) {
                failing.createRows();
                failing.context.getBindVariableService().setStr(0, "A");
                failing.assertConstructionFails("SELECT s,id FROM lp_covering_keys WHERE s=$1");
            }
        });
    }

    @Test
    public void testDeferredFilteredScanRetainsKeyAndFilterUntilFactoryClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<BoundFilter> filters = registerFilter();
            final IntList closes = new IntList();
            FullPartitionFrameCursorFactory.setCloseObserverForTesting(factory -> closes.add(1));
            try {
                sqlExecutionContext.getBindVariableService().setStr(0, "A");
                try (RecordCursorFactory factory = select("SELECT * FROM lp_index_keys WHERE s=$1 AND lp_index_filter(id)")) {
                    assertIndexScan(factory);
                    for (int pass = 0; pass < 2; pass++) {
                        sqlExecutionContext.getBindVariableService().setStr(0, pass == 0 ? "A" : "B");
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(pass == 0 ? 2 : 3, cursor.getRecord().getInt(1));
                            Assert.assertFalse(cursor.hasNext());
                        }
                        assertRetained(filters);
                        Assert.assertEquals(0, closes.size());
                    }
                }
                assertClosedOnce(filters);
                Assert.assertEquals(1, closes.size());
            } finally {
                FullPartitionFrameCursorFactory.clearCloseObserverForTesting();
                engine.getFunctionFactoryCache().getFactories().remove("lp_index_filter");
            }
        });
    }

    @Test
    public void testMultiKeyFilteredScanOwnsEachKeyAfterCallerListReuse() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<BoundFilter> filters = registerFilter();
            final IntList closes = new IntList();
            FullPartitionFrameCursorFactory.setCloseObserverForTesting(factory -> closes.add(1));
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                sqlExecutionContext.getBindVariableService().setStr(0, "A");
                sqlExecutionContext.getBindVariableService().setStr(1, "B");
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT * FROM lp_index_keys WHERE s IN ($1,$2) AND lp_index_filter(id)", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertIndexScan(factory);
                    compiler.compile("SELECT * FROM lp_index_keys WHERE s IN ('B','A')", sqlExecutionContext).getRecordCursorFactory().close();
                    final int compileCloses = closes.size();
                    for (int pass = 0; pass < 2; pass++) {
                        sqlExecutionContext.getBindVariableService().setStr(1, pass == 0 ? "B" : "A");
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(2, cursor.getRecord().getInt(1));
                            if (pass == 0) {
                                Assert.assertTrue(cursor.hasNext());
                                Assert.assertEquals(3, cursor.getRecord().getInt(1));
                            }
                            Assert.assertFalse(cursor.hasNext());
                        }
                        assertRetained(filters);
                        Assert.assertEquals(compileCloses, closes.size());
                    }
                    Assert.assertEquals(compileCloses, closes.size());
                    closes.clear();
                }
                assertClosedOnce(filters);
                Assert.assertEquals(1, closes.size());
            } finally {
                FullPartitionFrameCursorFactory.clearCloseObserverForTesting();
                engine.getFunctionFactoryCache().getFactories().remove("lp_index_filter");
            }
        });
    }

    @Test
    public void testMultiKeyConstructionFailureReleasesResolvedKeysAndFilter() throws Exception {
        assertMemoryLeak(() -> {
            try (ConstructionFailureEngine failing = new ConstructionFailureEngine(temp.newFolder().getAbsolutePath())) {
                failing.createRows();
                failing.assertConstructionFails("SELECT * FROM lp_index_keys WHERE s IN ('A','B') AND lp_index_filter(id)");
                failing.context.getBindVariableService().setStr(0, "A");
                failing.context.getBindVariableService().setStr(1, "B");
                failing.assertConstructionFails("SELECT * FROM lp_index_keys WHERE s IN ($1,$2) AND lp_index_filter(id)");
            }
        });
    }

    @Test
    public void testMultiKeyCoveringBackupFailureReleasesKeys() throws Exception {
        assertMemoryLeak(() -> {
            try (ConstructionFailureEngine failing = new ConstructionFailureEngine(temp.newFolder().getAbsolutePath())) {
                failing.createRows();
                failing.assertConstructionFails("SELECT s,id FROM lp_covering_keys WHERE s IN ('A',NULL)");
            }
        });
    }

    @Test
    public void testSingleKeyFilteredConstructionFailureClosesInputsOnce() throws Exception {
        assertMemoryLeak(() -> {
            try (ConstructionFailureEngine failing = new ConstructionFailureEngine(temp.newFolder().getAbsolutePath())) {
                failing.createRows();
                failing.assertConstructionFails("SELECT * FROM lp_index_keys WHERE s='A' AND lp_index_filter(id)");
                failing.context.getBindVariableService().setStr(0, "A");
                failing.assertConstructionFails("SELECT * FROM lp_index_keys WHERE s=$1 AND lp_index_filter(id)");
                failing.assertConstructionFails("SELECT * FROM lp_index_keys WHERE s='A'");
            }
        });
    }

    private static void assertClosedOnce(ObjList<BoundFilter> filters) {
        for (int i = 0, n = filters.size(); i < n; i++) {
            Assert.assertEquals(1, filters.getQuick(i).closeCount);
        }
    }

    private static void assertIndexScan(RecordCursorFactory factory) {
        final TextPlanSink plan = new TextPlanSink();
        plan.of(factory, sqlExecutionContext);
        TestUtils.assertContains(plan.getSink(), "Index forward scan on: s deferred: true");
        TestUtils.assertContains(plan.getSink(), "filter: ");
    }

    private static void assertRetained(ObjList<BoundFilter> filters) {
        int retained = 0;
        for (int i = 0, n = filters.size(); i < n; i++) {
            final int closeCount = filters.getQuick(i).closeCount;
            Assert.assertTrue(closeCount <= 1);
            if (closeCount == 0) {
                retained++;
            }
        }
        Assert.assertTrue(retained > 0);
    }

    private static ObjList<BoundFilter> registerFilter() throws SqlException {
        return registerFilter(engine.getFunctionFactoryCache());
    }

    private static ObjList<BoundFilter> registerFilter(FunctionFactoryCache cache) throws SqlException {
        final ObjList<BoundFilter> filters = new ObjList<>();
        final ObjList<FunctionFactoryDescriptor> descriptors = new ObjList<>();
        descriptors.add(new FunctionFactoryDescriptor(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "lp_index_filter(I)";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                        CairoConfiguration configuration, SqlExecutionContext executionContext) {
                final BoundFilter filter = new BoundFilter(args.getQuick(0));
                filters.add(filter);
                return filter;
            }
        }));
        cache.getFactories().put("lp_index_filter", descriptors);
        return filters;
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_index_keys(s SYMBOL INDEX,id INT,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_index_keys VALUES('A',1,1),('A',2,2),('B',3,3)");
    }

    /**
     * An engine whose page-frame cursor construction fails while armed. Every page-frame-backed
     * factory constructor reads the parquet cache size, so the failure lands inside the generator
     * after it has taken ownership of frames, keys and filter.
     */
    static final class ConstructionFailureEngine implements Closeable {
        final SqlExecutionContextImpl context;
        final CairoEngine engine;
        final RuntimeException failure = new RuntimeException("page frame cursor construction");
        final ObjList<BoundFilter> filters;
        private boolean isArmed;

        ConstructionFailureEngine(String root) throws SqlException {
            engine = new CairoEngine(new DefaultTestCairoConfiguration(root) {
                @Override
                public long getSqlParquetCacheMemorySize() {
                    if (isArmed) {
                        throw failure;
                    }
                    return super.getSqlParquetCacheMemorySize();
                }
            });
            context = new SqlExecutionContextImpl(engine, 1)
                    .with(AllowAllSecurityContext.INSTANCE, new BindVariableServiceImpl(engine.getConfiguration()));
            engine.load();
            filters = registerFilter(engine.getFunctionFactoryCache());
        }

        void assertConstructionFails(String sql) throws SqlException {
            filters.clear();
            final IntList closes = new IntList();
            FullPartitionFrameCursorFactory.setCloseObserverForTesting(factory -> closes.add(1));
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                isArmed = true;
                try {
                    compiler.compile(sql, context);
                    Assert.fail("expected construction failure: " + sql);
                } catch (RuntimeException e) {
                    Assert.assertSame(sql, failure, e);
                } finally {
                    isArmed = false;
                }
                Assert.assertEquals(sql, 1, closes.size());
                Assert.assertEquals(sql, sql.contains("lp_index_filter"), filters.size() > 0);
                assertClosedOnce(filters);
            } finally {
                FullPartitionFrameCursorFactory.clearCloseObserverForTesting();
            }
        }

        @Override
        public void close() {
            context.close();
            engine.close();
        }

        void createRows() throws SqlException {
            engine.execute("CREATE TABLE lp_index_keys(s SYMBOL INDEX,id INT,ts TIMESTAMP) TIMESTAMP(ts)", context);
            engine.execute("INSERT INTO lp_index_keys VALUES('A',1,1),('A',2,2),('B',3,3)", context);
            engine.execute("CREATE TABLE lp_covering_keys(s SYMBOL INDEX TYPE POSTING INCLUDE (id),id INT,ts TIMESTAMP) TIMESTAMP(ts)", context);
            engine.execute("INSERT INTO lp_covering_keys VALUES('A',1,1),('A',2,2),('B',3,3)", context);
            engine.execute("CREATE TABLE lp_latest_keys(s SYMBOL,id INT,ts TIMESTAMP) TIMESTAMP(ts)", context);
            engine.execute("INSERT INTO lp_latest_keys VALUES('A',1,1),('A',2,2),('B',3,3)", context);
        }
    }

    static class BoundFilter extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private int closeCount;

        private BoundFilter(Function arg) {
            this.arg = arg;
        }

        @Override
        public void close() {
            closeCount++;
            arg.close();
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            return arg.getInt(rec) > 1;
        }
    }
}
