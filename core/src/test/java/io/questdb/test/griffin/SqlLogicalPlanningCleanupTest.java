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
import io.questdb.cairo.pool.SqlCompilerPool;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.math.AddIntFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalPlanningCleanupTest extends AbstractCairoTest {
    @Test
    public void testPooledCompilerReleasesPreparedSourcesOnReturn() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    CairoEngine poolEngine = new CairoEngine(new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath()));
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(poolEngine, 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                poolEngine.load();
                final long pathMem = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PATH);
                try (SqlCompiler compiler = poolEngine.getSqlCompiler()) {
                    compiler.generateExecutionModel("SELECT pg_catalog.pg_class() z FROM long_sequence(2)", executionContext);
                    Assert.assertTrue(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PATH) > pathMem);
                }
                Assert.assertEquals(pathMem, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PATH));
            }
        });
    }

    @Test
    public void testValidationFailuresReleasePreparedFunctionsBeforeCompilerReuse() throws Exception {
        assertMemoryLeak(() -> {
            final int[] counts = {0, 0};
            try (
                    CairoEngine statementEngine = new CairoEngine(new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath()));
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(statementEngine, 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                statementEngine.load();
                statementEngine.execute("CREATE TABLE lp_source (id INT, ts TIMESTAMP) TIMESTAMP(ts)", executionContext);
                statementEngine.execute("INSERT INTO lp_source VALUES (1,'2020-01-01T00:00:00.000000Z')", executionContext);
                statementEngine.execute("CREATE TABLE lp_target (id INT)", executionContext);
                statementEngine.execute("CREATE VIEW lp_view AS (SELECT id,ts FROM lp_source)", executionContext);
                trackAddIntResults(statementEngine, counts);

                try (SqlCompilerImpl compiler = new SqlCompilerImpl(statementEngine)) {
                    assertRejected(compiler, executionContext,
                            "INSERT INTO lp_target (id) SELECT id,ts FROM lp_source WHERE id+1>0",
                            "column count mismatch", counts, 1);
                    assertRejected(compiler, executionContext,
                            "UPDATE lp_source SET missing=id WHERE id+1>0",
                            "Invalid column", counts, 2);
                    // Binding and INSERT authorization succeeded; this rejection occurs in
                    // compileUsingModel, after the model-validation catch has already returned.
                    assertRejected(compiler, executionContext,
                            "INSERT INTO lp_view SELECT id,ts FROM lp_source WHERE id+1>0",
                            "cannot modify view", counts, 3);

                    try (RecordCursorFactory factory = compiler.compile(
                            "SELECT id FROM lp_source WHERE id+1>0", executionContext
                    ).getRecordCursorFactory()) {
                        Assert.assertEquals(4, counts[0]);
                        Assert.assertEquals(3, counts[1]);
                        assertFactory(factory).withContext(executionContext).inferRandomAccess().inferTimestamp()
                                .sizeMayVary().returns("id\n1\n");
                    }
                    Assert.assertEquals(4, counts[1]);
                }
                Assert.assertEquals(4, counts[1]);
            }
        });
    }

    private static void assertRejected(
            SqlCompilerImpl compiler,
            SqlExecutionContext executionContext,
            String sql,
            String error,
            int[] counts,
            int expectedCount
    ) throws SqlException {
        try {
            compiler.compile(sql, executionContext);
            Assert.fail("expected validation failure: " + sql);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), error);
        }
        // Check before another compile/clear/close can release a stranded preparation.
        Assert.assertEquals(expectedCount, counts[0]);
        Assert.assertEquals(expectedCount, counts[1]);
    }

    private static void trackAddIntResults(CairoEngine engine, int[] counts) throws SqlException {
        final ObjList<FunctionFactoryDescriptor> overloads = engine.getFunctionFactoryCache().getOverloadList("+");
        for (int i = 0, n = overloads.size(); i < n; i++) {
            final FunctionFactory original = overloads.getQuick(i).getFactory();
            if (original.getClass() == AddIntFunctionFactory.class) {
                final FunctionFactory tracking = new FunctionFactory() {
                    @Override
                    public String getSignature() {
                        return original.getSignature();
                    }

                    @Override
                    public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                                CairoConfiguration configuration, SqlExecutionContext executionContext) throws SqlException {
                        final Function function = original.newInstance(position, args, argPositions, configuration, executionContext);
                        counts[0]++;
                        return new CountingIntFunction(function, counts);
                    }
                };
                // Retain the real AddInt signature and audited binding contract. Only this
                // isolated engine's descriptor adds a close counter around its actual result.
                overloads.setQuick(i, new FunctionFactoryDescriptor(original) {
                    @Override
                    public FunctionFactory getFactory() {
                        return tracking;
                    }
                });
                return;
            }
        }
        Assert.fail("missing AddInt registration");
    }

    private static final class CountingIntFunction extends IntFunction implements UnaryFunction {
        private final Function arg;
        private final int[] counts;

        private CountingIntFunction(Function arg, int[] counts) {
            this.arg = arg;
            this.counts = counts;
        }

        @Override
        public void close() {
            counts[1]++;
            arg.close();
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getInt(Record record) {
            return arg.getInt(record);
        }
    }
}
