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

package io.questdb.test.cairo.pool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.pool.SqlCompilerPool;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

public class SqlCompilerPoolTest extends AbstractCairoTest {
    private static final String CLOSE_FAILURE = "injected in-flight close failure";
    private static final long INACTIVE_TTL_MILLIS = 1_000;
    private static final String TRIM_FAILURE = "injected trim failure";
    private static int closeCount;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            protected Iterable<FunctionFactory> getFunctionFactories() {
                final ArrayList<FunctionFactory> factories = new ArrayList<>();
                super.getFunctionFactories().forEach(factories::add);
                factories.add(new CloseFailingCursorFunctionFactory());
                return factories;
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testDoesNotSupportRefreshAt() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    SqlCompiler compiler1 = engine.getSqlCompiler();
                    SqlCompiler compiler2 = engine.getSqlCompiler()
            ) {
                SqlCompilerPool.C c1 = (SqlCompilerPool.C) compiler1;
                SqlCompilerPool.C c2 = (SqlCompilerPool.C) compiler2;
                try {
                    c1.refreshAt(null, c2);
                    Assert.fail();
                } catch (UnsupportedOperationException ignore) {
                }
            }
        });
    }

    @Test
    public void testReleaseInactiveClosesCompilerItCannotTrim() throws Exception {
        assertMemoryLeak(() -> {
            final LogCapture capture = new LogCapture();
            capture.start();
            try (
                    CairoEngine idleEngine = new CairoEngine(inactiveTtlConfiguration()) {
                        @Override
                        public SqlCompilerFactory getSqlCompilerFactory() {
                            return compilerEngine -> new SqlCompilerImpl(compilerEngine) {
                                @Override
                                public void releaseRetainedCapacity() {
                                    throw new IllegalStateException(TRIM_FAILURE);
                                }
                            };
                        }
                    };
                    SqlExecutionContext context = TestUtils.createSqlExecutionCtx(idleEngine)
            ) {
                setCurrentMicros(1_000_000);
                try (SqlCompiler compiler = idleEngine.getSqlCompiler()) {
                    compileAndClose(compiler, "SELECT 1", context);
                }
                setCurrentMicros(1_000_000 + INACTIVE_TTL_MILLIS * 1000 + 1);
                Assert.assertTrue(idleEngine.releaseInactive());
                capture.drain();
                capture.assertLoggedRE("could not trim idle compiler, closing it \\[error=.*" + TRIM_FAILURE);
                Assert.assertEquals(0, idleEngine.getSqlCompilerPool().getBusyCount());
                try (SqlCompiler compiler = idleEngine.getSqlCompiler()) {
                    compileAndClose(compiler, "SELECT 1", context);
                }
            } finally {
                currentMicros = -1;
                capture.stop();
            }
        });
    }

    @Test
    public void testReleaseInactiveTrimsIdleCompilerOnly() throws Exception {
        assertMemoryLeak(() -> {
            final int modelFloor = configuration.getSqlModelPoolCapacity();
            final int characterFloor = configuration.getSqlCharacterStoreCapacity();
            try (
                    CairoEngine idleEngine = new CairoEngine(inactiveTtlConfiguration());
                    SqlExecutionContext context = TestUtils.createSqlExecutionCtx(idleEngine)
            ) {
                setCurrentMicros(1_000_000);
                final SqlCompiler idle = idleEngine.getSqlCompiler();
                final SqlCompiler busy = idleEngine.getSqlCompiler();
                final SqlCompilerImpl idleDelegate = (SqlCompilerImpl) ((SqlCompilerPool.C) idle).getDelegate();
                final SqlCompilerImpl busyDelegate = (SqlCompilerImpl) ((SqlCompilerPool.C) busy).getDelegate();
                try {
                    compileAndClose(idle, cteChain(8), context);
                    idle.close();
                    compileAndClose(busy, cteChain(8), context);
                    busy.clear();
                    Assert.assertTrue(idleDelegate.getQueryModelPoolCapacity() > modelFloor);
                    Assert.assertTrue(idleDelegate.getCharacterStoreCapacity() > characterFloor);
                    final int busyModels = busyDelegate.getQueryModelPoolCapacity();
                    Assert.assertTrue(busyModels > modelFloor);

                    setCurrentMicros(1_000_000 + INACTIVE_TTL_MILLIS * 1000);
                    idleEngine.releaseInactive();
                    Assert.assertTrue(idleDelegate.getQueryModelPoolCapacity() > modelFloor);

                    setCurrentMicros(1_000_000 + INACTIVE_TTL_MILLIS * 1000 + 1);
                    idleEngine.releaseInactive();
                    Assert.assertEquals(modelFloor, idleDelegate.getQueryModelPoolCapacity());
                    Assert.assertEquals(characterFloor, idleDelegate.getCharacterStoreCapacity());
                    Assert.assertEquals(configuration.getSqlExpressionPoolCapacity(), idleDelegate.getSqlNodePoolCapacity());
                    Assert.assertEquals(busyModels, busyDelegate.getQueryModelPoolCapacity());
                } finally {
                    busy.close();
                    currentMicros = -1;
                }

                try (SqlCompiler compiler = idleEngine.getSqlCompiler()) {
                    compileAndClose(compiler, cteChain(8), context);
                }
            }
        });
    }

    @Test
    public void testReturnToPoolLogsInFlightCloseFailure() throws Exception {
        assertMemoryLeak(() -> {
            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                closeCount = 0;
                final SqlCompiler delegate;
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    delegate = ((SqlCompilerPool.C) compiler).getDelegate();
                    // Model compilation binds the FROM table function but never generates a
                    // cursor, so its prepared CursorFunction stays in flight until the pool
                    // reclaims the compiler.
                    compiler.generateExecutionModel("SELECT * FROM close_failing_cursor()", sqlExecutionContext);
                    Assert.assertEquals(0, closeCount);
                }
                Assert.assertEquals(1, closeCount);
                capture.drain();
                capture.assertLoggedRE("could not free in-flight compilation resources \\[error=.*" + CLOSE_FAILURE);

                try (
                        RecordCursorFactory factory = delegate.compile("SELECT 1 x", sqlExecutionContext).getRecordCursorFactory();
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(1, cursor.getRecord().getInt(0));
                    Assert.assertFalse(cursor.hasNext());
                }
                Assert.assertEquals(1, closeCount);
            } finally {
                capture.stop();
            }
        });
    }

    private static void compileAndClose(SqlCompiler compiler, CharSequence sql, SqlExecutionContext context) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
            Assert.assertNotNull(factory);
        }
    }

    private static StringSink cteChain(int levels) {
        final StringSink sql = new StringSink();
        sql.put("WITH c0 AS (SELECT x l FROM long_sequence(3))");
        for (int i = 1; i <= levels; i++) {
            sql.put(", c").put(i).put(" AS (SELECT * FROM c").put(i - 1).put(" UNION ALL SELECT * FROM c").put(i - 1).put(')');
        }
        sql.put(" SELECT count() FROM c").put(levels);
        return sql;
    }

    private static CairoConfiguration inactiveTtlConfiguration() {
        return new CairoConfigurationWrapper(configuration) {
            @Override
            public long getInactiveReaderTTL() {
                return INACTIVE_TTL_MILLIS;
            }
        };
    }

    private static class CloseFailingCursorFunctionFactory implements FunctionFactory {
        @Override
        public String getSignature() {
            return "close_failing_cursor()";
        }

        @Override
        public boolean isCursor() {
            return true;
        }

        @Override
        public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
            Misc.freeObjList(args);
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("x", ColumnType.INT));
            return new CursorFunction(new EmptyTableRecordCursorFactory(metadata)) {
                @Override
                public void close() {
                    closeCount++;
                    super.close();
                    throw new IllegalStateException(CLOSE_FAILURE);
                }
            };
        }
    }
}
