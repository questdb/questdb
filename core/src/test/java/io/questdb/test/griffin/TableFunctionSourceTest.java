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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableFunctionSourceTest extends AbstractCairoTest {
    @Test
    public void testCatalogueSourcesPruneAndMaterializeSort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_a (id INT,ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_b (id INT)");
            assertFunctionSource("SELECT table_name FROM tables() ORDER BY table_name", "table_name\nlp_a\nlp_b\n", 1);
            assertFunctionSource("SELECT \"column\",type FROM table_columns('lp_a') ORDER BY \"column\"", "column\ttype\nid\tINT\nts\tTIMESTAMP\n", 2);
            assertFunctionSource("SELECT typname FROM pg_type() WHERE oid=23", "typname\nint4\n", 24);
            assertFunctionSource("SELECT typname FROM pg_catalog.pg_type() WHERE oid=23", "typname\nint4\n", 24);
        });
    }

    @Test
    public void testCatalogueSourcesWithoutParentheses() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_catalogue (id INT)");
            assertFunctionSource("SELECT table_name FROM tables ORDER BY table_name", "table_name\nlp_catalogue\n", 1);
            assertFunctionSource("SELECT typname FROM pg_type WHERE oid=23", "typname\nint4\n", 24);
            assertFunctionSource("SELECT p.typname FROM pg_catalog.pg_type p WHERE p.oid=23", "typname\nint4\n", 24);
        });
    }

    @Test
    public void testTableNameTakesPrecedenceOverCatalogueFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tables (id INT)");
            execute("INSERT INTO tables VALUES (42)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile("SELECT id FROM tables", sqlExecutionContext).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "id\n42\n");
                }
            }
            assertFunctionSource("SELECT table_name FROM tables()", "table_name\ntables\n", 1);
        });
    }

    @Test
    public void testUnresolvedTableNamesKeepDiagnosticsAndCompilerRemainsReusable() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                final ObjList<String> statements = new ObjList<>();
                statements.add("SELECT * FROM lp_missing");
                statements.add("SELECT * FROM long_sequence");
                statements.add("SELECT * FROM table_columns");
                for (int i = 0, n = statements.size(); i < n; i++) {
                    final String sql = statements.getQuick(i);
                    final String message;
                    final int position;
                    try {
                        compiler.compile(sql, sqlExecutionContext);
                        throw new AssertionError("expected missing table");
                    } catch (SqlException e) {
                        message = e.getFlyweightMessage().toString();
                        position = e.getPosition();
                    }
                    try {
                        compiler.compile(sql, sqlExecutionContext);
                        Assert.fail("expected missing table");
                    } catch (SqlException e) {
                        Assert.assertEquals(position, e.getPosition());
                        TestUtils.assertEquals(message, e.getFlyweightMessage());
                    }
                }
                try (RecordCursorFactory factory = compiler.compile("SELECT typname FROM pg_type WHERE oid=23", sqlExecutionContext).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "typname\nint4\n");
                }
            }
        });
    }

    @Test
    public void testFactorySurvivesCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT x+1 AS v FROM long_sequence(4) ORDER BY v DESC LIMIT 2", sqlExecutionContext)
                            .getRecordCursorFactory();
                    try (RecordCursorFactory other = compiler.compile("SELECT 7 AS value", sqlExecutionContext).getRecordCursorFactory()) {
                        assertRowsOnly(other, "value\n7\n");
                    }
                    compiler.clear();
                    assertRowsOnly(retained, "v\n5\n4\n");
                }
                assertRowsOnly(retained, "v\n5\n4\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testFailedProjectionConstructionClosesSource() throws Exception {
        assertMemoryLeak(() -> {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("unused", ColumnType.INT));
            metadata.add(new TableColumnMetadata("value", ColumnType.LONG));
            final int[] closeCount = {0};
            final RuntimeException failure = new RuntimeException("random access lookup failed");
            final EmptyTableRecordCursorFactory source = new EmptyTableRecordCursorFactory(metadata) {
                @Override
                public boolean recordCursorSupportsRandomAccess() {
                    throw failure;
                }

                @Override
                protected void _close() {
                    closeCount[0]++;
                    super._close();
                }
            };
            registerSource(new CursorFunction(source));
            try {
                final RuntimeException actual = Assert.assertThrows(RuntimeException.class, () -> select("SELECT value FROM lp_fn_source()"));
                Assert.assertSame(failure, actual);
            } finally {
                unregisterSource();
            }
            Assert.assertEquals(1, closeCount[0]);
        });
    }

    @Test
    public void testNoFromUsesOneRowFunctionSource() throws Exception {
        assertMemoryLeak(() -> {
            assertFunctionSource("SELECT 7 AS value,TRUE AS enabled", "value\tenabled\n7\ttrue\n", 1);
            assertFunctionSource("SELECT 7 AS value FROM long_sequence(3)", "value\n7\n7\n7\n", 1);
        });
    }

    @Test
    public void testPreparationFailureClosesBorrowedFactoryThroughWrapper() throws Exception {
        assertMemoryLeak(() -> {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("nested", ColumnType.RECORD));
            final int[] closeCounts = {0, 0};
            final EmptyTableRecordCursorFactory source = new EmptyTableRecordCursorFactory(metadata) {
                @Override
                protected void _close() {
                    closeCounts[0]++;
                    super._close();
                }
            };
            final CursorFunction wrapper = new CursorFunction(source) {
                @Override
                public void close() {
                    closeCounts[1]++;
                    super.close();
                }
            };
            registerSource(wrapper);
            try {
                final IllegalStateException e = Assert.assertThrows(IllegalStateException.class, () -> select("SELECT * FROM lp_fn_source()"));
                TestUtils.assertContains(e.getMessage(), "table function returned a RECORD column");
            } finally {
                unregisterSource();
            }
            Assert.assertEquals(1, closeCounts[0]);
            Assert.assertEquals(1, closeCounts[1]);
        });
    }

    @Test
    public void testPreparedWrapperOwnershipTransfer() throws Exception {
        assertMemoryLeak(() -> {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("unused", ColumnType.INT));
            metadata.add(new TableColumnMetadata("value", ColumnType.LONG));
            final int[] closeCounts = {0, 0};
            final EmptyTableRecordCursorFactory source = new EmptyTableRecordCursorFactory(metadata) {
                @Override
                protected void _close() {
                    closeCounts[0]++;
                    super._close();
                }
            };
            final CursorFunction wrapper = new CursorFunction(source) {
                @Override
                public void close() {
                    closeCounts[1]++;
                    super.close();
                }
            };
            registerSource(wrapper);
            try {
                try (RecordCursorFactory result = select("SELECT value FROM lp_fn_source()")) {
                    Assert.assertEquals(0, closeCounts[0]);
                    Assert.assertEquals(0, closeCounts[1]);
                    Assert.assertEquals(1, result.getMetadata().getColumnCount());
                    TestUtils.assertEquals("value", result.getMetadata().getColumnName(0));
                    assertRowsOnly(result, "value\n");
                }
                Assert.assertEquals(1, closeCounts[0]);
                Assert.assertEquals(0, closeCounts[1]);
            } finally {
                unregisterSource();
            }
        });
    }

    @Test
    public void testSequenceEmptyAndSeededSources() throws Exception {
        assertMemoryLeak(() -> {
            assertFunctionSource("SELECT x FROM long_sequence(0)", "x\n", 1);
            assertFunctionSource("SELECT x FROM long_sequence(-2)", "x\n", 1);
            assertFunctionSource("SELECT x FROM long_sequence(3,10,20)", "x\n1\n2\n3\n", 1);
        });
    }

    @Test
    public void testSequenceProjectionFilterOrderAndLimit() throws Exception {
        assertMemoryLeak(() -> {
            assertFunctionSource("SELECT x+1 AS v FROM long_sequence(5) WHERE x>2 ORDER BY v DESC LIMIT 2", "v\n6\n5\n", 1);
            assertFunctionSource("SELECT q.x FROM (SELECT x FROM long_sequence(5) ORDER BY x DESC LIMIT 3) q ORDER BY q.x", "x\n3\n4\n5\n", 1);
        });
    }

    @Test
    public void testSubquerySourceArgumentsReportConstantExpected() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try {
                    compiler.compile("SELECT x FROM long_sequence((SELECT 3))", sqlExecutionContext);
                    Assert.fail("expected unsupported source argument");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "constant expected");
                }
                try (RecordCursorFactory factory = compiler.compile("SELECT x FROM long_sequence(2)", sqlExecutionContext).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "x\n1\n2\n");
                }
            }
        });
    }

    private void assertFunctionSource(String sql, String expected, int expectedSourceColumnCount) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            LogicalPlan source = compiler.getPlanForTesting();
            while (source.inputCount() > 0) {
                source = source.inputAt(0);
            }
            Assert.assertTrue(source instanceof FunctionSourcePlan);
            Assert.assertEquals(expectedSourceColumnCount, source.getOutput().getColumnCount());
            assertRowsOnly(factory, expected);
        }
    }

    private static void registerSource(Function result) throws SqlException {
        final ObjList<FunctionFactoryDescriptor> descriptors = new ObjList<>();
        descriptors.add(new FunctionFactoryDescriptor(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "lp_fn_source()";
            }

            @Override
            public boolean isCursor() {
                return true;
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                        CairoConfiguration configuration, SqlExecutionContext executionContext) {
                return result;
            }
        }));
        engine.getFunctionFactoryCache().getFactories().put("lp_fn_source", descriptors);
    }

    private static void unregisterSource() {
        engine.getFunctionFactoryCache().getFactories().remove("lp_fn_source");
    }
}
