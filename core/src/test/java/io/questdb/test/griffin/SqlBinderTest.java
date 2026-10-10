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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.SqlCompilerFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class SqlBinderTest extends AbstractCairoTest {
    @Test
    public void testAuthorizationRequiresOnlyReferencedColumns() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final int[] authorizations = {0};
            final ObjList<CharSequence> requiredColumns = new ObjList<>();
            requiredColumns.add("id");
            try (
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1).with(new AllowAllSecurityContext() {
                        @Override
                        public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
                            TestUtils.assertEquals("lp_rows", tableToken.getTableName());
                            Assert.assertEquals(requiredColumns.size(), columnNames.size());
                            for (int i = 0; i < requiredColumns.size(); i++) {
                                TestUtils.assertEquals(requiredColumns.getQuick(i), columnNames.getQuick(i));
                            }
                            authorizations[0]++;
                        }
                    });
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine)
            ) {
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id FROM lp_rows", executionContext
                ).getRecordCursorFactory()) {
                    assertFactory(factory).withContext(executionContext).inferRandomAccess().inferTimestamp().sizeMayVary()
                            .returns("id\n3\n1\n4\n2\n");
                    Assert.assertTrue(authorizations[0] > 0);
                }
                requiredColumns.add("active");
                requiredColumns.add("ts");
                authorizations[0] = 0;
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id FROM lp_rows WHERE active ORDER BY ts DESC", executionContext
                ).getRecordCursorFactory()) {
                    assertFactory(factory).withContext(executionContext).inferRandomAccess().inferTimestamp().sizeMayVary()
                            .returns("id\n2\n4\n3\n");
                    Assert.assertTrue(authorizations[0] > 0);
                }
            }
        });
    }

    @Test
    public void testBooleanLiteralsAndEmptyLimit() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly("SELECT id FROM lp_rows WHERE TRUE ORDER BY id", "id\n1\n2\n3\n4\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE FALSE ORDER BY id", "id\n");
            assertRowsOnly("SELECT id FROM lp_rows ORDER BY id LIMIT 0", "id\n");
        });
    }

    @Test
    public void testBoundBitwiseAndNullIfFamilies() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_bitwise (unused STRING, id INT, i INT, l LONG, d DOUBLE)");
            execute("INSERT INTO lp_bitwise VALUES ('a',1,2,2,2),('b',2,3,3,3),('c',3,null,null,null)");
            for (String column : new String[]{"i", "l"}) {
                assertRowsOnly("SELECT id FROM lp_bitwise WHERE (" + column + " & 1)=1", "id\n2\n");
                assertRowsOnly("SELECT id FROM lp_bitwise WHERE (" + column + " | 1)=3 ORDER BY id", "id\n1\n2\n");
                assertRowsOnly("SELECT id FROM lp_bitwise WHERE (" + column + " ^ 1)=2", "id\n2\n");
                assertRowsOnly("SELECT id FROM lp_bitwise WHERE (~" + column + ")=-3", "id\n1\n");
                assertRowsOnly("SELECT id FROM lp_bitwise WHERE (~" + column + ")=null", "id\n3\n");
            }
            for (String column : new String[]{"i", "l", "d"}) {
                assertRowsOnly("SELECT id FROM lp_bitwise WHERE nullif(" + column + ",2)=null ORDER BY id", "id\n1\n3\n");
                assertRowsOnly("SELECT id FROM lp_bitwise WHERE nullif(" + column + ",2)=3", "id\n2\n");
            }
        });
    }

    @Test
    public void testBoundIntArithmeticAndBooleanClosures() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly("SELECT id FROM lp_rows WHERE (id * 2 - 1) / 3 = 1 ORDER BY id", "id\n2\n3\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE -id < -2 ORDER BY id", "id\n3\n4\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE id / 0 = null ORDER BY id", "id\n1\n2\n3\n4\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE active AND id > 2 ORDER BY id", "id\n3\n4\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE NOT active OR id = 3 ORDER BY id", "id\n1\n3\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE id > 0 AND id < 4 AND id <> 2 AND active", "id\n3\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE TRUE AND (id > 2) ORDER BY id", "id\n3\n4\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE FALSE OR (id > 2) ORDER BY id", "id\n3\n4\n");
            assertRowsOnly("SELECT label FROM lp_rows WHERE FALSE AND id > 2", "label\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE TRUE OR id > 2 ORDER BY id", "id\n1\n2\n3\n4\n");
        });
    }

    @Test
    public void testBoundIntComparisonsAndGeneratedAliases() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly("SELECT id FROM lp_rows WHERE id = 2", "id\n2\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE id <> 2 ORDER BY id", "id\n1\n3\n4\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE id != 2 ORDER BY id", "id\n1\n3\n4\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE id < 2", "id\n1\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE id <= 2 ORDER BY id", "id\n1\n2\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE id > 2 ORDER BY id", "id\n3\n4\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE id >= 2 ORDER BY id", "id\n2\n3\n4\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE id + 1 = 4", "id\n3\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE 1 + 1 = 2 ORDER BY id", "id\n1\n2\n3\n4\n");
        });
    }

    @Test
    public void testBoundIntFilterRelocatesAfterPruningAndDropsNullFoldedColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_int_filter (discarded INT, id INT, unused STRING, value INT)");
            execute("INSERT INTO lp_int_filter VALUES (99,1,'a',3),(98,2,'b',2),(97,3,'c',null),(96,4,'d',4)");
            assertRowsOnly("SELECT id FROM lp_int_filter WHERE value + 1 >= 4 ORDER BY id", "id\n1\n4\n");
            assertRowsOnly("SELECT id FROM lp_int_filter WHERE discarded + null = value", "id\n3\n");
            assertRowsOnly("SELECT id FROM lp_int_filter WHERE discarded + null = null ORDER BY id", "id\n1\n2\n3\n4\n");
            assertRowsOnly("SELECT id FROM lp_int_filter t WHERE t.value + 1 = 4", "id\n1\n");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id FROM lp_int_filter WHERE value + 1 = 4", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertFailure(compiler, "SELECT id FROM lp_int_filter WHERE lp_missing_fn(value + 1) = 4",
                            "unknown function name", -1);
                    assertFailure(compiler, "SELECT id FROM lp_int_filter WHERE value = (SELECT lp_missing_fn(id) FROM lp_int_filter LIMIT 1)",
                            "unknown function name", -1);
                    compiler.clear();
                    assertRowsOnly(factory, "id\n1\n");
                }
            }
        });
    }

    @Test
    public void testBoundNumericProjectionKeepsTypesAndLongPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_numeric_project (unused STRING, l LONG, f FLOAT, d DOUBLE)");
            execute("INSERT INTO lp_numeric_project VALUES ('unused',9007199254740992,1.25,2.5)");
            assertRowsOnly("SELECT l FROM lp_numeric_project WHERE l+1>9007199254740992", "l\n9007199254740992\n");
            final String sql = "SELECT l+1 AS a, f+f AS b, d+d AS c, 1.25+2.25 AS folded, f+d AS mixed FROM lp_numeric_project";
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (
                        RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    Assert.assertEquals(ColumnType.LONG, factory.getMetadata().getColumnType(0));
                    Assert.assertEquals(ColumnType.FLOAT, factory.getMetadata().getColumnType(1));
                    Assert.assertEquals(ColumnType.DOUBLE, factory.getMetadata().getColumnType(2));
                    Assert.assertEquals(ColumnType.DOUBLE, factory.getMetadata().getColumnType(3));
                    Assert.assertEquals(ColumnType.DOUBLE, factory.getMetadata().getColumnType(4));
                    compiler.clear();
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(9007199254740993L, cursor.getRecord().getLong(0));
                    Assert.assertEquals(2.5f, cursor.getRecord().getFloat(1), 0.0f);
                    Assert.assertEquals(5.0, cursor.getRecord().getDouble(2), 0.0);
                    Assert.assertEquals(3.5, cursor.getRecord().getDouble(3), 0.0);
                    Assert.assertEquals(3.75, cursor.getRecord().getDouble(4), 0.0);
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testBoundParametersInferTypesAndRefreshBetweenCursors() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                bindVariableService.clear();
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id FROM lp_rows WHERE id=$1+1 ORDER BY id", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    Assert.assertEquals(ColumnType.INT, bindVariableService.getFunction(0).getType());
                    bindVariableService.setInt(0, 1);
                    assertRowsOnly(factory, "id\n2\n");
                    bindVariableService.setInt(0, 3);
                    compiler.clear();
                    assertRowsOnly(factory, "id\n4\n");
                }
                bindVariableService.setInt("threshold", 2);
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id FROM lp_rows WHERE id > :threshold ORDER BY id", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "id\n3\n4\n");
                    bindVariableService.setInt("threshold", 3);
                    assertRowsOnly(factory, "id\n4\n");
                }
                bindVariableService.clear();
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT cast($1 AS long) AS value FROM lp_rows WHERE id=1", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    Assert.assertEquals(ColumnType.DOUBLE, bindVariableService.getFunction(0).getType());
                    bindVariableService.setDouble(0, 2.5);
                    assertRowsOnly(factory, "value\n2\n");
                    bindVariableService.setDouble(0, 5.5);
                    assertRowsOnly(factory, "value\n5\n");
                }
                bindVariableService.clear();
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT $1 AS value FROM lp_rows WHERE id=1", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    Assert.assertEquals(ColumnType.STRING, bindVariableService.getFunction(0).getType());
                    bindVariableService.setStr(0, "first");
                    assertRowsOnly(factory, "value\nfirst\n");
                    bindVariableService.setStr(0, "second");
                    assertRowsOnly(factory, "value\nsecond\n");
                }
                bindVariableService.clear();
                bindVariableService.setBoolean(0, true);
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id FROM lp_rows WHERE $1 ORDER BY id", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "id\n1\n2\n3\n4\n");
                    bindVariableService.setBoolean(0, false);
                    assertRowsOnly(factory, "id\n");
                }
            }
        });
    }

    @Test
    public void testBoundParametersInferTypesInArgumentOrder() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                bindVariableService.clear();
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT cast($1 AS varchar) || $2 AS v, ts BETWEEN $3 AND '2020-01-01T00:00:01' AS b FROM lp_rows WHERE id = $4 + 1",
                        sqlExecutionContext
                ).getRecordCursorFactory()) {
                    Assert.assertEquals(ColumnType.STRING, bindVariableService.getFunction(0).getType());
                    Assert.assertEquals(ColumnType.STRING, bindVariableService.getFunction(1).getType());
                    Assert.assertEquals(ColumnType.TIMESTAMP_MICRO, bindVariableService.getFunction(2).getType());
                    Assert.assertEquals(ColumnType.INT, bindVariableService.getFunction(3).getType());
                    bindVariableService.setStr(0, "x");
                    bindVariableService.setStr(1, "y");
                    bindVariableService.setTimestamp(2, 0);
                    bindVariableService.setInt(3, 2);
                    assertRowsOnly(factory, "v\tb\nxy\ttrue\n");
                }
            }
        });
    }

    @Test
    public void testBoundPrimitiveNumericFamiliesAndNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_numeric (unused STRING, id INT, l LONG, f FLOAT, d DOUBLE)");
            execute("INSERT INTO lp_numeric VALUES ('a',1,2,2,2),('b',2,4,4,4),('c',3,null,null,null),('d',4,0,0,0)");
            for (String column : new String[]{"l", "f", "d"}) {
                final String prefix = "SELECT id FROM lp_numeric WHERE ";
                assertRowsOnly(prefix + column + "+" + column + "=8", "id\n2\n");
                assertRowsOnly(prefix + column + "-" + column + "=0 ORDER BY id", "id\n1\n2\n4\n");
                assertRowsOnly(prefix + column + "*" + column + "=16", "id\n2\n");
                assertRowsOnly(prefix + column + "/" + column + "=1 ORDER BY id", "id\n1\n2\n");
                assertRowsOnly(prefix + "-" + column + "<0 ORDER BY id", "id\n1\n2\n");
                assertRowsOnly(prefix + column + "=null", "id\n3\n");
                assertRowsOnly(prefix + "null=" + column, "id\n3\n");
                assertRowsOnly(prefix + column + "<>null ORDER BY id", "id\n1\n2\n4\n");
                assertRowsOnly(prefix + column + "+null=null ORDER BY id", "id\n1\n2\n3\n4\n");
                assertRowsOnly(prefix + column + "/0=null ORDER BY id", "id\n1\n2\n3\n4\n");
                assertRowsOnly(prefix + column + "<=2 ORDER BY id", "id\n1\n4\n");
                assertRowsOnly(prefix + column + ">=2 ORDER BY id", "id\n1\n2\n");
                assertRowsOnly(prefix + column + ">2", "id\n2\n");
            }
            assertRowsOnly("SELECT id FROM lp_numeric WHERE l+f+d+id=7", "id\n1\n");
            assertRowsOnly("SELECT id FROM lp_numeric WHERE l+2147483648=2147483650", "id\n1\n");
            assertRowsOnly("SELECT id FROM lp_numeric WHERE f+0.5=2.5", "id\n1\n");
            assertRowsOnly("SELECT id FROM lp_numeric WHERE d+0.5=4.5", "id\n2\n");
            assertRowsOnly("SELECT id FROM lp_numeric WHERE 1.25+2.25=3.5 ORDER BY id", "id\n1\n2\n3\n4\n");
        });
    }

    @Test
    public void testBoundStringPredicatesAndEscapedConstants() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_string (unused INT, id INT, s STRING, t STRING)");
            execute("INSERT INTO lp_string VALUES (0,1,'aa','ab'),(0,2,'ab','ab'),(0,3,null,null)," +
                    "(0,4,'a','a'),(0,5,'''quoted''','''quoted'''),(0,6,'',''),(0,7,'中文','中文')");
            assertRowsOnly("SELECT id FROM lp_string WHERE s=t ORDER BY id", "id\n2\n3\n4\n5\n6\n7\n");
            assertRowsOnly("SELECT id FROM lp_string WHERE s<>t", "id\n1\n");
            assertRowsOnly("SELECT id FROM lp_string WHERE s<t", "id\n1\n");
            assertRowsOnly("SELECT id FROM lp_string WHERE s='a'", "id\n4\n");
            assertRowsOnly("SELECT id FROM lp_string WHERE 'a'=s", "id\n4\n");
            assertRowsOnly("SELECT id FROM lp_string WHERE s='ab'", "id\n2\n");
            assertRowsOnly("SELECT id FROM lp_string WHERE s='中文'", "id\n7\n");
            assertRowsOnly("SELECT id FROM lp_string WHERE s=null", "id\n3\n");
            assertRowsOnly("SELECT id FROM lp_string WHERE s<'ab' ORDER BY id", "id\n1\n4\n5\n6\n");
            assertRowsOnly("SELECT id FROM lp_string WHERE s>='ab' ORDER BY id", "id\n2\n7\n");
            assertRowsOnly("SELECT id FROM lp_string WHERE s='''quoted'''", "id\n5\n");
            assertRowsOnly("SELECT '''quoted''' AS value FROM lp_string WHERE id=5", "value\n'quoted'\n");
            assertRowsOnly("SELECT id FROM (SELECT id, s AS renamed FROM lp_string) WHERE renamed='''quoted'''", "id\n5\n");
        });
    }

    @Test
    public void testBoundTimestampPredicatesAndMixedPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_time (unused STRING, id INT, ts TIMESTAMP, ns TIMESTAMP_NS)");
            execute("INSERT INTO lp_time VALUES ('a',1,1,1000),('b',2,1,1001),('c',3,2,2000),('d',4,null,null)");
            assertRowsOnly("SELECT id FROM lp_time WHERE ts=ns AND id<4 ORDER BY id", "id\n1\n3\n");
            assertRowsOnly("SELECT id FROM lp_time WHERE ts<ns ORDER BY id", "id\n2\n");
            assertRowsOnly("SELECT id FROM lp_time WHERE ns>ts ORDER BY id", "id\n2\n");
            for (String column : new String[]{"ts", "ns"}) {
                assertRowsOnly("SELECT id FROM lp_time WHERE " + column + "=null", "id\n4\n");
                assertRowsOnly("SELECT id FROM lp_time WHERE null=" + column, "id\n4\n");
                assertRowsOnly("SELECT id FROM lp_time WHERE " + column + "<>null ORDER BY id", "id\n1\n2\n3\n");
            }
            assertRowsOnly("SELECT id FROM lp_time WHERE ns='1970-01-01T00:00:00.000001001Z'", "id\n2\n");
            assertRowsOnly("SELECT id FROM lp_time WHERE ts<'1970-01-01T00:00:00.000001001Z' ORDER BY id", "id\n1\n2\n");
            assertRowsOnly("SELECT id FROM lp_time WHERE ns>='1970-01-01T00:00:00.000001001Z' ORDER BY id", "id\n2\n3\n");
            assertRowsOnly("SELECT id FROM lp_time WHERE ns<='1970-01-01T00:00:00.000001001Z' ORDER BY id", "id\n1\n2\n");
            assertRowsOnly("SELECT id FROM lp_time WHERE ns=null::timestamp_ns", "id\n4\n");
            assertRowsOnly("SELECT ns::timestamp_ns AS stamp FROM lp_time WHERE id=2", "stamp\n1970-01-01T00:00:00.000001001Z\n");
        });
    }

    @Test
    public void testCharComparedToMultiCharTextFolds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_char (id INT, c CHAR)");
            execute("INSERT INTO lp_char VALUES (1,'a'),(2,'b'),(3,null)");
            assertRowsOnly("SELECT id FROM lp_char WHERE c='ab'", "id\n");
            assertRowsOnly("SELECT id FROM lp_char WHERE 'ab'=c", "id\n");
            assertRowsOnly("SELECT id FROM lp_char WHERE c=''", "id\n");
            assertRowsOnly("SELECT id FROM lp_char WHERE c!='ab' ORDER BY id", "id\n1\n2\n3\n");
            assertRowsOnly("SELECT id FROM lp_char WHERE NOT (c='ab') ORDER BY id", "id\n1\n2\n3\n");
            assertRowsOnly("SELECT id FROM lp_char WHERE c='ab' OR id=2", "id\n2\n");
            assertRowsOnly("SELECT id, c='ab' eq, c<>'ab' ne FROM lp_char ORDER BY id", "id\teq\tne\n1\tfalse\ttrue\n2\tfalse\ttrue\n3\tfalse\ttrue\n");
        });
    }

    @Test
    public void testCreateTableAsSelectBindsDuringDeferredExecution() throws Exception {
        assertMemoryLeak(() -> {
            final int[] generatedQueries = {0};
            try (
                    CairoEngine statementEngine = newGenerationCountingEngine(generatedQueries);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(statementEngine, 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                statementEngine.load();
                statementEngine.execute("CREATE TABLE lp_source (id INT, active BOOLEAN)", executionContext);
                statementEngine.execute("INSERT INTO lp_source VALUES (3,true),(1,false),(2,true)", executionContext);
                statementEngine.execute(
                        "CREATE TABLE lp_copy AS (SELECT id FROM lp_source WHERE active ORDER BY id)",
                        executionContext
                );
                Assert.assertEquals(1, generatedQueries[0]);
                final StringSink actual = new StringSink();
                TestUtils.printSql(statementEngine, executionContext, "SELECT id FROM lp_copy ORDER BY id", actual);
                TestUtils.assertEquals("id\n2\n3\n", actual);
                Assert.assertEquals(2, generatedQueries[0]);
                statementEngine.execute(
                        "CREATE TABLE lp_count AS (SELECT count() AS n FROM lp_source WHERE active)",
                        executionContext
                );
                Assert.assertEquals(3, generatedQueries[0]);
                TestUtils.printSql(statementEngine, executionContext, "SELECT n FROM lp_count", actual);
                TestUtils.assertEquals("n\n2\n", actual);
                Assert.assertEquals(4, generatedQueries[0]);
            }
        });
    }

    @Test
    public void testCreateViewUsesBoundWildcardMetadataAndExplainDoesNotCreate() throws Exception {
        assertMemoryLeak(() -> {
            final int[] generatedQueries = {0};
            try (
                    CairoEngine statementEngine = newGenerationCountingEngine(generatedQueries);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(statementEngine, 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                statementEngine.load();
                statementEngine.execute("CREATE TABLE lp_source (id INT, active BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts)", executionContext);
                statementEngine.execute("INSERT INTO lp_source VALUES (1,true,'2020-01-01T00:00:00.000000Z'),(2,false,'2020-01-01T00:00:01.000000Z')", executionContext);
                statementEngine.execute("CREATE VIEW lp_view AS (SELECT * FROM lp_source)", executionContext);
                Assert.assertEquals(1, generatedQueries[0]);
                drainWalAndViewQueues(statementEngine);
                final int queriesAfterViewCompilation = generatedQueries[0];
                final TableToken viewToken = statementEngine.getTableTokenIfExists("lp_view");
                Assert.assertNotNull(viewToken);
                Assert.assertTrue(viewToken.isView());
                try (TableMetadata metadata = statementEngine.getTableMetadata(viewToken)) {
                    Assert.assertEquals(3, metadata.getColumnCount());
                    TestUtils.assertEquals("id", metadata.getColumnName(0));
                    TestUtils.assertEquals("active", metadata.getColumnName(1));
                    TestUtils.assertEquals("ts", metadata.getColumnName(2));
                    Assert.assertEquals(ColumnType.INT, metadata.getColumnType(0));
                    Assert.assertEquals(ColumnType.BOOLEAN, metadata.getColumnType(1));
                    Assert.assertEquals(ColumnType.TIMESTAMP, metadata.getColumnType(2));
                    Assert.assertEquals(2, metadata.getTimestampIndex());
                }
                final ViewDefinition definition = statementEngine.getViewGraph().getViewDefinition(viewToken);
                Assert.assertNotNull(definition);
                Assert.assertEquals(1, definition.getDependencies().size());
                Assert.assertTrue(definition.getDependencies().get("lp_source").contains("*"));

                final StringSink actual = new StringSink();
                TestUtils.printSql(statementEngine, executionContext,
                        "EXPLAIN CREATE VIEW lp_explained AS (SELECT * FROM lp_source)", actual);
                TestUtils.assertContains(actual, "Create view table: lp_explained");
                TestUtils.assertContains(actual, "Frame forward scan on: lp_source");
                Assert.assertEquals(queriesAfterViewCompilation, generatedQueries[0]);
                Assert.assertNull(statementEngine.getTableTokenIfExists("lp_explained"));

                // Reading a view still uses the existing view-expansion path.
                try (SqlCompilerImpl readerCompiler = new SqlCompilerImpl(statementEngine)) {
                    TestUtils.printSql(readerCompiler, executionContext, "SELECT * FROM lp_view ORDER BY id", actual);
                    TestUtils.assertEquals("""
                            id\tactive\tts
                            1\ttrue\t2020-01-01T00:00:00.000000Z
                            2\tfalse\t2020-01-01T00:00:01.000000Z
                            """, actual);
                }
                Assert.assertEquals(queriesAfterViewCompilation, generatedQueries[0]);
            }
        });
    }

    @Test
    public void testExplainBindsQueryWithoutWriting() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            execute("CREATE TABLE lp_target (id LONG)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertExplainContains(compiler, "EXPLAIN SELECT id FROM lp_rows WHERE active", "Frame forward scan on: lp_rows");
                assertExplainContains(compiler, "EXPLAIN INSERT INTO lp_target SELECT id FROM lp_rows", "Insert into table: lp_target");
                assertExplainContains(compiler, "EXPLAIN CREATE TABLE lp_copy AS (SELECT id FROM lp_rows)", "Create table: lp_copy");
                Assert.assertNull(engine.getTableTokenIfExists("lp_copy"));
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id FROM lp_target", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "id\n");
                }
            }
        });
    }

    @Test
    public void testFactorySurvivesCompilerReuseResetAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(
                            "SELECT label AS name, sym FROM lp_rows WHERE active ORDER BY id",
                            sqlExecutionContext
                    ).getRecordCursorFactory();
                    assertRowsOnly(retained, "name\tsym\nb\tB\nc\tC\n\t\n");
                    try (RecordCursorFactory other = compiler.compile(
                            "SELECT id AS other FROM lp_rows ORDER BY id DESC LIMIT 1",
                            sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        assertRowsOnly(other, "other\n4\n");
                        assertRowsOnly(retained, "name\tsym\nb\tB\nc\tC\n\t\n");
                    }
                    compiler.clear();
                    assertRowsOnly(retained, "name\tsym\nb\tB\nc\tC\n\t\n");
                }
                assertRowsOnly(retained, "name\tsym\nb\tB\nc\tC\n\t\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testFailedShapesReportAndCompilerRecovers() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertFailure(compiler, "SELECT lp_missing_fn(id) FROM lp_rows", "unknown function name", -1);
                assertFailure(compiler, "SELECT id FROM lp_rows WHERE lp_missing_fn(id) > 1", "unknown function name", -1);
                assertFailure(compiler, "SELECT id AS missing FROM lp_rows ORDER BY lp_missing_fn(id)", "unknown function name", -1);
                assertFailure(compiler, "SELECT lp_missing_agg(id) FROM lp_rows", "unknown function name", -1);
                assertFailure(compiler, "SELECT id FROM lp_rows GROUP BY lp_missing_fn(id)", "unknown function name", -1);
                assertFailure(compiler, "SELECT DISTINCT lp_missing_fn(id) FROM lp_rows", "unknown function name", -1);
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id FROM lp_rows WHERE active ORDER BY id", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "id\n2\n3\n4\n");
                }
            }
        });
    }

    @Test
    public void testGenerationPreservesLogicalSchemasAndColumnIdentities() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine) {
                @Override
                protected RecordCursorFactory generateSelectOneShot(
                        QueryModel model,
                        SqlExecutionContext executionContext,
                        boolean generateProgressLogger
                ) throws SqlException {
                    final LogicalPlan plan = getPlanForTesting();
                    Assert.assertNotNull(plan);
                    final String before = describePlan(plan);
                    final RecordCursorFactory factory = super.generateSelectOneShot(model, executionContext, generateProgressLogger);
                    try {
                        Assert.assertSame(plan, getPlanForTesting());
                        Assert.assertEquals(before, describePlan(plan));
                        return factory;
                    } catch (Throwable th) {
                        Misc.free(factory);
                        throw th;
                    }
                }
            }) {
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id AS renamed, id AS duplicate FROM lp_rows WHERE active ORDER BY ts DESC LIMIT 2",
                        sqlExecutionContext
                ).getRecordCursorFactory()) {
                    final LogicalPlan root = compiler.getPlanForTesting();
                    final ProjectPlan project = (ProjectPlan) root.inputAt(0);
                    final int sourceId = ((ColumnExpression) project.getExpressions().getQuick(0)).getColumnId();
                    Assert.assertEquals(sourceId, ((ColumnExpression) project.getExpressions().getQuick(1)).getColumnId());
                    Assert.assertNotEquals(sourceId, project.getOutput().getColumnId(0));
                    Assert.assertNotEquals(project.getOutput().getColumnId(0), project.getOutput().getColumnId(1));
                    final String before = describePlan(root);
                    assertRowsOnly(factory, "renamed\tduplicate\n2\t2\n4\t4\n");
                    Assert.assertEquals(before, describePlan(root));
                }
            }
        });
    }

    @Test
    public void testHiddenOrderColumnAndDuplicateOrderDirection() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly("SELECT label FROM lp_rows WHERE active ORDER BY id, id DESC LIMIT 2", "label\nb\nc\n");
            assertRowsOnly("SELECT id FROM lp_rows WHERE active ORDER BY ts DESC LIMIT 2", "id\n2\n4\n");
            assertRowsOnly("SELECT label FROM lp_rows LIMIT 2", "label\nc\na\n");
        });
    }

    @Test
    public void testInsertSelectPreservesTargetMappingCastsAndWildcardValidation() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            execute("CREATE TABLE lp_target (name STRING, id LONG)");
            execute("CREATE TABLE lp_clone (id LONG, active BOOLEAN, label STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_invalid (id INT, active UUID)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                execute(compiler, "INSERT INTO lp_target (id,name) SELECT id,label FROM lp_rows WHERE active");
                Assert.assertNotNull(compiler.getPlanForTesting());
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT name,id FROM lp_target ORDER BY id", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "name\tid\nb\t2\nc\t3\n\t4\n");
                }
                execute(compiler, "INSERT INTO lp_clone SELECT * FROM lp_rows");
                Assert.assertEquals(5, compiler.getPlanForTesting().getOutput().getColumnCount());
                assertFailure(compiler, "INSERT INTO lp_target (name,id) SELECT * FROM lp_rows", "column count mismatch", -1);
                assertFailure(compiler, "INSERT INTO lp_invalid SELECT * FROM lp_rows", "inconvertible types", -1);
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id,label FROM lp_clone ORDER BY id", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "id\tlabel\n1\ta\n2\tb\n3\tc\n4\t\n");
                }
            }
        });
    }

    @Test
    public void testInsertValuesAndCommandsUseExistingStatementExecution() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                execute(compiler, "INSERT INTO lp_rows VALUES (5,true,'e','E','2020-01-01T00:00:04.000000Z')");
                Assert.assertNull(compiler.getPlanForTesting());
                execute(compiler, "ALTER TABLE lp_rows ADD COLUMN added INT");
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id,added FROM lp_rows ORDER BY id", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "id\tadded\n1\tnull\n2\tnull\n3\tnull\n4\tnull\n5\tnull\n");
                }
                execute(compiler, "TRUNCATE TABLE lp_rows");
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id FROM lp_rows", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "id\n");
                }
            }
        });
    }

    @Test
    public void testJoinedSourceFilterErrorKeepsCloseFailures() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_left (id LONG)");
            execute("CREATE TABLE lp_right (id LONG)");
            bindVariableService.clear();
            bindVariableService.setLong(0, 1);
            try (OwnershipFixture fixture = new OwnershipFixture(engine);
                 SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                final RuntimeException cleanup = new RuntimeException("owned_long close");
                fixture.failLongClose(cleanup);
                final SqlException actual = Assert.assertThrows(SqlException.class, () -> compiler.compile(
                        "SELECT l.id FROM lp_left l JOIN (SELECT id FROM lp_right WHERE abs(owned_long($1), id) = 1) r ON l.id = r.id",
                        sqlExecutionContext
                ));
                TestUtils.assertContains(actual.getFlyweightMessage(), "there is no matching function `abs`");
                Assert.assertTrue(OwnershipFixture.hasSuppressed(actual, cleanup));
                fixture.assertAllClosedOnce();
            }
        });
    }

    @Test
    public void testMetadataChangeRebindsOnRetry() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final int[] attempts = {0};
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine) {
                @Override
                protected RecordCursorFactory generateSelectOneShot(
                        QueryModel model,
                        SqlExecutionContext executionContext,
                        boolean generateProgressLogger
                ) throws SqlException {
                    if (++attempts[0] == 1) {
                        engine.execute("ALTER TABLE lp_rows ADD COLUMN added INT", executionContext);
                    }
                    return super.generateSelectOneShot(model, executionContext, generateProgressLogger);
                }
            }) {
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id FROM lp_rows ORDER BY id", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    Assert.assertEquals(2, attempts[0]);
                    assertRowsOnly(factory, "id\n1\n2\n3\n4\n");
                    LogicalPlan scan = compiler.getPlanForTesting();
                    while (scan.inputCount() > 0) {
                        scan = scan.inputAt(0);
                    }
                    Assert.assertEquals(1, scan.getOutput().getColumnCount());
                    TestUtils.assertEquals("id", scan.getOutput().getColumnName(0));
                }
            }
        });
    }

    @Test
    public void testNumericCastsIncludingIdentityNullAndNarrowing() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_casts (unused STRING, id INT, i INT, l LONG, f FLOAT, d DOUBLE)");
            execute("INSERT INTO lp_casts VALUES ('a',1,2,2,2,2),('b',2,-3,-3,-3,-3),('c',3,null,null,null,null)");
            for (String column : new String[]{"i", "l", "f", "d"}) {
                for (String type : new String[]{"int", "long", "float", "double"}) {
                    assertRowsOnly("SELECT id FROM lp_casts WHERE cast(" + column + " AS " + type + ") = -3", "id\n2\n");
                    assertRowsOnly("SELECT id FROM lp_casts WHERE cast(" + column + " AS " + type + ") = null", "id\n3\n");
                }
            }
            for (String type : new String[]{"int", "long", "float", "double"}) {
                assertRowsOnly("SELECT id FROM lp_casts WHERE cast(null AS " + type + ") = null ORDER BY id", "id\n1\n2\n3\n");
            }
            assertRowsOnly("SELECT cast(4294967297 AS int) AS wrapped FROM lp_casts WHERE id=1", "wrapped\n1\n");
            assertRowsOnly("SELECT id FROM lp_casts WHERE cast(2147483648.0 AS int) = null ORDER BY id", "id\n1\n2\n3\n");
            assertRowsOnly("SELECT id FROM lp_casts WHERE cast(i+2147483647 AS long) < 0 ORDER BY id", "id\n1\n");
        });
    }

    @Test
    public void testQualifiedQuotedAliasesAndOrderOrdinal() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly(
                    "SELECT \"row alias\".id AS \"Result ID\", \"row alias\".label AS \"Display Name\" "
                            + "FROM lp_rows AS \"row alias\" WHERE \"row alias\".active ORDER BY \"Result ID\" DESC",
                    "Result ID\tDisplay Name\n4\t\n3\tc\n2\tb\n"
            );
            assertRowsOnly("SELECT r.label AS name, r.id AS number FROM lp_rows r ORDER BY 2 DESC", "name\tnumber\n\t4\nc\t3\nb\t2\na\t1\n");
            assertRowsOnly("SELECT label AS \"r.id\" FROM lp_rows r ORDER BY r.id", "r.id\na\nb\nc\n\n");
        });
    }

    @Test
    public void testRecordAndSliceArgumentsReachTheirFactories() throws Exception {
        assertMemoryLeak(() -> {
            assertRowsOnly("SELECT typeof(1:2) v FROM long_sequence(1)", "v\nINTERVAL\n");
            final String record = " FROM (SELECT information_schema._pg_expandarray(ARRAY[1.0]) k FROM long_sequence(1)) i";
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int path = 0; path < 2; path++) {
                    assertExecutionFailure(compiler, "SELECT concat((i.k)) v" + record, "unsupported type: RECORD", 15);
                    assertExecutionFailure(compiler, "SELECT coalesce(k, k) v" + record, "inconvertible types: RECORD -> RECORD", 19);
                    assertExecutionFailure(compiler, "SELECT concat(1:2) v FROM long_sequence(1)", "unsupported type: INTERVAL", 15);
                }
            }
        });
    }

    @Test
    public void testSignedAndTwoBoundLimits() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly("SELECT id FROM lp_rows ORDER BY id LIMIT -2", "id\n3\n4\n");
            assertRowsOnly("SELECT id FROM lp_rows ORDER BY id LIMIT 1,3", "id\n2\n3\n");
            assertRowsOnly("SELECT id FROM lp_rows ORDER BY id LIMIT -3,-1", "id\n2\n3\n");
            assertRowsOnly("SELECT id FROM lp_rows ORDER BY id LIMIT ,2", "id\n1\n2\n");
        });
    }

    @Test
    public void testSpliceThenInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly("SELECT a.id FROM lp_rows a SPLICE JOIN lp_rows b ON a.id=b.id "
                    + "JOIN lp_rows c ON a.id=c.id ORDER BY a.id", "id\n1\n2\n3\n4\n");
        });
    }

    @Test
    public void testTimestampDesignationWithRepeatedProjectionAliases() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertDesignatedTimestamp(compiler, "SELECT ts AS other, ts FROM lp_rows", 1, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT ts AS other, ts FROM lp_rows ORDER BY ts", 1, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT ts AS other, ts FROM lp_rows ORDER BY other", 0, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT ts AS ts1, ts AS ts2 FROM lp_rows ORDER BY ts2", 1, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT ts AS ts1, ts AS ts2 FROM lp_rows ORDER BY 2", 1, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT ts AS ts1, ts AS ts2 FROM lp_rows ORDER BY ts", -1, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT ts AS other, ts FROM lp_rows ORDER BY lp_rows.ts", -1, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT ts AS ts1, ts AS ts2 FROM lp_rows ORDER BY lp_rows.ts", -1, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT ts FROM lp_rows ORDER BY lp_rows.ts", 0, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT ts AS other FROM lp_rows ORDER BY lp_rows.ts", 0, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT lp_rows.ts FROM lp_rows ORDER BY lp_rows.ts", 0, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT lp_rows.ts AS other, ts FROM lp_rows ORDER BY lp_rows.ts", -1, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT ts AS other, lp_rows.ts FROM lp_rows ORDER BY lp_rows.ts", -1, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT * FROM lp_rows ORDER BY lp_rows.ts", 4, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT *, ts AS other FROM lp_rows ORDER BY lp_rows.ts", 5, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT ts, ts AS other FROM lp_rows ORDER BY lp_rows.ts", 1, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT ts AS other, * FROM lp_rows ORDER BY lp_rows.ts", -1, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT lp_rows.ts AS ts1, lp_rows.ts AS ts2 FROM lp_rows ORDER BY lp_rows.ts", 1, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT id, id AS duplicate, ts AS time FROM lp_rows ORDER BY lp_rows.ts", -1, RecordCursorFactory.SCAN_DIRECTION_FORWARD);
                assertDesignatedTimestamp(compiler, "SELECT ts AS ts1, ts AS ts2 FROM lp_rows WHERE active ORDER BY 2 DESC LIMIT 2", 1, RecordCursorFactory.SCAN_DIRECTION_BACKWARD);
            }
        });
    }

    @Test
    public void testTimestampPredicateLiteralScopeAndPartialDates() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_partial (id INT, ts TIMESTAMP, other TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lp_partial VALUES (1,'2020-01-01','2020-01-01')," +
                    "(2,'2020-01-15','2020-01-15'),(3,'2020-02-01','2020-02-01')");
            execute("CREATE TABLE lp_partial_ns (id INT, ts TIMESTAMP_NS) TIMESTAMP(ts)");
            execute("INSERT INTO lp_partial_ns VALUES (4,'2020-01-01T00:00:00.000000001Z')");
            for (String column : new String[]{"ts", "other"}) {
                assertRowsOnly("SELECT id FROM lp_partial WHERE " + column + ">'2020' ORDER BY id", "id\n2\n3\n");
                assertRowsOnly("SELECT id FROM lp_partial WHERE " + column + "<='2020-01' ORDER BY id", "id\n1\n");
                assertRowsOnly("SELECT id FROM lp_partial WHERE " + column + "='2020-01' ORDER BY id", "id\n1\n");
                assertRowsOnly("SELECT " + column + ">'2020' AS value FROM lp_partial ORDER BY id", "value\nfalse\ntrue\ntrue\n");
            }
            final String nanoLiteral = "'2020-01-01T00:00:00.000000001Z'";
            assertRowsOnly("SELECT id FROM lp_partial WHERE ts=" + nanoLiteral, "id\n");
            assertRowsOnly("SELECT id FROM lp_partial WHERE ts!='2020'", "id\n2\n3\n");
            assertRowsOnly("SELECT id FROM lp_partial WHERE ts!='2020-01'", "id\n2\n3\n");
            assertRowsOnly("SELECT id FROM (SELECT id, ts AS renamed FROM lp_partial) WHERE renamed=" + nanoLiteral, "id\n");
            assertRowsOnly("SELECT id FROM (SELECT id, ts AS a, ts AS b FROM lp_partial) WHERE b=" + nanoLiteral, "id\n");
            assertRowsOnly("SELECT id FROM (SELECT id, ts::timestamp AS renamed FROM lp_partial) WHERE renamed=" + nanoLiteral, "id\n");
            assertRowsOnly("SELECT id FROM (SELECT id, ts AS renamed FROM lp_partial WHERE id>0) WHERE renamed=" + nanoLiteral, "id\n");
            assertRowsOnly("SELECT id FROM (SELECT id, ts AS renamed FROM lp_partial LIMIT 2) WHERE renamed=" + nanoLiteral, "id\n");
            assertRowsOnly("SELECT id FROM lp_partial WHERE ts<" + nanoLiteral, "id\n1\n");
            assertRowsOnly("SELECT id FROM lp_partial WHERE other<" + nanoLiteral, "id\n1\n");
            assertRowsOnly("SELECT id FROM lp_partial WHERE ts::timestamp<" + nanoLiteral, "id\n1\n");
            assertRowsOnly("SELECT id FROM lp_partial WHERE ts::timestamp=" + nanoLiteral, "id\n");
            assertRowsOnly("SELECT id FROM lp_partial WHERE ts::timestamp!=" + nanoLiteral + " ORDER BY id", "id\n1\n2\n3\n");
            assertRowsOnly("SELECT ts<" + nanoLiteral + " AS value FROM lp_partial ORDER BY id", "value\ntrue\nfalse\nfalse\n");
            assertRowsOnly("SELECT id FROM lp_partial WHERE ts='2020-01-01' OR ts='2020-02-01' ORDER BY id", "id\n1\n3\n");
            assertRowsOnly("SELECT id FROM lp_partial WHERE ts='2020-01-01' OR id=3 ORDER BY id", "id\n1\n3\n");
            assertRowsOnly("SELECT id FROM lp_partial WHERE ts=" + nanoLiteral + " OR ts='2020-02-01' ORDER BY id", "id\n3\n");
            assertRowsOnly("SELECT id FROM (SELECT id, ts FROM lp_partial UNION ALL SELECT id, ts FROM lp_partial) WHERE ts=" + nanoLiteral,
                    "id\n");
            final String mixedSet = "SELECT id FROM (SELECT id, ts FROM lp_partial UNION ALL SELECT id, ts FROM lp_partial_ns) WHERE ts=";
            assertRowsOnly(mixedSet + nanoLiteral + " ORDER BY id", "id\n4\n");
            bindVariableService.clear();
            bindVariableService.setTimestampNano(0, 1_577_836_800_000_000_001L);
            assertRowsOnly(mixedSet + "$1 ORDER BY id", "id\n4\n");
            assertRowsOnly("SELECT id FROM lp_partial WHERE ts=$1", "id\n");
        });
    }

    @Test
    public void testTimestampSetNumericBoundsRequireBranchSemantics() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_epoch_us (id INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_epoch_ns (id INT, ts TIMESTAMP_NS) TIMESTAMP(ts)");
            execute("INSERT INTO lp_epoch_us VALUES (1,1)");
            execute("INSERT INTO lp_epoch_ns VALUES (2,1)");
            final String source = "SELECT id FROM (SELECT id, ts FROM lp_epoch_us UNION ALL SELECT id, ts FROM lp_epoch_ns) WHERE ts=";
            bindVariableService.clear();
            bindVariableService.setLong(0, 1);
            assertRowsOnly(source + "1 ORDER BY id", "id\n1\n2\n");
            assertRowsOnly(source + "$1 ORDER BY id", "id\n1\n2\n");
        });
    }

    @Test
    public void testUpdateBindsSourceQueryAndValidatesTargets() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_update (id INT, copied INT, active BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO lp_update VALUES (1,0,true,'2020-01-01T00:00:00.000000Z'),(2,0,false,'2020-01-01T00:00:01.000000Z')");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                execute(compiler, "UPDATE lp_update AS u SET COPIED = u.id WHERE u.active");
                Assert.assertNotNull(compiler.getPlanForTesting());
                assertFailure(compiler, "UPDATE lp_update SET ts = ts", "Designated timestamp column cannot be updated", -1);
                assertFailure(compiler, "UPDATE lp_update SET missing = id", "Invalid column", -1);
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id,copied FROM lp_update ORDER BY id", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "id\tcopied\n1\t1\n2\t0\n");
                }
            }
        });
    }

    @Test
    public void testValidationFailureThenCompilerReuse() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                final String missing = "SELECT missing FROM lp_rows";
                assertFailure(compiler, missing, "Invalid column", missing.indexOf("missing"));
                final String predicate = "SELECT id FROM lp_rows WHERE id";
                assertFailure(compiler, predicate, "boolean expression expected", predicate.lastIndexOf("id"));
                assertFailure(compiler, "SELECT id FROM lp_rows ORDER BY 0", "order column position is out of range [max=1]", -1);
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT label, id FROM lp_rows ORDER BY id LIMIT 1", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "label\tid\na\t1\n");
                }
            }
        });
    }

    @Test
    public void testWildcardPreservesTypesAndNullValues() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly("SELECT * FROM lp_rows ORDER BY id", """
                    id\tactive\tlabel\tsym\tts
                    1\tfalse\ta\tA\t2020-01-01T00:00:01.000000Z
                    2\ttrue\tb\tB\t2020-01-01T00:00:03.000000Z
                    3\ttrue\tc\tC\t2020-01-01T00:00:00.000000Z
                    4\ttrue\t\t\t2020-01-01T00:00:02.000000Z
                    """);
            assertRowsOnly("SELECT ts AS event_time, id FROM lp_rows LIMIT 2", """
                    event_time\tid
                    2020-01-01T00:00:00.000000Z\t3
                    2020-01-01T00:00:01.000000Z\t1
                    """);
        });
    }

    private static String describePlan(LogicalPlan plan) {
        final StringSink sink = new StringSink();
        describePlan(plan, sink);
        return sink.toString();
    }

    private static void describePlan(LogicalPlan plan, StringSink sink) {
        sink.put(plan.getClass().getSimpleName()).put(':').put(plan.getPosition()).put('[');
        final OutputSchema output = plan.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            sink.put(output.getColumnId(i)).put(':').put(output.getColumnName(i)).put(':')
                    .put(output.getColumnType(i)).put(':').put(output.isVisible(i)).put(';');
        }
        sink.put(']').put(output.getTimestampColumnId());
        if (plan instanceof ProjectPlan) {
            final ProjectPlan project = (ProjectPlan) plan;
            for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                sink.put('>').put(((ColumnExpression) project.getExpressions().getQuick(i)).getColumnId());
            }
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            describePlan(plan.inputAt(i), sink);
        }
    }

    private void assertDesignatedTimestamp(SqlCompilerImpl compiler, String sql, int timestampIndex, int scanDirection) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.assertEquals(sql, timestampIndex, factory.getMetadata().getTimestampIndex());
            Assert.assertEquals(sql, scanDirection, factory.getScanDirection());
        }
    }

    private void assertExecutionFailure(SqlCompilerImpl compiler, String sql, String message, int position) throws Exception {
        try (
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                RecordCursor ignored = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.fail("expected failure: " + sql);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
            Assert.assertEquals(position, e.getPosition());
        }
    }

    private void assertExplainContains(SqlCompilerImpl compiler, String sql, String expected) throws Exception {
        try (
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertNotNull(compiler.getPlanForTesting());
            final StringSink plan = new StringSink();
            while (cursor.hasNext()) {
                plan.put(cursor.getRecord().getStrA(0)).put('\n');
            }
            TestUtils.assertContains(plan, expected);
            TestUtils.assertContains(plan, "Frame forward scan on: lp_rows");
        }
    }

    private void assertFailure(SqlCompilerImpl compiler, String sql, String message, int position) {
        try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.fail("expected compilation failure: " + sql);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
            if (position >= 0) {
                Assert.assertEquals(position, e.getPosition());
            }
        }
    }

    private void createRows() throws SqlException {
        execute("CREATE TABLE lp_rows (id INT, active BOOLEAN, label STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("""
                INSERT INTO lp_rows VALUES
                    (3, true, 'c', 'C', '2020-01-01T00:00:00.000000Z'),
                    (1, false, 'a', 'A', '2020-01-01T00:00:01.000000Z'),
                    (4, true, null, null, '2020-01-01T00:00:02.000000Z'),
                    (2, true, 'b', 'B', '2020-01-01T00:00:03.000000Z')
                """);
    }

    private CairoEngine newGenerationCountingEngine(int[] generatedQueries) throws IOException {
        return new CairoEngine(new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath())) {
            @Override
            public SqlCompilerFactory getSqlCompilerFactory() {
                return engine -> {
                    final SqlCompilerImpl compiler = new SqlCompilerImpl(engine) {
                        @Override
                        protected RecordCursorFactory generateSelectOneShot(
                                QueryModel model,
                                SqlExecutionContext executionContext,
                                boolean generateProgressLogger
                        ) throws SqlException {
                            Assert.assertNotNull(getPlanForTesting());
                            generatedQueries[0]++;
                            return super.generateSelectOneShot(model, executionContext, generateProgressLogger);
                        }
                    };
                    return compiler;
                };
            }
        };
    }
}
