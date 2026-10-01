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

import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalStatementExpressionTest extends AbstractCairoTest {
    @Test
    public void testInsertExpressionsWithImplicitAndExplicitColumns() throws Exception {
        assertMemoryLeak(() -> {
            final String table = "lp_values";
            execute("CREATE TABLE " + table + " (id INT,value LONG,active BOOLEAN,label STRING,ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                execute(compiler, "INSERT INTO " + table + " VALUES "
                        + "(abs(-3),2+3,2 IN (1,2),'a','2020-01-01T00:00:00.000000001Z'),"
                        + "(4,null,false,null,'2020-01-01T00:00:00.000000002Z')");
                execute(compiler, "INSERT INTO " + table + " (label,ts,id) VALUES "
                        + "('omitted','2020-01-01T00:00:00.000000003Z',abs(-5))");
                Assert.assertNull(compiler.getLogicalPlanForTesting());
            }
            assertQuery("SELECT * FROM " + table + " ORDER BY id").expectSize().returns("""
                    id\tvalue\tactive\tlabel\tts
                    3\t5\ttrue\ta\t2020-01-01T00:00:00.000000001Z
                    4\tnull\tfalse\t\t2020-01-01T00:00:00.000000002Z
                    5\tnull\tfalse\tomitted\t2020-01-01T00:00:00.000000003Z
                    """);
        });
    }

    @Test
    public void testInsertOperationOwnsExpressionsAfterCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_retained_values (id INT,value LONG,label STRING,dt DATE,ip IPv4,ts TIMESTAMP) TIMESTAMP(ts)");
            bindVariableService.setLong(1, -4);
            InsertOperation retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    final CompiledQuery query = compiler.compile(
                            "INSERT INTO lp_retained_values VALUES ($1,abs($2)+1,$3,$4,$5,$6)", sqlExecutionContext
                    );
                    Assert.assertEquals(CompiledQuery.INSERT, query.getType());
                    retained = query.popInsertOperation();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT 7 AS value", sqlExecutionContext).getRecordCursorFactory()) {
                        compiler.clear();
                    }
                }
                bindVariableService.setInt(0, 1);
                bindVariableService.setLong(1, -4);
                bindVariableService.setStr(2, "first");
                bindVariableService.setDate(3, 0);
                bindVariableService.setIPv4(4, "127.0.0.1");
                bindVariableService.setTimestamp(5, 0);
                insert(retained, 1);

                bindVariableService.setInt(0, 2);
                bindVariableService.setLong(1, -8);
                bindVariableService.setStr(2, "second");
                bindVariableService.setDate(3, 86_400_000L);
                bindVariableService.setIPv4(4, "192.168.1.1");
                bindVariableService.setTimestamp(5, 1_000_000L);
                insert(retained, 1);
            } finally {
                Misc.free(retained);
            }
            assertQuery("SELECT * FROM lp_retained_values ORDER BY id").expectSize().returns("""
                    id\tvalue\tlabel\tdt\tip\tts
                    1\t5\tfirst\t1970-01-01T00:00:00.000Z\t127.0.0.1\t1970-01-01T00:00:00.000000Z
                    2\t9\tsecond\t1970-01-02T00:00:00.000Z\t192.168.1.1\t1970-01-01T00:00:01.000000Z
                    """);
        });
    }

    @Test
    public void testInsertValidationFailureAndCompilerRecovery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_invalid_values (id INT,value UUID)");
            execute("CREATE TABLE lp_required_ts (id INT,ts TIMESTAMP) TIMESTAMP(ts)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertError(compiler, "INSERT INTO lp_invalid_values VALUES (1,null),(2,true)", 49, "inconvertible types: BOOLEAN -> UUID [from=true, to=value]");
                assertError(compiler, "INSERT INTO lp_invalid_values VALUES (1)", 39, "row value count does not match column count [expected=2, actual=1, tuple=1]");
                assertError(compiler, "INSERT INTO lp_invalid_values (missing) VALUES (1)", 31, "Invalid column: missing");
                assertError(compiler, "INSERT INTO lp_required_ts VALUES (1,null)", 37, "designated timestamp column cannot be NULL");
                assertError(compiler, "INSERT INTO lp_required_ts (id) VALUES (1)", 0, "insert statement must populate timestamp");
                execute(compiler, "INSERT INTO lp_invalid_values (id) VALUES (abs(-7)),(8)");
                execute(compiler, "INSERT INTO lp_required_ts VALUES (abs(-9),'2020-01-01')");
            }
            assertQuery("SELECT id FROM lp_invalid_values ORDER BY id").expectSize().returns("id\n7\n8\n");
            assertQuery("SELECT id FROM lp_required_ts").expectSize().returns("id\n9\n");
        });
    }

    @Test
    public void testMultiTupleInsertKeepsIndependentNativeExpressionClosures() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_multi_values (id INT,matched BOOLEAN,value LONG)");
            bindVariableService.setLong(0, 2);
            InsertOperation retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("""
                            INSERT INTO lp_multi_values VALUES
                                (1,$1 IN (1,2,3),abs($1)),
                                (2,$1 IN (4,5,6),$1+1),
                                (3,$1 IN (2,5,8),$1+2)
                            """, sqlExecutionContext).popInsertOperation();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT 7 AS value", sqlExecutionContext).getRecordCursorFactory()) {
                        compiler.clear();
                    }
                }
                bindVariableService.setLong(0, 5);
                insert(retained, 3);
                bindVariableService.setLong(0, 2);
                insert(retained, 3);
            } finally {
                Misc.free(retained);
            }
            assertQuery("SELECT * FROM lp_multi_values ORDER BY id,value").expectSize().returns("""
                    id\tmatched\tvalue
                    1\ttrue\t2
                    1\tfalse\t5
                    2\tfalse\t3
                    2\ttrue\t6
                    3\ttrue\t4
                    3\ttrue\t7
                    """);
        });
    }

    @Test
    public void testRejectedInsertTypeClosesNativeExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_native_validation (value UUID)");
            bindVariableService.setLong(0, 2);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int pass = 0; pass < 2; pass++) {
                    final String columns = pass == 0 ? "" : " (value)";
                    try {
                        final CompiledQuery query = compiler.compile(
                                "INSERT INTO lp_native_validation" + columns + " VALUES ($1 IN (1,2,3))", sqlExecutionContext
                        );
                        query.closeAllButSelect();
                        Assert.fail("BOOLEAN value must not be accepted by a UUID column");
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible types");
                    }
                }
                execute(compiler, "INSERT INTO lp_native_validation VALUES (null)");
            }
            assertQuery("SELECT count() FROM lp_native_validation").noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    @Test
    public void testPartitionPredicateUsesTimestampOnlyBindingScope() throws Exception {
        assertMemoryLeak(() -> {
            for (int precision = 0; precision < 2; precision++) {
                final String table = "lp_partition_" + precision;
                final String type = precision == 0 ? "TIMESTAMP" : "TIMESTAMP_NS";
                execute("CREATE TABLE " + table + " (id INT,ts " + type + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("INSERT INTO " + table + " VALUES (1,'2020-01-01'),(2,'2020-01-02'),(3,'2020-01-03')");
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    execute(compiler, "ALTER TABLE " + table + " DROP PARTITION WHERE ts < '2020-01-03' AND length('abc')=3");
                }
                assertQuery("SELECT id FROM " + table).expectSize().returns("id\n3\n");
            }
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertError(compiler, "ALTER TABLE lp_partition_0 DROP PARTITION WHERE id=3", 48, "Invalid column: id");
                assertError(compiler, "ALTER TABLE lp_partition_0 DROP PARTITION WHERE ts", 48, "boolean expression expected");
                execute(compiler, "ALTER TABLE lp_partition_0 DROP PARTITION WHERE ts='2020-01-03'");
            }
            assertQuery("SELECT id FROM lp_partition_0").expectSize().returns("id\n");
        });
    }

    @Test
    public void testColumnFreeInsertFunctionUsesLogicalBinding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_bound_values (value DOUBLE)");
            bindVariableService.setDouble(0, 4);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                execute(compiler, "INSERT INTO lp_bound_values VALUES (greatest($1, 2.0))");
                execute(compiler, "INSERT INTO lp_bound_values VALUES (abs(-4.0))");
            }
            assertQuery("SELECT * FROM lp_bound_values").expectSize().returns("value\n4.0\n4.0\n");
        });
    }

    private void assertError(SqlCompilerImpl compiler, String sql, int position, String message) {
        try {
            final CompiledQuery query = compiler.compile(sql, sqlExecutionContext);
            query.closeAllButSelect();
            Assert.fail("expected validation failure");
        } catch (SqlException e) {
            Assert.assertEquals(position, e.getPosition());
            TestUtils.assertEquals(message, e.getFlyweightMessage());
        }
    }

    private void insert(InsertOperation operation, long expectedRows) throws SqlException {
        try (InsertMethod method = operation.createMethod(sqlExecutionContext)) {
            Assert.assertEquals(expectedRows, method.execute(sqlExecutionContext));
            method.commit();
        }
    }
}
