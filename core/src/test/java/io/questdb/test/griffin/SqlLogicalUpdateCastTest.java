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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalUpdateCastTest extends AbstractCairoTest {
    @Test
    public void testGetterWideningAndBindVariableTypes() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.clear();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertUpdate(compiler, "UPDATE uc_target SET wide=id,total=id+1", 2);
                assertRows(compiler, "SELECT id,wide,total FROM uc_target ORDER BY id",
                        "id\twide\ttotal\n1\t1\t2.0\n2\t2\t3.0\n");
                assertUpdate(compiler, "UPDATE uc_target SET total=id+1,wide=id", 2);
                assertRows(compiler, "SELECT id,wide,total FROM uc_target ORDER BY id",
                        "id\twide\ttotal\n1\t1\t2.0\n2\t2\t3.0\n");
                try (UpdateOperation ignored = compiler.compile(
                        "UPDATE uc_target SET wide=$1,total=$2", sqlExecutionContext).getUpdateOperation()) {
                    Assert.assertEquals(ColumnType.LONG, bindVariableService.getFunction(0).getType());
                    Assert.assertEquals(ColumnType.DOUBLE, bindVariableService.getFunction(1).getType());
                }
                bindVariableService.setLong(0, 21);
                bindVariableService.setDouble(1, 2.5);
                assertUpdate(compiler, "UPDATE uc_target SET wide=$1,total=$2", 2);
                assertRows(compiler, "SELECT wide,total FROM uc_target", "wide\ttotal\n21\t2.5\n21\t2.5\n");
                bindVariableService.clear();
                bindVariableService.setInt(0, 31);
                assertUpdate(compiler, "UPDATE uc_target SET wide=$1 WHERE id=1", 1);
                Assert.assertEquals(ColumnType.INT, bindVariableService.getFunction(0).getType());
                assertRows(compiler, "SELECT id,wide FROM uc_target ORDER BY id", "id\twide\n1\t31\n2\t21\n");
            }
            execute("DROP TABLE uc_target");
        });
    }

    @Test
    public void testTextTimestampPrecisionsAndNullSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (String value : new String[]{"'1969-12-31T23:59:59.999999999Z'", "'1969-12-31T23:59:59.999999999Z'::varchar"}) {
                    assertUpdate(compiler, "UPDATE uc_target SET stamp=" + value + ",nano=" + value + ",code=NULL", 2);
                    assertRows(compiler, "SELECT id,stamp::long us,nano::long ns,code FROM uc_target ORDER BY id",
                            "id\tus\tns\tcode\n1\t0\t-1\t\n2\t0\t-1\t\n");
                }
                assertUpdate(compiler, "UPDATE uc_target SET code='XYZ',stamp=NULL,nano=NULL", 2);
                assertRows(compiler, "SELECT id,code,stamp,nano FROM uc_target ORDER BY id",
                        "id\tcode\tstamp\tnano\n1\tXYZ\t\t\n2\tXYZ\t\t\n");
                assertUpdate(compiler, "UPDATE uc_target SET identifier='not-a-uuid'", 2);
                assertRows(compiler, "SELECT id FROM uc_target WHERE identifier IS NULL ORDER BY id", "id\n1\n2\n");
            }
            execute("DROP TABLE uc_target");
        });
    }

    @Test
    public void testJoinConversionsRebuildArrayLayoutsAndKeepSymbols() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            execute("CREATE TABLE uc_source(unused INT,id INT,label SYMBOL,stamp STRING,uuid_text STRING,a DOUBLE[])");
            execute("INSERT INTO uc_source VALUES (9,1,'ONE','1969-12-31T23:59:59.999999999Z',"
                    + "'00000000-0000-0000-0000-000000000001',ARRAY[1.0,2.0,3.0]),"
                    + "(8,2,'TWO','1970-01-01T00:00:00.000001999Z','00000000-0000-0000-0000-000000000002',ARRAY[4.0,5.0])");
            execute("ALTER TABLE uc_source DROP COLUMN unused");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertUpdate(compiler, "UPDATE uc_target t SET wide=s.id,total=dim_length(s.a,1),"
                        + "code=s.label,stamp=s.stamp,identifier=s.uuid_text FROM uc_source s WHERE t.id=s.id", 2);
                assertRows(compiler, "SELECT id,wide,total,code,stamp::long us,identifier FROM uc_target ORDER BY id",
                        "id\twide\ttotal\tcode\tus\tidentifier\n"
                                + "1\t1\t3.0\tONE\t0\t00000000-0000-0000-0000-000000000001\n"
                                + "2\t2\t2.0\tTWO\t1\t00000000-0000-0000-0000-000000000002\n");
            }
            execute("DROP TABLE uc_source");
            execute("DROP TABLE uc_target");
        });
    }

    @Test
    public void testImplicitCastAndRuntimeFailuresKeepCompilerReusable() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertUpdateFailsAndCompilerRecovers(compiler, "UPDATE uc_target SET wide=TRUE", 26, "inconvertible types: BOOLEAN -> LONG [from=, to=wide]");
                assertUpdateFailsAndCompilerRecovers(compiler, "UPDATE uc_target SET wide=1.25", 26, "inconvertible types: DOUBLE -> LONG [from=, to=wide]");
                assertUpdateFailsAndCompilerRecovers(compiler, "UPDATE uc_target SET identifier=ARRAY[1.0]", 37, "inconvertible types: DOUBLE[] -> UUID [from=, to=identifier]");
                final CompiledQuery query = compiler.compile("UPDATE uc_target SET wide='bad'", sqlExecutionContext);
                try (UpdateOperation ignored = query.getUpdateOperation(); OperationFuture future = query.execute(null)) {
                    future.await();
                    Assert.fail("runtime numeric cast must fail");
                } catch (ImplicitCastException expected) {
                    TestUtils.assertContains(expected.getMessage(), "bad");
                }
                assertRows(compiler, "SELECT id,wide FROM uc_target ORDER BY id", "id\twide\n1\t0\n2\t0\n");
                assertUpdate(compiler, "UPDATE uc_target SET wide=id", 2);
                assertRows(compiler, "SELECT id,wide FROM uc_target ORDER BY id", "id\twide\n1\t1\n2\t2\n");
            }
        });
    }

    @Test
    public void testPreparedAssignmentsSurviveCompilerClearAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            UpdateOperation retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("UPDATE uc_target SET wide=id,identifier='00000000-0000-0000-0000-000000000009'",
                            sqlExecutionContext).getUpdateOperation();
                    compiler.clear();
                    try (UpdateOperation ignored = compiler.compile("UPDATE uc_target SET total=id+4,code=NULL",
                            sqlExecutionContext).getUpdateOperation()) {
                        compiler.clear();
                    }
                }
                assertFactory(retained.getFactory()).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary()
                        .returns("wide\tidentifier\n1\t00000000-0000-0000-0000-000000000009\n2\t00000000-0000-0000-0000-000000000009\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    private void assertRows(SqlCompilerImpl compiler, String sql, String expected) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
        }
    }

    private void assertUpdateFailsAndCompilerRecovers(SqlCompilerImpl compiler, String sql, int position, String message) throws Exception {
        try (UpdateOperation ignored = compiler.compile(sql, sqlExecutionContext).getUpdateOperation()) {
            Assert.fail(sql);
        } catch (SqlException e) {
            Assert.assertEquals(sql, position, e.getPosition());
            TestUtils.assertEquals(message, e.getFlyweightMessage());
        }
        assertRows(compiler, "SELECT id,wide FROM uc_target ORDER BY id", "id\twide\n1\t0\n2\t0\n");
    }

    private void assertUpdate(SqlCompilerImpl compiler, String sql, long affectedRows) throws Exception {
        final CompiledQuery query = compiler.compile(sql, sqlExecutionContext);
        try (UpdateOperation update = query.getUpdateOperation()) {
            Assert.assertTrue(update.getFactory().supportsUpdateRowId(engine.verifyTableName("uc_target")));
            final RecordMetadata metadata = update.getFactory().getMetadata();
            final OutputSchema output = compiler.getLogicalPlanForTesting().getOutput();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                Assert.assertEquals(metadata.getColumnType(i), output.getColumnType(i));
                Assert.assertEquals(metadata.isSymbolTableStatic(i), output.isSymbolTableStatic(i));
            }
            try (OperationFuture future = query.execute(null)) {
                future.await();
                Assert.assertEquals(affectedRows, future.getAffectedRowsCount());
            }
        }
    }

    private void createRows() throws SqlException {
        execute("CREATE TABLE uc_target(id INT,wide LONG,total DOUBLE,code SYMBOL,stamp TIMESTAMP,nano TIMESTAMP_NS,identifier UUID,ts TIMESTAMP)"
                + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO uc_target VALUES (1,0,0.0,'A',null,null,null,'2020-01-01'),(2,0,0.0,'B',null,null,null,'2020-01-02')");
    }
}
