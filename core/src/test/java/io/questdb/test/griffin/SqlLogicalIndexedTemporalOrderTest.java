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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalIndexedTemporalOrderTest extends AbstractCairoTest {
    @Test
    public void testExplicitMasterOrderRetainsIndexAdvice() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final ObjList<String> joins = new ObjList<>("ASOF", "LT", "SPLICE");
            for (int i = 0; i < joins.size(); i++) {
                final String join = joins.getQuick(i);
                for (int limited = 0; limited < 2; limited++) {
                    final String sql = "SELECT a.id AS master,b.id AS slave FROM "
                            + "(SELECT * FROM lp_index_master WHERE s='A' ORDER BY s"
                            + (limited == 0 ? "" : " LIMIT 2")
                            + ") a TIMESTAMP(ts) " + join + " JOIN lp_index_slave b ON s";
                    final String rows = switch (join) {
                        case "ASOF" -> limited == 0 ? "1\t11\n2\t11\n4\t12\n" : "1\t11\n2\t11\n";
                        case "LT" -> limited == 0 ? "1\tnull\n2\t11\n4\t12\n" : "1\tnull\n2\t11\n";
                        default -> limited == 0 ? "1\t11\n2\t11\n2\t12\n4\t12\n" : "1\t11\n2\t11\n2\t12\n";
                    };
                    assertIndexOrderedRows(sql, "master\tslave\n" + rows);
                }
            }
        });
    }

    @Test
    public void testExplicitOrderRestoresTimestampRequirementOnSuccessAndFailure() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.pushTimestampRequiredFlag(true);
            try {
                for (int limited = 0; limited < 2; limited++) {
                    final String limit = limited == 0 ? "" : " LIMIT 2";
                    try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                        final SqlException error = Assert.assertThrows(SqlException.class,
                                () -> compiler.compile("SELECT id FROM lp_index_master WHERE ts IN 'xyz' LATEST ON ts PARTITION BY s ORDER BY s" + limit,
                                        sqlExecutionContext));
                        TestUtils.assertContains(error.getFlyweightMessage(), "Invalid date");
                        Assert.assertTrue(sqlExecutionContext.isTimestampRequired());
                        try (RecordCursorFactory factory = compiler.compile(
                                "SELECT id FROM lp_index_master WHERE s='A' ORDER BY s" + limit,
                                sqlExecutionContext).getRecordCursorFactory()) {
                            Assert.assertTrue(sqlExecutionContext.isTimestampRequired());
                            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                                    .sizeMayVary().returns(limited == 0 ? "id\n1\n2\n4\n" : "id\n1\n2\n");
                        }
                    }
                }
            } finally {
                sqlExecutionContext.popTimestampRequiredFlag();
            }
        });
    }

    private void assertIndexOrderedRows(String sql, String rows) throws Exception {
        try (RecordCursorFactory factory = compile(sql)) {
            final TextPlanSink plan = new TextPlanSink();
            plan.of(factory, sqlExecutionContext);
            TestUtils.assertContains(plan.getSink(), "Index forward scan");
            Assert.assertFalse(plan.getSink().toString().contains("sort"));
            assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(rows);
        }
    }

    private RecordCursorFactory compile(String sql) throws SqlException {
        final RecordCursorFactory retained;
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
            try {
                try (RecordCursorFactory ignored = compiler.compile("SELECT id FROM lp_index_slave WHERE s='A'",
                        sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertNotNull(ignored);
                }
                compiler.clear();
            } catch (Throwable th) {
                retained.close();
                throw th;
            }
        }
        return retained;
    }

    private void createTables() throws SqlException {
        execute("CREATE TABLE lp_index_master(id INT,s SYMBOL INDEX,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("""
                INSERT INTO lp_index_master VALUES
                (1,'A','2020-01-01T00:00:01'),
                (2,'A','2020-01-01T00:00:02'),
                (3,'B','2020-01-01T00:00:03'),
                (4,'A','2020-01-01T00:00:04')
                """);
        execute("CREATE TABLE lp_index_slave(id INT,s SYMBOL INDEX,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_index_slave VALUES(11,'A','2020-01-01T00:00:01'),(12,'A','2020-01-01T00:00:03')");
    }
}
