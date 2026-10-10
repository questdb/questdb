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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitRecordCursorFactory;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LimitPlanningTest extends AbstractCairoTest {
    @Test
    public void testConstantAndRuntimeExpressions() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setLong(0, 2);
            bindVariableService.setLong("end", 4);
            {
                final String limit = "1+2";
                assertRowsOnly("SELECT id FROM lp_limit ORDER BY id LIMIT " + limit, """
                        id
                        1
                        2
                        3
                        """);
            }
            {
                final String limit = "-$1";
                assertRowsOnly("SELECT id FROM lp_limit ORDER BY id LIMIT " + limit, """
                        id
                        3
                        4
                        """);
            }
            {
                final String limit = "$1,:end";
                assertRowsOnly("SELECT id FROM lp_limit ORDER BY id LIMIT " + limit, """
                        id
                        3
                        4
                        """);
            }
            {
                final String limit = "0,1+$1";
                assertRowsOnly("SELECT id FROM lp_limit ORDER BY id LIMIT " + limit, """
                        id
                        1
                        2
                        3
                        """);
            }
            {
                final String limit = "nullif(1,1)";
                assertRowsOnly("SELECT id FROM lp_limit ORDER BY id LIMIT " + limit, """
                        id
                        1
                        2
                        3
                        4
                        """);
            }
            {
                final String limit = "-3,-1";
                assertRowsOnly("SELECT id FROM lp_limit ORDER BY id LIMIT " + limit, """
                        id
                        2
                        3
                        """);
            }
        });
    }

    @Test
    public void testFactorySurvivesCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setLong(0, 2);
            RecordCursorFactory factory = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    factory = compiler.compile("SELECT id FROM lp_limit ORDER BY id LIMIT $1+1", sqlExecutionContext).getRecordCursorFactory();
                    assertRowsOnly(factory, "id\n1\n2\n3\n");
                    try (RecordCursorFactory other = compiler.compile("SELECT id FROM lp_limit LIMIT 1", sqlExecutionContext).getRecordCursorFactory()) {
                        assertRowsOnly(other, "id\n1\n");
                    }
                    compiler.clear();
                }
                bindVariableService.setLong(0, 1);
                assertRowsOnly(factory, "id\n1\n2\n");
            } finally {
                Misc.free(factory);
            }
        });
    }

    @Test
    public void testLimitOwnsBothExpressionsEvenWhenBaseCloseFails() {
        final int[] closes = {0, 0, 0};
        final RuntimeException baseFailure = new RuntimeException("base close");
        final RuntimeException loFailure = new RuntimeException("low close");
        final RecordCursorFactory base = new AbstractRecordCursorFactory(new GenericRecordMetadata()) {
            @Override
            public RecordCursor getCursor(SqlExecutionContext executionContext) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean recordCursorSupportsRandomAccess() {
                return false;
            }

            @Override
            protected void _close() {
                closes[0]++;
                throw baseFailure;
            }
        };
        final Function lo = new LongFunction() {
            @Override
            public void close() {
                closes[1]++;
                throw loFailure;
            }

            @Override
            public long getLong(Record record) {
                return 1;
            }
        };
        final Function hi = new LongFunction() {
            @Override
            public void close() {
                closes[2]++;
            }

            @Override
            public long getLong(Record record) {
                return 2;
            }
        };
        final LimitRecordCursorFactory factory = new LimitRecordCursorFactory(base, lo, hi, 0);
        try {
            factory.close();
            Assert.fail("expected close failure");
        } catch (RuntimeException e) {
            Assert.assertSame(baseFailure, e);
            Assert.assertArrayEquals(new Throwable[]{loFailure}, e.getSuppressed());
        }
        Assert.assertArrayEquals(new int[]{1, 1, 1}, closes);
    }

    @Test
    public void testNestedLimitStillBlocksFilterAndReverseScanAdvice() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setLong(0, 3);
            assertRowsOnly(
                    "SELECT id FROM (SELECT id,ts FROM lp_limit LIMIT $1) WHERE id>1 ORDER BY ts DESC",
                    """
                            id
                            3
                            2
                            """
            );
            assertRowsOnly(
                    "SELECT id FROM lp_limit UNION ALL SELECT id FROM lp_limit ORDER BY id LIMIT $1+1",
                    """
                            id
                            1
                            1
                            2
                            2
                            """
            );
        });
    }

    @Test
    public void testRepeatedCursorUsesCurrentSignedBounds() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setLong(0, 1);
            bindVariableService.setLong(1, 3);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id FROM lp_limit ORDER BY id LIMIT $1,$2", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "id\n2\n3\n");
                    bindVariableService.setLong(0, -3);
                    bindVariableService.setLong(1, -1);
                    assertRowsOnly(factory, "id\n2\n3\n");
                    bindVariableService.setLong(0, 0);
                    bindVariableService.setLong(1, 4);
                    assertRowsOnly(factory, "id\n1\n2\n3\n4\n");
                }
            }
        });
    }

    @Test
    public void testUndefinedBoundsBecomeLong() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.clear();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id FROM lp_limit LIMIT $1,$2", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    Assert.assertEquals(ColumnType.LONG, bindVariableService.getFunction(0).getType());
                    Assert.assertEquals(ColumnType.LONG, bindVariableService.getFunction(1).getType());
                    bindVariableService.setLong(0, 1);
                    bindVariableService.setLong(1, 2);
                    assertRowsOnly(factory, "id\n2\n");
                }
            }
        });
    }

    @Test
    public void testValidationAndCompilerRecovers() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertLimitFailsOnCompilerReuse(compiler, "1.5", 30, "invalid type: DOUBLE");
                assertLimitFailsOnCompilerReuse(compiler, "true", 30, "LIMIT expressions must be convertible to INT");
                assertLimitFailsOnCompilerReuse(compiler, "id", 30, "Invalid column: id");
                assertLimitFailsOnCompilerReuse(compiler, "1+2,1.5", 34, "invalid type: DOUBLE");
                try (RecordCursorFactory factory = compiler.compile("SELECT id FROM lp_limit LIMIT 1", sqlExecutionContext).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "id\n1\n");
                }
            }
        });
    }

    private void assertLimitFailsOnCompilerReuse(SqlCompilerImpl compiler, String limit, int position, String message) throws Exception {
        final String sql = "SELECT id FROM lp_limit LIMIT " + limit;
        for (int reuse = 0; reuse < 2; reuse++) {
            try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.fail("expected invalid LIMIT: " + sql);
            } catch (SqlException e) {
                Assert.assertEquals(sql, position, e.getPosition());
                TestUtils.assertEquals(message, e.getFlyweightMessage());
            }
        }
    }

    private void createRows() throws SqlException {
        execute("CREATE TABLE lp_limit (id INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_limit VALUES (1,0),(2,1),(3,2),(4,3)");
    }
}
