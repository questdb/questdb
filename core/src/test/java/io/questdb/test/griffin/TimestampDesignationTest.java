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

import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.jit.JitUtil;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TimestampDesignationTest extends AbstractCairoTest {
    @Test
    public void testDeclareTimestampOnUnorderedTable() throws Exception {
        assertMemoryLeak(() -> {
            createUnorderedTable();
            assertQueryRows("SELECT * FROM lp_timestamp TIMESTAMP(ts)", """
                    id	ts	other
                    1	2020-01-01T00:00:00.000000Z	2020-01-03T00:00:00.000000000Z
                    2	2020-01-02T00:00:00.000000Z	2020-01-02T00:00:00.000000000Z
                    """);
            assertQueryRows("SELECT * FROM lp_timestamp TIMESTAMP(other)", """
                    id	ts	other
                    1	2020-01-01T00:00:00.000000Z	2020-01-03T00:00:00.000000000Z
                    2	2020-01-02T00:00:00.000000Z	2020-01-02T00:00:00.000000000Z
                    """);
            assertQueryRows("SELECT id FROM lp_timestamp TIMESTAMP(ts)", """
                    id
                    1
                    2
                    """);
            assertQueryRows("SELECT ts AS a,ts AS b,id FROM lp_timestamp TIMESTAMP(ts)", """
                    a	b	id
                    2020-01-01T00:00:00.000000Z	2020-01-01T00:00:00.000000Z	1
                    2020-01-02T00:00:00.000000Z	2020-01-02T00:00:00.000000Z	2
                    """);
            assertQueryRows(
                    "SELECT id,other FROM lp_timestamp TIMESTAMP(other) ORDER BY other DESC",
                    """
                            id	other
                            2	2020-01-02T00:00:00.000000000Z
                            1	2020-01-03T00:00:00.000000000Z
                            """
            );
        });
    }

    @Test
    public void testDerivedTimestampPreservesOrderAndLimit() throws Exception {
        assertMemoryLeak(() -> {
            createUnorderedTable();
            assertQueryRows(
                    "SELECT * FROM (SELECT * FROM lp_timestamp ORDER BY ts DESC) TIMESTAMP(ts)",
                    """
                            id	ts	other
                            2	2020-01-02T00:00:00.000000Z	2020-01-02T00:00:00.000000000Z
                            1	2020-01-01T00:00:00.000000Z	2020-01-03T00:00:00.000000000Z
                            """
            );
            assertQueryRows(
                    "SELECT * FROM (SELECT * FROM lp_timestamp ORDER BY ts) TIMESTAMP(other)",
                    """
                            id	ts	other
                            1	2020-01-01T00:00:00.000000Z	2020-01-03T00:00:00.000000000Z
                            2	2020-01-02T00:00:00.000000Z	2020-01-02T00:00:00.000000000Z
                            """
            );
            assertQueryRows(
                    "SELECT id FROM (SELECT * FROM lp_timestamp ORDER BY ts DESC LIMIT 1) TIMESTAMP(other)",
                    """
                            id
                            2
                            """
            );
            assertQueryRows(
                    "SELECT id,other FROM (SELECT * FROM lp_timestamp ORDER BY ts DESC) q TIMESTAMP(other) WHERE id > 0",
                    """
                            id	other
                            2	2020-01-02T00:00:00.000000000Z
                            1	2020-01-03T00:00:00.000000000Z
                            """
            );
            assertQueryRows(
                    "SELECT * FROM (SELECT ts AS t,id FROM lp_timestamp ORDER BY ts) q TIMESTAMP(t)",
                    """
                            t	id
                            2020-01-01T00:00:00.000000Z	1
                            2020-01-02T00:00:00.000000Z	2
                            """
            );
            assertQueryRows(
                    "SELECT * FROM (SELECT * FROM (SELECT * FROM lp_timestamp ORDER BY ts DESC) TIMESTAMP(ts)) TIMESTAMP(other)",
                    """
                            id	ts	other
                            2	2020-01-02T00:00:00.000000Z	2020-01-02T00:00:00.000000000Z
                            1	2020-01-01T00:00:00.000000Z	2020-01-03T00:00:00.000000000Z
                            """
            );
        });
    }

    @Test
    public void testJitWidensTimestampConstantsWithoutChangingRecordPrecision() throws Exception {
        assertMemoryLeak(() -> {
            createUnorderedTable();
            execute("INSERT INTO lp_timestamp VALUES (3,null,null),(4,'2020-01-02','2020-01-02T00:00:00.000000001Z')");
            final int oldMode = sqlExecutionContext.getJitMode();
            try {
                final ObjList<String> operators = new ObjList<>("=", "!=", "<", "<=", ">", ">=");
                for (int i = 0; i < operators.size() * 2; i++) {
                    final String sql = "SELECT id FROM lp_timestamp WHERE " + (i < operators.size()
                            ? "other" + operators.getQuick(i) + "'2020-01-02'"
                            : "'2020-01-02'" + operators.getQuick(i - operators.size()) + "other");
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    try (RecordCursorFactory expected = compile(sql)) {
                        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                        try (RecordCursorFactory actual = compile(sql)) {
                            Assert.assertEquals(sql, JitUtil.isJitSupported(), actual.usesCompiledFilter());
                            try (RecordCursor left = expected.getCursor(sqlExecutionContext); RecordCursor right = actual.getCursor(sqlExecutionContext)) {
                                TestUtils.assertEquals(left, expected.getMetadata(), right, actual.getMetadata(), false);
                            }
                        }
                    }
                }
                execute("CREATE TABLE lp_timestamp_empty(id INT,other TIMESTAMP_NS)");
                try (RecordCursorFactory empty = compile("SELECT id FROM lp_timestamp_empty WHERE other > '1600-01-01'");
                     RecordCursor cursor = empty.getCursor(sqlExecutionContext)) {
                    Assert.assertFalse(empty.usesCompiledFilter());
                    Assert.assertFalse(cursor.hasNext());
                }
            } finally {
                sqlExecutionContext.setJitMode(oldMode);
            }
        });
    }

    @Test
    public void testNativeIntervalsUseNativeTimestampAfterDesignation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_timestamp(id INT,ts TIMESTAMP,other TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            insertRows();
            assertQueryRows(
                    "SELECT * FROM lp_timestamp TIMESTAMP(other) WHERE ts >= '2020-01-02'",
                    """
                            id	ts	other
                            2	2020-01-02T00:00:00.000000Z	2020-01-02T00:00:00.000000000Z
                            """
            );
            assertQueryRows(
                    "SELECT * FROM lp_timestamp TIMESTAMP(other) WHERE other >= '2020-01-03'",
                    """
                            id	ts	other
                            1	2020-01-01T00:00:00.000000Z	2020-01-03T00:00:00.000000000Z
                            """
            );
            assertQueryRows(
                    "SELECT id,other FROM lp_timestamp TIMESTAMP(other) WHERE ts < '2020-01-01T00:00:00.000000001Z'",
                    """
                            id	other
                            1	2020-01-03T00:00:00.000000000Z
                            """
            );
            assertQueryRows(
                    "SELECT id,other FROM lp_timestamp TIMESTAMP(other) WHERE other < '2020-01-02T00:00:00.000000001Z'",
                    """
                            id	other
                            2	2020-01-02T00:00:00.000000000Z
                            """
            );
            assertQueryRows(
                    "SELECT id FROM lp_timestamp TIMESTAMP(other) WHERE ts >= '2020-01-02' AND other < '2020-01-03'",
                    """
                            id
                            2
                            """
            );
            assertQueryRows(
                    "SELECT ts FROM (SELECT ts,other FROM lp_timestamp) TIMESTAMP(other)",
                    """
                            ts
                            2020-01-01T00:00:00.000000Z
                            2020-01-02T00:00:00.000000Z
                            """
            );
            assertQueryRows(
                    "SELECT ts FROM (SELECT ts,other FROM lp_timestamp ORDER BY ts) TIMESTAMP(other)",
                    """
                            ts
                            2020-01-01T00:00:00.000000Z
                            2020-01-02T00:00:00.000000Z
                            """
            );
            assertQueryRows(
                    "SELECT ts FROM (SELECT ts,other FROM lp_timestamp) TIMESTAMP(other) WHERE ts >= '2020-01-02'",
                    """
                            ts
                            2020-01-02T00:00:00.000000Z
                            """
            );
        });
    }

    @Test
    public void testPostingIntervalsUseNativeTimestampAfterDesignation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_timestamp(sym SYMBOL INDEX TYPE POSTING,ts TIMESTAMP,other TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO lp_timestamp VALUES ('a','2020-01-01','2020-01-03'),('b','2020-01-02','2020-01-02'),('a','2020-01-03','2020-01-01')");
            assertQueryRows(
                    "SELECT DISTINCT sym FROM lp_timestamp TIMESTAMP(other) WHERE other >= '2020-01-03' ORDER BY sym",
                    """
                            sym
                            a
                            """
            );
            assertQueryRows(
                    "SELECT DISTINCT sym FROM lp_timestamp TIMESTAMP(other) WHERE ts >= '2020-01-02' ORDER BY sym",
                    """
                            sym
                            a
                            b
                            """
            );
            assertQueryRows(
                    "SELECT DISTINCT sym FROM lp_timestamp TIMESTAMP(other) WHERE ts >= '2020-01-02' AND other >= '2020-01-02' ORDER BY sym",
                    """
                            sym
                            b
                            """
            );
        });
    }

    @Test
    public void testTimestampDeclarationBelowGrouping() throws Exception {
        assertMemoryLeak(() -> {
            createUnorderedTable();
            assertQueryRows(
                    "SELECT id,sum(id) total FROM (SELECT * FROM lp_timestamp ORDER BY ts DESC) TIMESTAMP(ts) GROUP BY id ORDER BY id",
                    """
                            id	total
                            1	1
                            2	2
                            """
            );
            assertQueryRows(
                    "SELECT count(*) FROM (SELECT * FROM lp_timestamp ORDER BY ts DESC) TIMESTAMP(ts)",
                    """
                            count
                            2
                            """
            );
            assertQueryRows(
                    "SELECT count(*) FROM (SELECT * FROM lp_timestamp ORDER BY ts DESC LIMIT 1) TIMESTAMP(other)",
                    """
                            count
                            1
                            """
            );
        });
    }

    @Test
    public void testTimestampErrorsAndCompilerReuse() throws Exception {
        assertMemoryLeak(() -> {
            createUnorderedTable();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertFailure(compiler, "SELECT * FROM lp_timestamp TIMESTAMP(id)", 37, "not a TIMESTAMP");
                assertFailure(compiler, "SELECT * FROM lp_timestamp TIMESTAMP(missing)", 37, "Invalid column: missing");
                assertFailure(compiler, "SELECT * FROM (SELECT id,ts FROM lp_timestamp) TIMESTAMP(id)", 57, "not a TIMESTAMP");
                assertFailure(compiler, "SELECT * FROM (SELECT id FROM lp_timestamp) TIMESTAMP(ts)", 54, "Invalid column: ts");
                assertFailure(compiler, "SELECT * FROM long_sequence(2) TIMESTAMP(x)", 41, "not a TIMESTAMP");
                try (RecordCursorFactory factory = compiler.compile("SELECT * FROM lp_timestamp TIMESTAMP(ts)", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertEquals(1, factory.getMetadata().getTimestampIndex());
                }
            }
        });
    }

    private void assertFailure(SqlCompilerImpl compiler, String sql, int position, String message) throws SqlException {
        try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.fail(sql);
        } catch (SqlException e) {
            Assert.assertEquals(sql, position, e.getPosition());
            TestUtils.assertEquals(message, e.getFlyweightMessage());
        }
    }

    private RecordCursorFactory compile(String sql) throws SqlException {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            return compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
        }
    }

    private void createUnorderedTable() throws SqlException {
        execute("CREATE TABLE lp_timestamp(id INT,ts TIMESTAMP,other TIMESTAMP_NS)");
        insertRows();
    }

    private void insertRows() throws SqlException {
        execute("INSERT INTO lp_timestamp VALUES (1,'2020-01-01','2020-01-03'),(2,'2020-01-02','2020-01-02')");
    }

    private void assertQueryRows(String sql, String expected) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().sizeMayVary().returns(expected);
        }
    }
}
