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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CteScopeTest extends AbstractCairoTest {
    @Test
    public void testFactorySurvivesCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("""
                            WITH q AS (SELECT id,value+1 AS value FROM lp_with WHERE active)
                            SELECT * FROM (SELECT * FROM q UNION ALL SELECT * FROM q) ORDER BY id,value
                            """, sqlExecutionContext).getRecordCursorFactory();
                    Assert.assertNotNull(compiler.getPlanForTesting());
                    assertRowsOnly(retained, "id\tvalue\n1\t11\n1\t11\n3\t31\n3\t31\n4\t41\n4\t41\n");
                    try (RecordCursorFactory other = compiler.compile(
                            "WITH q AS (SELECT id FROM lp_with WHERE id=2) SELECT * FROM q", sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        assertRowsOnly(other, "id\n2\n");
                    }
                    compiler.clear();
                }
                assertRowsOnly(retained, "id\tvalue\n1\t11\n1\t11\n3\t31\n3\t31\n4\t41\n4\t41\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testInsertSelectAndExplainOverCte() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            execute("CREATE TABLE lp_with_insert (value LONG,id INT)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                execute(compiler, "WITH q AS (SELECT id,value FROM lp_with WHERE active) "
                        + "INSERT INTO lp_with_insert (id,value) SELECT * FROM q");
                Assert.assertNotNull(compiler.getPlanForTesting());
                assertExplain(compiler, "EXPLAIN WITH q AS (SELECT id FROM lp_with) SELECT * FROM q", "lp_with");
                assertExplain(compiler, "EXPLAIN WITH q AS (SELECT id,value FROM lp_with) "
                        + "INSERT INTO lp_with_insert (id,value) SELECT * FROM q", "Insert into table: lp_with_insert");
            }
            assertQuery("SELECT id,value FROM lp_with_insert ORDER BY id").expectSize()
                    .returns("id\tvalue\n1\t10\n3\t30\n4\t40\n");
        });
    }

    @Test
    public void testNestedScopesAndTableNameShadowing() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertWith("""
                    WITH lp_with AS (SELECT id,value FROM lp_with WHERE id>1),
                         later AS (SELECT id FROM lp_with WHERE id<4)
                    SELECT id FROM (SELECT id FROM later UNION ALL SELECT id FROM lp_with) ORDER BY id
                    """, "id\n2\n2\n3\n3\n4\n");
            assertWith("""
                    WITH outerq AS (SELECT id,value FROM lp_with WHERE active)
                    SELECT id FROM (
                        WITH innerq AS (SELECT id FROM outerq WHERE value>10)
                        SELECT id FROM innerq UNION ALL SELECT id FROM outerq
                    ) ORDER BY id
                    """, "id\n1\n3\n3\n4\n4\n");
        });
    }

    @Test
    public void testRepeatedReferencesKeepIndependentProjectionAndLimit() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertWith("""
                    WITH q AS (SELECT id,value,active FROM lp_with ORDER BY id DESC LIMIT 3)
                    SELECT result FROM (
                        SELECT id AS result FROM q WHERE active
                        UNION ALL SELECT value AS result FROM q WHERE id<4
                    ) ORDER BY result
                    """, "result\n3\n4\n20\n30\n");
            assertWith("""
                    WITH q AS (SELECT active,sum(value+1) AS total FROM lp_with GROUP BY active)
                    SELECT total FROM (SELECT total FROM q UNION ALL SELECT total FROM q) ORDER BY total
                    """, "total\n21\n21\n83\n83\n");
        });
    }

    @Test
    public void testUnusedUnsupportedDefinitionIsNotBound() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertWith("""
                    WITH unused AS (SELECT greatest(missing) FROM no_such_table),
                         q AS (SELECT id FROM lp_with WHERE id=2)
                    SELECT id FROM q
                    """, "id\n2\n");
        });
    }

    @Test
    public void testUsedUnsupportedDefinitionFailsAndCompilerRecovers() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory ignored = compiler.compile(
                        "WITH q AS (SELECT lp_missing_fn(id) AS value FROM lp_with) SELECT * FROM q", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    Assert.fail("expected unknown function");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "unknown function name");
                }
                try (RecordCursorFactory factory = compiler.compile(
                        "WITH q AS (SELECT abs(id) AS value FROM lp_with WHERE id=1) SELECT * FROM q", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    Assert.assertNotNull(compiler.getPlanForTesting());
                    assertRowsOnly(factory, "value\n1\n");
                }
            }
        });
    }

    @Test
    public void testWithSubQueryInExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SYMBOL, y INT)");
            execute("INSERT INTO t VALUES ('1', 10), ('2', 20)");
            execute("CREATE TABLE yt (x SYMBOL)");
            execute("INSERT INTO yt VALUES ('1')");
            assertQuery("""
                    WITH yt AS (SELECT '2'::SYMBOL x)
                    SELECT * FROM t WHERE x IN (WITH b AS (SELECT x FROM yt) SELECT x FROM b)
                    """).noLeakCheck().returns("x\ty\n2\t20\n");
            assertQuery("SELECT * FROM t WHERE x IN (WITH b AS (SELECT x FROM yt) SELECT x FROM b)")
                    .noLeakCheck()
                    .returns("x\ty\n1\t10\n");
            assertQuery("SELECT * FROM t WHERE x NOT IN (WITH b AS (SELECT x FROM yt) SELECT x FROM b)")
                    .noLeakCheck()
                    .returns("x\ty\n2\t20\n");
            assertQuery("""
                    WITH b AS (SELECT '2'::SYMBOL x)
                    SELECT * FROM t WHERE x IN (WITH b AS (SELECT x FROM yt) SELECT x FROM b)
                    """).noLeakCheck().returns("x\ty\n1\t10\n");
            assertQuery("""
                    WITH a AS (SELECT '1'::SYMBOL x)
                    SELECT * FROM t WHERE x IN (DECLARE @v := 1 WITH b AS (SELECT x FROM a) SELECT x FROM b)
                    """).noLeakCheck().returns("x\ty\n1\t10\n");
            assertQuery("SELECT * FROM t WHERE y > (WITH b AS (SELECT min(y) m FROM t) SELECT m FROM b)")
                    .noLeakCheck()
                    .returns("x\ty\n2\t20\n");
        });
    }

    private void assertExplain(SqlCompilerImpl compiler, String sql, String expected) throws Exception {
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
            TestUtils.assertContains(plan, "lp_with");
        }
    }

    private void assertWith(String sql, String expected) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            Assert.assertNotNull(compiler.getPlanForTesting());
            assertRowsOnly(factory, expected);
        }
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_with (id INT,value INT,active BOOLEAN)");
        execute("INSERT INTO lp_with VALUES (1,10,true),(2,20,false),(3,30,true),(4,40,true)");
    }
}
