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
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DeclareMacroTest extends AbstractCairoTest {
    @Test
    public void testBindVariablesSurviveCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setInt(0, 10);
            bindVariableService.setLong(1, 1);
            bindVariableService.setLong(2, 3);
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("""
                            DECLARE @delta := $1, @lo := $2, @hi := $3
                            SELECT id+@delta AS value FROM lp_declare ORDER BY id LIMIT @lo,@hi
                            """, sqlExecutionContext).getRecordCursorFactory();
                    Assert.assertNotNull(compiler.getPlanForTesting());
                    assertResult(retained, "value\n12\n13\n");
                    try (RecordCursorFactory other = compiler.compile(
                            "DECLARE @value := 9 SELECT @value AS value", sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        assertResult(other, "value\n9\n");
                    }
                    compiler.clear();
                }
                bindVariableService.setInt(0, 20);
                bindVariableService.setLong(1, 0);
                bindVariableService.setLong(2, 2);
                assertResult(retained, "value\n21\n22\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testParserErrorsAndUnsupportedExpressionRecover() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertError(compiler, "DECLARE @values := (1,2,3) SELECT id FROM lp_declare WHERE id IN @values", 27, "unexpected token [@values] - unexpected bind expression - bracket lists are not supported");
                assertError(compiler, "DECLARE @value := 2 SELECT @missing FROM lp_declare", 27, "tried to use undeclared variable `@missing`");
                assertError(compiler, "DECLARE @value = 2 SELECT @value", 15, "unexpected token [=] - expected variable assignment operator `:=`");
                assertError(compiler, "DECLARE @member := (id IN (1,2,3)) SELECT @member FROM lp_declare", 23, "too few arguments for 'in' [found=1,expected=2]");
                try (RecordCursorFactory ignored = compiler.compile(
                        "DECLARE @value := lp_missing_fn(i) SELECT @value FROM lp_declare", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    Assert.fail("expected unknown declared function");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "unknown function name");
                }
                try (RecordCursorFactory factory = compiler.compile(
                        "DECLARE @value := abs(-7) SELECT @value AS value", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertResult(factory, "value\n7\n");
                }
            }
        });
    }

    @Test
    public void testRepeatedQueryOccurrencesKeepOriginalGroupingShape() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String distinctQuery = "SELECT DISTINCT sum(i*2) AS total FROM lp_declare GROUP BY k";
            assertQueryRows(
                    "DECLARE @q := (" + distinctQuery + ") SELECT * FROM (SELECT * FROM @q UNION ALL SELECT * FROM @q) ORDER BY total",
                    "total\nnull\nnull\n6\n6\n"
            );
            final String orderedQuery = "SELECT k,sum(i+2) AS total FROM lp_declare GROUP BY k ORDER BY sum(i*2),k LIMIT 3";
            assertQueryRows(
                    "DECLARE @q := (" + orderedQuery + ") SELECT * FROM (SELECT * FROM @q UNION ALL SELECT * FROM @q) ORDER BY k,total",
                    "k\ttotal\n1\t7\n1\t7\n3\tnull\n3\tnull\n4\tnull\n4\tnull\n"
            );
        });
    }

    @Test
    public void testRepeatedScalarMacrosOwnIndependentFunctions() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    """
                            DECLARE @member := id, @value := (i+1)
                            SELECT id,@member IN (1,2,3) AS a,@member IN (1,2,3) AS b,@value AS x,@value AS y
                            FROM lp_declare WHERE @member IN (1,2,3) AND (@member IN (1,2,3) AND true) ORDER BY id
                            """,
                    """
                            id	a	b	x	y
                            1	true	true	2	2
                            2	true	true	3	3
                            3	true	true	4	4
                            """
            );
            assertQueryRows(
                    """
                            DECLARE @predicate := (active AND true), @value := CASE WHEN @predicate THEN i ELSE 7 END
                            SELECT @value AS a,@value AS b FROM lp_declare WHERE @predicate OR false ORDER BY a,b
                            """,
                    """
                            a	b
                            null	null
                            1	1
                            3	3
                            """
            );
        });
    }

    @Test
    public void testScalarMacrosAcrossClauses() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    """
                            DECLARE @table := lp_declare, @column := id, @delta := 2,
                                    @adjusted := (@column+@delta), @predicate := (active AND true)
                            SELECT @adjusted AS value FROM @table WHERE @predicate ORDER BY @adjusted LIMIT 2
                            """,
                    """
                            value
                            3
                            5
                            """
            );
            assertQueryRows(
                    """
                            DECLARE @key := k, @total := sum(i+2), @lo := 0, @hi := 3
                            SELECT @key,@total AS total FROM lp_declare GROUP BY @key ORDER BY @key LIMIT @lo,@hi
                            """,
                    """
                            k	total
                            1	7
                            2	5
                            3	null
                            """
            );
            assertQueryRows(
                    """
                            DECLARE @unused := greatest(i), @value := 3
                            WITH q AS (SELECT id+@value AS value FROM lp_declare WHERE id=1)
                            SELECT value FROM q
                            """,
                    """
                            value
                            4
                            """
            );
        });
    }

    @Test
    public void testSingleQueryMacrosAndNestedDeclarationScopes() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    """
                            DECLARE @delta := 2, @query := (
                                DECLARE @delta := 7 SELECT id+@delta AS value FROM lp_declare WHERE active
                            )
                            SELECT value+@delta AS result FROM @query ORDER BY result
                            """,
                    """
                            result
                            10
                            12
                            13
                            """
            );
            assertQueryRows(
                    """
                            DECLARE @value := 2
                            SELECT q.inner_value,@value AS outer_value FROM (
                                DECLARE @value := 7 SELECT @value AS inner_value FROM lp_declare WHERE id=1
                            ) q
                            """,
                    """
                            inner_value	outer_value
                            7	2
                            """
            );
            assertQueryRows(
                    """
                            DECLARE OVERRIDABLE @value := 5
                            SELECT @value AS value FROM lp_declare WHERE id=1
                            """,
                    """
                            value
                            5
                            """
            );
        });
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void assertError(SqlCompilerImpl compiler, String sql, int position, String message) throws Exception {
        try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.fail("expected parser error");
        } catch (SqlException e) {
            Assert.assertEquals(position, e.getPosition());
            TestUtils.assertEquals(message, e.getFlyweightMessage());
        }
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_declare (id INT,k INT,i INT,active BOOLEAN)");
        execute("INSERT INTO lp_declare VALUES (1,1,1,true),(2,1,2,false),(3,2,3,true),(4,3,null,true),(5,4,null,false)");
    }

    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
