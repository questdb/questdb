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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SymbolCaseTest extends AbstractCairoTest {
    @Test
    public void testStaticAndDynamicSymbolKeysAndTextBranches() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<String> expressions = new ObjList<>(
                    "CASE s WHEN 'a' THEN 10 ELSE -1 END",
                    "CASE s WHEN 'a' THEN 10 WHEN 'b' THEN id WHEN null THEN 30 ELSE 40 END",
                    "CASE s WHEN 'a' THEN label WHEN 'b' THEN v WHEN null THEN s ELSE t END",
                    "CASE WHEN active THEN s ELSE t END",
                    "CASE WHEN active THEN s ELSE label END",
                    "CASE WHEN active THEN s ELSE v END",
                    "CASE WHEN active THEN label ELSE v END",
                    "CASE s WHEN null THEN 1 WHEN null THEN 2 ELSE 3 END",
                    "CASE s WHEN 'a' THEN s END"
            );
            {
                final int i = 0;
                final String expression = expressions.getQuick(i);
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM lp_sym_case ORDER BY id",
                        """
                                id	val
                                1	10
                                2	-1
                                3	-1
                                4	-1
                                """
                );
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM (lp_sym_case UNION ALL lp_sym_case) ORDER BY id",
                        """
                                id	val
                                1	10
                                1	10
                                2	-1
                                2	-1
                                3	-1
                                3	-1
                                4	-1
                                4	-1
                                """
                );
            }
            {
                final int i = 1;
                final String expression = expressions.getQuick(i);
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM lp_sym_case ORDER BY id",
                        """
                                id	val
                                1	10
                                2	2
                                3	30
                                4	40
                                """
                );
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM (lp_sym_case UNION ALL lp_sym_case) ORDER BY id",
                        """
                                id	val
                                1	10
                                1	10
                                2	2
                                2	2
                                3	30
                                3	30
                                4	40
                                4	40
                                """
                );
            }
            {
                final int i = 2;
                final String expression = expressions.getQuick(i);
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM lp_sym_case ORDER BY id",
                        """
                                id	val
                                1	first
                                2	中
                                3\t
                                4\t
                                """
                );
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM (lp_sym_case UNION ALL lp_sym_case) ORDER BY id",
                        """
                                id	val
                                1	first
                                1	first
                                2	中
                                2	中
                                3\t
                                3\t
                                4\t
                                4\t
                                """
                );
            }
            {
                final int i = 3;
                final String expression = expressions.getQuick(i);
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM lp_sym_case ORDER BY id",
                        """
                                id	val
                                1	a
                                2	y
                                3\t
                                4\t
                                """
                );
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM (lp_sym_case UNION ALL lp_sym_case) ORDER BY id",
                        """
                                id	val
                                1	a
                                1	a
                                2	y
                                2	y
                                3\t
                                3\t
                                4\t
                                4\t
                                """
                );
            }
            {
                final int i = 4;
                final String expression = expressions.getQuick(i);
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM lp_sym_case ORDER BY id",
                        """
                                id	val
                                1	a
                                2	second
                                3\t
                                4	last
                                """
                );
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM (lp_sym_case UNION ALL lp_sym_case) ORDER BY id",
                        """
                                id	val
                                1	a
                                1	a
                                2	second
                                2	second
                                3\t
                                3\t
                                4	last
                                4	last
                                """
                );
            }
            {
                final int i = 5;
                final String expression = expressions.getQuick(i);
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM lp_sym_case ORDER BY id",
                        """
                                id	val
                                1	a
                                2	中
                                3\t
                                4	四
                                """
                );
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM (lp_sym_case UNION ALL lp_sym_case) ORDER BY id",
                        """
                                id	val
                                1	a
                                1	a
                                2	中
                                2	中
                                3\t
                                3\t
                                4	四
                                4	四
                                """
                );
            }
            {
                final int i = 6;
                final String expression = expressions.getQuick(i);
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM lp_sym_case ORDER BY id",
                        """
                                id	val
                                1	first
                                2	中
                                3\t
                                4	四
                                """
                );
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM (lp_sym_case UNION ALL lp_sym_case) ORDER BY id",
                        """
                                id	val
                                1	first
                                1	first
                                2	中
                                2	中
                                3\t
                                3\t
                                4	四
                                4	四
                                """
                );
            }
            {
                final int i = 7;
                final String expression = expressions.getQuick(i);
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM lp_sym_case ORDER BY id",
                        """
                                id	val
                                1	3
                                2	3
                                3	2
                                4	3
                                """
                );
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM (lp_sym_case UNION ALL lp_sym_case) ORDER BY id",
                        """
                                id	val
                                1	3
                                1	3
                                2	3
                                2	3
                                3	2
                                3	2
                                4	3
                                4	3
                                """
                );
            }
            {
                final int i = 8;
                final String expression = expressions.getQuick(i);
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM lp_sym_case ORDER BY id",
                        """
                                id	val
                                1	a
                                2\t
                                3\t
                                4\t
                                """
                );
                assertRowsOnly(
                        "SELECT id," + expression + " val FROM (lp_sym_case UNION ALL lp_sym_case) ORDER BY id",
                        """
                                id	val
                                1	a
                                1	a
                                2\t
                                2\t
                                3\t
                                3\t
                                4\t
                                4\t
                                """
                );
            }
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile("SELECT CASE WHEN active THEN s ELSE t END val FROM lp_sym_case", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertEquals(ColumnType.STRING, factory.getMetadata().getColumnType(0));
                }
                try (RecordCursorFactory factory = compiler.compile("SELECT CASE WHEN active THEN s ELSE v END val FROM lp_sym_case", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertEquals(ColumnType.VARCHAR, factory.getMetadata().getColumnType(0));
                }
            }
        });
    }

    @Test
    public void testDiscardedNativeBranchesAndFailureRecovery() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly(
                    "SELECT id,CASE active WHEN true THEN true WHEN false THEN false ELSE id IN (1,2,3) END val FROM lp_sym_case ORDER BY id",
                    """
                            id	val
                            1	true
                            2	false
                            3	true
                            4	false
                            """
            );
            assertRowsOnly(
                    "SELECT id,CASE label WHEN null THEN id IN (1,2,3) WHEN null THEN true ELSE false END val FROM lp_sym_case ORDER BY id",
                    """
                            id	val
                            1	false
                            2	false
                            3	true
                            4	false
                            """
            );
            assertRowsOnly(
                    "SELECT id,CASE s WHEN null THEN id IN (1,2,3) WHEN null THEN true ELSE false END val FROM lp_sym_case ORDER BY id",
                    """
                            id	val
                            1	false
                            2	false
                            3	true
                            4	false
                            """
            );
            assertRowsOnly(
                    "SELECT id,CASE s WHEN null THEN id IN (1,2,3) WHEN null THEN true ELSE false END val FROM (lp_sym_case UNION ALL lp_sym_case) ORDER BY id",
                    """
                            id	val
                            1	false
                            1	false
                            2	false
                            2	false
                            3	true
                            3	true
                            4	false
                            4	false
                            """
            );
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int path = 0; path < 2; path++) {
                    try (RecordCursorFactory ignored = compiler.compile("SELECT CASE s WHEN null THEN id IN (1,2,3) WHEN null THEN true WHEN 'a' THEN true WHEN 'a' THEN false END FROM lp_sym_case", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail();
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "duplicate branch");
                    }
                    try (RecordCursorFactory factory = compiler.compile("SELECT CASE s WHEN 'a' THEN 1 ELSE 0 END val FROM lp_sym_case ORDER BY id", sqlExecutionContext).getRecordCursorFactory()) {
                        assertRowsOnly(factory, "val\n1\n0\n0\n0\n");
                    }
                }
            }
        });
    }

    @Test
    public void testMixedTextBranchesInAggregatesAndScalarFilters() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly(
                    "SELECT CASE WHEN active THEN s ELSE label END k,count() FROM lp_sym_case GROUP BY 1 ORDER BY k",
                    """
                            k	count
                            	1
                            a	1
                            last	1
                            second	1
                            """
            );
            assertRowsOnly(
                    "SELECT sum(CASE s WHEN 'a' THEN id ELSE 0 END) FROM lp_sym_case",
                    """
                            sum
                            1
                            """
            );
            assertRowsOnly(
                    "SELECT id FROM (lp_sym_case LIMIT 4) WHERE CASE s WHEN 'a' THEN true ELSE false END ORDER BY id",
                    """
                            id
                            1
                            """
            );
            assertRowsOnly(
                    "SELECT id,CASE v WHEN '中' THEN s ELSE label END val FROM lp_sym_case ORDER BY id",
                    """
                            id	val
                            1	first
                            2	b
                            3\t
                            4	last
                            """
            );
        });
    }

    @Test
    public void testFactorySurvivesCompilerReuseAndDictionaryGrowth() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                retained = compiler.compile("SELECT id,CASE s WHEN 'a' THEN 10 WHEN 'new' THEN 20 ELSE -1 END val FROM lp_sym_case ORDER BY id", sqlExecutionContext).getRecordCursorFactory();
                try {
                    try (RecordCursorFactory ignored = compiler.compile("SELECT CASE s WHEN 'a' THEN id WHEN 'a' THEN 3 END FROM lp_sym_case", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail();
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "duplicate branch");
                    }
                    try (RecordCursorFactory ignored = compiler.compile("SELECT 1 FROM lp_sym_case", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                } catch (Throwable th) {
                    retained.close();
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained) {
                assertRowsOnly(factory, "id\tval\n1\t10\n2\t-1\n3\t-1\n4\t-1\n");
                execute("INSERT INTO lp_sym_case VALUES(5,'new','other','new','新',true)");
                assertRowsOnly(factory, "id\tval\n1\t10\n2\t-1\n3\t-1\n4\t-1\n5\t20\n");
            }
        });
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_sym_case(id INT,s SYMBOL,t SYMBOL,label STRING,v VARCHAR,active BOOLEAN)");
        execute("INSERT INTO lp_sym_case VALUES(1,'a','x','first','一',true),(2,'b','y','second','中',false),(3,null,'z',null,null,true),(4,'c',null,'last','四',false)");
    }
}
