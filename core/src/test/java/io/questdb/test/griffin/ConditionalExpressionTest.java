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
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class ConditionalExpressionTest extends AbstractCairoTest {
    @Test
    public void testSearchedCaseAndParserGeneratedSwitch() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,CASE WHEN i>0 THEN i ELSE l END value FROM lp_case ORDER BY id",
                    """
                            id	value
                            1	1
                            2	2
                            3	30
                            4	null
                            """
            );
            assertQueryRows(
                    "SELECT id,CASE WHEN i=1 THEN 10 WHEN i=2 THEN i+20 ELSE null END value FROM lp_case ORDER BY id",
                    """
                            id	value
                            1	10
                            2	22
                            3	null
                            4	null
                            """
            );
            assertQueryRows(
                    "SELECT id,CASE WHEN active THEN label ELSE 'fallback' END value FROM lp_case ORDER BY id",
                    """
                            id	value
                            1	a
                            2	fallback
                            3	c
                            4	fallback
                            """
            );
            assertQueryRows(
                    "SELECT id,CASE WHEN active THEN i END value FROM lp_case ORDER BY id",
                    """
                            id	value
                            1	1
                            2	null
                            3	-1
                            4	null
                            """
            );
            assertQueryRows(
                    "SELECT id,CASE WHEN active THEN null END value FROM lp_case ORDER BY id",
                    """
                            id	value
                            1	null
                            2	null
                            3	null
                            4	null
                            """
            );
            assertQueryRows(
                    "SELECT id,CASE WHEN active THEN ts ELSE ts END value FROM lp_case ORDER BY id",
                    """
                            id	value
                            1	2020-01-01T00:00:00.000000Z
                            2	2020-01-02T00:00:00.000000Z
                            3	2020-01-03T00:00:00.000000Z
                            4\t
                            """
            );
            assertQueryRows(
                    "SELECT id,CASE WHEN active THEN b ELSE f END value FROM lp_case ORDER BY id",
                    """
                            id	value
                            1	1.0
                            2	2.0
                            3	3.0
                            4	null
                            """
            );
        });
    }

    @Test
    public void testSwitchKeyTypesNullBranchesAndTypedParameter() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,CASE i WHEN 1 THEN l WHEN 2 THEN i ELSE -1 END value FROM lp_case ORDER BY id",
                    """
                            id	value
                            1	10
                            2	2
                            3	-1
                            4	-1
                            """
            );
            assertQueryRows(
                    "SELECT id,CASE label WHEN 'a' THEN 1 WHEN null THEN 2 ELSE 3 END value FROM lp_case ORDER BY id",
                    """
                            id	value
                            1	1
                            2	3
                            3	3
                            4	2
                            """
            );
            assertQueryRows(
                    "SELECT id,CASE label WHEN null THEN 1 WHEN null THEN 2 ELSE 3 END value FROM lp_case ORDER BY id",
                    """
                            id	value
                            1	3
                            2	3
                            3	3
                            4	2
                            """
            );
            assertQueryRows(
                    "SELECT id,CASE active WHEN true THEN i WHEN false THEN l ELSE 9 END value FROM lp_case ORDER BY id",
                    """
                            id	value
                            1	1
                            2	20
                            3	-1
                            4	null
                            """
            );
            assertQueryRows(
                    "SELECT id,CASE b WHEN 1 THEN i WHEN 2 THEN l END value FROM lp_case ORDER BY id",
                    """
                            id	value
                            1	1
                            2	20
                            3	null
                            4	null
                            """
            );
            bindVariableService.setBoolean(0, true);
            assertQueryRows(
                    "SELECT id,CASE WHEN $1 THEN i ELSE i+1 END value FROM lp_case ORDER BY id",
                    """
                            id	value
                            1	1
                            2	2
                            3	-1
                            4	null
                            """
            );
        });
    }

    @Test
    public void testConditionalAggregatesFiltersAndNestedProjection() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT sum(CASE WHEN active THEN i ELSE 0 END),count(CASE WHEN active THEN l END) FROM lp_case",
                    """
                            sum	count
                            0	2
                            """
            );
            assertQueryRows(
                    "SELECT CASE WHEN active THEN 1 ELSE 2 END k,sum(i) FROM lp_case GROUP BY 1 ORDER BY k",
                    """
                            k	sum
                            1	0
                            2	2
                            """
            );
            assertQueryRows(
                    "SELECT id FROM lp_case WHERE CASE WHEN active THEN i>0 ELSE l>10 END ORDER BY id",
                    """
                            id
                            1
                            2
                            """
            );
            assertQueryRows(
                    "SELECT id,value FROM (SELECT id,CASE WHEN active THEN l ELSE i END value FROM lp_case) WHERE value>0 ORDER BY id",
                    """
                            id	value
                            1	10
                            2	2
                            3	30
                            """
            );
        });
    }

    @Test
    public void testConditionalFactorySurvivesCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                retained = compiler.compile("SELECT id,CASE WHEN i IN (1,2,3) THEN label ELSE 'other' END value FROM lp_case ORDER BY id", sqlExecutionContext).getRecordCursorFactory();
                try {
                    try (RecordCursorFactory ignored = compiler.compile("SELECT abs(i) FROM lp_case", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                } catch (Throwable th) {
                    retained.close();
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained) {
                assertResult(factory, "id\tvalue\n1\ta\n2\tb\n3\tother\n4\tother\n");
            }
        });
    }

    @Test
    public void testConditionalValidationAndOwnershipGatesRecover() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT CASE WHEN i THEN 1 ELSE 2 END FROM lp_case").noLeakCheck().fails(17, "BOOLEAN expected, found INT");
            assertQuery("SELECT CASE i WHEN 1 THEN 2 WHEN 1 THEN 3 END FROM lp_case").noLeakCheck().fails(33, "duplicate branch");
            bindVariableService.clear();
            assertQuery("SELECT CASE WHEN i IN (1,2,3) THEN 1 ELSE $1 END FROM lp_case").noLeakCheck().fails(42, "CASE values cannot be bind variables");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertQueryRows(
                        "SELECT CASE active WHEN true THEN true WHEN false THEN false ELSE i IN (1,2,3) END FROM lp_case",
                        """
                                switch
                                true
                                false
                                true
                                false
                                """
                );
                assertQueryRows(
                        "SELECT CASE label WHEN null THEN i IN (1,2,3) WHEN null THEN true ELSE false END FROM lp_case",
                        """
                                switch
                                false
                                false
                                false
                                true
                                """
                );
                assertQueryRows(
                        "SELECT CASE WHEN active THEN c ELSE 'more' END FROM lp_case",
                        """
                                case
                                a
                                more
                                c
                                more
                                """
                );
                try (RecordCursorFactory factory = compiler.compile("SELECT count() FROM lp_case", sqlExecutionContext).getRecordCursorFactory()) {
                    assertResult(factory, "count\n4\n");
                }
            }
        });
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_case(id INT,i INT,l LONG,b BYTE,f FLOAT,c CHAR,label STRING,active BOOLEAN,ts TIMESTAMP)");
        execute("""
                INSERT INTO lp_case VALUES
                (1,1,10,1,1,'a','a',true,'2020-01-01'),
                (2,2,20,2,2,'b','b',false,'2020-01-02'),
                (3,-1,30,3,3,'c','c',true,'2020-01-03'),
                (4,null,null,0,null,null,null,false,null)
                """);
    }

    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
