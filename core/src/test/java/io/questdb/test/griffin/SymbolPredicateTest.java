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
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SymbolPredicateTest extends AbstractCairoTest {
    @Test
    public void testStaticSymbolComparisonsNullsAndAliases() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,s='alpha','alpha'=s,s!='alpha',s<>t,s=t,s=null,null=s,s='a',length(s),s::STRING,s::VARCHAR FROM lp_symbol ORDER BY id",
                    """
                            id	column	column1	column2	column3	column4	column5	column6	column7	length	cast	cast1
                            1	true	true	false	false	true	false	false	false	5	alpha	alpha
                            2	false	false	true	true	false	false	false	false	5	Alpha	Alpha
                            3	false	false	true	false	true	false	false	false	1	中	中
                            4	false	false	true	false	true	true	true	false	-1	\t
                            5	false	false	true	true	false	false	false	true	1	a	a
                            6	false	false	true	false	true	false	false	false	0	\t
                            """
            );
            assertQueryRows(
                    "SELECT id FROM (SELECT id,s,t FROM lp_symbol LIMIT 100) WHERE s='alpha' OR s=t ORDER BY id",
                    """
                            id
                            1
                            3
                            4
                            6
                            """
            );
            assertQueryRows(
                    "SELECT id FROM lp_symbol WHERE t='alpha' OR t='中' ORDER BY id",
                    """
                            id
                            1
                            2
                            3
                            """
            );
            assertQueryRows(
                    "SELECT id,s, count() FROM lp_symbol GROUP BY id,s ORDER BY id",
                    """
                            id	s	count
                            1	alpha	1
                            2	Alpha	1
                            3	中	1
                            4		1
                            5	a	1
                            6		1
                            """
            );
            assertQueryRows(
                    "SELECT id FROM (SELECT id,s renamed,t FROM lp_symbol LIMIT 100) WHERE renamed=t OR renamed='中' ORDER BY id",
                    """
                            id
                            1
                            3
                            4
                            6
                            """
            );
            assertQueryRows(
                    "SELECT l.id FROM lp_symbol l JOIN lp_symbol r ON l.id=r.id WHERE l.s=r.t ORDER BY l.id",
                    """
                            id
                            1
                            3
                            4
                            6
                            """
            );
        });
    }

    @Test
    public void testMembershipLikeRuntimeParametersAndUnicode() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,s IN ('alpha','中',null),s NOT IN ('a',null),s LIKE 'a%',s ILIKE 'AL%',s LIKE '%h%',s LIKE 'a_pha',s LIKE '',s ILIKE null FROM lp_symbol ORDER BY id",
                    """
                            id	column	column1	column2	column3	column4	column5	column6	column7
                            1	true	true	true	true	true	true	false	false
                            2	false	true	false	true	true	false	false	false
                            3	true	true	false	false	false	false	false	false
                            4	true	false	false	false	false	false	false	false
                            5	false	false	true	false	false	false	false	false
                            6	false	true	false	false	false	false	false	false
                            """
            );
            bindVariableService.setStr(0, "alpha");
            bindVariableService.setStr(1, "中");
            assertQueryRows(
                    "SELECT id,s IN ($1,$2,null),s=$1,s<>$2 FROM lp_symbol ORDER BY id",
                    """
                            id	column	column1	column2
                            1	true	true	true
                            2	false	false	true
                            3	true	false	false
                            4	true	false	true
                            5	false	false	true
                            6	false	false	true
                            """
            );
            bindVariableService.setStr(0, "a%");
            assertQueryRows("SELECT id,s LIKE $1,s ILIKE $1 FROM lp_symbol ORDER BY id", """
                    id	column	column1
                    1	true	true
                    2	false	true
                    3	false	false
                    4	false	false
                    5	true	true
                    6	false	false
                    """);
            bindVariableService.setStr(0, null);
            assertQueryRows(
                    "SELECT id,s LIKE $1,s IN ($1,$2,null) FROM lp_symbol ORDER BY id",
                    """
                            id	column	column1
                            1	false	false
                            2	false	false
                            3	false	true
                            4	false	true
                            5	false	false
                            6	false	false
                            """
            );
        });
    }

    @Test
    public void testDynamicUnionAndStaticIntersectCapabilities() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT s,s='alpha',s LIKE 'a%',s IN ('中',null),length(s),s::VARCHAR FROM (SELECT s FROM lp_symbol UNION ALL SELECT t FROM lp_symbol)",
                    """
                            s	column	column1	column2	length	cast
                            alpha	true	true	false	5	alpha
                            Alpha	false	false	false	5	Alpha
                            中	false	false	true	1	中
                            	false	false	true	-1\t
                            a	false	true	false	1	a
                            	false	false	false	0\t
                            alpha	true	true	false	5	alpha
                            alpha	true	true	false	5	alpha
                            中	false	false	true	1	中
                            	false	false	true	-1\t
                            b	false	false	false	1	b
                            	false	false	false	0\t
                            """
            );
            assertQueryRows(
                    "SELECT s,s='alpha',s LIKE 'a%',s IN ('中',null) FROM (SELECT s FROM lp_symbol UNION SELECT t FROM lp_symbol)",
                    """
                            s	column	column1	column2
                            alpha	true	true	false
                            Alpha	false	false	false
                            中	false	false	true
                            	false	false	true
                            a	false	true	false
                            	false	false	false
                            b	false	false	false
                            """
            );
            assertQueryRows(
                    "SELECT s,s='alpha',s LIKE 'a%' FROM (SELECT s FROM lp_symbol INTERSECT SELECT t FROM lp_symbol)",
                    """
                            s	column	column1
                            alpha	true	true
                            中	false	false
                            	false	false
                            	false	false
                            """
            );
            assertQueryRows(
                    "SELECT s,s='alpha',s LIKE 'a%' FROM (SELECT s FROM lp_symbol EXCEPT SELECT t FROM lp_symbol)",
                    """
                            s	column	column1
                            Alpha	false	false
                            a	false	true
                            """
            );
        });
    }

    @Test
    public void testPreparedFactorySurvivesCompilerAndDictionaryGrowth() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT id,t s FROM lp_symbol WHERE t IN ('alpha','new',null) ORDER BY id";
            final RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                try {
                    try (RecordCursorFactory ignored = compiler.compile("SELECT s LIKE t FROM lp_symbol", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail();
                    } catch (SqlException expected) {
                        Assert.assertTrue(expected.getFlyweightMessage().toString().contains("use constant or bind variable"));
                    }
                    compiler.clear();
                } catch (Throwable th) {
                    retained.close();
                    throw th;
                }
            }
            try (RecordCursorFactory actual = retained) {
                assertFactory(actual).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary()
                        .returns("id\ts\n1\talpha\n2\talpha\n4\t\n");
                execute("INSERT INTO lp_symbol VALUES(90,7,'new','new')");
                assertFactory(actual).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary()
                        .returns("id\ts\n1\talpha\n2\talpha\n4\t\n7\tnew\n");
            }
        });
    }

    @Test
    public void testNativeSymbolExclusion() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows("SELECT id FROM lp_symbol WHERE s NOT IN ('alpha','中')", """
                    id
                    2
                    4
                    5
                    6
                    """);
        });
    }

    private void createRows() throws SqlException {
        execute("CREATE TABLE lp_symbol(unused INT,id INT,s SYMBOL INDEX,t SYMBOL)");
        execute("INSERT INTO lp_symbol VALUES(81,1,'alpha','alpha'),(82,2,'Alpha','alpha'),(83,3,'中','中'),(84,4,null,null),(85,5,'a','b'),(86,6,'','')");
    }

    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
