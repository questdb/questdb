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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CountDistinctRewriteTest extends AbstractCairoTest {
    @Test
    public void testColumnsRetainNullAndEmptyInputSemantics() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cd_types (i INT,l LONG,d DOUBLE,s STRING,v VARCHAR,ip IPv4)");
            execute("INSERT INTO cd_types VALUES (1,1,1,'a','a','1.2.3.4'),(1,1,1,'a','a','1.2.3.4'),"
                    + "(2,2,2,'b','b','1.2.3.5'),(null,null,null,null,null,null)");
            final ObjList<String> columns = new ObjList<>("i", "l", "d", "s", "v", "ip");
            final ObjList<String> notNull = new ObjList<>("i!=null", "l!=null", "d is not null", "s is not null", "v is not null", "ip!='null'");
            for (int i = 0, n = columns.size(); i < n; i++) {
                final String column = columns.getQuick(i);
                assertCountDistinct("SELECT count_distinct(" + column + ") c FROM cd_types", "c\n2\n", countPlan(column, notNull.getQuick(i)));
                assertCountDistinct("SELECT count(DISTINCT " + column + ") c FROM cd_types WHERE i<0", "c\n0\n", countPlan(column, "(i<0 and " + notNull.getQuick(i) + ")"));
                assertCountDistinct("SELECT count_distinct(" + column + ") c FROM cd_types WHERE i=null", "c\n0\n", countPlan(column, "(i=null and " + notNull.getQuick(i) + ")"));
            }
        });
    }

    @Test
    public void testComputedKeysOuterExpressionsAndOrderAliases() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cd_expr (x LONG,s STRING,substring STRING)");
            execute("INSERT INTO cd_expr VALUES (1,'ab','present'),(1,'ac','present'),(2,'bc',null),(null,null,null)");
            assertCountDistinct("SELECT count_distinct(x+1) c FROM cd_expr WHERE x>0", "c\n2\n", """
                    Count
                        Async Group By workers: 1
                          keys: [column]
                          keyFunctions: [x+1]
                          filter: (0<x and x+1!=null)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: cd_expr
                    """);
            assertCountDistinct("SELECT count_distinct(x+1) c FROM cd_expr WHERE x>0 ORDER BY 1", "c\n2\n", """
                    Count
                        Async Group By workers: 1
                          keys: [column]
                          keyFunctions: [x+1]
                          filter: (0<x and x+1!=null)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: cd_expr
                    """);
            assertCountDistinct("SELECT count_distinct(x+1) c FROM cd_expr WHERE x>0 ORDER BY c DESC LIMIT 1", "c\n2\n", """
                    Limit value: 1 skip-rows: 0 take-rows: 1
                        Count
                            Async Group By workers: 1
                              keys: [column]
                              keyFunctions: [x+1]
                              filter: (0<x and x+1!=null)
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: cd_expr
                    """);
            assertCountDistinct("SELECT count_distinct(substring(s,1,1)) c FROM cd_expr", "c\n2\n", """
                    Count
                        Async Group By workers: 1
                          keys: [substring]
                          keyFunctions: [substring(s,1,1)]
                          filter: substring(s,1,1) is not null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: cd_expr
                    """);
            assertCountDistinct("SELECT count_distinct(x) c FROM cd_expr ORDER BY count_distinct(x)", "c\n2\n", """
                    Count
                        Async Group By workers: 1
                          keys: [x]
                          filter: x!=null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: cd_expr
                    """);
            assertCountDistinct("SELECT count_distinct(x)+1 c FROM cd_expr", "c\n3\n", """
                    VirtualRecord
                      functions: [count_distinct+1]
                        Count
                            Async Group By workers: 1
                              keys: [x]
                              filter: x!=null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: cd_expr
                    """);
            assertCountDistinct("SELECT count_distinct(x) c,42 k FROM cd_expr", "c\tk\n2\t42\n", """
                    VirtualRecord
                      functions: [c,42]
                        Count
                            Async Group By workers: 1
                              keys: [x]
                              filter: x!=null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: cd_expr
                    """);
            assertCountDistinct("SELECT count_distinct(x) a,count_distinct(x) b FROM cd_expr ORDER BY 2", "a\tb\n2\t2\n", """
                    SelectedRecord
                        Count
                            Async Group By workers: 1
                              keys: [x]
                              filter: x!=null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: cd_expr
                    """);
            assertCountDistinct("SELECT count_distinct(substring(s,1,1)) c FROM cd_expr WHERE substring!=null", "c\n1\n", """
                    Count
                        Async Group By workers: 1
                          keys: [substring]
                          keyFunctions: [substring(s,1,1)]
                          filter: (substring is not null and substring(s,1,1) is not null)
                            PageFrame
                                Row forward scan
                                Frame forward scan on: cd_expr
                    """);
            assertCountDistinct("SELECT count_distinct('M'::CHAR) a0 FROM cd_expr ORDER BY 1", "a0\n1\n", """
                    Count
                        Async Group By workers: 1
                          keys: [cast]
                          keyFunctions: ['M']
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: cd_expr
                    """);
            assertCountDistinct("SELECT count_distinct(null::LONG) a0 FROM cd_expr ORDER BY 1", "a0\n0\n", """
                    Count
                        GroupBy vectorized: false
                          keys: [cast]
                            Empty table
                    """);
        });
    }

    @Test
    public void testNonRewrittenAggregateShapesAndSourceBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cd_a (x INT,s SYMBOL)");
            execute("CREATE TABLE cd_b (y INT)");
            execute("INSERT INTO cd_a VALUES (1,'b'),(1,'a'),(2,'b'),(null,null)");
            execute("INSERT INTO cd_b VALUES (1),(2)");
            assertCountDistinct("SELECT count_distinct(s) c FROM cd_a", "c\n2\n", """
                    GroupBy vectorized: false
                      values: [count_distinct(s)]
                        PageFrame
                            Row forward scan
                            Frame forward scan on: cd_a
                    """);
            assertCountDistinct("SELECT count_distinct(10) c FROM cd_a", "c\n1\n", """
                    Async Group By workers: 1
                      vectorized: false
                      values: [count_distinct(10)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Frame forward scan on: cd_a
                    """);
            assertCountDistinct("SELECT count_distinct(x) a,count_distinct(s) b FROM cd_a", "a\tb\n2\t2\n", """
                    Async Group By workers: 1
                      vectorized: false
                      values: [count_distinct(x),count_distinct(s)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Frame forward scan on: cd_a
                    """);
            assertCountDistinct("SELECT s,count_distinct(x) c FROM cd_a GROUP BY s ORDER BY s", """
                    s	c
                    	0
                    a	1
                    b	2
                    """, """
                    Encode sort light
                      keys: [s]
                        Async Group By workers: 1
                          keys: [s]
                          values: [count_distinct(x)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: cd_a
                    """);
            assertCountDistinct("SELECT count_distinct(x) c FROM (SELECT x FROM cd_a LIMIT 2)", "c\n1\n", """
                    GroupBy vectorized: false
                      values: [count_distinct(x)]
                        Limit value: 2 skip-rows: 0 take-rows: 2
                            PageFrame
                                Row forward scan
                                Frame forward scan on: cd_a
                    """);
            assertCountDistinct("SELECT count_distinct(x) c FROM (SELECT x FROM cd_a)", "c\n2\n", """
                    Async Group By workers: 1
                      vectorized: false
                      values: [count_distinct(x)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Frame forward scan on: cd_a
                    """);
            assertCountDistinct("SELECT count_distinct(a.x) c FROM cd_a a CROSS JOIN cd_b b", "c\n2\n", """
                    GroupBy vectorized: false
                      values: [count_distinct(a.x)]
                        Cross Join
                            PageFrame
                                Row forward scan
                                Frame forward scan on: cd_a
                            PageFrame
                                Row forward scan
                                Frame forward scan on: cd_b
                    """);
            assertCountDistinct("SELECT count_distinct(x) c FROM long_sequence(4)", "c\n4\n", """
                    Count
                        GroupBy vectorized: false
                          keys: [x]
                            Filter filter: x!=null
                                long_sequence count: 4
                    """);
        });
    }

    @Test
    public void testLatestWhereAndUnionKeepTheirOccurrenceBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cd_latest (ts TIMESTAMP,k SYMBOL,x INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO cd_latest VALUES (0,'a',1),(1,'b',2),(2,'a',null),(3,'b',2)");
            assertCountDistinct("SELECT count_distinct(x) c FROM cd_latest LATEST ON ts PARTITION BY k", """
                    c
                    2
                    """, """
                    Count
                        GroupBy vectorized: false
                          keys: [x]
                            LatestByDeferredListValuesFiltered
                              filter: x!=null
                                Frame backward scan on: cd_latest
                    """);
            assertCountDistinct("SELECT count_distinct(x) c FROM cd_latest WHERE k='a' LATEST ON ts PARTITION BY k", """
                    c
                    1
                    """, """
                    Count
                        GroupBy vectorized: false
                          keys: [x]
                            LatestByValueFiltered
                                Row backward scan
                                  symbolFilter: k=0
                                  filter: x!=null
                                Frame backward scan on: cd_latest
                    """);
            assertCountDistinct("SELECT count_distinct(x) c FROM cd_latest UNION ALL SELECT count_distinct(x) c FROM cd_latest WHERE x=1",
                    "c\n2\n1\n", """
                            Union All
                                Count
                                    Async Group By workers: 1
                                      keys: [x]
                                      filter: x!=null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cd_latest
                                Count
                                    Async Group By workers: 1
                                      keys: [x]
                                      filter: (x=1 and x!=null)
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cd_latest
                            """);
            assertCountDistinct("WITH q AS (SELECT * FROM cd_latest) SELECT count_distinct(x) c FROM q UNION ALL SELECT count_distinct(x) c FROM q WHERE x=1",
                    "c\n2\n1\n", """
                            Union All
                                Async Group By workers: 1
                                  vectorized: false
                                  values: [count_distinct(x)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cd_latest
                                Async Group By workers: 1
                                  vectorized: false
                                  values: [count_distinct(x)]
                                  filter: x=1
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: cd_latest
                            """);
            assertCountDistinct("WITH q AS (SELECT * FROM cd_latest) SELECT count_distinct(x) c FROM q "
                            + "WHERE lower(CASE WHEN x IN (1,2) THEN x::STRING ELSE '' END)='1'",
                    "c\n1\n", """
                            Async Group By workers: 1
                              vectorized: false
                              values: [count_distinct(x)]
                              filter: to_lowercase(case([x in [1,2],x::string,'']))='1'
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: cd_latest
                            """);
        });
    }

    @Test
    public void testInvalidColumnAndOrderingDiagnosticsRecover() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cd_errors (x INT)");
            execute("INSERT INTO cd_errors VALUES (1)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                assertErrorRecovers(compiler, "SELECT count_distinct(missing) FROM cd_errors", 22, "Invalid column: missing");
                assertErrorRecovers(compiler, "SELECT count_distinct(x) c FROM cd_errors ORDER BY 2", 51, "order column position is out of range [max=1]");
                assertErrorRecovers(compiler, "SELECT count_distinct(x) c FROM cd_errors ORDER BY missing", 51, "Invalid column: missing");
            }
        });
    }

    @Test
    public void testRetainedComputedKeySurvivesCompilerReuseAndReopens() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cd_lifetime (x INT)");
            execute("INSERT INTO cd_lifetime VALUES (1),(1),(2),(3),(null)");
            final String sql = "SELECT count_distinct(CASE WHEN x IN (1,2) THEN x ELSE null END) c FROM cd_lifetime";
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    try (RecordCursorFactory ignored = compiler.compile("SELECT count_distinct(x+10) other FROM cd_lifetime", sqlExecutionContext).getRecordCursorFactory()) {
                        compiler.clear();
                    }
                }
                assertResult(retained, "c\n2\n");
                execute("INSERT INTO cd_lifetime VALUES (2),(1),(null)");
                assertResult(retained, "c\n2\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    private static String countPlan(String key, String filter) {
        return "Count\n"
                + "    Async Group By workers: 1\n"
                + "      keys: [" + key + "]\n"
                + "      filter: " + filter + "\n"
                + "        PageFrame\n"
                + "            Row forward scan\n"
                + "            Frame forward scan on: cd_types\n";
    }

    private void assertCountDistinct(String sql, String expectedRows, String expectedPlan) throws Exception {
        final int oldMode = sqlExecutionContext.getJitMode();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try {
            assertQuery(sql).noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().withPlan(expectedPlan).returns(expectedRows);
        } finally {
            sqlExecutionContext.setJitMode(oldMode);
        }
    }

    private void assertErrorRecovers(SqlCompilerImpl compiler, String sql, int position, String message) throws Exception {
        try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.fail(sql);
        } catch (SqlException e) {
            Assert.assertEquals(sql, position, e.getPosition());
            TestUtils.assertEquals(message, e.getFlyweightMessage());
        }
        try (RecordCursorFactory factory = compiler.compile("SELECT count_distinct(x) c FROM cd_errors", sqlExecutionContext).getRecordCursorFactory()) {
            assertResult(factory, "c\n1\n");
        }
    }

    private void assertResult(RecordCursorFactory factory, String rows) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(rows);
    }
}
