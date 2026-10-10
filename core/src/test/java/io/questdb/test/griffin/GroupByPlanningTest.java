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
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class GroupByPlanningTest extends AbstractCairoTest {
    @Test
    public void testAggregateFactorySurvivesCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setInt(0, 1);
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(
                            "SELECT k+1 AS key,sum(i+$1) AS total FROM lp_group GROUP BY key ORDER BY key",
                            sqlExecutionContext
                    ).getRecordCursorFactory();
                    assertRowsOnly(retained, "key\ttotal\nnull\tnull\n2\t8\n3\t9\n4\tnull\n");
                    try (RecordCursorFactory other = compiler.compile(
                            "SELECT l,min(d),max(d) FROM lp_group ORDER BY l", sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        assertRowsOnly(other, "l\tmin\tmax\nnull\tnull\tnull\n10\t2.0\t4.0\n20\t8.0\t8.0\n30\tnull\tnull\n");
                    }
                    compiler.clear();
                }
                bindVariableService.setInt(0, 2);
                assertRowsOnly(retained, "key\ttotal\nnull\tnull\n2\t10\n3\t10\n4\tnull\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testComputedKeysAliasesOrdinalsAndOuterExpressions() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly(
                    "SELECT k+1 AS key,sum(i) AS total FROM lp_group GROUP BY key ORDER BY key",
                    """
                            key	total
                            null	null
                            2	6
                            3	8
                            4	null
                            """
            );
            assertRowsOnly(
                    "SELECT k+1 AS key,sum(i) AS total FROM lp_group GROUP BY 1,1 ORDER BY 1",
                    """
                            key	total
                            null	null
                            2	6
                            3	8
                            4	null
                            """
            );
            assertRowsOnly(
                    "SELECT k+1 AS key,sum(i) AS total FROM lp_group GROUP BY k+1 ORDER BY key",
                    """
                            key	total
                            null	null
                            2	6
                            3	8
                            4	null
                            """
            );
            assertRowsOnly(
                    "SELECT t.k AS key,sum(i) AS total FROM lp_group t GROUP BY k,t.k ORDER BY key",
                    """
                            key	total
                            null	null
                            1	6
                            2	8
                            3	null
                            """
            );
            assertRowsOnly(
                    "SELECT t.k+1 AS key,sum(i) AS total FROM lp_group t GROUP BY k+1 ORDER BY key",
                    """
                            key	total
                            null	null
                            2	6
                            3	8
                            4	null
                            """
            );
            assertRowsOnly(
                    "SELECT k+1 AS key,sum(i)+count() AS total FROM lp_group GROUP BY k ORDER BY key",
                    """
                            key	total
                            null	null
                            2	8
                            3	10
                            4	null
                            """
            );
            assertRowsOnly("SELECT k+sum(i) AS total FROM lp_group ORDER BY total", """
                    total
                    null
                    null
                    7
                    10
                    """);
            assertRowsOnly(
                    "SELECT k+1 AS key,sum(i*2) AS total FROM lp_group ORDER BY key",
                    """
                            key	total
                            null	null
                            2	12
                            3	16
                            4	null
                            """
            );
            assertRowsOnly(
                    "SELECT k AS a,k AS b,count() FROM lp_group GROUP BY b,a ORDER BY a",
                    """
                            a	b	count
                            null	null	1
                            1	1	2
                            2	2	2
                            3	3	1
                            """
            );
            assertRowsOnly("SELECT k,sum(i) FROM lp_group GROUP BY k,l ORDER BY k", """
                    k	sum
                    null	null
                    1	6
                    2	8
                    3	null
                    """);
            assertRowsOnly(
                    "SELECT t.\"a.b\",sum(i) FROM (SELECT k AS \"a.b\",i FROM lp_group) t GROUP BY t.\"a.b\" ORDER BY 1",
                    """
                            a.b	sum
                            null	null
                            1	6
                            2	8
                            3	null
                            """
            );
        });
    }

    @Test
    public void testDefaultDistinctGroupsOnlyTheVisibleTuple() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly("SELECT DISTINCT k FROM lp_group ORDER BY k", """
                    k
                    null
                    1
                    2
                    3
                    """);
            assertRowsOnly("SELECT DISTINCT k AS a,k AS b FROM lp_group ORDER BY a", """
                    a	b
                    null	null
                    1	1
                    2	2
                    3	3
                    """);
            assertRowsOnly("SELECT DISTINCT k+1 AS key FROM lp_group ORDER BY k+1", """
                    key
                    null
                    2
                    3
                    4
                    """);
            assertRowsOnly("SELECT DISTINCT k AS key FROM lp_group ORDER BY key+1", """
                    key
                    null
                    1
                    2
                    3
                    """);
            assertRowsOnly("SELECT DISTINCT k FROM lp_group ORDER BY k+1", """
                    k
                    null
                    1
                    2
                    3
                    """);
            assertError("SELECT DISTINCT k FROM lp_group ORDER BY i+1,k", 41, "ORDER BY expressions must appear in select list. Invalid column: i");
            assertRowsOnly(
                    "SELECT DISTINCT 7 AS fixed,k AS key FROM lp_group ORDER BY key DESC LIMIT 2",
                    """
                            fixed	key
                            7	3
                            7	2
                            """
            );
            assertRowsOnly("SELECT DISTINCT 7 AS fixed FROM lp_group", """
                    fixed
                    7
                    """);
            assertRowsOnly("SELECT DISTINCT 7 AS fixed FROM lp_group WHERE false", """
                    fixed
                    7
                    """);
            assertPlanContains("SELECT DISTINCT 7 AS fixed FROM lp_group WHERE false", "Count");
            assertRowsOnly(
                    "SELECT k FROM (SELECT DISTINCT k,s FROM lp_group) ORDER BY k",
                    """
                            k
                            null
                            1
                            2
                            3
                            """
            );
            assertRowsOnly(
                    "SELECT 7 AS value FROM (SELECT DISTINCT k,s FROM lp_group) ORDER BY value",
                    """
                            value
                            7
                            7
                            7
                            7
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT k,sum(i) AS total FROM lp_group GROUP BY k ORDER BY k",
                    """
                            k	total
                            null	null
                            1	6
                            2	8
                            3	null
                            """
            );
            assertError("SELECT DISTINCT k FROM lp_group ORDER BY i", 41, "ORDER BY expressions must appear in select list. Invalid column: i");
        });
    }

    @Test
    public void testDistinctOverAggregationAppliesToSelectedTuple() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertError("SELECT DISTINCT sum(i) AS total FROM lp_group GROUP BY k ORDER BY total,k", 72, "ORDER BY expressions must appear in select list. Invalid column: k");
            assertRowsOnly(
                    "SELECT DISTINCT sum(i)+1 AS total FROM lp_group GROUP BY k ORDER BY total",
                    """
                            total
                            null
                            7
                            9
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT k,sum(i) AS total FROM lp_group GROUP BY k ORDER BY sum(i)+1,k",
                    """
                            k	total
                            null	null
                            3	null
                            1	6
                            2	8
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT k,sum(i) AS total FROM lp_group GROUP BY k ORDER BY count(),k",
                    """
                            k	total
                            null	null
                            3	null
                            1	6
                            2	8
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT k,sum(i) AS total FROM lp_group GROUP BY k ORDER BY count(),3,k",
                    """
                            k	total
                            null	null
                            3	null
                            1	6
                            2	8
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT k,sum(i) AS total FROM lp_group GROUP BY k ORDER BY 3,count(),k",
                    """
                            k	total
                            null	null
                            3	null
                            1	6
                            2	8
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT sum(i) AS total FROM lp_group WHERE k>0 AND k<3 GROUP BY k ORDER BY total",
                    """
                            total
                            6
                            8
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT sum(i) AS total FROM lp_group WHERE k>0 AND k<3 GROUP BY lp_group.k ORDER BY total",
                    """
                            total
                            6
                            8
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT sum(i) AS total FROM lp_group WHERE k>0 AND k<3 GROUP BY K ORDER BY total",
                    """
                            total
                            6
                            8
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT k AS key,sum(i) AS total FROM lp_group GROUP BY k ORDER BY key",
                    """
                            key	total
                            null	null
                            1	6
                            2	8
                            3	null
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT k AS key,sum(i) AS total FROM lp_group GROUP BY key ORDER BY key",
                    """
                            key	total
                            null	null
                            1	6
                            2	8
                            3	null
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT 7 AS fixed FROM lp_group GROUP BY k ORDER BY fixed",
                    """
                            fixed
                            7
                            """
            );
            assertRowsOnly("SELECT DISTINCT k FROM lp_group GROUP BY k,i ORDER BY k", """
                    k
                    null
                    1
                    2
                    3
                    """);
            assertError("SELECT DISTINCT k,sum(i) AS total FROM lp_group GROUP BY k ORDER BY i", 68, "ORDER BY expressions must appear in select list. Invalid column: i");
            assertError("SELECT DISTINCT sum(i)+1 AS total FROM lp_group GROUP BY k ORDER BY total+1", 68, "Invalid column: total");
        });
    }

    @Test
    public void testDistinctOverGroupingKeepsOnlySelectedColumns() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly("SELECT DISTINCT count() c FROM lp_group GROUP BY k ORDER BY c", """
                    c
                    1
                    2
                    """);
            assertRowsOnly("SELECT DISTINCT count() FROM lp_group GROUP BY s ORDER BY 1", """
                    count
                    1
                    2
                    3
                    """);
            assertRowsOnly("SELECT DISTINCT k % 2 AS parity FROM lp_group GROUP BY k ORDER BY parity", """
                    parity
                    null
                    0
                    1
                    """);
            assertRowsOnly("SELECT DISTINCT active FROM lp_group GROUP BY active, k ORDER BY active", """
                    active
                    false
                    true
                    """);
            assertRowsOnly("SELECT count() FROM (SELECT DISTINCT count() c FROM lp_group GROUP BY k)", """
                    count
                    2
                    """);
            assertRowsOnly(
                    "SELECT DISTINCT sum(i) AS total FROM lp_group WHERE k>0 AND k<3 GROUP BY k+1 ORDER BY total",
                    """
                            total
                            6
                            8
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT sum(i) AS total FROM lp_group WHERE k>0 AND k<3 GROUP BY 1+k,k*2 ORDER BY total",
                    """
                            total
                            6
                            8
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT sum(i) AS total FROM lp_group GROUP BY 7+1 ORDER BY total",
                    """
                            total
                            14
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT sum(i) AS total FROM lp_group WHERE k>0 AND k<3 GROUP BY k,7+1 ORDER BY total",
                    """
                            total
                            6
                            8
                            """
            );
            assertError("SELECT DISTINCT sum(i) AS total FROM lp_group GROUP BY k+1,k*2 ORDER BY total,k+1", 78, "ORDER BY expressions must appear in select list. Invalid column: k");
        });
    }

    @Test
    public void testDistinctOrderMustBeDeterminedBySelectList() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertError("SELECT DISTINCT k FROM lp_group ORDER BY k+i", 43, "ORDER BY expressions must appear in select list. Invalid column: i");
            assertError("SELECT DISTINCT k FROM lp_group ORDER BY s", 41, "ORDER BY expressions must appear in select list. Invalid column: s");
            assertError("SELECT DISTINCT count() c FROM lp_group GROUP BY k ORDER BY k", 60, "ORDER BY expressions must appear in select list. Invalid column: k");
            assertError("SELECT DISTINCT k FROM lp_group GROUP BY k, s ORDER BY s", 55, "ORDER BY expressions must appear in select list. Invalid column: s");
            assertError("SELECT DISTINCT k FROM lp_group GROUP BY k, s ORDER BY count()", 55, "ORDER BY expressions must appear in select list. Invalid column: count");
            assertError("SELECT DISTINCT k, sum(i) total FROM lp_group GROUP BY k, s ORDER BY max(i)", 69, "ORDER BY expressions must appear in select list. Invalid column: max");
            assertError("SELECT DISTINCT k, max(i) OVER () m FROM lp_group ORDER BY i", 59, "ORDER BY expressions must appear in select list. Invalid column: i");
            assertError("SELECT DISTINCT k, max(i) OVER () m FROM lp_group ORDER BY k+i", 61, "ORDER BY expressions must appear in select list. Invalid column: i");
            assertError("SELECT DISTINCT sum(i) total FROM lp_group SAMPLE BY 1d ORDER BY count()", 65, "ORDER BY expressions must appear in select list. Invalid column: count");
            assertRowsOnly("SELECT DISTINCT s FROM lp_group ORDER BY count(), s", """
                    s
                    
                    b
                    a
                    """);
            assertRowsOnly("SELECT DISTINCT k, s FROM lp_group ORDER BY k+1, 2", """
                    k\ts
                    null\t
                    1\ta
                    2\tb
                    3\ta
                    """);
            assertRowsOnly("SELECT DISTINCT k AS key FROM lp_group ORDER BY lp_group.k DESC", """
                    key
                    3
                    2
                    1
                    null
                    """);
            assertRowsOnly("SELECT DISTINCT k, max(i) OVER () m FROM lp_group ORDER BY m, k+1", """
                    k\tm
                    null\t8
                    1\t8
                    2\t8
                    3\t8
                    """);
        });
    }

    @Test
    public void testIntegerSumNormalizationPreservesOverflowNullsAndOrderProjection() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly(
                    "SELECT DISTINCT k,sum(i*2) AS total FROM lp_group GROUP BY k ORDER BY total,k",
                    """
                            k	total
                            null	null
                            3	null
                            1	12
                            2	16
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT k,sum(2*i) AS total FROM lp_group GROUP BY k ORDER BY total,k",
                    """
                            k	total
                            null	null
                            3	null
                            1	12
                            2	16
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT k,sum(i+2) AS total FROM lp_group GROUP BY k ORDER BY total,k",
                    """
                            k	total
                            null	null
                            3	null
                            1	10
                            2	10
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT k,sum(2-i) AS total FROM lp_group GROUP BY k ORDER BY total,k",
                    """
                            k	total
                            null	null
                            3	null
                            2	-6
                            1	-2
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT k,sum(i/2) AS total FROM lp_group GROUP BY k ORDER BY total,k",
                    """
                            k	total
                            null	null
                            3	null
                            1	3
                            2	4
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT k,sum(i*2) AS total FROM lp_group GROUP BY k ORDER BY sum(i*2)+1,k",
                    """
                            k	total
                            null	null
                            3	null
                            1	12
                            2	16
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT k,sum(i) AS total FROM lp_group GROUP BY k ORDER BY sum(i*2),k",
                    """
                            k	total
                            null	null
                            3	null
                            1	6
                            2	8
                            """
            );
            assertRowsOnly(
                    "SELECT k,sum(i+2) AS total FROM lp_group GROUP BY k ORDER BY sum(i*2),k LIMIT 3",
                    """
                            k	total
                            null	null
                            3	null
                            1	10
                            """
            );
            assertRowsOnly(
                    "SELECT k,sum(i) AS total FROM lp_group GROUP BY k ORDER BY sum(i)+1,sum(i*2),k",
                    """
                            k	total
                            null	null
                            3	null
                            1	6
                            2	8
                            """
            );
            assertRowsOnly(
                    "SELECT k,sum(i) AS total FROM lp_group GROUP BY k ORDER BY sum(i*2),sum(i*2) DESC,k",
                    """
                            k	total
                            null	null
                            3	null
                            1	6
                            2	8
                            """
            );
            assertRowsOnly(
                    "SELECT k,sum(i*2147483647) AS total,sum(i+2147483647) AS plus FROM lp_group GROUP BY k ORDER BY k",
                    """
                            k	total	plus
                            null	null	null
                            1	12884901882	4294967300
                            2	17179869176	2147483655
                            3	null	null
                            """
            );
            assertRowsOnly(
                    "SELECT sum(i*2147483647) AS total,sum(i*2147483647)+1 AS nested FROM lp_group",
                    """
                            total	nested
                            30064771058	-13
                            """
            );
            assertRowsOnly(
                    "SELECT sum(i+2) AS total,sum(2-i) AS reversed FROM lp_group WHERE false",
                    """
                            total	reversed
                            null	null
                            """
            );
            assertRowsOnly("SELECT sum(i*2) AS total FROM (SELECT i FROM lp_group)", """
                    total
                    28
                    """);
            assertRowsOnly(
                    "SELECT k,sum(d*2) AS total FROM lp_group GROUP BY k ORDER BY k",
                    """
                            k	total
                            null	null
                            1	12.0
                            2	16.0
                            3	null
                            """
            );
        });
    }

    @Test
    public void testEmptyInputAndNullAggregateArguments() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly(
                    "SELECT k,count(),count(i),sum(i),min(i),max(i),avg(i) FROM lp_group ORDER BY k",
                    """
                            k	count	count1	sum	min	max	avg
                            null	1	0	null	null	null	null
                            1	2	2	6	2	4	3.0
                            2	2	1	8	8	8	8.0
                            3	1	0	null	null	null	null
                            """
            );
            assertRowsOnly(
                    "SELECT count(),count(i),sum(i),min(i),max(i),avg(i) FROM lp_group WHERE false",
                    """
                            count	count1	sum	min	max	avg
                            0	0	null	null	null	null
                            """
            );
            assertRowsOnly(
                    "SELECT k,count(),sum(i),avg(i) FROM lp_group WHERE false GROUP BY k ORDER BY k",
                    """
                            k	count	sum	avg
                            """
            );
            assertRowsOnly("SELECT k FROM lp_group WHERE false GROUP BY k ORDER BY k", """
                    k
                    """);
            assertRowsOnly("SELECT 7 AS fixed FROM lp_group WHERE false GROUP BY fixed", """
                    fixed
                    """);
            assertRowsOnly("SELECT 7 AS fixed,count() FROM lp_group WHERE false", """
                    fixed	count
                    7	0
                    """);
            assertRowsOnly("SELECT true AS fixed,max(i) FROM lp_group GROUP BY fixed", """
                    fixed	max
                    true	8
                    """);
            assertRowsOnly(
                    "SELECT true AS fixed,max(i) FROM lp_group WHERE false GROUP BY fixed",
                    """
                            fixed	max
                            """
            );
            assertRowsOnly(
                    "SELECT true AS a,7 AS b,max(i) FROM lp_group WHERE false GROUP BY a,b",
                    """
                            a	b	max
                            """
            );
            assertRowsOnly("SELECT 12+3 AS fixed,count() FROM lp_group GROUP BY fixed", """
                    fixed	count
                    15	6
                    """);
            assertRowsOnly(
                    "SELECT 12+3 AS fixed,count() FROM lp_group WHERE false GROUP BY fixed",
                    """
                            fixed	count
                            """
            );
            assertRowsOnly("SELECT count() FROM lp_group GROUP BY 12+3", """
                    count
                    6
                    """);
            assertRowsOnly("SELECT count() FROM lp_group WHERE false GROUP BY 12+3", """
                    count
                    """);
        });
    }

    @Test
    public void testFiltersIntervalsAndOuterOrderLimit() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly(
                    "SELECT k,sum(i) AS total FROM lp_group WHERE active GROUP BY k ORDER BY total DESC,k LIMIT 2",
                    """
                            k	total
                            2	8
                            1	2
                            """
            );
            assertRowsOnly(
                    "SELECT k,sum(i) AS total FROM lp_group WHERE ts>='2020-01-03' GROUP BY k ORDER BY k",
                    """
                            k	total
                            null	null
                            2	8
                            3	null
                            """
            );
            assertRowsOnly(
                    "SELECT k,sum(i) AS total FROM lp_group WHERE ts>='2020-01-02' AND active GROUP BY k ORDER BY k",
                    """
                            k	total
                            null	null
                            2	8
                            """
            );
            assertRowsOnly(
                    "SELECT key,total FROM (SELECT k AS key,sum(i) AS total FROM lp_group) WHERE total>5 ORDER BY key DESC LIMIT 1",
                    """
                            key	total
                            2	8
                            """
            );
            assertRowsOnly(
                    "SELECT k,sum(i) AS total FROM (SELECT k,i FROM lp_group ORDER BY i DESC LIMIT 2) GROUP BY k ORDER BY k",
                    """
                            k	total
                            1	4
                            2	8
                            """
            );
            assertRowsOnly(
                    "SELECT k,sum(i) AS total FROM lp_group GROUP BY k ORDER BY sum(i) DESC,k LIMIT 1,3",
                    """
                            k	total
                            1	6
                            null	null
                            """
            );
            assertRowsOnly(
                    "SELECT k,sum(i) AS total FROM lp_group GROUP BY k ORDER BY sum(i)+1 DESC,k",
                    """
                            k	total
                            2	8
                            1	6
                            null	null
                            3	null
                            """
            );
        });
    }

    @Test
    public void testGroupValidationAndCompilerRecovers() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertError("SELECT k,sum(i) FROM lp_group GROUP BY 3", 39, "GROUP BY position 3 is not in select list");
            assertError("SELECT k,sum(i) FROM lp_group GROUP BY 0", 39, "GROUP BY position 0 is not in select list");
            assertError("SELECT k,sum(i) AS total FROM lp_group GROUP BY total", 48, "aggregate functions are not allowed in GROUP BY");
            assertError("SELECT k,sum(i) FROM lp_group GROUP BY 2", 39, "aggregate functions are not allowed in GROUP BY");
            assertError("SELECT k,i FROM lp_group GROUP BY k", 9, "column must appear in GROUP BY clause or aggregate function");
            assertError("SELECT k,k+i FROM lp_group GROUP BY k", 11, "column must appear in GROUP BY clause or aggregate function");
            assertError("SELECT k+1 AS key,sum(i) FROM lp_group GROUP BY key+1", 48, "Invalid column: key");
            assertError("SELECT k,sum(i) FROM lp_group GROUP BY k,sum(i)", 41, "aggregate functions are not allowed in GROUP BY");
            assertError("SELECT k,sum(i) AS total FROM lp_group GROUP BY k ORDER BY total+1 DESC,k", 59, "Invalid column: total");
            assertError("SELECT sum(i) FROM lp_group GROUP BY k ORDER BY i", 48, "ORDER BY expressions must appear in select list. Invalid column: i");
            assertError("SELECT k FROM lp_group GROUP BY k ORDER BY missing", 43, "Invalid column: missing");
        });
    }

    @Test
    public void testKeyOnlyGroupsKeepHiddenKeysAndMultiplicity() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly("SELECT k FROM lp_group GROUP BY k ORDER BY k", """
                    k
                    null
                    1
                    2
                    3
                    """);
            assertRowsOnly("SELECT k+1 AS key FROM lp_group GROUP BY key ORDER BY key", """
                    key
                    null
                    2
                    3
                    4
                    """);
            assertRowsOnly("SELECT k FROM lp_group GROUP BY k,i ORDER BY k", """
                    k
                    null
                    1
                    1
                    2
                    2
                    3
                    """);
            assertRowsOnly(
                    "SELECT k+1 AS key,k FROM lp_group GROUP BY k+1,k ORDER BY key",
                    """
                            key	k
                            null	null
                            2	1
                            3	2
                            4	3
                            """
            );
            assertRowsOnly(
                    "SELECT s,label FROM lp_group GROUP BY label,s ORDER BY s,label",
                    """
                            s	label
                            \t
                            a	x
                            a	z
                            b	y
                            """
            );
        });
    }

    @Test
    public void testPostingIndexDistinctAndResidualFallback() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly("SELECT DISTINCT indexed FROM lp_group ORDER BY indexed", """
                    indexed
                    
                    a
                    b
                    """);
            assertRowsOnly(
                    "SELECT indexed FROM lp_group GROUP BY indexed ORDER BY indexed",
                    """
                            indexed
                            
                            a
                            b
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT indexed FROM lp_group WHERE ts>='2020-01-03' ORDER BY indexed",
                    """
                            indexed
                            
                            a
                            b
                            """
            );
            assertRowsOnly(
                    "SELECT DISTINCT indexed FROM lp_group WHERE ts>='2020-01-03' AND active ORDER BY indexed",
                    """
                            indexed
                            
                            b
                            """
            );
            assertPlanContains("SELECT DISTINCT indexed FROM lp_group", "PostingIndex op: distinct");
            assertPlanContains("SELECT DISTINCT indexed FROM lp_group WHERE ts>='2020-01-03'", "PostingIndex op: distinct");
            assertPlanExcludes("SELECT DISTINCT indexed FROM lp_group WHERE active", "PostingIndex");
        });
    }

    @Test
    public void testPostingRuntimeBoundsRebindAcrossSpecializationAndDeclinedPaths() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final long day = 1_578_009_600_000_000L;
            for (boolean isInitiallyNull : new boolean[]{false, true}) {
                for (int route = 0; route < 3; route++) {
                    bindVariableService.setTimestamp(0, isInitiallyNull ? Numbers.LONG_NULL : day);
                    final String key = route == 1 ? "s" : "indexed";
                    final String sql = "SELECT DISTINCT " + key + " FROM lp_group WHERE ts>=$1"
                            + (route == 2 ? " AND active" : "") + " ORDER BY " + key;
                    final String matchingDay = route == 2 ? key + "\n\nb\n" : key + "\n\na\nb\n";
                    final String matchingLater = route == 2 ? key + "\n\n" : key + "\n\na\n";
                    try (RecordCursorFactory actual = compile(sql)) {
                        if (route == 0 && !isInitiallyNull) {
                            final TextPlanSink plan = new TextPlanSink();
                            plan.of(actual, sqlExecutionContext);
                            TestUtils.assertContains(plan.getSink(), "PostingIndex op: distinct");
                        }
                        bindVariableService.setTimestamp(0, day);
                        assertRowsOnly(actual, matchingDay);
                        bindVariableService.setTimestamp(0, Numbers.LONG_NULL);
                        assertRowsOnly(actual, key + "\n");
                        bindVariableService.setTimestamp(0, day + 172_800_000_000L);
                        assertRowsOnly(actual, matchingLater);
                        bindVariableService.setTimestamp(0, day);
                        assertRowsOnly(actual, matchingDay);
                    }
                }
            }
        });
    }

    @Test
    public void testPrimitiveAggregatesWithImplicitAndExplicitGroups() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<String> columns = new ObjList<>("i", "l", "f", "d");
            {
                final int i = 0;
                final String column = columns.getQuick(i);
                final String select = "SELECT k,count(" + column + "),sum(" + column + "),min(" + column
                        + "),max(" + column + "),avg(" + column + ") FROM lp_group";
                assertRowsOnly(select + " GROUP BY k ORDER BY k", """
                        k	count	sum	min	max	avg
                        null	0	null	null	null	null
                        1	2	6	2	4	3.0
                        2	1	8	8	8	8.0
                        3	0	null	null	null	null
                        """);
                assertRowsOnly(select + " ORDER BY k", """
                        k	count	sum	min	max	avg
                        null	0	null	null	null	null
                        1	2	6	2	4	3.0
                        2	1	8	8	8	8.0
                        3	0	null	null	null	null
                        """);
                assertRowsOnly(
                        "SELECT count(" + column + "),sum(" + column + "),min(" + column
                                + "),max(" + column + "),avg(" + column + ") FROM lp_group",
                        """
                                count	sum	min	max	avg
                                3	14	2	8	4.666666666666667
                                """
                );
            }
            {
                final int i = 1;
                final String column = columns.getQuick(i);
                final String select = "SELECT k,count(" + column + "),sum(" + column + "),min(" + column
                        + "),max(" + column + "),avg(" + column + ") FROM lp_group";
                assertRowsOnly(select + " GROUP BY k ORDER BY k", """
                        k	count	sum	min	max	avg
                        null	0	null	null	null	null
                        1	2	20	10	10	10.0
                        2	2	40	20	20	20.0
                        3	1	30	30	30	30.0
                        """);
                assertRowsOnly(select + " ORDER BY k", """
                        k	count	sum	min	max	avg
                        null	0	null	null	null	null
                        1	2	20	10	10	10.0
                        2	2	40	20	20	20.0
                        3	1	30	30	30	30.0
                        """);
                assertRowsOnly(
                        "SELECT count(" + column + "),sum(" + column + "),min(" + column
                                + "),max(" + column + "),avg(" + column + ") FROM lp_group",
                        """
                                count	sum	min	max	avg
                                5	90	10	30	18.0
                                """
                );
            }
            {
                final int i = 2;
                final String column = columns.getQuick(i);
                final String select = "SELECT k,count(" + column + "),sum(" + column + "),min(" + column
                        + "),max(" + column + "),avg(" + column + ") FROM lp_group";
                assertRowsOnly(select + " GROUP BY k ORDER BY k", """
                        k	count	sum	min	max	avg
                        null	0	null	null	null	null
                        1	2	6.0	2.0	4.0	3.0
                        2	1	8.0	8.0	8.0	8.0
                        3	0	null	null	null	null
                        """);
                assertRowsOnly(select + " ORDER BY k", """
                        k	count	sum	min	max	avg
                        null	0	null	null	null	null
                        1	2	6.0	2.0	4.0	3.0
                        2	1	8.0	8.0	8.0	8.0
                        3	0	null	null	null	null
                        """);
                assertRowsOnly(
                        "SELECT count(" + column + "),sum(" + column + "),min(" + column
                                + "),max(" + column + "),avg(" + column + ") FROM lp_group",
                        """
                                count	sum	min	max	avg
                                3	14.0	2.0	8.0	4.666666666666667
                                """
                );
            }
            {
                final int i = 3;
                final String column = columns.getQuick(i);
                final String select = "SELECT k,count(" + column + "),sum(" + column + "),min(" + column
                        + "),max(" + column + "),avg(" + column + ") FROM lp_group";
                assertRowsOnly(select + " GROUP BY k ORDER BY k", """
                        k	count	sum	min	max	avg
                        null	0	null	null	null	null
                        1	2	6.0	2.0	4.0	3.0
                        2	1	8.0	8.0	8.0	8.0
                        3	0	null	null	null	null
                        """);
                assertRowsOnly(select + " ORDER BY k", """
                        k	count	sum	min	max	avg
                        null	0	null	null	null	null
                        1	2	6.0	2.0	4.0	3.0
                        2	1	8.0	8.0	8.0	8.0
                        3	0	null	null	null	null
                        """);
                assertRowsOnly(
                        "SELECT count(" + column + "),sum(" + column + "),min(" + column
                                + "),max(" + column + "),avg(" + column + ") FROM lp_group",
                        """
                                count	sum	min	max	avg
                                3	14.0	2.0	8.0	4.666666666666667
                                """
                );
            }
        });
    }

    @Test
    public void testRostiAndAsyncAlgorithmSelection() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final boolean wasParallel = sqlExecutionContext.isParallelGroupByEnabled();
            sqlExecutionContext.setParallelGroupByEnabled(true);
            try {
                assertPlanContains("SELECT k,sum(i) FROM lp_group GROUP BY k", "GroupBy vectorized: true");
                assertPlanContains("SELECT k FROM lp_group GROUP BY k", "GroupBy vectorized: true");
                assertPlanContains("SELECT DISTINCT k FROM lp_group", "GroupBy vectorized: true");
                assertPlanContains("SELECT sum(i) FROM lp_group", "Async Group By");
                assertPlanContains("SELECT l,sum(i) FROM lp_group GROUP BY l", "Async Group By");
                assertPlanContains("SELECT k+1 AS key,sum(i) FROM lp_group GROUP BY key", "Async Group By");
                assertPlanContains("SELECT k,l,sum(i) FROM lp_group GROUP BY k,l", "Async Group By");
                assertPlanContains("SELECT l FROM lp_group GROUP BY l", "Async Group By");
                assertRowsOnly("SELECT l,sum(i) FROM lp_group GROUP BY l ORDER BY l", """
                        l	sum
                        null	null
                        10	6
                        20	8
                        30	null
                        """);
                assertRowsOnly(
                        "SELECT k+1 AS key,sum(i) FROM lp_group GROUP BY key ORDER BY key",
                        """
                                key	sum
                                null	null
                                2	6
                                3	8
                                4	null
                                """
                );
            } finally {
                sqlExecutionContext.setParallelGroupByEnabled(wasParallel);
            }
        });
    }

    @Test
    public void testSerialAlgorithmSelection() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final boolean wasParallel = sqlExecutionContext.isParallelGroupByEnabled();
            sqlExecutionContext.setParallelGroupByEnabled(false);
            try {
                assertPlanContains("SELECT k,sum(i) FROM lp_group GROUP BY k", "GroupBy vectorized: false");
                assertPlanContains("SELECT l,sum(i) FROM lp_group GROUP BY l", "GroupBy vectorized: false");
                assertPlanContains("SELECT sum(i),avg(d) FROM lp_group", "GroupBy vectorized: false");
                assertRowsOnly("SELECT k,sum(i),avg(d) FROM lp_group GROUP BY k ORDER BY k", """
                        k	sum	avg
                        null	null	null
                        1	6	3.0
                        2	8	8.0
                        3	null	null
                        """);
                assertRowsOnly("SELECT l,sum(i),avg(d) FROM lp_group GROUP BY l ORDER BY l", """
                        l	sum	avg
                        null	null	null
                        10	6	3.0
                        20	8	8.0
                        30	null	null
                        """);
            } finally {
                sqlExecutionContext.setParallelGroupByEnabled(wasParallel);
            }
        });
    }

    @Test
    public void testSingleRowOrderKeepsDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE single_row (k SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO single_row VALUES ('a', '2024-01-01'), ('b', '2024-01-02')");
            assertQuery("SELECT 'Z' AS e0, last(ts) AS a0 FROM single_row ORDER BY e0 LIMIT 27")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("a0")
                    .returns("""
                            e0\ta0
                            Z\t2024-01-02T00:00:00.000000Z
                            """);
        });
    }

    private static boolean hasAggregate(LogicalPlan plan) {
        if (plan instanceof AggregatePlan) {
            return true;
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            if (hasAggregate(plan.inputAt(i))) {
                return true;
            }
        }
        return false;
    }

    private void assertPlanContains(String sql, String planPart) throws Exception {
        try (RecordCursorFactory factory = compile(sql)) {
            final TextPlanSink planSink = new TextPlanSink();
            planSink.of(factory, sqlExecutionContext);
            TestUtils.assertContains(planSink.getSink(), planPart);
        }
    }

    private void assertError(String sql, int position, String message) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                Assert.fail(sql);
            } catch (SqlException e) {
                Assert.assertEquals(sql, position, e.getPosition());
                TestUtils.assertEquals(message, e.getFlyweightMessage());
            }
            try (RecordCursorFactory recovered = compiler.compile(
                    "SELECT k,count() FROM lp_group GROUP BY k ORDER BY k", sqlExecutionContext
            ).getRecordCursorFactory()) {
                assertRowsOnly(recovered, "k\tcount\nnull\t1\n1\t2\n2\t2\n3\t1\n");
            }
        }
    }

    private void assertPlanExcludes(String sql, String planPart) throws Exception {
        try (RecordCursorFactory factory = compile(sql)) {
            final TextPlanSink planSink = new TextPlanSink();
            planSink.of(factory, sqlExecutionContext);
            Assert.assertFalse(planSink.getSink().toString(), planSink.getSink().toString().contains(planPart));
        }
    }

    private RecordCursorFactory compile(String sql) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            final RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
            try {
                Assert.assertTrue(sql, hasAggregate(compiler.getPlanForTesting()));
            } catch (Throwable th) {
                factory.close();
                throw th;
            }
            return factory;
        }
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_group (k INT,l LONG,i INT,f FLOAT,d DOUBLE,s SYMBOL,indexed SYMBOL INDEX TYPE POSTING,label STRING,active BOOLEAN,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO lp_group VALUES
                (1,10,2,2,2,'a','a','x',true,'2020-01-01'),
                (1,10,4,4,4,'a','a','x',false,'2020-01-02'),
                (2,20,null,null,null,'b','b','y',true,'2020-01-03'),
                (2,20,8,8,8,'b','b','y',true,'2020-01-04'),
                (null,null,null,null,null,null,null,null,true,'2020-01-05'),
                (3,30,null,null,null,'a','a','z',false,'2020-01-06')
                """);
    }
}
