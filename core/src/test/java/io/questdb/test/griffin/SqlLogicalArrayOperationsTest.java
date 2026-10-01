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

import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class SqlLogicalArrayOperationsTest extends AbstractCairoTest {
    @Test
    public void testSearchAndEqualityPreserveNullsShapesAndSlices() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,array_position(a,2.0),array_position(a,NULL),"
                    + "insertion_point(array_sort(a),2.0),insertion_point(array_sort(a),2.0,true),"
                    + "insertion_point(array_sort(a),2.0,false) FROM lp_array_ops ORDER BY id",
                    """
                            id	array_position	array_position1	insertion_point	insertion_point1	insertion_point2
                            1	2	4	4	2	4
                            2	null	null	null	null	null
                            3	null	-2147483647	1	1	1
                            4	null	-2147483647	2	1	2
                            5	null	-2147483647	1	1	1
                            6	null	null	null	null	null
                            """
            );
            assertQueryRows(
                    "SELECT id,array_position(m[1:3,2],2.5),insertion_point(m[1:3,2],2.5),"
                    + "a=a,a!=a,a<>a,a=NULL::DOUBLE[],a=m FROM lp_array_ops ORDER BY id",
                    """
                            id	array_position	insertion_point	column	column1	column2	column3	column4
                            1	1	2	true	false	false	false	false
                            2	null	null	true	false	false	false	false
                            3	null	1	true	false	false	false	false
                            4	null	1	true	false	false	false	false
                            5	null	null	true	false	false	false	false
                            6	null	null	true	false	false	false	false
                            """
            );
            assertQueryRows(
                    "SELECT array_position(ARRAY[]::DOUBLE[],1.0),insertion_point(ARRAY[]::DOUBLE[],1.0),"
                    + "ARRAY[1.0,NULL]=ARRAY[1.0,NULL],ARRAY[1.0]<>ARRAY[2.0]",
                    """
                            array_position	insertion_point	column	column1
                            null	2	true	true
                            """
            );
            bindVariableService.setDouble(0, 2);
            bindVariableService.setBoolean(1, true);
            assertQueryRows(
                    "SELECT id,array_position(a,$1),insertion_point(array_sort(a),$1,$2) FROM lp_array_ops ORDER BY id",
                    """
                            id	array_position	insertion_point
                            1	2	2
                            2	null	null
                            3	null	1
                            4	null	1
                            5	null	1
                            6	null	null
                            """
            );
            assertQueryRows(
                    "SELECT id FROM lp_array_ops WHERE array_position(a,$1)>0 AND a<>ARRAY[]::DOUBLE[] ORDER BY id",
                    """
                            id
                            1
                            """
            );
        });
    }

    @Test
    public void testFlattenNullDoesNotReusePreviousRowsBuffer() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    try (RecordCursorFactory constant = compiler.compile(
                            "SELECT flatten(NULL::DOUBLE[][]) f", sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(constant, "f\nnull\n");
                    }
                    retained = compiler.compile("SELECT id,flatten(transpose(m)) f FROM lp_array_ops ORDER BY id",
                            sqlExecutionContext).getRecordCursorFactory();
                }
                assertResult(retained, "id\tf\n"
                        + "1\t[1.25,3.75,2.5,4.5]\n"
                        + "2\tnull\n"
                        + "3\t[5.0,7.0,6.0,8.0]\n"
                        + "4\t[9.0,11.0,10.0,12.0]\n"
                        + "5\tnull\n"
                        + "6\tnull\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testTransformsPreserveMultidimensionalAndStridedValues() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id,flatten(m),transpose(m),flatten(transpose(m)),array_cum_sum(m),"
                    + "array_cum_sum(m[1:3,2]),shift(m,1),shift(m,-1,99.0),round(m,1) FROM lp_array_ops ORDER BY id",
                    """
                            id	flatten	transpose	flatten1	array_cum_sum	array_cum_sum1	shift	shift1	round
                            1	[1.25,2.5,3.75,4.5]	[[1.25,3.75],[2.5,4.5]]	[1.25,3.75,2.5,4.5]	[1.25,3.75,7.5,12.0]	[2.5,7.0]	[[null,1.25],[null,3.75]]	[[2.5,99.0],[4.5,99.0]]	[[1.3,2.5],[3.8000000000000003,4.5]]
                            2	null	null	null	null	null	null	null	null
                            3	[5.0,6.0,7.0,8.0]	[[5.0,7.0],[6.0,8.0]]	[5.0,7.0,6.0,8.0]	[5.0,11.0,18.0,26.0]	[6.0,14.0]	[[null,5.0],[null,7.0]]	[[6.0,99.0],[8.0,99.0]]	[[5.0,6.0],[7.0,8.0]]
                            4	[9.0,10.0,11.0,12.0]	[[9.0,11.0],[10.0,12.0]]	[9.0,11.0,10.0,12.0]	[9.0,19.0,30.0,42.0]	[10.0,22.0]	[[null,9.0],[null,11.0]]	[[10.0,99.0],[12.0,99.0]]	[[9.0,10.0],[11.0,12.0]]
                            5	null	null	null	null	null	null	null	null
                            6	null	null	null	null	null	null	null	null
                            """
            );
            assertQueryRows(
                    "SELECT id,shift(a,0),shift(a,1),shift(a,-1,99.0),shift(a,99),"
                    + "round(a,-1),round(a,999),round(a,NULL::INT),array_cum_sum(a) FROM lp_array_ops ORDER BY id",
                    """
                            id	shift	shift1	shift2	shift3	round	round1	round2	array_cum_sum
                            1	[1.0,2.0,2.0,null]	[null,1.0,2.0,2.0]	[2.0,2.0,null,99.0]	[null,null,null,null]	[0.0,0.0,0.0,null]	[null,null,null,null]	[null,null,null,null]	[1.0,3.0,5.0,5.0]
                            2	null	null	null	null	null	null	null	null
                            3	[3.0,4.0]	[null,3.0]	[4.0,99.0]	[null,null]	[0.0,0.0]	[null,null]	[null,null]	[3.0,7.0]
                            4	[]	[]	[]	[]	[]	[]	[]	null
                            5	[5.0,6.0]	[null,5.0]	[6.0,99.0]	[null,null]	[10.0,10.0]	[null,null]	[null,null]	[5.0,11.0]
                            6	null	null	null	null	null	null	null	null
                            """
            );
            assertQueryRows(
                    "SELECT flatten(ARRAY[ARRAY[1.0,2.0],ARRAY[3.0,4.0]]),"
                    + "flatten(NULL::DOUBLE[][]),"
                    + "transpose(ARRAY[ARRAY[1.0,2.0],ARRAY[3.0,4.0]]),array_cum_sum(ARRAY[1.0,NULL,3.0]),"
                    + "shift(ARRAY[1.0,2.0],1,9.0),round(ARRAY[1.25,2.55],1)",
                    """
                            flatten	flatten1	transpose	array_cum_sum	shift	round
                            [1.0,2.0,3.0,4.0]	null	[[1.0,3.0],[2.0,4.0]]	[1.0,1.0,4.0]	[9.0,1.0]	[1.3,2.6]
                            """
            );
            assertQueryRows(
                    "SELECT id,shift(m[1:3,2],-1),round(m[1:3,2],1),"
                    + "array_sum(flatten(transpose(m))) FROM lp_array_ops ORDER BY id",
                    """
                            id	shift	round	array_sum
                            1	[4.5,null]	[2.5,4.5]	12.0
                            2	null	null	null
                            3	[8.0,null]	[6.0,8.0]	26.0
                            4	[12.0,null]	[10.0,12.0]	42.0
                            5	null	null	null
                            6	null	null	null
                            """
            );
            assertQueryRows(
                    "SELECT t.id,u.value FROM lp_array_ops t,UNNEST(flatten(transpose(t.m))) u ORDER BY t.id,u.value",
                    """
                            id	value
                            1	1.25
                            1	2.5
                            1	3.75
                            1	4.5
                            3	5.0
                            3	6.0
                            3	7.0
                            3	8.0
                            4	9.0
                            4	10.0
                            4	11.0
                            4	12.0
                            """
            );
        });
    }

    @Test
    public void testArrayAggregatesKeepNullEmptyGroupingAndFullOutputTypes() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT k,array_agg(d),array_agg(a),first(a),last(a),first_not_null(a),last_not_null(a) "
                    + "FROM lp_array_ops GROUP BY k ORDER BY k",
                    """
                            k	array_agg	array_agg1	first	last	first_not_null	last_not_null
                            1	[1.25,null,5.5,null]	[1.0,2.0,2.0,null,5.0,6.0]	[1.0,2.0,2.0,null]	null	[1.0,2.0,2.0,null]	[5.0,6.0]
                            2	[3.75,4.25]	[3.0,4.0]	[3.0,4.0]	[]	[3.0,4.0]	[]
                            """
            );
            assertQueryRows(
                    "SELECT first(m),last(m),first_not_null(m),last_not_null(m),"
                    + "array_agg(m[1:3,2]) FROM lp_array_ops",
                    """
                            first	last	first_not_null	last_not_null	array_agg
                            [[1.25,2.5],[3.75,4.5]]	null	[[1.25,2.5],[3.75,4.5]]	[[9.0,10.0],[11.0,12.0]]	[2.5,4.5,6.0,8.0,10.0,12.0]
                            """
            );
            assertQueryRows(
                    "SELECT array_agg(d),array_agg(a),first(a),last(a),first_not_null(a),last_not_null(a) "
                    + "FROM lp_array_ops WHERE id<0",
                    """
                            array_agg	array_agg1	first	last	first_not_null	last_not_null
                            null	null	null	null	null	null
                            """
            );
            assertQueryRows(
                    "SELECT array_agg(ARRAY[]::DOUBLE[]),array_agg(NULL::DOUBLE[]),"
                    + "first(ARRAY[]::DOUBLE[]),last_not_null(ARRAY[]::DOUBLE[]) FROM lp_array_ops",
                    """
                            array_agg	array_agg1	first	last_not_null
                            null	null	[]	[]
                            """
            );
            assertQueryRows(
                    "SELECT array_sum(vals),dim_length(vals,1) FROM (SELECT array_agg(a) vals FROM lp_array_ops)",
                    """
                            array_sum	dim_length
                            23.0	8
                            """
            );
            assertQueryRows(
                    "SELECT k,array_agg(array_reverse(a)),first(shift(a,1)),last_not_null(round(a,1)) "
                    + "FROM lp_array_ops GROUP BY k ORDER BY k",
                    """
                            k	array_agg	first	last_not_null
                            1	[null,2.0,2.0,1.0,6.0,5.0]	[null,1.0,2.0,2.0]	[5.0,6.0]
                            2	[4.0,3.0]	[null,3.0]	[]
                            """
            );
        });
    }

    @Test
    public void testOrderedSubqueriesRemainVisibleToArrayAggregates() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT array_agg(d),array_agg(a) "
                    + "FROM (SELECT d,a FROM lp_array_ops ORDER BY id DESC)",
                    """
                            array_agg	array_agg1
                            [null,5.5,4.25,3.75,null,1.25]	[5.0,6.0,3.0,4.0,1.0,2.0,2.0,null]
                            """
            );
            assertQueryRows(
                    "SELECT array_agg(d),array_agg(a),first(a),last(a),first_not_null(a),last_not_null(a) "
                    + "FROM (SELECT d,a FROM lp_array_ops ORDER BY id DESC)",
                    """
                            array_agg	array_agg1	first	last	first_not_null	last_not_null
                            [null,5.5,4.25,3.75,null,1.25]	[5.0,6.0,3.0,4.0,1.0,2.0,2.0,null]	null	[1.0,2.0,2.0,null]	[5.0,6.0]	[1.0,2.0,2.0,null]
                            """
            );
            assertQueryRows(
                    "SELECT k,array_agg(d),array_agg(a),first_not_null(a),last_not_null(a) "
                    + "FROM (SELECT k,d,a FROM lp_array_ops ORDER BY id DESC) GROUP BY k ORDER BY k",
                    """
                            k	array_agg	array_agg1	first_not_null	last_not_null
                            1	[null,5.5,null,1.25]	[5.0,6.0,1.0,2.0,2.0,null]	[5.0,6.0]	[1.0,2.0,2.0,null]
                            2	[4.25,3.75]	[3.0,4.0]	[]	[3.0,4.0]
                            """
            );
            assertQueryRows(
                    "SELECT array_agg(d),array_agg(a),first_not_null(a),last_not_null(a) "
                    + "FROM (SELECT d,a FROM lp_array_ops ORDER BY id DESC LIMIT 3)",
                    """
                            array_agg	array_agg1	first_not_null	last_not_null
                            [null,5.5,4.25]	[5.0,6.0]	[5.0,6.0]	[]
                            """
            );
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                String expectedPlan = null;
                for (int reuse = 0; reuse < 2; reuse++) {
                    try (RecordCursorFactory factory = compiler.compile("SELECT array_agg(id::DOUBLE) vals "
                            + "FROM (SELECT id FROM lp_array_ops ORDER BY id DESC)", sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(factory, "vals\n[6.0,5.0,4.0,3.0,2.0,1.0]\n");
                        final TextPlanSink plan = new TextPlanSink();
                        plan.of(factory, sqlExecutionContext);
                        if (reuse == 0) {
                            expectedPlan = plan.getSink().toString();
                        } else {
                            TestUtils.assertEquals(expectedPlan, plan.getSink());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSamplingUsesExistingArrayAggregateRuntime() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT ts,array_agg(d),array_agg(a),first_not_null(a),last_not_null(a) "
                    + "FROM lp_array_ops SAMPLE BY 1h FILL(NONE)",
                    """
                            ts	array_agg	array_agg1	first_not_null	last_not_null
                            2026-01-01T00:00:00.000000Z	[1.25,null]	[1.0,2.0,2.0,null]	[1.0,2.0,2.0,null]	[1.0,2.0,2.0,null]
                            2026-01-01T01:00:00.000000Z	[3.75,4.25]	[3.0,4.0]	[3.0,4.0]	[]
                            2026-01-01T02:00:00.000000Z	[5.5,null]	[5.0,6.0]	[5.0,6.0]	[5.0,6.0]
                            """
            );
            assertQueryRows(
                    "SELECT ts,k,array_agg(a),first(m),last(m) FROM lp_array_ops SAMPLE BY 1h FILL(NONE)",
                    """
                            ts	k	array_agg	first	last
                            2026-01-01T00:00:00.000000Z	1	[1.0,2.0,2.0,null]	[[1.25,2.5],[3.75,4.5]]	null
                            2026-01-01T01:00:00.000000Z	2	[3.0,4.0]	[[5.0,6.0],[7.0,8.0]]	[[9.0,10.0],[11.0,12.0]]
                            2026-01-01T02:00:00.000000Z	1	[5.0,6.0]	null	null
                            """
            );
        });
    }

    @Test
    public void testRetainedFactoriesReopenAfterCompilerCloseAndParameterRebinding() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setInt(0, 1);
            bindVariableService.setDouble(1, 9);
            final String sql = "SELECT k,array_agg(array_cum_sum(shift(a,$1,$2))),"
                    + "first(round(a,$1)),last_not_null(transpose(m)) FROM lp_array_ops GROUP BY k ORDER BY k";
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    compiler.clear();
                    try (RecordCursorFactory recovery = compiler.compile("SELECT count() FROM lp_array_ops", sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(recovery, "count\n6\n");
                    }
                }
                for (int pass = 0; pass < 3; pass++) {
                    bindVariableService.setInt(0, pass - 1);
                    bindVariableService.setDouble(1, pass == 2 ? Double.NaN : pass + 7);
                    try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                         RecordCursorFactory baseline = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        assertResult(retained, print(baseline));
                    }
                }
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testDimensionErrorsReleaseNativeArgumentsAndCompilerRecovers() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT array_position(m,1.0) FROM lp_array_ops").noLeakCheck().fails(22, "array is not one-dimensional");
            assertQuery("SELECT insertion_point(m,1.0) FROM lp_array_ops").noLeakCheck().fails(23, "array is not one-dimensional");
            assertQuery("SELECT insertion_point(m,1.0,true) FROM lp_array_ops").noLeakCheck().fails(23, "array is not one-dimensional");
            assertQuery("SELECT array_agg(m) FROM lp_array_ops").noLeakCheck().fails(17, "array is not one-dimensional");
            assertQuery("SELECT ts,array_agg(a) FROM lp_array_ops SAMPLE BY 1h FILL(LINEAR)").noLeakCheck().fails(59, "support for LINEAR fill is not yet implemented [function=array_agg(a), class=io.questdb.griffin.engine.functions.groupby.ArrayAggDoubleArrayGroupByFunction]");
            assertQuery("SELECT ts,first(a) FROM lp_array_ops SAMPLE BY 1h FILL(1)").noLeakCheck().fails(55, "support for VALUE fill is not yet implemented [function=first(a), class=io.questdb.griffin.engine.functions.groupby.FirstArrayGroupByFunction]");
            assertQuery("SELECT array_position(transpose(ARRAY[ARRAY[1.0,2.0],ARRAY[3.0,4.0]]),1.0)").noLeakCheck().fails(22, "array is not one-dimensional");
            assertQueryRows(
                    "SELECT array_sort(ARRAY[2.0,1.0])=transpose(ARRAY[ARRAY[1.0,2.0],ARRAY[3.0,4.0]])",
                    """
                            column
                            false
                            """
            );
            bindVariableService.setInt(0, Numbers.INT_NULL);
            assertQueryRows("SELECT id,round(a,$1) FROM lp_array_ops ORDER BY id", """
                    id	round
                    1	[null,null,null,null]
                    2	null
                    3	[null,null]
                    4	[]
                    5	[null,null]
                    6	null
                    """);
        });
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_array_ops(unused INT,id INT,k INT,d DOUBLE,a DOUBLE[],m DOUBLE[][],ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_array_ops VALUES"
                + "(91,1,1,1.25,ARRAY[1.0,2.0,2.0,NULL],ARRAY[ARRAY[1.25,2.5],ARRAY[3.75,4.5]],'2026-01-01T00:00:00Z'),"
                + "(92,2,1,NULL,NULL,NULL,'2026-01-01T00:30:00Z'),"
                + "(93,3,2,3.75,ARRAY[3.0,4.0],ARRAY[ARRAY[5.0,6.0],ARRAY[7.0,8.0]],'2026-01-01T01:00:00Z'),"
                + "(94,4,2,4.25,ARRAY[],ARRAY[ARRAY[9.0,10.0],ARRAY[11.0,12.0]],'2026-01-01T01:30:00Z'),"
                + "(95,5,1,5.5,ARRAY[5.0,6.0],NULL,'2026-01-01T02:00:00Z'),"
                + "(96,6,1,NULL,NULL,NULL,'2026-01-01T02:30:00Z')");
    }

    private String print(RecordCursorFactory factory) throws Exception {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final StringSink sink = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
            return sink.toString();
        }
    }
    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
