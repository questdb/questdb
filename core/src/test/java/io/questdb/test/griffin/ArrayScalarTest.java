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
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ArrayScalarTest extends AbstractCairoTest {
    @Test
    public void testReducersPreserveNullEmptyAndStridedValues() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly(
                    "SELECT id,array_sum(a),array_avg(a),array_min(a),array_max(a),array_count(a),"
                            + "array_stddev(a),array_stddev_samp(a),array_stddev_pop(a) FROM lp_array_scalar ORDER BY id",
                    """
                            id	array_sum	array_avg	array_min	array_max	array_count	array_stddev	array_stddev_samp	array_stddev_pop
                            1	6.0	2.0	1.0	3.0	3	1.0	1.0	0.816496580927726
                            2	4.0	2.0	-1.0	5.0	2	4.242640687119285	4.242640687119285	3.0
                            3	null	null	null	null	0	null	null	null
                            4	null	null	null	null	0	null	null	null
                            """
            );
            assertRowsOnly(
                    "SELECT id,array_sum(m),array_avg(m),array_count(m),array_stddev_pop(m),"
                            + "array_min(m[1:3,2]),array_max(m[1:3,2]),array_sum(m[1:3,2]),"
                            + "array_avg(m[1:3,2]),array_stddev(m[1:3,2]),array_count(m[1:3,2]) FROM lp_array_scalar ORDER BY id",
                    """
                            id	array_sum	array_avg	array_count	array_stddev_pop	array_min	array_max	array_sum1	array_avg1	array_stddev	array_count1
                            1	12.0	3.0	4	1.8708286933869707	1.0	2.0	3.0	1.5	0.7071067811865476	2
                            2	26.0	6.5	4	1.118033988749895	6.0	8.0	14.0	7.0	1.4142135623730951	2
                            3	null	null	0	null	null	null	null	null	null	0
                            4	null	null	0	null	null	null	null	null	null	0
                            """
            );
            assertRowsOnly(
                    "SELECT array_sum(ARRAY[1.0,NULL,3.0]),array_avg(ARRAY[1.0,NULL,3.0]),"
                            + "array_count(ARRAY[1.0,NULL,3.0]),array_stddev_samp(ARRAY[1.0,3.0]),"
                            + "array_stddev_pop(ARRAY[1.0,3.0]),array_sum(ARRAY[]::DOUBLE[]),"
                            + "array_min(ARRAY[]::DOUBLE[]),array_max(ARRAY[]::DOUBLE[])",
                    """
                            array_sum	array_avg	array_count	array_stddev_samp	array_stddev_pop	array_sum1	array_min	array_max
                            4.0	2.0	2	1.4142135623730951	1.0	null	null	null
                            """
            );
            assertRowsOnly(
                    "SELECT id FROM lp_array_scalar WHERE array_sum(a)>0 AND array_count(a)>1 ORDER BY id",
                    """
                            id
                            1
                            2
                            """
            );
            assertRowsOnly(
                    "SELECT sum(array_sum(a)),avg(array_avg(a)),max(array_max(a)) FROM lp_array_scalar",
                    """
                            sum	avg	max
                            10.0	2.0	5.0
                            """
            );
        });
    }

    @Test
    public void testSortReverseAndConstantFlagsKeepArrayShapes() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly(
                    "SELECT id,array_reverse(a),array_sort(a),array_sort(a,false),array_sort(a,true),"
                            + "array_sort(a,false,true),array_sort(a,true,false),array_sort(a,true,true) FROM lp_array_scalar ORDER BY id",
                    """
                            id	array_reverse	array_sort	array_sort1	array_sort2	array_sort3	array_sort4	array_sort5
                            1	[2.0,1.0,null,3.0]	[1.0,2.0,3.0,null]	[1.0,2.0,3.0,null]	[null,3.0,2.0,1.0]	[null,1.0,2.0,3.0]	[3.0,2.0,1.0,null]	[null,3.0,2.0,1.0]
                            2	[5.0,-1.0]	[-1.0,5.0]	[-1.0,5.0]	[5.0,-1.0]	[-1.0,5.0]	[5.0,-1.0]	[5.0,-1.0]
                            3	null	null	null	null	null	null	null
                            4	[]	[]	[]	[]	[]	[]	[]
                            """
            );
            assertRowsOnly(
                    "SELECT id,array_reverse(m),array_sort(m),array_sort(m[1:3,2],true),"
                            + "array_reverse(array_sort(m[1:3,2],false,true)) FROM lp_array_scalar ORDER BY id",
                    """
                            id	array_reverse	array_sort	array_sort1	array_reverse1
                            1	[[2.0,6.0],[1.0,3.0]]	[[2.0,6.0],[1.0,3.0]]	[2.0,1.0]	[2.0,1.0]
                            2	[[6.0,5.0],[8.0,7.0]]	[[5.0,6.0],[7.0,8.0]]	[8.0,6.0]	[8.0,6.0]
                            3	null	null	null	null
                            4	null	null	null	null
                            """
            );
            assertRowsOnly(
                    "SELECT array_sort(ARRAY[3.0,NULL,1.0,2.0],true,false),"
                            + "array_reverse(ARRAY[3.0,NULL,1.0,2.0]),array_sort(ARRAY[]::DOUBLE[]),array_reverse(NULL::DOUBLE[])",
                    """
                            array_sort	array_reverse	array_sort1	array_reverse1
                            [3.0,2.0,1.0,null]	[2.0,1.0,null,3.0]	[]	null
                            """
            );
            assertRowsOnly(
                    "SELECT t.id,u.value FROM lp_array_scalar t,UNNEST(array_sort(t.a)) u ORDER BY t.id,u.value",
                    """
                            id	value
                            1	1.0
                            1	2.0
                            1	3.0
                            1	null
                            2	-1.0
                            2	5.0
                            """
            );
            assertRowsOnly("SELECT * FROM UNNEST(array_reverse(ARRAY[1.0,2.0,3.0])) u", """
                    value
                    3.0
                    2.0
                    1.0
                    """);
        });
    }

    @Test
    public void testDimensionLengthWithPruningNullAndRuntimeDimension() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsOnly(
                    "SELECT id,dim_length(a,1),dim_length(m,1),dim_length(m,2),dim_length(m,NULL::INT) FROM lp_array_scalar ORDER BY id",
                    """
                            id	dim_length	dim_length1	dim_length2	dim_length3
                            1	4	2	2	null
                            2	2	2	2	null
                            3	null	null	null	null
                            4	0	null	null	null
                            """
            );
            assertRowsOnly(
                    "SELECT dim_length(renamed,2)+1 n FROM (SELECT m renamed FROM lp_array_scalar) ORDER BY n",
                    """
                            n
                            null
                            null
                            3
                            3
                            """
            );
            assertRowsOnly(
                    "SELECT dim_length(array_reverse(m),1),dim_length(m[1:3,2],1),"
                            + "dim_length(ARRAY[1.0,2.0],NULL::INT) FROM lp_array_scalar",
                    """
                            dim_length	dim_length1	dim_length2
                            2	2	null
                            2	2	null
                            null	null	null
                            null	null	null
                            """
            );
            bindVariableService.setInt(0, 2);
            assertRowsOnly(
                    "SELECT id,dim_length(m,$1) FROM lp_array_scalar ORDER BY id",
                    """
                            id	dim_length
                            1	2
                            2	2
                            3	null
                            4	null
                            """
            );
            bindVariableService.setInt(0, 1);
            assertRowsOnly(
                    "SELECT id,dim_length(m,$1) FROM lp_array_scalar ORDER BY id",
                    """
                            id	dim_length
                            1	2
                            2	2
                            3	null
                            4	null
                            """
            );
            assertRowsOnly(
                    "SELECT dim_length(ARRAY[]::DOUBLE[],$1) FROM lp_array_scalar",
                    """
                            dim_length
                            0
                            0
                            0
                            0
                            """
            );
            bindVariableService.setInt(0, Numbers.INT_NULL);
            assertRowsOnly("SELECT dim_length(m,$1) FROM lp_array_scalar", """
                    dim_length
                    null
                    null
                    null
                    null
                    """);
        });
    }

    @Test
    public void testRetainedFactoriesSurviveCompilerResetAndParameterRebinding() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setDouble(0, 9);
            bindVariableService.setInt(1, 1);
            final String sql = "SELECT id,array_sort(ARRAY[id::DOUBLE,$1],true),array_reverse(a),"
                    + "array_avg(a),dim_length(m,$2) FROM lp_array_scalar ORDER BY id";
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                    compiler.clear();
                    try (RecordCursorFactory recovery = compiler.compile("SELECT count() FROM lp_array_scalar", sqlExecutionContext).getRecordCursorFactory()) {
                        assertRowsOnly(recovery, "count\n4\n");
                    }
                }
                for (int pass = 0; pass < 3; pass++) {
                    bindVariableService.setDouble(0, pass - 2);
                    bindVariableService.setInt(1, pass == 2 ? Numbers.INT_NULL : pass + 1);
                    try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                         RecordCursorFactory baseline = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        assertRowsOnly(retained, printFactory(baseline));
                    }
                }
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testValidationAndNativeConstantFailureCleanup() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT dim_length(ARRAY[1.0,2.0],2)").noLeakCheck().fails(33, "array dimension out of bounds [dim=2, dims=1]");
            assertQuery("SELECT dim_length(array_sort(ARRAY[2.0,1.0]),2)").noLeakCheck().fails(45, "array dimension out of bounds [dim=2, dims=1]");
            assertQuery("SELECT dim_length(m,0) FROM lp_array_scalar").noLeakCheck().fails(20, "array dimension out of bounds [dim=0]");
            assertQuery("SELECT array_sort(a,id>0) FROM lp_array_scalar").noLeakCheck().fails(7, "there is no matching function `array_sort` with the argument types: (DOUBLE[], BOOLEAN)");
            assertQuery("SELECT array_sort(a,true,id>0) FROM lp_array_scalar").noLeakCheck().fails(7, "there is no matching function `array_sort` with the argument types: (DOUBLE[], BOOLEAN, BOOLEAN)");
            assertQuery("SELECT array_sum(ARRAY[])").noLeakCheck().fails(22, "argument type mismatch for function `array_sum` at #1 expected: DOUBLE, actual: ARRAY");
            assertQuery("SELECT array_min(ARRAY[])").noLeakCheck().fails(22, "argument type mismatch for function `array_min` at #1 expected: DOUBLE, actual: ARRAY");
            assertQuery("SELECT array_max(ARRAY[])").noLeakCheck().fails(22, "argument type mismatch for function `array_max` at #1 expected: DOUBLE, actual: ARRAY");
            assertQuery("SELECT array_sort(ARRAY[])").noLeakCheck().fails(7, "there is no matching function `array_sort` with the argument types: (ARRAY)");
            assertRowsOnly(
                    "SELECT dim_length(array_sort(ARRAY[3.0,1.0]),NULL::INT),"
                            + "array_sum(array_reverse(ARRAY[3.0,1.0]))",
                    """
                            dim_length	array_sum
                            null	4.0
                            """
            );
        });
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_array_scalar(unused INT,id INT,a DOUBLE[],m DOUBLE[][])");
        execute("INSERT INTO lp_array_scalar VALUES"
                + "(91,1,ARRAY[3.0,NULL,1.0,2.0],ARRAY[ARRAY[6.0,2.0],ARRAY[3.0,1.0]]),"
                + "(92,2,ARRAY[-1.0,5.0],ARRAY[ARRAY[5.0,6.0],ARRAY[7.0,8.0]]),"
                + "(93,3,NULL,NULL),(94,4,ARRAY[],NULL)");
    }
}
