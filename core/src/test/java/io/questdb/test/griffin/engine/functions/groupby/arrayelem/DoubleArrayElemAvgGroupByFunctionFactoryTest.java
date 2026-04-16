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

package io.questdb.test.griffin.engine.functions.groupby.arrayelem;

import org.junit.Test;

public class DoubleArrayElemAvgGroupByFunctionFactoryTest extends AbstractDoubleArrayElemGroupByFunctionTest {

    @Override
    protected String funcName() {
        return "array_elem_avg";
    }

    @Test
    public void test2dAllSameShape() throws Exception {
        assertGroupByTyped("DOUBLE[][]", "[[5.0,6.0],[7.0,8.0]]",
                "ARRAY[[1.0, 2.0], [3.0, 4.0]]",
                "ARRAY[[5.0, 6.0], [7.0, 8.0]]",
                "ARRAY[[9.0, 10.0], [11.0, 12.0]]"
        );
    }

    @Test
    public void test2dAvgShapeGrowthPlusCounts() throws Exception {
        // row1=(2,2) all finite, row2=(3,3) with NaN at [0][0]
        // [0][0] count=1 (only row1), positions [2][*] count=1 (only row2)
        assertGroupByTyped("DOUBLE[][]", "[[1.0,11.0,30.0],[21.5,27.0,60.0],[70.0,80.0,90.0]]",
                "ARRAY[[1.0, 2.0], [3.0, 4.0]]",
                "ARRAY[[null, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]]"
        );
    }

    @Test
    public void test2dFirstRowNull() throws Exception {
        assertGroupByTyped("DOUBLE[][]", "[[5.5,11.0,3.0],[17.0,22.5,6.0],[50.0,60.0,null]]",
                "null",
                "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]",
                "ARRAY[[10.0, 20.0], [30.0, 40.0], [50.0, 60.0]]"
        );
    }

    @Test
    public void test2dGrowingBothDims() throws Exception {
        assertGroupByTyped("DOUBLE[][]", "[[5.5,11.0,30.0],[21.5,27.0,60.0],[70.0,80.0,90.0]]",
                "ARRAY[[1.0, 2.0], [3.0, 4.0]]",
                "ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]]"
        );
    }

    @Test
    public void test2dIncrementalNoRemap() throws Exception {
        assertGroupByTyped("DOUBLE[][]", "[[37.0,74.0,111.0,220.0,275.0,600.0,700.0]]",
                "ARRAY[[1.0, 2.0, 3.0]]",
                "ARRAY[[10.0, 20.0, 30.0, 40.0, 50.0]]",
                "ARRAY[[100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0]]"
        );
    }

    @Test
    public void test2dMixedNoRemapThenRemap() throws Exception {
        assertGroupByTyped("DOUBLE[][]",
                "[[37.0,74.0,111.0,148.0,500.0,600.0],[375.0,430.0,485.0,540.0,1100.0,1200.0]]",
                "ARRAY[[1.0, 2.0, 3.0, 4.0]]",
                "ARRAY[[10.0, 20.0, 30.0, 40.0], [50.0, 60.0, 70.0, 80.0]]",
                "ARRAY[[100.0, 200.0, 300.0, 400.0, 500.0, 600.0], [700.0, 800.0, 900.0, 1000.0, 1100.0, 1200.0]]"
        );
    }

    @Test
    public void test2dNoRemapInnerGrowsOuterOne() throws Exception {
        assertGroupByTyped("DOUBLE[][]", "[[5.5,11.0,16.5,40.0,50.0]]",
                "ARRAY[[1.0, 2.0, 3.0]]",
                "ARRAY[[10.0, 20.0, 30.0, 40.0, 50.0]]"
        );
    }

    @Test
    public void test2dNoRemapOuterGrows() throws Exception {
        assertGroupByTyped("DOUBLE[][]",
                "[[5.5,11.0,16.5],[22.0,27.5,33.0],[70.0,80.0,90.0],[100.0,110.0,120.0]]",
                "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]",
                "ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0], [100.0, 110.0, 120.0]]"
        );
    }

    @Test
    public void test2dRemapInnerGrows() throws Exception {
        assertGroupByTyped("DOUBLE[][]",
                "[[5.5,11.0,16.5,40.0,50.0],[32.0,37.5,43.0,90.0,100.0]]",
                "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]",
                "ARRAY[[10.0, 20.0, 30.0, 40.0, 50.0], [60.0, 70.0, 80.0, 90.0, 100.0]]"
        );
    }

    @Test
    public void test2dShrinkingThenGrowing() throws Exception {
        assertGroupByTyped("DOUBLE[][]", "[[7.0,15.0,36.0],[28.5,36.0,72.0],[84.0,96.0,108.0]]",
                "ARRAY[[3.0, 6.0], [9.0, 12.0]]",
                "ARRAY[[6.0]]",
                "ARRAY[[12.0, 24.0, 36.0], [48.0, 60.0, 72.0], [84.0, 96.0, 108.0]]"
        );
    }

    @Test
    public void test2dAvgRemapWithNan() throws Exception {
        assertGroupByTyped("DOUBLE[][]",
                "[[5.5,20.0,16.5,null,50.0],[60.0,5.0,43.0,90.0,100.0]]",
                "ARRAY[[1.0, null, 3.0], [null, 5.0, 6.0]]",
                "ARRAY[[10.0, 20.0, 30.0, null, 50.0], [60.0, null, 80.0, 90.0, 100.0]]"
        );
    }

    @Test
    public void test3dNoRemapInnermostGrowsAllOuterOne() throws Exception {
        assertGroupByTyped("DOUBLE[][][]", "[[[5.5,11.0,16.5,40.0,50.0]]]",
                "ARRAY[[[1.0, 2.0, 3.0]]]",
                "ARRAY[[[10.0, 20.0, 30.0, 40.0, 50.0]]]"
        );
    }

    @Test
    public void test3dProgressiveGrowth() throws Exception {
        assertGroupByTyped("DOUBLE[][][]",
                "[[[37.0,74.0],[300.0,400.0]],[[265.0,320.0],[700.0,800.0]]]",
                "ARRAY[[[1.0, 2.0]]]",
                "ARRAY[[[10.0, 20.0]], [[30.0, 40.0]]]",
                "ARRAY[[[100.0, 200.0], [300.0, 400.0]], [[500.0, 600.0], [700.0, 800.0]]]"
        );
    }

    @Test
    public void test3dRemapInnermostGrowsOuterGtOne() throws Exception {
        assertGroupByTyped("DOUBLE[][][]",
                "[[[5.5,11.0,16.5,40.0,50.0]],[[32.0,37.5,43.0,90.0,100.0]]]",
                "ARRAY[[[1.0, 2.0, 3.0]], [[4.0, 5.0, 6.0]]]",
                "ARRAY[[[10.0, 20.0, 30.0, 40.0, 50.0]], [[60.0, 70.0, 80.0, 90.0, 100.0]]]"
        );
    }

    @Test
    public void test4dGroupBy() throws Exception {
        assertGroupByTyped("DOUBLE[][][][]",
                "[[[[5.5,11.0]],[[30.0,40.0]]],[[[50.0,60.0]],[[70.0,80.0]]]]",
                "ARRAY[[[[1.0, 2.0]]]]",
                "ARRAY[[[[10.0, 20.0]], [[30.0, 40.0]]], [[[50.0, 60.0]], [[70.0, 80.0]]]]"
        );
    }

    @Test
    public void testAllNanRowInMiddle() throws Exception {
        assertGroupBy("[2.0,3.0]",
                "ARRAY[1.0, 2.0]",
                "ARRAY[null, null]",
                "ARRAY[3.0, 4.0]"
        );
    }

    @Test
    public void testAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (null)");
            execute("INSERT INTO tab VALUES (null)");
            assertQueryNoLeakCheck("arr\nnull\n", "SELECT array_elem_avg(arr) arr FROM tab", null, false, true);
        });
    }

    @Test
    public void testAvgKeyedDifferentNanPatternsPerGroup() throws Exception {
        assertKeyedGroupBy(
                "grp\tarr\n1\t[2.0,4.0]\n2\t[null,4.0]\n",
                new String[][]{
                        {"1", "ARRAY[1.0, null]", "ARRAY[3.0, 4.0]"},
                        {"2", "ARRAY[null, 2.0]", "ARRAY[null, 6.0]"}
                }
        );
    }

    @Test
    public void testAvgVariableModeFromFirstRow() throws Exception {
        assertGroupBy("[2.5,5.0,4.5]",
                "ARRAY[1.0, null, 3.0]",
                "ARRAY[4.0, 5.0, 6.0]"
        );
    }

    @Test
    public void testCrossNanDifferentPositions() throws Exception {
        assertGroupBy("[2.5,4.0,4.0]",
                "ARRAY[null, 2.0, 3.0]",
                "ARRAY[1.0, null, 5.0]",
                "ARRAY[4.0, 6.0, null]"
        );
    }

    @Test
    public void testEmptyArraySkipped() throws Exception {
        assertGroupBy("[2.0,3.0]",
                "ARRAY[1.0, 2.0]",
                "ARRAY[]::double[]",
                "ARRAY[3.0, 4.0]"
        );
    }

    @Test
    public void testKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp INT, arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (1, ARRAY[10.0, 20.0])");
            execute("INSERT INTO tab VALUES (1, ARRAY[30.0, 40.0])");
            execute("INSERT INTO tab VALUES (2, ARRAY[100.0, 200.0])");
            assertQueryNoLeakCheck("grp\tarr\n1\t[20.0,30.0]\n2\t[100.0,200.0]\n",
                    "SELECT grp, array_elem_avg(arr) arr FROM tab ORDER BY grp", null, true, true);
        });
    }

    @Test
    public void testNanElements() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[3.0, null, 6.0])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0, 2.0])");
            assertQueryNoLeakCheck("arr\n[2.0,2.0,4.0]\n", "SELECT array_elem_avg(arr) arr FROM tab", null, false, true);
        });
    }

    @Test
    public void testNotKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0])");
            execute("INSERT INTO tab VALUES (ARRAY[3.0, 4.0])");
            assertQueryNoLeakCheck("arr\n[2.0,3.0]\n", "SELECT array_elem_avg(arr) arr FROM tab", null, false, true);
        });
    }

    @Test
    public void testNullArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (null)");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0])");
            assertQueryNoLeakCheck("arr\n[1.0,2.0]\n", "SELECT array_elem_avg(arr) arr FROM tab", null, false, true);
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP NOT NULL, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00', ARRAY[1.0, 2.0])");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:30:00', ARRAY[3.0, 4.0])");
            execute("INSERT INTO tab VALUES ('2024-01-01T01:00:00', ARRAY[10.0, 20.0])");
            assertQueryNoLeakCheck("ts\tarr\n2024-01-01T00:00:00.000000Z\t[2.0,3.0]\n2024-01-01T01:00:00.000000Z\t[10.0,20.0]\n",
                    "SELECT ts, array_elem_avg(arr) arr FROM tab SAMPLE BY 1h", "ts", true, true);
        });
    }

    @Test
    public void testSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0])");
            assertQueryNoLeakCheck("arr\n[1.0,2.0]\n", "SELECT array_elem_avg(arr) arr FROM tab", null, false, true);
        });
    }

    @Test
    public void testTransposedAsymmetricGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[][])");
            execute("INSERT INTO tab VALUES (ARRAY[[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])");
            execute("INSERT INTO tab VALUES (ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0]])");
            assertQueryNoLeakCheck(
                    "arr\n[[5.5,21.5,5.0],[11.0,27.0,6.0],[30.0,60.0,null]]\n",
                    "SELECT array_elem_avg(transpose(arr)) arr FROM tab",
                    null, false, true
            );
        });
    }

    @Test
    public void testTransposedGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[][])");
            execute("INSERT INTO tab VALUES (ARRAY[[1.0, 2.0], [3.0, 4.0]])");
            execute("INSERT INTO tab VALUES (ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0]])");
            assertQueryNoLeakCheck(
                    "arr\n[[5.5,21.5],[11.0,27.0],[30.0,60.0]]\n",
                    "SELECT array_elem_avg(transpose(arr)) arr FROM tab",
                    null, false, true
            );
        });
    }

    @Test
    public void testKahanCompensation() throws Exception {
        // Without Kahan: 1e15 + 1.0 = 1e15 (1.0 lost to rounding), so naive avg's
        // internal sum ≈ 0 instead of 1000, giving avg ≈ [0, ...] instead of [~0.998, 1.0].
        // With Kahan: sum = 1000.0, count = 1002, avg = 1000.0/1002 ≈ 0.998003992015968.
        // Position 1: sum = 1000.0, count = 1002, avg = 1000.0/1002.
        // Both positions should give the same result since they have the same pattern.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1e15, 1e15])");
            for (int i = 0; i < 1000; i++) {
                execute("INSERT INTO tab VALUES (ARRAY[1.0, 1.0])");
            }
            execute("INSERT INTO tab VALUES (ARRAY[-1e15, -1e15])");
            // sum = 1000.0, count = 1002, avg = 1000/1002
            double expected = 1000.0 / 1002.0;
            assertQueryNoLeakCheck("arr\n[" + expected + "," + expected + "]\n",
                    "SELECT array_elem_avg(arr) arr FROM tab", null, false, true);
        });
    }

    @Test
    public void testNonVanillaNanDetection() throws Exception {
        // NaN detection iterates flat buffer indices, but for a slice the NaN at a
        // logical position may sit at a buffer offset beyond cardinality.
        // Slice [1:3, 1:2] on (2,3) selects col 0 with strides [3,1], so logical
        // [1][0] maps to buffer offset 3 while cardinality is only 2.
        // Row 1 slice: [[10],[40]] (all finite, uniform count=1)
        // Row 2 slice: [[1],[NaN]] (NaN at [1][0], should trigger variable mode)
        // Bug: flat check reads buffer[0..1] = {1,2}, misses NaN → uniform count=2 → 40/2=20
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[][])");
            execute("INSERT INTO tab VALUES (ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0]])");
            execute("INSERT INTO tab VALUES (ARRAY[[1.0, 2.0, 3.0], [null, 5.0, 6.0]])");
            assertQueryNoLeakCheck(
                    "arr\n[[5.5],[40.0]]\n",
                    "SELECT array_elem_avg(arr[1:3, 1:2]) arr FROM tab",
                    null, false, true
            );
        });
    }

    @Test
    public void testVariableLengthArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0])");
            execute("INSERT INTO tab VALUES (ARRAY[3.0, 4.0, 5.0])");
            assertQueryNoLeakCheck("arr\n[2.0,3.0,5.0]\n", "SELECT array_elem_avg(arr) arr FROM tab", null, false, true);
        });
    }

    @Test
    public void testVariableLengthPlusNan() throws Exception {
        assertGroupBy("[1.0,3.0,5.0]",
                "ARRAY[1.0, 2.0]",
                "ARRAY[null, 4.0, 5.0]"
        );
    }
}
