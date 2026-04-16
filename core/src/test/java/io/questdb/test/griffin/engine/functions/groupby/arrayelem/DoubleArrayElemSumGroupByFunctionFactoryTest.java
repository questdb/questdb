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

public class DoubleArrayElemSumGroupByFunctionFactoryTest extends AbstractDoubleArrayElemGroupByFunctionTest {

    @Override
    protected String funcName() {
        return "array_elem_sum";
    }

    @Test
    public void test2dAllSameShape() throws Exception {
        assertGroupByTyped("DOUBLE[][]", "[[15.0,18.0],[21.0,24.0]]",
                "ARRAY[[1.0, 2.0], [3.0, 4.0]]",
                "ARRAY[[5.0, 6.0], [7.0, 8.0]]",
                "ARRAY[[9.0, 10.0], [11.0, 12.0]]"
        );
    }

    @Test
    public void test2dFirstRowNull() throws Exception {
        assertGroupByTyped("DOUBLE[][]", "[[11.0,22.0,3.0],[34.0,45.0,6.0],[50.0,60.0,null]]",
                "null",
                "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]",
                "ARRAY[[10.0, 20.0], [30.0, 40.0], [50.0, 60.0]]"
        );
    }

    @Test
    public void test2dGrowingBothDims() throws Exception {
        assertGroupByTyped("DOUBLE[][]", "[[11.0,22.0,30.0],[43.0,54.0,60.0],[70.0,80.0,90.0]]",
                "ARRAY[[1.0, 2.0], [3.0, 4.0]]",
                "ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]]"
        );
    }

    @Test
    public void test2dIncrementalNoRemap() throws Exception {
        assertGroupByTyped("DOUBLE[][]", "[[111.0,222.0,333.0,440.0,550.0,600.0,700.0]]",
                "ARRAY[[1.0, 2.0, 3.0]]",
                "ARRAY[[10.0, 20.0, 30.0, 40.0, 50.0]]",
                "ARRAY[[100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0]]"
        );
    }

    @Test
    public void test2dMixedNoRemapThenRemap() throws Exception {
        assertGroupByTyped("DOUBLE[][]",
                "[[111.0,222.0,333.0,444.0,500.0,600.0],[750.0,860.0,970.0,1080.0,1100.0,1200.0]]",
                "ARRAY[[1.0, 2.0, 3.0, 4.0]]",
                "ARRAY[[10.0, 20.0, 30.0, 40.0], [50.0, 60.0, 70.0, 80.0]]",
                "ARRAY[[100.0, 200.0, 300.0, 400.0, 500.0, 600.0], [700.0, 800.0, 900.0, 1000.0, 1100.0, 1200.0]]"
        );
    }

    @Test
    public void test2dNoRemapInnerGrowsOuterOne() throws Exception {
        assertGroupByTyped("DOUBLE[][]", "[[11.0,22.0,33.0,40.0,50.0]]",
                "ARRAY[[1.0, 2.0, 3.0]]",
                "ARRAY[[10.0, 20.0, 30.0, 40.0, 50.0]]"
        );
    }

    @Test
    public void test2dNoRemapOuterGrows() throws Exception {
        assertGroupByTyped("DOUBLE[][]",
                "[[11.0,22.0,33.0],[44.0,55.0,66.0],[70.0,80.0,90.0],[100.0,110.0,120.0]]",
                "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]",
                "ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0], [100.0, 110.0, 120.0]]"
        );
    }

    @Test
    public void test2dRemapInnerGrows() throws Exception {
        assertGroupByTyped("DOUBLE[][]",
                "[[11.0,22.0,33.0,40.0,50.0],[64.0,75.0,86.0,90.0,100.0]]",
                "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]",
                "ARRAY[[10.0, 20.0, 30.0, 40.0, 50.0], [60.0, 70.0, 80.0, 90.0, 100.0]]"
        );
    }

    @Test
    public void test2dShrinkingThenGrowing() throws Exception {
        assertGroupByTyped("DOUBLE[][]", "[[21.0,30.0,36.0],[57.0,72.0,72.0],[84.0,96.0,108.0]]",
                "ARRAY[[3.0, 6.0], [9.0, 12.0]]",
                "ARRAY[[6.0]]",
                "ARRAY[[12.0, 24.0, 36.0], [48.0, 60.0, 72.0], [84.0, 96.0, 108.0]]"
        );
    }

    @Test
    public void test3dNoRemapInnermostGrowsAllOuterOne() throws Exception {
        assertGroupByTyped("DOUBLE[][][]", "[[[11.0,22.0,33.0,40.0,50.0]]]",
                "ARRAY[[[1.0, 2.0, 3.0]]]",
                "ARRAY[[[10.0, 20.0, 30.0, 40.0, 50.0]]]"
        );
    }

    @Test
    public void test3dProgressiveGrowth() throws Exception {
        assertGroupByTyped("DOUBLE[][][]",
                "[[[111.0,222.0],[300.0,400.0]],[[530.0,640.0],[700.0,800.0]]]",
                "ARRAY[[[1.0, 2.0]]]",
                "ARRAY[[[10.0, 20.0]], [[30.0, 40.0]]]",
                "ARRAY[[[100.0, 200.0], [300.0, 400.0]], [[500.0, 600.0], [700.0, 800.0]]]"
        );
    }

    @Test
    public void test3dRemapInnermostGrowsOuterGtOne() throws Exception {
        assertGroupByTyped("DOUBLE[][][]",
                "[[[11.0,22.0,33.0,40.0,50.0]],[[64.0,75.0,86.0,90.0,100.0]]]",
                "ARRAY[[[1.0, 2.0, 3.0]], [[4.0, 5.0, 6.0]]]",
                "ARRAY[[[10.0, 20.0, 30.0, 40.0, 50.0]], [[60.0, 70.0, 80.0, 90.0, 100.0]]]"
        );
    }

    @Test
    public void test4dGroupBy() throws Exception {
        assertGroupByTyped("DOUBLE[][][][]",
                "[[[[11.0,22.0]],[[30.0,40.0]]],[[[50.0,60.0]],[[70.0,80.0]]]]",
                "ARRAY[[[[1.0, 2.0]]]]",
                "ARRAY[[[[10.0, 20.0]], [[30.0, 40.0]]], [[[50.0, 60.0]], [[70.0, 80.0]]]]"
        );
    }

    @Test
    public void testAllNanRowInMiddle() throws Exception {
        assertGroupBy("[4.0,6.0]",
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
            assertQueryNoLeakCheck("arr\nnull\n", "SELECT array_elem_sum(arr) arr FROM tab", null, false, true);
        });
    }

    @Test
    public void testCrossNanDifferentPositions() throws Exception {
        assertGroupBy("[5.0,8.0,8.0]",
                "ARRAY[null, 2.0, 3.0]",
                "ARRAY[1.0, null, 5.0]",
                "ARRAY[4.0, 6.0, null]"
        );
    }

    @Test
    public void testEmptyArraySkipped() throws Exception {
        assertGroupBy("[4.0,6.0]",
                "ARRAY[1.0, 2.0]",
                "ARRAY[]::double[]",
                "ARRAY[3.0, 4.0]"
        );
    }

    @Test
    public void testKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp INT, arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (1, ARRAY[10.0, 11.0])");
            execute("INSERT INTO tab VALUES (1, ARRAY[20.0, 21.0])");
            execute("INSERT INTO tab VALUES (2, ARRAY[40.0, 41.0])");
            execute("INSERT INTO tab VALUES (2, ARRAY[50.0, 51.0])");
            assertQueryNoLeakCheck("grp\tarr\n1\t[30.0,32.0]\n2\t[90.0,92.0]\n",
                    "SELECT grp, array_elem_sum(arr) arr FROM tab ORDER BY grp", null, true, true);
        });
    }

    @Test
    public void testNanElements() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0, 1.0])");
            execute("INSERT INTO tab VALUES (ARRAY[3.0, null, 5.0])");
            assertQueryNoLeakCheck("arr\n[4.0,2.0,6.0]\n", "SELECT array_elem_sum(arr) arr FROM tab", null, false, true);
        });
    }

    @Test
    public void testNotKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0])");
            execute("INSERT INTO tab VALUES (ARRAY[3.0, 4.0])");
            assertQueryNoLeakCheck("arr\n[4.0,6.0]\n", "SELECT array_elem_sum(arr) arr FROM tab", null, false, true);
        });
    }

    @Test
    public void testNullArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (null)");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0])");
            assertQueryNoLeakCheck("arr\n[1.0,2.0]\n", "SELECT array_elem_sum(arr) arr FROM tab", null, false, true);
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP NOT NULL, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00', ARRAY[1.0, 2.0])");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:30:00', ARRAY[3.0, 4.0])");
            execute("INSERT INTO tab VALUES ('2024-01-01T01:00:00', ARRAY[10.0, 20.0])");
            assertQueryNoLeakCheck("ts\tarr\n2024-01-01T00:00:00.000000Z\t[4.0,6.0]\n2024-01-01T01:00:00.000000Z\t[10.0,20.0]\n",
                    "SELECT ts, array_elem_sum(arr) arr FROM tab SAMPLE BY 1h", "ts", true, true);
        });
    }

    @Test
    public void testSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0])");
            assertQueryNoLeakCheck("arr\n[1.0,2.0]\n", "SELECT array_elem_sum(arr) arr FROM tab", null, false, true);
        });
    }

    @Test
    public void testTransposedAsymmetricGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[][])");
            execute("INSERT INTO tab VALUES (ARRAY[[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])");
            execute("INSERT INTO tab VALUES (ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0]])");
            assertQueryNoLeakCheck(
                    "arr\n[[11.0,43.0,5.0],[22.0,54.0,6.0],[30.0,60.0,null]]\n",
                    "SELECT array_elem_sum(transpose(arr)) arr FROM tab",
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
                    "arr\n[[11.0,43.0],[22.0,54.0],[30.0,60.0]]\n",
                    "SELECT array_elem_sum(transpose(arr)) arr FROM tab",
                    null, false, true
            );
        });
    }

    @Test
    public void testKahanCompensation() throws Exception {
        // Without Kahan: 1e15 + 1.0 = 1e15 (1.0 lost to rounding), so naive sum at
        // position 0 would be ≈ 0 (1e15 - 1e15, with the 1.0s lost). With Kahan:
        // compensation preserves the 1.0s, giving exactly 1000.0.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1e15, 0.0])");
            for (int i = 0; i < 1000; i++) {
                execute("INSERT INTO tab VALUES (ARRAY[1.0, 1.0])");
            }
            execute("INSERT INTO tab VALUES (ARRAY[-1e15, 0.0])");
            assertQueryNoLeakCheck("arr\n[1000.0,1000.0]\n", "SELECT array_elem_sum(arr) arr FROM tab", null, false, true);
        });
    }

    @Test
    public void testVariableLengthArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0])");
            execute("INSERT INTO tab VALUES (ARRAY[3.0, 4.0, 5.0])");
            assertQueryNoLeakCheck("arr\n[4.0,6.0,5.0]\n", "SELECT array_elem_sum(arr) arr FROM tab", null, false, true);
        });
    }

    @Test
    public void testVariableLengthPlusNan() throws Exception {
        assertGroupBy("[1.0,6.0,5.0]",
                "ARRAY[1.0, 2.0]",
                "ARRAY[null, 4.0, 5.0]"
        );
    }
}
