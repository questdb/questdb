/*******************************************************************************
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

package io.questdb.test.griffin.engine.functions.array;

import org.junit.Test;

public class DoubleArrayElemAvgFunctionFactoryTest extends AbstractDoubleArrayElemFunctionTest {

    @Override
    protected String funcName() {
        return "array_elem_avg";
    }
    @Test
    public void test2dBothDimsDiffer() throws Exception {
        assertElemWise(
                "[[5.5,11.0,16.5,4.0],[22.5,28.0,33.5,8.0],[70.0,80.0,90.0,null]]",
                "ARRAY[[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]]",
                "ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]]"
        );
    }

    @Test
    public void test2dExtremeAsymmetry() throws Exception {
        assertElemWise(
                "[[5.5,2.0,3.0],[20.0,null,null],[30.0,null,null]]",
                "ARRAY[[1.0, 2.0, 3.0]]",
                "ARRAY[[10.0], [20.0], [30.0]]"
        );
    }

    @Test
    public void test2dOneMuchLarger() throws Exception {
        assertElemWise(
                "[[5.5,20.0],[30.0,40.0],[50.0,60.0]]",
                "ARRAY[[1.0]]",
                "ARRAY[[10.0, 20.0], [30.0, 40.0], [50.0, 60.0]]"
        );
    }

    @Test
    public void test2dOutOfBoundsNotCounted() throws Exception {
        // (2,2) + (1,3): [0][2] count=1 not 2, [1][2] NaN
        assertElemWise(
                "[[3.5,7.5,12.0],[8.0,10.0,null]]",
                "ARRAY[[4.0, 6.0], [8.0, 10.0]]",
                "ARRAY[[3.0, 9.0, 12.0]]"
        );
    }

    @Test
    public void test2dSameShape() throws Exception {
        assertElemWise(
                "[[4.0,5.0,6.0],[7.0,8.0,9.0]]",
                "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]",
                "ARRAY[[7.0, 8.0, 9.0], [10.0, 11.0, 12.0]]"
        );
    }

    @Test
    public void test3dAllDimsDiffer() throws Exception {
        assertElemWise(
                "[[[5.5,11.0,30.0],[21.5,27.0,60.0],[70.0,80.0,90.0]],[[52.5,58.0,120.0],[68.5,74.0,150.0],[160.0,170.0,180.0]],[[190.0,200.0,210.0],[220.0,230.0,240.0],[250.0,260.0,270.0]]]",
                "ARRAY[[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]]",
                "ARRAY[[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]], [[100.0, 110.0, 120.0], [130.0, 140.0, 150.0], [160.0, 170.0, 180.0]], [[190.0, 200.0, 210.0], [220.0, 230.0, 240.0], [250.0, 260.0, 270.0]]]"
        );
    }

    @Test
    public void test3dExtremeAsymmetry() throws Exception {
        assertElemWise(
                "[[[5.5,2.0,3.0]],[[20.0,null,null]],[[30.0,null,null]]]",
                "ARRAY[[[1.0, 2.0, 3.0]]]",
                "ARRAY[[[10.0]], [[20.0]], [[30.0]]]"
        );
    }

    @Test
    public void test3dInnermostDimDiffers() throws Exception {
        assertElemWise(
                "[[[5.5,11.0,30.0,40.0],[26.5,32.0,70.0,80.0]]]",
                "ARRAY[[[1.0, 2.0], [3.0, 4.0]]]",
                "ARRAY[[[10.0, 20.0, 30.0, 40.0], [50.0, 60.0, 70.0, 80.0]]]"
        );
    }

    @Test
    public void test3dSameShape() throws Exception {
        assertElemWise(
                "[[[5.0,6.0],[7.0,8.0]],[[9.0,10.0],[11.0,12.0]]]",
                "ARRAY[[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]]",
                "ARRAY[[[9.0, 10.0], [11.0, 12.0]], [[13.0, 14.0], [15.0, 16.0]]]"
        );
    }

    @Test
    public void test4dMixedGrowth() throws Exception {
        assertElemWise(
                "[[[[5.5,11.0]],[[30.0,40.0]]],[[[3.0,4.0]],[[null,null]]]]",
                "ARRAY[[[[1.0, 2.0]]], [[[3.0, 4.0]]]]",
                "ARRAY[[[[10.0, 20.0]], [[30.0, 40.0]]]]"
        );
    }

    @Test
    public void testAllElementsNanAtOnePosition() throws Exception {
        assertElemWise("[null,2.0]", "ARRAY[null, 1.0]", "ARRAY[null, 2.0]", "ARRAY[null, 3.0]");
    }

    @Test
    public void testAllNullArrays() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "array_elem_avg\nnull\n",
                "SELECT array_elem_avg(null::double[], null::double[])",
                null, true, true
        ));
    }

    @Test
    public void testAvgDifferentCountsPerPosition() throws Exception {
        assertElemWise("[3.0,4.0,7.0]", "ARRAY[1.0, 2.0, null]", "ARRAY[3.0, null, null]", "ARRAY[5.0, 6.0, 7.0]");
    }

    @Test
    public void testDifferentLengths() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "array_elem_avg\n[2.0,3.0,5.0]\n",
                "SELECT array_elem_avg(ARRAY[1.0, 2.0], ARRAY[3.0, 4.0, 5.0])",
                null, true, true
        ));
    }

    @Test
    public void testFromTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (a DOUBLE[], b DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0], ARRAY[3.0, 4.0])");
            assertQueryNoLeakCheck(
                    "array_elem_avg\n[2.0,3.0]\n",
                    "SELECT array_elem_avg(a, b) FROM tab",
                    null, true, true
            );
        });
    }

    @Test
    public void testNanElements() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "array_elem_avg\n[3.0,2.0]\n",
                "SELECT array_elem_avg(ARRAY[3.0, null], ARRAY[null, 2.0])",
                null, true, true
        ));
    }

    @Test
    public void testOneNullArray() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "array_elem_avg\n[1.0,2.0]\n",
                "SELECT array_elem_avg(null::double[], ARRAY[1.0, 2.0])",
                null, true, true
        ));
    }

    @Test
    public void testSingleElementArrays() throws Exception {
        assertElemWise("[4.0,4.0,5.0]", "ARRAY[5.0]", "ARRAY[3.0, 4.0, 5.0]");
    }

    @Test
    public void testSliceChain() throws Exception {
        assertElemWise("[10.5,15.5]",
                "ARRAY[10.0, 20.0, 30.0, 40.0, 50.0][2:5][1:3]",
                "ARRAY[1.0, 1.0]"
        );
    }

    @Test
    public void testSliced1d() throws Exception {
        assertElemWise("[12.5,17.5]",
                "ARRAY[10.0, 20.0, 30.0, 40.0][2:4]",
                "ARRAY[5.0, 5.0]"
        );
    }

    @Test
    public void testSliced2dInnerDim() throws Exception {
        assertElemWise("[[6.0,11.5],[17.5,23.0]]",
                "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]][1:3, 2:4]",
                "ARRAY[[10.0, 20.0], [30.0, 40.0]]"
        );
    }

    @Test
    public void testSliced2dOuterDim() throws Exception {
        assertElemWise("[[6.5,12.0],[17.5,23.0]]",
                "ARRAY[[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]][2:4]",
                "ARRAY[[10.0, 20.0], [30.0, 40.0]]"
        );
    }

    @Test
    public void testSlicedPlusDifferentShape() throws Exception {
        assertElemWise("[[6.0,11.5,30.0],[40.0,50.0,60.0]]",
                "ARRAY[[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]][1:2, 2:4]",
                "ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0]]"
        );
    }

    @Test
    public void testThreeArraysSameLength() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "array_elem_avg\n[3.0,4.0]\n",
                "SELECT array_elem_avg(ARRAY[1.0, 2.0], ARRAY[3.0, 4.0], ARRAY[5.0, 6.0])",
                null, true, true
        ));
    }

    @Test
    public void testTransposedBothDifferentShapes() throws Exception {
        assertElemWise("[[5.5,3.0,5.0],[11.0,4.0,6.0],[30.0,null,null]]",
                "transpose(ARRAY[[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])",
                "transpose(ARRAY[[10.0, 20.0, 30.0]])"
        );
    }

    @Test
    public void testTransposedInput() throws Exception {
        assertElemWise("[[1.5,4.5],[4.5,7.5]]",
                "transpose(ARRAY[[2.0, 4.0], [6.0, 8.0]])",
                "ARRAY[[1.0, 3.0], [5.0, 7.0]]"
        );
    }

    @Test
    public void testTransposedPlusSlicedPlusVanilla() throws Exception {
        assertElemWise("[[37.0,200.0,300.0],[16.0,null,null],[3.0,null,null]]",
                "transpose(ARRAY[[1.0, 2.0, 3.0]])",
                "ARRAY[[10.0, 20.0], [30.0, 40.0], [50.0, 60.0]][1:3, 1:2]",
                "ARRAY[[100.0, 200.0, 300.0]]"
        );
    }

    @Test
    public void testTransposedPlusVanillaDifferentShapes() throws Exception {
        assertElemWise("[[5.5,12.0,30.0,40.0],[26.0,32.5,70.0,80.0],[3.0,6.0,null,null]]",
                "transpose(ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])",
                "ARRAY[[10.0, 20.0, 30.0, 40.0], [50.0, 60.0, 70.0, 80.0]]"
        );
    }

    @Test
    public void testTransposedPlusVanillaSameShape() throws Exception {
        assertElemWise("[[5.5,12.0],[16.0,22.5],[26.5,33.0]]",
                "transpose(ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])",
                "ARRAY[[10.0, 20.0], [30.0, 40.0], [50.0, 60.0]]"
        );
    }

    @Test
    public void testTwoArraysSameLength() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "array_elem_avg\n[2.0,3.0]\n",
                "SELECT array_elem_avg(ARRAY[1.0, 2.0], ARRAY[3.0, 4.0])",
                null, true, true
        ));
    }
}
