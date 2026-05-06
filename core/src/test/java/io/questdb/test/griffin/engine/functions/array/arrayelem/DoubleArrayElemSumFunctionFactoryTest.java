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

package io.questdb.test.griffin.engine.functions.array.arrayelem;

import org.junit.Test;

public class DoubleArrayElemSumFunctionFactoryTest extends AbstractDoubleArrayElemFunctionTest {

    @Override
    protected String funcName() {
        return "array_elem_sum";
    }

    @Test
    public void test2dBothDimsDiffer() throws Exception {
        assertElemWise(
                "[[11.0,22.0,33.0,4.0],[45.0,56.0,67.0,8.0],[70.0,80.0,90.0,null]]",
                "ARRAY[[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]]",
                "ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]]"
        );
    }

    @Test
    public void test2dExtremeAsymmetry() throws Exception {
        assertElemWise(
                "[[11.0,2.0,3.0],[20.0,null,null],[30.0,null,null]]",
                "ARRAY[[1.0, 2.0, 3.0]]",
                "ARRAY[[10.0], [20.0], [30.0]]"
        );
    }

    @Test
    public void test2dOneMuchLarger() throws Exception {
        assertElemWise(
                "[[11.0,20.0],[30.0,40.0],[50.0,60.0]]",
                "ARRAY[[1.0]]",
                "ARRAY[[10.0, 20.0], [30.0, 40.0], [50.0, 60.0]]"
        );
    }

    @Test
    public void test2dOutOfBoundsContribution() throws Exception {
        // (2,2) + (3,1): [2][1] is out-of-bounds for both → NaN
        assertElemWise(
                "[[11.0,2.0],[23.0,4.0],[30.0,null]]",
                "ARRAY[[1.0, 2.0], [3.0, 4.0]]",
                "ARRAY[[10.0], [20.0], [30.0]]"
        );
    }

    @Test
    public void test2dSameShape() throws Exception {
        assertElemWise(
                "[[8.0,10.0,12.0],[14.0,16.0,18.0]]",
                "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]",
                "ARRAY[[7.0, 8.0, 9.0], [10.0, 11.0, 12.0]]"
        );
    }

    @Test
    public void test3dAllDimsDiffer() throws Exception {
        assertElemWise(
                "[[[11.0,22.0,30.0],[43.0,54.0,60.0],[70.0,80.0,90.0]],[[105.0,116.0,120.0],[137.0,148.0,150.0],[160.0,170.0,180.0]],[[190.0,200.0,210.0],[220.0,230.0,240.0],[250.0,260.0,270.0]]]",
                "ARRAY[[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]]",
                "ARRAY[[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]], [[100.0, 110.0, 120.0], [130.0, 140.0, 150.0], [160.0, 170.0, 180.0]], [[190.0, 200.0, 210.0], [220.0, 230.0, 240.0], [250.0, 260.0, 270.0]]]"
        );
    }

    @Test
    public void test3dExtremeAsymmetry() throws Exception {
        assertElemWise(
                "[[[11.0,2.0,3.0]],[[20.0,null,null]],[[30.0,null,null]]]",
                "ARRAY[[[1.0, 2.0, 3.0]]]",
                "ARRAY[[[10.0]], [[20.0]], [[30.0]]]"
        );
    }

    @Test
    public void test3dInnermostDimDiffers() throws Exception {
        assertElemWise(
                "[[[11.0,22.0,30.0,40.0],[53.0,64.0,70.0,80.0]]]",
                "ARRAY[[[1.0, 2.0], [3.0, 4.0]]]",
                "ARRAY[[[10.0, 20.0, 30.0, 40.0], [50.0, 60.0, 70.0, 80.0]]]"
        );
    }

    @Test
    public void test3dSameShape() throws Exception {
        assertElemWise(
                "[[[10.0,12.0],[14.0,16.0]],[[18.0,20.0],[22.0,24.0]]]",
                "ARRAY[[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]]",
                "ARRAY[[[9.0, 10.0], [11.0, 12.0]], [[13.0, 14.0], [15.0, 16.0]]]"
        );
    }

    @Test
    public void test4dMixedGrowth() throws Exception {
        assertElemWise(
                "[[[[11.0,22.0]],[[30.0,40.0]]],[[[3.0,4.0]],[[null,null]]]]",
                "ARRAY[[[[1.0, 2.0]]], [[[3.0, 4.0]]]]",
                "ARRAY[[[[10.0, 20.0]], [[30.0, 40.0]]]]"
        );
    }

    @Test
    public void testAllElementsNanAtOnePosition() throws Exception {
        assertElemWise("[null,6.0]", "ARRAY[null, 1.0]", "ARRAY[null, 2.0]", "ARRAY[null, 3.0]");
    }

    @Test
    public void testAllNullArrays() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "array_elem_sum\nnull\n",
                "SELECT array_elem_sum(null::double[], null::double[])",
                null, true, true
        ));
    }

    @Test
    public void testDifferentLengths() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "array_elem_sum\n[4.0,6.0,5.0]\n",
                "SELECT array_elem_sum(ARRAY[1.0, 2.0], ARRAY[3.0, 4.0, 5.0])",
                null, true, true
        ));
    }

    @Test
    public void testFromTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (a DOUBLE[], b DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0], ARRAY[3.0, 4.0])");
            assertQueryNoLeakCheck(
                    "array_elem_sum\n[4.0,6.0]\n",
                    "SELECT array_elem_sum(a, b) FROM tab",
                    null, true, true
            );
        });
    }

    @Test
    public void testNanElements() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "array_elem_sum\n[1.0,2.0]\n",
                "SELECT array_elem_sum(ARRAY[1.0, null], ARRAY[null, 2.0])",
                null, true, true
        ));
    }

    @Test
    public void testOneNullArray() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "array_elem_sum\n[1.0,2.0]\n",
                "SELECT array_elem_sum(null::double[], ARRAY[1.0, 2.0])",
                null, true, true
        ));
    }

    @Test
    public void testSingleElementArrays() throws Exception {
        assertElemWise("[8.0,4.0,5.0]", "ARRAY[5.0]", "ARRAY[3.0, 4.0, 5.0]");
    }

    @Test
    public void testSliceChain() throws Exception {
        assertElemWise("[21.0,31.0]",
                "ARRAY[10.0, 20.0, 30.0, 40.0, 50.0][2:5][1:3]",
                "ARRAY[1.0, 1.0]"
        );
    }

    @Test
    public void testSliced1d() throws Exception {
        assertElemWise("[25.0,35.0]",
                "ARRAY[10.0, 20.0, 30.0, 40.0][2:4]",
                "ARRAY[5.0, 5.0]"
        );
    }

    @Test
    public void testSliced2dInnerDim() throws Exception {
        assertElemWise("[[12.0,23.0],[35.0,46.0]]",
                "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]][1:3, 2:4]",
                "ARRAY[[10.0, 20.0], [30.0, 40.0]]"
        );
    }

    @Test
    public void testSliced2dOuterDim() throws Exception {
        assertElemWise("[[13.0,24.0],[35.0,46.0]]",
                "ARRAY[[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]][2:4]",
                "ARRAY[[10.0, 20.0], [30.0, 40.0]]"
        );
    }

    @Test
    public void testSlicedPlusDifferentShape() throws Exception {
        assertElemWise("[[12.0,23.0,30.0],[40.0,50.0,60.0]]",
                "ARRAY[[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]][1:2, 2:4]",
                "ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0]]"
        );
    }

    @Test
    public void testThreeArraysSameLength() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "array_elem_sum\n[9.0,12.0]\n",
                "SELECT array_elem_sum(ARRAY[1.0, 2.0], ARRAY[3.0, 4.0], ARRAY[5.0, 6.0])",
                null, true, true
        ));
    }

    @Test
    public void testTransposedBothDifferentShapes() throws Exception {
        assertElemWise("[[11.0,3.0,5.0],[22.0,4.0,6.0],[30.0,null,null]]",
                "transpose(ARRAY[[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])",
                "transpose(ARRAY[[10.0, 20.0, 30.0]])"
        );
    }

    @Test
    public void testTransposedInput() throws Exception {
        assertElemWise("[[3.0,9.0],[9.0,15.0]]",
                "transpose(ARRAY[[2.0, 4.0], [6.0, 8.0]])",
                "ARRAY[[1.0, 3.0], [5.0, 7.0]]"
        );
    }

    @Test
    public void testTransposedPlusSlicedPlusVanilla() throws Exception {
        assertElemWise("[[111.0,200.0,300.0],[32.0,null,null],[3.0,null,null]]",
                "transpose(ARRAY[[1.0, 2.0, 3.0]])",
                "ARRAY[[10.0, 20.0], [30.0, 40.0], [50.0, 60.0]][1:3, 1:2]",
                "ARRAY[[100.0, 200.0, 300.0]]"
        );
    }

    @Test
    public void testTransposedPlusVanillaDifferentShapes() throws Exception {
        assertElemWise("[[11.0,24.0,30.0,40.0],[52.0,65.0,70.0,80.0],[3.0,6.0,null,null]]",
                "transpose(ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])",
                "ARRAY[[10.0, 20.0, 30.0, 40.0], [50.0, 60.0, 70.0, 80.0]]"
        );
    }

    @Test
    public void testTransposedPlusVanillaSameShape() throws Exception {
        assertElemWise("[[11.0,24.0],[32.0,45.0],[53.0,66.0]]",
                "transpose(ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])",
                "ARRAY[[10.0, 20.0], [30.0, 40.0], [50.0, 60.0]]"
        );
    }

    @Test
    public void testTwoArraysSameLength() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "array_elem_sum\n[4.0,6.0]\n",
                "SELECT array_elem_sum(ARRAY[1.0, 2.0], ARRAY[3.0, 4.0])",
                null, true, true
        ));
    }
}
