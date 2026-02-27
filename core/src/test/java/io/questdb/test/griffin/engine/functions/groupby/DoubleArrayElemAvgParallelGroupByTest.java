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

package io.questdb.test.griffin.engine.functions.groupby;

import org.junit.Test;

public class DoubleArrayElemAvgParallelGroupByTest extends AbstractDoubleArrayElemParallelGroupByTest {

    @Override
    protected String funcName() {
        return "array_elem_avg";
    }

    @Test
    public void testParallelMergeDifferentShapes() throws Exception {
        assertKeyedParallelGroupBy(
                "grp\tarr\n" +
                        "1\t[[7.0,8.0],[9.0,10.0]]\n" +
                        "2\t[[80.0,90.0,120.0],[105.0,115.0,150.0]]\n",
                new String[][]{
                        {"2024-01-01T00:00:00", "1", "ARRAY[[1.0, 2.0], [3.0, 4.0]]"},
                        {"2024-01-01T01:00:00", "2", "ARRAY[[10.0, 20.0], [30.0, 40.0]]"},
                        {"2024-01-02T00:00:00", "1", "ARRAY[[5.0, 6.0], [7.0, 8.0]]"},
                        {"2024-01-02T01:00:00", "2", "ARRAY[[50.0, 60.0, 70.0], [80.0, 90.0, 100.0]]"},
                        {"2024-01-03T00:00:00", "1", "ARRAY[[9.0, 10.0], [11.0, 12.0]]"},
                        {"2024-01-03T01:00:00", "2", "ARRAY[[110.0, 120.0], [130.0, 140.0]]"},
                        {"2024-01-04T00:00:00", "1", "ARRAY[[13.0, 14.0], [15.0, 16.0]]"},
                        {"2024-01-04T01:00:00", "2", "ARRAY[[150.0, 160.0, 170.0], [180.0, 190.0, 200.0]]"}
                }
        );
    }

    @Test
    public void testParallelMergeSameShape() throws Exception {
        assertKeyedParallelGroupBy(
                "grp\tarr\n" +
                        "1\t[[7.0,8.0],[9.0,10.0]]\n" +
                        "2\t[[70.0,80.0],[90.0,100.0]]\n",
                new String[][]{
                        {"2024-01-01T00:00:00", "1", "ARRAY[[1.0, 2.0], [3.0, 4.0]]"},
                        {"2024-01-01T01:00:00", "2", "ARRAY[[10.0, 20.0], [30.0, 40.0]]"},
                        {"2024-01-02T00:00:00", "1", "ARRAY[[5.0, 6.0], [7.0, 8.0]]"},
                        {"2024-01-02T01:00:00", "2", "ARRAY[[50.0, 60.0], [70.0, 80.0]]"},
                        {"2024-01-03T00:00:00", "1", "ARRAY[[9.0, 10.0], [11.0, 12.0]]"},
                        {"2024-01-03T01:00:00", "2", "ARRAY[[90.0, 100.0], [110.0, 120.0]]"},
                        {"2024-01-04T00:00:00", "1", "ARRAY[[13.0, 14.0], [15.0, 16.0]]"},
                        {"2024-01-04T01:00:00", "2", "ARRAY[[130.0, 140.0], [150.0, 160.0]]"}
                }
        );
    }

    @Test
    public void testParallelMergeWithNaN() throws Exception {
        // Group 1 has NaN positions (variable count mode) — exercises variable count merging.
        // Group 2 is all-finite (uniform count mode) — exercises uniform count merging.
        assertKeyedParallelGroupBy(
                "grp\tarr\n" +
                        "1\t[[14.0,12.0],[9.0,15.0]]\n" +
                        "2\t[[70.0,80.0],[90.0,100.0]]\n",
                new String[][]{
                        {"2024-01-01T00:00:00", "1", "ARRAY[[null, 2.0], [3.0, 4.0]]"},
                        {"2024-01-01T01:00:00", "2", "ARRAY[[10.0, 20.0], [30.0, 40.0]]"},
                        {"2024-01-02T00:00:00", "1", "ARRAY[[6.0, 8.0], [null, 12.0]]"},
                        {"2024-01-02T01:00:00", "2", "ARRAY[[50.0, 60.0], [70.0, 80.0]]"},
                        {"2024-01-03T00:00:00", "1", "ARRAY[[null, 14.0], [15.0, 16.0]]"},
                        {"2024-01-03T01:00:00", "2", "ARRAY[[90.0, 100.0], [110.0, 120.0]]"},
                        {"2024-01-04T00:00:00", "1", "ARRAY[[22.0, 24.0], [null, 28.0]]"},
                        {"2024-01-04T01:00:00", "2", "ARRAY[[130.0, 140.0], [150.0, 160.0]]"}
                }
        );
    }

    @Test
    public void testParallelAvgUniformDifferentShape() throws Exception {
        assertParallelGroupBy("DOUBLE[][]",
                "[[5.5,11.0,3.0],[17.0,22.5,6.0],[50.0,60.0,null]]",
                new String[][]{
                        {"2024-01-01T00:00:00", "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]"},
                        {"2024-01-02T00:00:00", "ARRAY[[10.0, 20.0], [30.0, 40.0], [50.0, 60.0]]"}
                }
        );
    }

    @Test
    public void testParallelAvgUniformSameShape() throws Exception {
        assertParallelGroupBy("DOUBLE[][]",
                "[[5.0,6.0],[7.0,8.0]]",
                new String[][]{
                        {"2024-01-01T00:00:00", "ARRAY[[1.0, 2.0], [3.0, 4.0]]"},
                        {"2024-01-02T00:00:00", "ARRAY[[5.0, 6.0], [7.0, 8.0]]"},
                        {"2024-01-03T00:00:00", "ARRAY[[9.0, 10.0], [11.0, 12.0]]"}
                }
        );
    }

    @Test
    public void testParallelAvgVariablePlusUniform() throws Exception {
        assertParallelGroupBy("DOUBLE[][]",
                "[[10.0,11.0],[16.5,40.0]]",
                new String[][]{
                        {"2024-01-01T00:00:00", "ARRAY[[null, 2.0], [3.0, null]]"},
                        {"2024-01-02T00:00:00", "ARRAY[[10.0, 20.0], [30.0, 40.0]]"}
                }
        );
    }

    @Test
    public void testParallelAvgVariablePlusVariable() throws Exception {
        assertParallelGroupBy("DOUBLE[][]",
                "[[1.0,20.0,3.0],[30.0,5.0,null],[50.0,null,null]]",
                new String[][]{
                        {"2024-01-01T00:00:00", "ARRAY[[1.0, null, 3.0], [null, 5.0, null]]"},
                        {"2024-01-02T00:00:00", "ARRAY[[null, 20.0], [30.0, null], [50.0, null]]"}
                }
        );
    }

    @Test
    public void testParallelDifferentShapes() throws Exception {
        assertParallelGroupBy("DOUBLE[][]",
                "[[5.5,11.0,3.0],[17.0,22.5,6.0],[50.0,60.0,null],[70.0,80.0,null]]",
                new String[][]{
                        {"2024-01-01T00:00:00", "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]"},
                        {"2024-01-02T00:00:00", "ARRAY[[10.0, 20.0], [30.0, 40.0], [50.0, 60.0], [70.0, 80.0]]"}
                }
        );
    }

    @Test
    public void testParallelOneGroupNull() throws Exception {
        assertParallelGroupBy("DOUBLE[][]",
                "[[5.5,11.0,16.5],[22.0,27.5,33.0],[38.5,44.0,49.5]]",
                new String[][]{
                        {"2024-01-01T00:00:00", "null"},
                        {"2024-01-02T00:00:00", "null"},
                        {"2024-01-03T00:00:00", "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]"},
                        {"2024-01-04T00:00:00", "ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]]"}
                }
        );
    }

    @Test
    public void testParallelSameShapes() throws Exception {
        assertParallelGroupBy("DOUBLE[][]",
                "[[28.25,56.25,84.25],[112.25,140.25,168.25],[196.25,224.25,252.25]]",
                new String[][]{
                        {"2024-01-01T00:00:00", "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]"},
                        {"2024-01-02T00:00:00", "ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]]"},
                        {"2024-01-03T00:00:00", "ARRAY[[100.0, 200.0, 300.0], [400.0, 500.0, 600.0], [700.0, 800.0, 900.0]]"},
                        {"2024-01-04T00:00:00", "ARRAY[[2.0, 3.0, 4.0], [5.0, 6.0, 7.0], [8.0, 9.0, 10.0]]"}
                }
        );
    }
}
