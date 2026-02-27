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

public class DoubleArrayElemMinParallelGroupByTest extends AbstractDoubleArrayElemParallelGroupByTest {

    @Override
    protected String funcName() {
        return "array_elem_min";
    }

    @Test
    public void testParallelMergeDifferentShapes() throws Exception {
        assertKeyedParallelGroupBy(
                "grp\tarr\n" +
                        "1\t[[1.0,2.0],[3.0,4.0]]\n" +
                        "2\t[[10.0,20.0,70.0],[30.0,40.0,100.0]]\n",
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
                        "1\t[[1.0,2.0],[3.0,4.0]]\n" +
                        "2\t[[10.0,20.0],[30.0,40.0]]\n",
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
    public void testParallelDifferentShapes() throws Exception {
        assertParallelGroupBy("DOUBLE[][]",
                "[[1.0,2.0,3.0],[4.0,5.0,6.0],[50.0,60.0,null],[70.0,80.0,null]]",
                new String[][]{
                        {"2024-01-01T00:00:00", "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]"},
                        {"2024-01-02T00:00:00", "ARRAY[[10.0, 20.0], [30.0, 40.0], [50.0, 60.0], [70.0, 80.0]]"}
                }
        );
    }

    @Test
    public void testParallelOneGroupNull() throws Exception {
        assertParallelGroupBy("DOUBLE[][]",
                "[[1.0,2.0,3.0],[4.0,5.0,6.0],[7.0,8.0,9.0]]",
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
                "[[1.0,2.0,3.0],[4.0,5.0,6.0],[7.0,8.0,9.0]]",
                new String[][]{
                        {"2024-01-01T00:00:00", "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]"},
                        {"2024-01-02T00:00:00", "ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]]"},
                        {"2024-01-03T00:00:00", "ARRAY[[100.0, 200.0, 300.0], [400.0, 500.0, 600.0], [700.0, 800.0, 900.0]]"},
                        {"2024-01-04T00:00:00", "ARRAY[[2.0, 3.0, 4.0], [5.0, 6.0, 7.0], [8.0, 9.0, 10.0]]"}
                }
        );
    }
}
