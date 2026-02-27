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

public class DoubleArrayElemMaxParallelGroupByTest extends AbstractDoubleArrayElemParallelGroupByTest {

    @Override
    protected String funcName() {
        return "array_elem_max";
    }

    @Test
    public void testParallelDifferentShapes() throws Exception {
        assertParallelGroupBy("DOUBLE[][]",
                "[[10.0,20.0,3.0],[30.0,40.0,6.0],[50.0,60.0,null],[70.0,80.0,null]]",
                new String[][]{
                        {"2024-01-01T00:00:00", "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]"},
                        {"2024-01-02T00:00:00", "ARRAY[[10.0, 20.0], [30.0, 40.0], [50.0, 60.0], [70.0, 80.0]]"}
                }
        );
    }

    @Test
    public void testParallelOneGroupNull() throws Exception {
        assertParallelGroupBy("DOUBLE[][]",
                "[[10.0,20.0,30.0],[40.0,50.0,60.0],[70.0,80.0,90.0]]",
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
                "[[100.0,200.0,300.0],[400.0,500.0,600.0],[700.0,800.0,900.0]]",
                new String[][]{
                        {"2024-01-01T00:00:00", "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]"},
                        {"2024-01-02T00:00:00", "ARRAY[[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]]"},
                        {"2024-01-03T00:00:00", "ARRAY[[100.0, 200.0, 300.0], [400.0, 500.0, 600.0], [700.0, 800.0, 900.0]]"},
                        {"2024-01-04T00:00:00", "ARRAY[[2.0, 3.0, 4.0], [5.0, 6.0, 7.0], [8.0, 9.0, 10.0]]"}
                }
        );
    }
}
