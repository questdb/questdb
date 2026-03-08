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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class FunctionComplexityTest extends AbstractCairoTest {

    @Test
    public void testAddComplexityOverflowCap() throws Exception {
        assertMemoryLeak(() -> {
            int result = Function.addComplexity(Function.COMPLEXITY_MAX, 100);
            Assert.assertEquals(Function.COMPLEXITY_MAX, result);
        });
    }

    @Test
    public void testAddComplexitySum() throws Exception {
        assertMemoryLeak(() -> {
            int result = Function.addComplexity(3, 5);
            Assert.assertEquals(8, result);
        });
    }

    @Test
    public void testCastAddsComplexity() throws Exception {
        assertMemoryLeak(() -> {
            // a::DOUBLE on a column has complexity COMPLEXITY_CAST + COMPLEXITY_COLUMN = 4,
            // which exceeds default threshold (3) and triggers materialization
            execute("CREATE TABLE t (a INT, ts TIMESTAMP) TIMESTAMP(ts)");
            assertPlanNoLeakCheck(
                    "SELECT a::DOUBLE AS x FROM t ORDER BY x",
                    """
                            Sort light
                              keys: [x]
                                Materialize sort keys
                                    VirtualRecord
                                      functions: [a::double]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testColumnComplexity() throws Exception {
        assertMemoryLeak(() -> {
            try (Function col = IntColumn.newInstance(0)) {
                Assert.assertEquals(Function.COMPLEXITY_COLUMN, col.getComplexity());
            }
        });
    }

    @Test
    public void testComplexityConstants() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(0, Function.COMPLEXITY_NONE);
            Assert.assertEquals(1, Function.COMPLEXITY_COLUMN);
            Assert.assertEquals(2, Function.COMPLEXITY_ARITHMETIC);
            Assert.assertEquals(3, Function.COMPLEXITY_CAST);
            Assert.assertEquals(10, Function.COMPLEXITY_STRING_OP);
            Assert.assertEquals(30, Function.COMPLEXITY_GEO);
            Assert.assertEquals(50, Function.COMPLEXITY_REGEX);
            Assert.assertEquals(80, Function.COMPLEXITY_JSON);
            Assert.assertEquals(1000, Function.COMPLEXITY_SUBQUERY);
            Assert.assertEquals(10_000, Function.COMPLEXITY_MAX);
        });
    }

    @Test
    public void testConstantComplexity() throws Exception {
        assertMemoryLeak(() -> {
            try (Function constant = IntConstant.newInstance(42)) {
                Assert.assertEquals(Function.COMPLEXITY_NONE, constant.getComplexity());
            }
        });
    }

    @Test
    public void testLargeAddComplexityDoesNotOverflow() throws Exception {
        assertMemoryLeak(() -> {
            int result = Function.addComplexity(Integer.MAX_VALUE, Integer.MAX_VALUE);
            Assert.assertEquals(Function.COMPLEXITY_MAX, result);
        });
    }
}
