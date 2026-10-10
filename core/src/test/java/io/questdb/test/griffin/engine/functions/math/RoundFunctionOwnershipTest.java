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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.math.RoundDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.RoundDownDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.RoundHalfEvenDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.RoundUpDoubleFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class RoundFunctionOwnershipTest extends AbstractCairoTest {
    private static final ObjList<FunctionFactory> FACTORIES = new ObjList<>(
            new RoundDoubleFunctionFactory(), new RoundDownDoubleFunctionFactory(),
            new RoundUpDoubleFunctionFactory(), new RoundHalfEvenDoubleFunctionFactory()
    );

    @Test
    public void testAcceptedConstantScaleRetainsValueUntilResultCloses() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < FACTORIES.size(); i++) {
                for (int scale : new int[]{0, 2, -1}) {
                    final CountingDoubleFunction value = new CountingDoubleFunction();
                    try (Function result = FACTORIES.getQuick(i).newInstance(0,
                            new ObjList<>(value, IntConstant.newInstance(scale)), new IntList(), configuration, sqlExecutionContext)) {
                        Assert.assertEquals(0, value.closeCount);
                        Assert.assertFalse(Double.isNaN(result.getDouble(null)));
                        Assert.assertEquals(1, value.readCount);
                    }
                    Assert.assertEquals(1, value.closeCount);
                }
            }
        });
    }

    @Test
    public void testDiscardedValueClosesExactlyOnceWithoutEvaluation() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < FACTORIES.size(); i++) {
                for (int scale : new int[]{Numbers.INT_NULL, 1000, -1000}) {
                    final CountingDoubleFunction value = new CountingDoubleFunction();
                    try (Function result = FACTORIES.getQuick(i).newInstance(0,
                            new ObjList<>(value, IntConstant.newInstance(scale)), new IntList(), configuration, sqlExecutionContext)) {
                        Assert.assertSame(DoubleConstant.NULL, result);
                        Assert.assertEquals(1, value.closeCount);
                        Assert.assertEquals(0, value.readCount);
                        Assert.assertTrue(Double.isNaN(result.getDouble(null)));
                    }
                    Assert.assertEquals(1, value.closeCount);
                }
            }
        });
    }

    @Test
    public void testDynamicScaleRetainsBothChildrenUntilResultCloses() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < FACTORIES.size(); i++) {
                final CountingDoubleFunction value = new CountingDoubleFunction();
                final CountingIntFunction scale = new CountingIntFunction();
                try (Function result = FACTORIES.getQuick(i).newInstance(0,
                        new ObjList<>(value, scale), new IntList(), configuration, sqlExecutionContext)) {
                    Assert.assertEquals(0, value.closeCount);
                    Assert.assertEquals(0, scale.closeCount);
                    Assert.assertFalse(Double.isNaN(result.getDouble(null)));
                    Assert.assertEquals(1, value.readCount);
                    Assert.assertEquals(1, scale.readCount);
                }
                Assert.assertEquals(1, value.closeCount);
                Assert.assertEquals(1, scale.closeCount);
            }
        });
    }

    private static class CountingDoubleFunction extends DoubleFunction {
        private int closeCount;
        private int readCount;

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public double getDouble(Record rec) {
            readCount++;
            return 14.7778;
        }
    }

    private static class CountingIntFunction extends IntFunction {
        private int closeCount;
        private int readCount;

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public int getInt(Record rec) {
            readCount++;
            return 2;
        }
    }
}
