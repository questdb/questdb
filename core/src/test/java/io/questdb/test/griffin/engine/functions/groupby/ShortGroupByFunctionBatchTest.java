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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.griffin.engine.functions.columns.ShortColumn;
import io.questdb.griffin.engine.functions.groupby.AvgShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumShortGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class ShortGroupByFunctionBatchTest extends AbstractGroupByFunctionBatchTest {
    @Test
    public void testAvgShortBatch() throws Exception {
        assertMemoryLeak(() -> {
            AvgShortGroupByFunction function = new AvgShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateShorts((short) 2, (short) 4, (short) 6);
                function.computeBatch(value, ptr, 3, 0);

                Assert.assertEquals(4.0, function.getDouble(value), 0.0);
                Assert.assertTrue(function.supportsBatchComputation());
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testAvgShortSetEmpty() throws Exception {
        assertMemoryLeak(() -> {
            AvgShortGroupByFunction function = new AvgShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                Assert.assertTrue(Double.isNaN(function.getDouble(value)));
            }
        });
    }

    @Test
    public void testFirstShortBatch() throws Exception {
        assertMemoryLeak(() -> {
            FirstShortGroupByFunction function = new FirstShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateShorts((short) 5, (short) 6, (short) 7);
                function.computeBatch(value, ptr, 3, 0);

                Assert.assertEquals(5, function.getShort(value));
                Assert.assertTrue(function.supportsBatchComputation());
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testFirstShortBatchAccumulates() throws Exception {
        assertMemoryLeak(() -> {
            FirstShortGroupByFunction function = new FirstShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateShorts((short) 5, (short) 6);
                function.computeBatch(value, ptr, 2, 0);

                ptr = allocateShorts((short) 7, (short) 8);
                function.computeBatch(value, ptr, 2, 2);

                Assert.assertEquals(5, function.getShort(value));
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testFirstShortBatchAllNull() throws Exception {
        assertMemoryLeak(() -> {
            FirstShortGroupByFunction function = new FirstShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateShorts(Short.MIN_VALUE, (short) 1);
                function.computeBatch(value, ptr, 2, 0);

                Assert.assertEquals(Short.MIN_VALUE, function.getShort(value));
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testFirstShortBatchEmpty() throws Exception {
        assertMemoryLeak(() -> {
            FirstShortGroupByFunction function = new FirstShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                function.setNull(value);

                function.computeBatch(value, 0, 0, 0);

                Assert.assertEquals(0, function.getShort(value));
            }
        });
    }

    @Test
    public void testFirstShortSetEmpty() throws Exception {
        assertMemoryLeak(() -> {
            FirstShortGroupByFunction function = new FirstShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                Assert.assertEquals(0, function.getShort(value));
            }
        });
    }

    @Test
    public void testLastShortBatch() throws Exception {
        assertMemoryLeak(() -> {
            LastShortGroupByFunction function = new LastShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                function.setNull(value);

                long ptr = allocateShorts((short) 11, (short) 22, (short) 33);
                function.computeBatch(value, ptr, 3, 0);

                Assert.assertEquals(2, value.getLong(0));
                Assert.assertEquals(33, function.getShort(value));
                Assert.assertTrue(function.supportsBatchComputation());
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testLastShortBatchAccumulates() throws Exception {
        assertMemoryLeak(() -> {
            LastShortGroupByFunction function = new LastShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                function.setNull(value);

                long ptr = allocateShorts((short) 11, (short) 22);
                function.computeBatch(value, ptr, 2, 0);

                ptr = allocateShorts((short) 33, (short) 44);
                function.computeBatch(value, ptr, 2, 2);

                Assert.assertEquals(44, function.getShort(value));
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testLastShortBatchAllNull() throws Exception {
        assertMemoryLeak(() -> {
            LastShortGroupByFunction function = new LastShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                function.setNull(value);

                long ptr = allocateShorts((short) 11, Short.MIN_VALUE);
                function.computeBatch(value, ptr, 2, 0);

                Assert.assertEquals(Short.MIN_VALUE, function.getShort(value));
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testLastShortSetEmpty() throws Exception {
        assertMemoryLeak(() -> {
            LastShortGroupByFunction function = new LastShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                Assert.assertEquals(0, function.getShort(value));
            }
        });
    }

    @Test
    public void testMaxShortBatch() throws Exception {
        assertMemoryLeak(() -> {
            MaxShortGroupByFunction function = new MaxShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateShorts((short) 4, (short) 1, (short) 9, (short) 3);
                function.computeBatch(value, ptr, 4, 0);

                Assert.assertEquals(9, function.getInt(value));
                Assert.assertTrue(function.supportsBatchComputation());
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testMaxShortBatchAccumulates() throws Exception {
        assertMemoryLeak(() -> {
            MaxShortGroupByFunction function = new MaxShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateShorts((short) 1, (short) 5);
                function.computeBatch(value, ptr, 2, 0);

                ptr = allocateShorts((short) 7, (short) 3);
                function.computeBatch(value, ptr, 2, 2);

                Assert.assertEquals(7, function.getInt(value));
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testMaxShortBatchKeepsExistingHigher() throws Exception {
        assertMemoryLeak(() -> {
            MaxShortGroupByFunction function = new MaxShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateShorts((short) 50);
                function.computeBatch(value, ptr, 1, 0);

                ptr = allocateShorts((short) 10, (short) 20);
                function.computeBatch(value, ptr, 2, 1);

                Assert.assertEquals(50, function.getInt(value));
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testMaxShortBatchNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            MaxShortGroupByFunction function = new MaxShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateShorts((short) -10, (short) -5, (short) -20);
                function.computeBatch(value, ptr, 3, 0);

                Assert.assertEquals(-5, function.getInt(value));
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testMaxShortBatchZeroCountKeepsExistingValue() throws Exception {
        assertMemoryLeak(() -> {
            MaxShortGroupByFunction function = new MaxShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                value.putInt(0, 42);

                function.computeBatch(value, 0, 0, 0);

                Assert.assertEquals(42, function.getInt(value));
            }
        });
    }

    @Test
    public void testMaxShortSetEmpty() throws Exception {
        assertMemoryLeak(() -> {
            MaxShortGroupByFunction function = new MaxShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
            }
        });
    }

    @Test
    public void testMinShortBatch() throws Exception {
        assertMemoryLeak(() -> {
            MinShortGroupByFunction function = new MinShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateShorts((short) 4, (short) 1, (short) 9, (short) 3);
                function.computeBatch(value, ptr, 4, 0);

                Assert.assertEquals(1, function.getInt(value));
                Assert.assertTrue(function.supportsBatchComputation());
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testMinShortBatchAccumulates() throws Exception {
        assertMemoryLeak(() -> {
            MinShortGroupByFunction function = new MinShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateShorts((short) 7, (short) 5);
                function.computeBatch(value, ptr, 2, 0);

                ptr = allocateShorts((short) 1, (short) 3);
                function.computeBatch(value, ptr, 2, 2);

                Assert.assertEquals(1, function.getInt(value));
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testMinShortBatchKeepsExistingLower() throws Exception {
        assertMemoryLeak(() -> {
            MinShortGroupByFunction function = new MinShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateShorts((short) 5);
                function.computeBatch(value, ptr, 1, 0);

                ptr = allocateShorts((short) 10, (short) 20);
                function.computeBatch(value, ptr, 2, 1);

                Assert.assertEquals(5, function.getInt(value));
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testMinShortBatchNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            MinShortGroupByFunction function = new MinShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateShorts((short) -10, (short) -5, (short) -20);
                function.computeBatch(value, ptr, 3, 0);

                Assert.assertEquals(-20, function.getInt(value));
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testMinShortBatchZeroCountKeepsExistingValue() throws Exception {
        assertMemoryLeak(() -> {
            MinShortGroupByFunction function = new MinShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                value.putInt(0, 42);

                function.computeBatch(value, 0, 0, 0);

                Assert.assertEquals(42, function.getInt(value));
            }
        });
    }

    @Test
    public void testMinShortSetEmpty() throws Exception {
        assertMemoryLeak(() -> {
            MinShortGroupByFunction function = new MinShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
            }
        });
    }

    @Test
    public void testSumShortBatch() throws Exception {
        assertMemoryLeak(() -> {
            SumShortGroupByFunction function = new SumShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateShorts((short) 1, (short) 2, (short) 3, (short) 4);
                function.computeBatch(value, ptr, 4, 0);

                Assert.assertEquals(10L, function.getLong(value));
                Assert.assertTrue(function.supportsBatchComputation());
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testSumShortBatchAccumulates() throws Exception {
        assertMemoryLeak(() -> {
            SumShortGroupByFunction function = new SumShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateShorts((short) 1, (short) 2);
                function.computeBatch(value, ptr, 2, 0);

                ptr = allocateShorts((short) 3, (short) 4);
                function.computeBatch(value, ptr, 2, 0);

                Assert.assertEquals(10L, function.getLong(value));
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testSumShortBatchAllZero() throws Exception {
        assertMemoryLeak(() -> {
            SumShortGroupByFunction function = new SumShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateShorts((short) 0, (short) 0);
                function.computeBatch(value, ptr, 2, 0);

                Assert.assertEquals(0L, function.getLong(value));
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testSumShortBatchZeroCountKeepsExistingValue() throws Exception {
        assertMemoryLeak(() -> {
            SumShortGroupByFunction function = new SumShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                value.putLong(0, 55);

                function.computeBatch(value, 0, 0, 0);

                Assert.assertEquals(55L, function.getLong(value));
            }
        });
    }

    @Test
    public void testSumShortSetEmpty() throws Exception {
        assertMemoryLeak(() -> {
            SumShortGroupByFunction function = new SumShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
            }
        });
    }
}
