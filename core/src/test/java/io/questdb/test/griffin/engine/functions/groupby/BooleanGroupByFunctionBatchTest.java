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

import io.questdb.griffin.engine.functions.columns.BooleanColumn;
import io.questdb.griffin.engine.functions.groupby.FirstBooleanGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastBooleanGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class BooleanGroupByFunctionBatchTest extends AbstractGroupByFunctionBatchTest {
    @Test
    public void testFirstBooleanBatch() throws Exception {
        assertMemoryLeak(() -> {
            FirstBooleanGroupByFunction function = new FirstBooleanGroupByFunction(BooleanColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateBooleans(true, false, true);
                function.computeBatch(value, ptr, 3, 0);

                Assert.assertTrue(function.getBool(value));
                Assert.assertTrue(function.supportsBatchComputation());
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testFirstBooleanBatchAccumulates() throws Exception {
        assertMemoryLeak(() -> {
            FirstBooleanGroupByFunction function = new FirstBooleanGroupByFunction(BooleanColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                long ptr = allocateBooleans(true, false);
                function.computeBatch(value, ptr, 2, 0);

                ptr = allocateBooleans(false, true);
                function.computeBatch(value, ptr, 2, 2);

                Assert.assertTrue(function.getBool(value));
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testFirstBooleanBatchEmpty() throws Exception {
        assertMemoryLeak(() -> {
            FirstBooleanGroupByFunction function = new FirstBooleanGroupByFunction(BooleanColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                function.setNull(value);

                function.computeBatch(value, 0, 0, 0);

                Assert.assertFalse(function.getBool(value));
            }
        });
    }

    @Test
    public void testFirstBooleanSetEmpty() throws Exception {
        assertMemoryLeak(() -> {
            FirstBooleanGroupByFunction function = new FirstBooleanGroupByFunction(BooleanColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                Assert.assertFalse(function.getBool(value));
            }
        });
    }

    @Test
    public void testLastBooleanBatch() throws Exception {
        assertMemoryLeak(() -> {
            LastBooleanGroupByFunction function = new LastBooleanGroupByFunction(BooleanColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                function.setNull(value);

                long ptr = allocateBooleans(false, true, false, true);
                function.computeBatch(value, ptr, 4, 0);

                Assert.assertEquals(3, value.getLong(0));
                Assert.assertTrue(function.getBool(value));
                Assert.assertTrue(function.supportsBatchComputation());
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testLastBooleanBatchAccumulates() throws Exception {
        assertMemoryLeak(() -> {
            LastBooleanGroupByFunction function = new LastBooleanGroupByFunction(BooleanColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                function.setNull(value);

                long ptr = allocateBooleans(true, false);
                function.computeBatch(value, ptr, 2, 0);

                ptr = allocateBooleans(false, true);
                function.computeBatch(value, ptr, 2, 2);

                Assert.assertTrue(function.getBool(value));
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testLastBooleanBatchSingle() throws Exception {
        assertMemoryLeak(() -> {
            LastBooleanGroupByFunction function = new LastBooleanGroupByFunction(BooleanColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                function.setNull(value);

                long ptr = allocateBooleans(false);
                function.computeBatch(value, ptr, 1, 0);

                Assert.assertFalse(function.getBool(value));
            } finally {
                freeLast();
            }
        });
    }

    @Test
    public void testLastBooleanSetEmpty() throws Exception {
        assertMemoryLeak(() -> {
            LastBooleanGroupByFunction function = new LastBooleanGroupByFunction(BooleanColumn.newInstance(COLUMN_INDEX));
            try (SimpleMapValue value = prepare(function)) {
                Assert.assertFalse(function.getBool(value));
            }
        });
    }
}
