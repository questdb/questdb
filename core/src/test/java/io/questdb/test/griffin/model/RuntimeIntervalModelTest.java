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

package io.questdb.test.griffin.model;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.model.IntervalDynamicIndicator;
import io.questdb.griffin.model.IntervalOperation;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.model.RuntimeIntervalModel;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Covers the legacy position-list fallback of {@link RuntimeIntervalModel}: models constructed
 * without a dynamic-range position list (or with a shorter one, as legacy callers could produce)
 * must evaluate their dynamic intervals normally, falling back to position 0 for error
 * reporting instead of failing on the missing list.
 */
public class RuntimeIntervalModelTest extends AbstractCairoTest {

    @Test
    public void testDynamicIntervalWithNullPositionList() throws Exception {
        assertMemoryLeak(() -> {
            final LongList intervals = new LongList();
            IntervalUtils.encodeInterval(
                    1_000L,
                    0,
                    (short) 0,
                    IntervalDynamicIndicator.IS_HI_DYNAMIC,
                    IntervalOperation.INTERSECT,
                    intervals
            );
            final ObjList<Function> dynamicFunctions = new ObjList<>();
            dynamicFunctions.add(TimestampConstant.newInstance(5_000L, ColumnType.TIMESTAMP));
            // legacy constructor: no position list at all
            final RuntimeIntervalModel model = new RuntimeIntervalModel(
                    ColumnType.getTimestampDriver(ColumnType.TIMESTAMP),
                    PartitionBy.DAY,
                    intervals,
                    dynamicFunctions
            );
            try {
                final LongList out = model.calculateIntervals(sqlExecutionContext);
                Assert.assertEquals(2, out.size());
                Assert.assertEquals(1_000L, out.getQuick(0));
                Assert.assertEquals(5_000L, out.getQuick(1));
            } finally {
                model.close();
            }
        });
    }

    @Test
    public void testDynamicIntervalWithShortPositionList() throws Exception {
        assertMemoryLeak(() -> {
            final LongList intervals = new LongList();
            IntervalUtils.encodeInterval(
                    2_000L,
                    0,
                    (short) 0,
                    IntervalDynamicIndicator.IS_HI_DYNAMIC,
                    IntervalOperation.INTERSECT,
                    intervals
            );
            final ObjList<Function> dynamicFunctions = new ObjList<>();
            dynamicFunctions.add(TimestampConstant.newInstance(7_000L, ColumnType.TIMESTAMP));
            // position list shorter than the dynamic function list
            final RuntimeIntervalModel model = new RuntimeIntervalModel(
                    ColumnType.getTimestampDriver(ColumnType.TIMESTAMP),
                    PartitionBy.DAY,
                    intervals,
                    dynamicFunctions,
                    new IntList()
            );
            try {
                final LongList out = model.calculateIntervals(sqlExecutionContext);
                Assert.assertEquals(2, out.size());
                Assert.assertEquals(2_000L, out.getQuick(0));
                Assert.assertEquals(7_000L, out.getQuick(1));
            } finally {
                model.close();
            }
        });
    }
}
