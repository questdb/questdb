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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.table.AsyncFilterContext;
import io.questdb.griffin.engine.table.AsyncFilterResources;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class AsyncFilterTest extends AbstractCairoTest {
    private static final int NATIVE_FRAME = 2;
    private static final int PARQUET_FRAME = 1;
    private static final int ROWS_PER_DAY = 1_000;

    @Test
    public void testFilterFrameKeepsPassingRowsAndSamplesParquetFramesOnly() throws Exception {
        assertMemoryLeak(() -> {
            // three days of 1_000 rows, a = 1..3_000, the first two days in Parquet
            execute("CREATE TABLE t (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT x::INT, timestamp_sequence('2024-01-01', 86_400_000_000L / " + ROWS_PER_DAY + ") FROM long_sequence(3_000)");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
            final AtMostFilter filter = new AtMostFilter();
            final IntHashSet filterColumns = new IntHashSet();
            filterColumns.add(0);
            try (
                    RecordCursorFactory factory = select("t");
                    PageFrameCursor frameCursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    PageFrameAddressCache addressCache = new PageFrameAddressCache();
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
                    AsyncFilterContext filterCtx = new AsyncFilterContext(
                            configuration,
                            new AsyncFilterResources(null, null, null, null, filter, filterColumns, null),
                            0,
                            0,
                            Long.MAX_VALUE,
                            Long.MAX_VALUE
                    )
            ) {
                addressCache.of(factory.getMetadata(), frameCursor.getColumnMapping(), frameCursor.isExternal());
                int frameCount = 0;
                PageFrame frame;
                while ((frame = frameCursor.next()) != null) {
                    addressCache.add(frameCount++, frame);
                }
                Assert.assertEquals(3, frameCount);
                Assert.assertEquals(PartitionFormat.PARQUET, addressCache.getFrameFormat(PARQUET_FRAME));
                Assert.assertEquals(PartitionFormat.NATIVE, addressCache.getFrameFormat(NATIVE_FRAME));
                filterCtx.initMemoryPools(addressCache, null);
                record.of(frameCursor);

                // a <= 1_500 keeps the first 500 rows of the second day, a <= 2_100 the first 100 of the third
                filter.max = 1_500;
                assertFilteredRows(filterCtx, record, addressCache, PARQUET_FRAME, 500);
                filter.max = 2_100;
                assertFilteredRows(filterCtx, record, addressCache, NATIVE_FRAME, 100);

                // two Parquet frames where every row passes settle the selectivity at 1
                filterCtx.getSelectivityStats(-1).clear();
                filter.max = Integer.MAX_VALUE;
                assertFilteredRows(filterCtx, record, addressCache, PARQUET_FRAME, ROWS_PER_DAY);
                assertFilteredRows(filterCtx, record, addressCache, PARQUET_FRAME, ROWS_PER_DAY);
                Assert.assertFalse(filterCtx.shouldUseLateMaterialization(-1, true, false));

                // a native frame leaves the statistics alone, however few of its rows pass
                filter.max = 0;
                for (int i = 0; i < 10; i++) {
                    assertFilteredRows(filterCtx, record, addressCache, NATIVE_FRAME, 0);
                }
                Assert.assertFalse(filterCtx.shouldUseLateMaterialization(-1, true, false));

                // a Parquet frame without a passing row counts as selectivity 0: the average falls to 0.7^4 = 0.24
                // after four of them, still above the 0.2 threshold, and to 0.7^5 = 0.17 after the fifth
                for (int i = 0; i < 4; i++) {
                    assertFilteredRows(filterCtx, record, addressCache, PARQUET_FRAME, 0);
                }
                Assert.assertFalse(filterCtx.shouldUseLateMaterialization(-1, true, false));
                assertFilteredRows(filterCtx, record, addressCache, PARQUET_FRAME, 0);
                Assert.assertTrue(filterCtx.shouldUseLateMaterialization(-1, true, false));
            }
        });
    }

    private static void assertFilteredRows(
            AsyncFilterContext filterCtx,
            PageFrameMemoryRecord record,
            PageFrameAddressCache addressCache,
            int frameIndex,
            long expectedRowCount
    ) {
        final PageFrameMemory frameMemory = filterCtx.getMemoryPool(-1).navigateTo(frameIndex);
        record.init(frameMemory);
        final long frameRowCount = addressCache.getFrameSize(frameIndex);
        Assert.assertEquals(ROWS_PER_DAY, frameRowCount);
        Assert.assertEquals(expectedRowCount, filterCtx.filterFrame(-1, frameMemory, record, addressCache, frameRowCount));
        final DirectLongList rows = filterCtx.getFilteredRows(-1);
        Assert.assertEquals(expectedRowCount, rows.size());
        for (long i = 0; i < expectedRowCount; i++) {
            Assert.assertEquals(i, rows.get(i));
        }
    }

    private static class AtMostFilter extends BooleanFunction {
        private int max;

        @Override
        public boolean getBool(Record rec) {
            return rec.getInt(0) <= max;
        }
    }
}
