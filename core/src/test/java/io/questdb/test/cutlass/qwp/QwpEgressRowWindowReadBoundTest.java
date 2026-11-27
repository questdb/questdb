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

package io.questdb.test.cutlass.qwp;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.table.AbstractPageFrameRecordCursor;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The bound a QWP row window hands {@code RecordCursor.skipRows} for the rows
 * it reads after the skip, {@link QwpEgressUpgradeProcessor#rowWindowReadBound}.
 * A parquet frame decodes only that many rows. A requested row count lifts
 * the bound only when the cursor does not know its size: then the count reads
 * the rest of the cursor, which the bound would cut short. The row windows and
 * counts this produces on the wire are pinned by
 * {@link QwpEgressQueryDetailsWireTest#testWindowOnParquetPartitions}.
 */
public class QwpEgressRowWindowReadBoundTest extends AbstractCairoTest {

    @Test
    public void testCountedWindowOnParquetDecodesOnlyTheWindow() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100_000);
            execute("CREATE TABLE t (ts TIMESTAMP, a LONG, b LONG, c LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // two partitions of 200_000 rows; the first goes to parquet, two row groups
            execute("INSERT INTO t SELECT (((x - 1) / 200_000) * 86_400_000_000 + (x - 1) % 200_000)::TIMESTAMP, x, x, x "
                    + "FROM long_sequence(400_000)");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            try (RecordCursorFactory factory = select("SELECT * FROM t")) {
                final RecordCursorFactory scan = factory instanceof QueryProgress ? factory.getBaseFactory() : factory;
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                for (int counted = 0; counted < 2; counted++) {
                    try (RecordCursor cursor = scan.getCursor(sqlExecutionContext)) {
                        final long size = cursor.size();
                        Assert.assertEquals("a table scan knows its size", 400_000, size);
                        final long bound = QwpEgressUpgradeProcessor.rowWindowReadBound(100, counted == 1, size);
                        Assert.assertEquals(100, bound);
                        Assert.assertEquals(100 * 4 * Long.BYTES, readWindow(cursor, counter, 1_000, bound));
                        // the count is the cursor's size, read without the rows past the window
                        Assert.assertEquals(400_000, cursor.size());
                    }
                }
                // without the bound, the landing row group decodes to its end
                try (RecordCursor cursor = scan.getCursor(sqlExecutionContext)) {
                    final long decodedBytes = readWindow(cursor, counter, 1_000, RecordCursor.UNBOUNDED_ROW_COUNT);
                    Assert.assertTrue(String.valueOf(decodedBytes), decodedBytes > 100 * 100 * 4 * Long.BYTES);
                }
            }
        });
    }

    @Test
    public void testReadBound() {
        // without a count, the window bounds the read
        Assert.assertEquals(100, QwpEgressUpgradeProcessor.rowWindowReadBound(100, false, -1));
        Assert.assertEquals(100, QwpEgressUpgradeProcessor.rowWindowReadBound(100, false, 5_000));
        Assert.assertEquals(0, QwpEgressUpgradeProcessor.rowWindowReadBound(0, false, -1));
        // a count taken from the cursor's size keeps the bound, even at 0 rows
        Assert.assertEquals(100, QwpEgressUpgradeProcessor.rowWindowReadBound(100, true, 5_000));
        Assert.assertEquals(0, QwpEgressUpgradeProcessor.rowWindowReadBound(0, true, 0));
        // a count that reads the rest of the cursor lifts it
        Assert.assertEquals(RecordCursor.UNBOUNDED_ROW_COUNT, QwpEgressUpgradeProcessor.rowWindowReadBound(100, true, -1));
        Assert.assertEquals(RecordCursor.UNBOUNDED_ROW_COUNT, QwpEgressUpgradeProcessor.rowWindowReadBound(0, true, -1));
        // a window without max_rows has no bound to keep
        Assert.assertEquals(RecordCursor.UNBOUNDED_ROW_COUNT, QwpEgressUpgradeProcessor.rowWindowReadBound(Long.MAX_VALUE, true, 5_000));
    }

    /**
     * Skips {@code skipRows} rows with {@code bound} as the rows read after
     * the skip, as a QWP row window does, reads the 100 rows of the window and
     * returns the bytes the cursor's page frames decoded.
     */
    private static long readWindow(RecordCursor cursor, RecordCursor.Counter counter, long skipRows, long bound) {
        counter.set(skipRows);
        cursor.skipRows(counter, bound);
        Assert.assertEquals(0, counter.get());
        for (int i = 0; i < 100; i++) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(skipRows + i + 1, cursor.getRecord().getLong(1));
        }
        return ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool().getCachedBytes();
    }
}
