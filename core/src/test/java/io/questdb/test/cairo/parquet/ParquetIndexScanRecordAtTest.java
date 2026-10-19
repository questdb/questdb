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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.LongList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * An index scan over a parquet partition decodes every row group frame, matching or not, and
 * the page frame memory pool recycles its decode buffers once its cache is full. A record bound
 * to a recycled buffer must not keep serving it: {@code recordAt()} on a row of an early frame,
 * after the scan decoded later frames into that frame's buffer, has to decode the frame again.
 */
public class ParquetIndexScanRecordAtTest extends AbstractCairoTest {

    @Test
    public void testRecordAtAfterLaterFramesRecycleTheBuffer() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createParquetPartition();
            try (
                    RecordCursorFactory factory = select("SELECT s, v, ts FROM pt WHERE s = 'z' AND ts IN '2024-01-01'");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                final Record record = cursor.getRecord();
                final LongList rowIds = new LongList();
                while (cursor.hasNext()) {
                    Assert.assertEquals(2, record.getInt(1));
                    rowIds.add(record.getRowId());
                }
                Assert.assertEquals(1, rowIds.size());
                cursor.recordAt(record, rowIds.getQuick(0));
                Assert.assertEquals(2, record.getInt(1));
                Assert.assertEquals("z", record.getSymA(0).toString());
            }
        });
    }

    @Test
    public void testRecordAtBatteryOnIndexScan() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createParquetPartition();
            assertQuery("SELECT s, v, ts FROM pt WHERE s = 'z' AND ts IN '2024-01-01'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            s\tv\tts
                            z\t2\t2024-01-01T00:02:00.000000Z
                            """);
        });
    }

    private static void createParquetPartition() throws Exception {
        execute("CREATE TABLE pt (s SYMBOL INDEX, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        // 40 rows in 10 row groups of 4; the only 'z' sits in the first row group
        execute("""
                INSERT INTO pt
                SELECT CASE WHEN x = 2 THEN 'z' ELSE rnd_symbol('a', 'b', 'c') END, x::INT, '2024-01-01'::TIMESTAMP + x * 60_000_000L
                FROM long_sequence(40)
                """);
        execute("INSERT INTO pt VALUES ('a', 99, '2024-01-02T00:00:00.000000Z')");
        drainWalQueue();
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
    }
}
