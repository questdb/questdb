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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.lv.MergeBuffer;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MergeBufferTest extends AbstractCairoTest {

    @Test
    public void testAddRowCountsLateRows() throws Exception {
        assertMemoryLeak(() -> {
            try (MergeBuffer buffer = new MergeBuffer(longTsMetadata())) {
                buffer.addRow(row(100, 1_000_000L));
                buffer.addRow(row(200, 2_000_000L));
                buffer.addRow(row(300, 3_000_000L));

                // Drain with watermark = 2s: rows at 1s and 2s drain; 3s retained.
                try (RecordCursor c = buffer.drain(2_000_000L)) {
                    int drained = 0;
                    while (c.hasNext()) drained++;
                    assertEquals(2, drained);
                }
                assertEquals(2_000_000L, buffer.getLastDrainedWatermark());
                assertEquals(0, buffer.getLateRowCount());
                assertEquals(1, buffer.size());

                // Fresh row with ts <= lastDrainedWatermark counts as late.
                buffer.addRow(row(400, 1_500_000L));
                assertEquals(1, buffer.getLateRowCount());

                // Fresh row with ts > lastDrainedWatermark is not late.
                buffer.addRow(row(500, 5_000_000L));
                assertEquals(1, buffer.getLateRowCount());
            }
        });
    }

    @Test
    public void testCompactionPreservesRetainedRowsInSortedOrder() throws Exception {
        assertMemoryLeak(() -> {
            try (MergeBuffer buffer = new MergeBuffer(longTsMetadata())) {
                // Add rows out-of-order; sort index puts them in ts order.
                buffer.addRow(row(3, 3_000_000L));
                buffer.addRow(row(1, 1_000_000L));
                buffer.addRow(row(2, 2_000_000L));
                buffer.addRow(row(4, 4_000_000L));

                // Drain watermark = 2s; rows at 1s and 2s drain; rows at 3s and 4s retained.
                try (RecordCursor c = buffer.drain(2_000_000L)) {
                    Record r = c.getRecord();
                    assertTrue(c.hasNext());
                    assertEquals(1, r.getLong(0));
                    assertTrue(c.hasNext());
                    assertEquals(2, r.getLong(0));
                    assertFalse(c.hasNext());
                }
                assertEquals(2, buffer.size());

                // Re-drain with larger watermark; retained rows drain in ts-sorted order.
                try (RecordCursor c = buffer.drain(10_000_000L)) {
                    Record r = c.getRecord();
                    assertTrue(c.hasNext());
                    assertEquals(3, r.getLong(0));
                    assertTrue(c.hasNext());
                    assertEquals(4, r.getLong(0));
                    assertFalse(c.hasNext());
                }
                assertTrue(buffer.isEmpty());
            }
        });
    }

    @Test
    public void testEmptyDrain() throws Exception {
        assertMemoryLeak(() -> {
            try (MergeBuffer buffer = new MergeBuffer(longTsMetadata())) {
                try (RecordCursor c = buffer.drain(1_000_000L)) {
                    assertFalse(c.hasNext());
                }
                assertTrue(buffer.isEmpty());
            }
        });
    }

    @Test
    public void testPendingLateCountClearsAfterDrain() throws Exception {
        assertMemoryLeak(() -> {
            try (MergeBuffer buffer = new MergeBuffer(longTsMetadata())) {
                buffer.addRow(row(1, 1_000_000L));
                buffer.addRow(row(2, 2_000_000L));
                try (RecordCursor c = buffer.drain(2_000_000L)) {
                    while (c.hasNext()) ;
                }
                assertEquals(0, buffer.getPendingLateCount());

                // Two late rows: pendingLateCount == 2 before the next drain.
                buffer.addRow(row(3, 1_500_000L));
                buffer.addRow(row(4, 500_000L));
                assertEquals(2, buffer.getPendingLateCount());

                try (RecordCursor c = buffer.replay(2_500_000L)) {
                    while (c.hasNext()) ;
                }
                // replay() consumed the pending batch; count resets.
                assertEquals(0, buffer.getPendingLateCount());
                // lateRowCount is cumulative across the lifetime of the buffer.
                assertEquals(2, buffer.getLateRowCount());
            }
        });
    }

    @Test
    public void testReplayReemitsRetainedRowsInSortedOrder() throws Exception {
        assertMemoryLeak(() -> {
            try (MergeBuffer buffer = new MergeBuffer(longTsMetadata())) {
                buffer.addRow(row(1, 1_000_000L));
                buffer.addRow(row(2, 2_000_000L));
                buffer.addRow(row(3, 3_000_000L));
                try (RecordCursor c = buffer.drain(3_000_000L)) {
                    while (c.hasNext()) ;
                }

                // Late row lands between the already-drained row 1 and row 2.
                buffer.addRow(row(4, 1_500_000L));

                // replay re-iterates every retained row up to the watermark, in sort order.
                try (RecordCursor c = buffer.replay(3_000_000L)) {
                    Record r = c.getRecord();
                    assertTrue(c.hasNext());
                    assertEquals(1, r.getLong(0));
                    assertTrue(c.hasNext());
                    assertEquals(4, r.getLong(0));
                    assertTrue(c.hasNext());
                    assertEquals(2, r.getLong(0));
                    assertTrue(c.hasNext());
                    assertEquals(3, r.getLong(0));
                    assertFalse(c.hasNext());
                }
                // After replay, a subsequent incremental drain has nothing new to emit.
                assertTrue(buffer.isEmpty());
            }
        });
    }

    @Test
    public void testResetClearsBuffer() throws Exception {
        assertMemoryLeak(() -> {
            try (MergeBuffer buffer = new MergeBuffer(longTsMetadata())) {
                buffer.addRow(row(1, 1_000_000L));
                buffer.addRow(row(2, 2_000_000L));
                try (RecordCursor c = buffer.drain(1_500_000L)) {
                    while (c.hasNext()) ;
                }
                assertEquals(1_500_000L, buffer.getLastDrainedWatermark());

                buffer.reset();
                assertTrue(buffer.isEmpty());
                assertEquals(Long.MIN_VALUE, buffer.getLastDrainedWatermark());
                assertEquals(0, buffer.getLateRowCount());
            }
        });
    }

    @Test
    public void testRetentionAndCompactReclaimPrefix() throws Exception {
        assertMemoryLeak(() -> {
            try (MergeBuffer buffer = new MergeBuffer(longTsMetadata())) {
                buffer.addRow(row(1, 1_000_000L));
                buffer.addRow(row(2, 2_000_000L));
                buffer.addRow(row(3, 3_000_000L));
                buffer.addRow(row(4, 4_000_000L));
                try (RecordCursor c = buffer.drain(4_000_000L)) {
                    while (c.hasNext()) ;
                }
                // All four rows drained. Retention of 1s evicts rows at ts 1s, 2s, 3s;
                // only row at 4s survives (cutoff = maxTs - 1s = 3s; ts > 3s).
                buffer.applyRetention(1_000_000L);
                assertTrue(buffer.needsCompact());
                buffer.compact();

                // After compact, a replay of the retained range emits only row 4.
                try (RecordCursor c = buffer.replay(4_000_000L)) {
                    Record r = c.getRecord();
                    assertTrue(c.hasNext());
                    assertEquals(4, r.getLong(0));
                    assertFalse(c.hasNext());
                }
            }
        });
    }

    private static GenericRecordMetadata longTsMetadata() {
        GenericRecordMetadata m = new GenericRecordMetadata();
        m.add(new TableColumnMetadata("value", ColumnType.LONG));
        m.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP));
        m.setTimestampIndex(1);
        return m;
    }

    private static Record row(long value, long ts) {
        return new Record() {
            @Override
            public long getLong(int col) {
                return col == 0 ? value : 0;
            }

            @Override
            public long getTimestamp(int col) {
                return col == 1 ? ts : 0;
            }
        };
    }
}
