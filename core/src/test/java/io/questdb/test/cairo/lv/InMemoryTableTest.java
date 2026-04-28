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
import io.questdb.cairo.lv.InMemoryTable;
import io.questdb.cairo.lv.LiveViewRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.std.str.GcUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class InMemoryTableTest extends AbstractCairoTest {

    @Test
    public void testApplyRetentionAdvancesReadStartFixedSize() throws Exception {
        assertMemoryLeak(() -> {
            try (InMemoryTable table = new InMemoryTable()) {
                table.init(longTsMetadata());
                for (int i = 0; i < 10; i++) {
                    appendLongTsRow(table, i, (i + 1) * 1_000_000L);
                }
                long dataSizeBefore = table.getDataSize(0);
                long physicalBefore = table.getPhysicalRowCount();
                assertEquals(10, table.getRowCount());
                assertEquals(0, table.getReadStart());

                // RETENTION 3s: maxTs=10s, cutoff=7s. Rows with ts <= 7s evicted (7 rows). Visible: 8s, 9s, 10s.
                table.applyRetention(3_000_000L);

                assertEquals(7, table.getReadStart());
                assertEquals(3, table.getRowCount());
                // The bytes stay allocated — physical row count and data size are unchanged.
                assertEquals(physicalBefore, table.getPhysicalRowCount());
                assertEquals(dataSizeBefore, table.getDataSize(0));

                // Visible rows surface through the LiveViewRecord at virtual indices 0, 1, 2.
                LiveViewRecord rec = new LiveViewRecord(table);
                rec.setRow(0);
                assertEquals(7, rec.getLong(0));
                assertEquals(8_000_000L, rec.getTimestamp(1));
                rec.setRow(2);
                assertEquals(9, rec.getLong(0));
                assertEquals(10_000_000L, rec.getTimestamp(1));
            }
        });
    }

    @Test
    public void testApplyRetentionEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            try (InMemoryTable table = new InMemoryTable()) {
                table.init(longTsMetadata());
                table.applyRetention(1_000_000L);
                assertEquals(0, table.getReadStart());
                assertEquals(0, table.getRowCount());
                assertEquals(0, table.getPhysicalRowCount());
            }
        });
    }

    @Test
    public void testApplyRetentionEvictsAllRows() throws Exception {
        assertMemoryLeak(() -> {
            try (InMemoryTable table = new InMemoryTable()) {
                table.init(longTsMetadata());
                appendLongTsRow(table, 1, 1_000_000L);
                appendLongTsRow(table, 2, 2_000_000L);
                // RETENTION 0 evicts everything with ts <= maxTs (which is every row).
                table.applyRetention(0);
                // Implementation skips zero/negative retention as a no-op (see applyRetention guard).
                assertEquals(0, table.getReadStart());
                assertEquals(2, table.getRowCount());
            }
        });
    }

    @Test
    public void testApplyRetentionTwiceMonotonicallyAdvancesReadStart() throws Exception {
        assertMemoryLeak(() -> {
            try (InMemoryTable table = new InMemoryTable()) {
                table.init(longTsMetadata());
                for (int i = 0; i < 10; i++) {
                    appendLongTsRow(table, i, (i + 1) * 1_000_000L);
                }
                table.applyRetention(5_000_000L); // cutoff = 5s; visible: 6s..10s
                long firstReadStart = table.getReadStart();
                assertEquals(5, firstReadStart);
                assertEquals(5, table.getRowCount());

                // Tighter retention; readStart advances further.
                table.applyRetention(2_000_000L); // cutoff = 8s; visible: 9s, 10s
                assertEquals(8, table.getReadStart());
                assertEquals(2, table.getRowCount());
            }
        });
    }

    @Test
    public void testClearRowsResetsReadStart() throws Exception {
        assertMemoryLeak(() -> {
            try (InMemoryTable table = new InMemoryTable()) {
                table.init(longTsMetadata());
                for (int i = 0; i < 5; i++) {
                    appendLongTsRow(table, i, (i + 1) * 1_000_000L);
                }
                table.applyRetention(1_000_000L);
                assertNotEquals(0, table.getReadStart());

                table.clearRows();
                assertEquals(0, table.getReadStart());
                assertEquals(0, table.getRowCount());
                assertEquals(0, table.getPhysicalRowCount());
            }
        });
    }

    @Test
    public void testCopyFromCompactsEvictedPrefixFixedSize() throws Exception {
        assertMemoryLeak(() -> {
            try (InMemoryTable src = new InMemoryTable(); InMemoryTable dst = new InMemoryTable()) {
                src.init(longTsMetadata());
                dst.init(longTsMetadata());
                for (int i = 0; i < 10; i++) {
                    appendLongTsRow(src, i, (i + 1) * 1_000_000L);
                }
                src.applyRetention(2_000_000L); // maxTs=10s, cutoff=8s; visible: 9s, 10s
                long srcDataSize = src.getDataSize(0);

                dst.copyFrom(src);

                assertEquals(0, dst.getReadStart());
                assertEquals(2, dst.getRowCount());
                assertEquals(2, dst.getPhysicalRowCount());
                // Compaction shrinks the destination data region to just the live rows.
                assertEquals(2 * Long.BYTES, dst.getDataSize(0));
                // Source is untouched.
                assertEquals(8, src.getReadStart());
                assertEquals(srcDataSize, src.getDataSize(0));

                LiveViewRecord rec = new LiveViewRecord(dst);
                rec.setRow(0);
                assertEquals(8, rec.getLong(0));
                assertEquals(9_000_000L, rec.getTimestamp(1));
                rec.setRow(1);
                assertEquals(9, rec.getLong(0));
                assertEquals(10_000_000L, rec.getTimestamp(1));
            }
        });
    }

    @Test
    public void testCopyFromCompactsEvictedPrefixVarchar() throws Exception {
        assertMemoryLeak(() -> {
            try (InMemoryTable src = new InMemoryTable(); InMemoryTable dst = new InMemoryTable()) {
                src.init(varcharTsMetadata());
                dst.init(varcharTsMetadata());

                // Mix of fully-inlined values (<= 9 bytes) and out-of-line values to exercise
                // both VARCHAR descriptor branches in the aux shift.
                String[] values = {
                        "aaa",                                 // inlined
                        "bbbbbbbbbbbbbb",                      // out of line
                        "cc",                                  // inlined
                        "ddddddddddddddddddddddddd",           // out of line
                        "eee",                                 // inlined
                        "ffffffffffffffffffff",                // out of line
                };
                for (int i = 0; i < values.length; i++) {
                    appendVarcharTsRow(src, values[i], (i + 1) * 1_000_000L);
                }
                long srcDataSize = src.getDataSize(0);
                src.applyRetention(2_000_000L); // maxTs=6s, cutoff=4s; visible: rows ts=5s, 6s
                assertEquals(4, src.getReadStart());
                assertEquals(2, src.getRowCount());
                // Source data region still holds all 6 rows' bytes.
                assertEquals(srcDataSize, src.getDataSize(0));

                dst.copyFrom(src);

                assertEquals(0, dst.getReadStart());
                assertEquals(2, dst.getRowCount());
                // Destination data holds only the surviving out-of-line value ("ffffffffffffffffffff");
                // "eee" is fully inlined into the aux entry and contributes nothing to data.
                assertEquals(values[5].length(), dst.getDataSize(0));

                LiveViewRecord rec = new LiveViewRecord(dst);
                rec.setRow(0);
                Utf8Sequence v0 = rec.getVarcharA(0);
                assertEquals(values[4], v0.toString());
                assertEquals(5_000_000L, rec.getTimestamp(1));
                rec.setRow(1);
                Utf8Sequence v1 = rec.getVarcharA(0);
                assertEquals(values[5], v1.toString());
                assertEquals(6_000_000L, rec.getTimestamp(1));
            }
        });
    }

    @Test
    public void testCopyFromCompactsEvictedPrefixString() throws Exception {
        assertMemoryLeak(() -> {
            try (InMemoryTable src = new InMemoryTable(); InMemoryTable dst = new InMemoryTable()) {
                src.init(stringTsMetadata());
                dst.init(stringTsMetadata());

                String[] values = {"alpha", "bravo", "charlie", "delta", "echo"};
                for (int i = 0; i < values.length; i++) {
                    appendStringTsRow(src, values[i], (i + 1) * 1_000_000L);
                }
                src.applyRetention(2_000_000L); // visible: rows ts=4s, 5s -> "delta", "echo"

                dst.copyFrom(src);

                assertEquals(0, dst.getReadStart());
                assertEquals(2, dst.getRowCount());

                LiveViewRecord rec = new LiveViewRecord(dst);
                rec.setRow(0);
                CharSequence s0 = rec.getStrA(0);
                assertEquals("delta", s0.toString());
                rec.setRow(1);
                CharSequence s1 = rec.getStrA(0);
                assertEquals("echo", s1.toString());
            }
        });
    }

    @Test
    public void testCopyFromEmptyVisibleResetsDestination() throws Exception {
        assertMemoryLeak(() -> {
            try (InMemoryTable src = new InMemoryTable(); InMemoryTable dst = new InMemoryTable()) {
                src.init(varcharTsMetadata());
                dst.init(varcharTsMetadata());

                // Pre-populate dst so we can observe the reset.
                appendVarcharTsRow(dst, "old-value", 100L);
                assertEquals(1, dst.getRowCount());

                // src has no rows at all.
                dst.copyFrom(src);
                assertEquals(0, dst.getReadStart());
                assertEquals(0, dst.getRowCount());
                assertEquals(0, dst.getPhysicalRowCount());
                assertEquals(0, dst.getDataSize(0));
            }
        });
    }

    @Test
    public void testCopyFromMatchesEvictionThenAppendRoundTrip() throws Exception {
        // Simulates a refresh cycle: apply retention on the published buffer, then copy
        // into the write buffer (which compacts), then append a new row.
        assertMemoryLeak(() -> {
            try (InMemoryTable published = new InMemoryTable(); InMemoryTable writeBuf = new InMemoryTable()) {
                published.init(longTsMetadata());
                writeBuf.init(longTsMetadata());

                for (int i = 0; i < 6; i++) {
                    appendLongTsRow(published, i, (i + 1) * 1_000_000L);
                }
                published.applyRetention(2_000_000L); // maxTs=6s, cutoff=4s; visible: 5s, 6s

                writeBuf.copyFrom(published);
                assertEquals(2, writeBuf.getRowCount());

                // Append a fresh delta row and re-apply retention.
                appendLongTsRow(writeBuf, 99, 7_000_000L);
                assertEquals(3, writeBuf.getRowCount());
                writeBuf.applyRetention(2_000_000L); // maxTs=7s, cutoff=5s; visible: 6s, 7s

                assertEquals(2, writeBuf.getRowCount());

                LiveViewRecord rec = new LiveViewRecord(writeBuf);
                rec.setRow(0);
                assertEquals(5, rec.getLong(0));
                assertEquals(6_000_000L, rec.getTimestamp(1));
                rec.setRow(1);
                assertEquals(99, rec.getLong(0));
                assertEquals(7_000_000L, rec.getTimestamp(1));
            }
        });
    }

    @Test
    public void testGetTimestampAtVirtualIndex() throws Exception {
        assertMemoryLeak(() -> {
            try (InMemoryTable table = new InMemoryTable()) {
                table.init(longTsMetadata());
                for (int i = 0; i < 5; i++) {
                    appendLongTsRow(table, i, (i + 1) * 1_000_000L);
                }
                table.applyRetention(2_000_000L); // maxTs=5s, cutoff=3s; visible: 4s, 5s
                assertEquals(3, table.getReadStart());

                // Virtual indices 0, 1 -> physical 3, 4 -> ts 4_000_000, 5_000_000
                assertEquals(4_000_000L, table.getTimestampAt(0));
                assertEquals(5_000_000L, table.getTimestampAt(1));

                // Out-of-range virtual index returns Numbers.LONG_NULL (== Long.MIN_VALUE).
                assertEquals(Long.MIN_VALUE, table.getTimestampAt(2));
            }
        });
    }

    private static void appendLongTsRow(InMemoryTable table, long value, long ts) {
        table.appendRow(new Record() {
            @Override
            public long getLong(int col) {
                return col == 0 ? value : 0;
            }

            @Override
            public long getTimestamp(int col) {
                return col == 1 ? ts : 0;
            }
        });
    }

    private static void appendStringTsRow(InMemoryTable table, String value, long ts) {
        table.appendRow(new Record() {
            @Override
            public CharSequence getStrA(int col) {
                return col == 0 ? value : null;
            }

            @Override
            public long getTimestamp(int col) {
                return col == 1 ? ts : 0;
            }
        });
    }

    private static void appendVarcharTsRow(InMemoryTable table, String value, long ts) {
        Utf8Sequence utf8 = new GcUtf8String(value);
        table.appendRow(new Record() {
            @Override
            public long getTimestamp(int col) {
                return col == 1 ? ts : 0;
            }

            @Override
            public Utf8Sequence getVarcharA(int col) {
                return col == 0 ? utf8 : null;
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

    private static GenericRecordMetadata stringTsMetadata() {
        GenericRecordMetadata m = new GenericRecordMetadata();
        m.add(new TableColumnMetadata("value", ColumnType.STRING));
        m.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP));
        m.setTimestampIndex(1);
        return m;
    }

    private static GenericRecordMetadata varcharTsMetadata() {
        GenericRecordMetadata m = new GenericRecordMetadata();
        m.add(new TableColumnMetadata("value", ColumnType.VARCHAR));
        m.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP));
        m.setTimestampIndex(1);
        return m;
    }
}
