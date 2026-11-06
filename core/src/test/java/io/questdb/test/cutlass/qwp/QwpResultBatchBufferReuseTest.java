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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.codec.QwpEgressConnSymbolDict;
import io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The per-column scratch buffers of a {@link QwpResultBatchBuffer} live as long as the
 * connection, and are trimmed at each query's start to twice the largest batch of the query
 * before. A query run again on the same connection must then fit the buffers it left: from its
 * second run on, it reallocates nothing, whether its rows ship as one batch or as several.
 */
public class QwpResultBatchBufferReuseTest {

    @Test
    public void testRepeatedQueryReallocatesNothingManyBatches() throws Exception {
        assertNoReallocOnRepeat(3_000, 4);
    }

    @Test
    public void testRepeatedQueryReallocatesNothingOneBatch() throws Exception {
        assertNoReallocOnRepeat(3_000, 1);
    }

    @Test
    public void testSmallerQueryAfterLargeTrimsOnce() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final ObjList<QwpEgressColumnDef> columns = columns();
            try (
                    QwpResultBatchBuffer buffer = new QwpResultBatchBuffer();
                    QwpEgressConnSymbolDict dict = new QwpEgressConnSymbolDict()
            ) {
                runQuery(buffer, dict, columns, 100_000, 1);
                // a query trims what the query before it grew past four times its peak: the first
                // small query starts with the large one's peak, the second trims to the first's
                runQuery(buffer, dict, columns, 1_000, 1);
                final long before = Unsafe.getReallocCount();
                runQuery(buffer, dict, columns, 1_000, 1);
                Assert.assertTrue("the large query's buffers are trimmed", Unsafe.getReallocCount() > before);
                // and from then on the small query fits what it left
                final long settled = Unsafe.getReallocCount();
                for (int i = 0; i < 5; i++) {
                    runQuery(buffer, dict, columns, 1_000, 1);
                }
                Assert.assertEquals(settled, Unsafe.getReallocCount());
            }
        });
    }

    private static void addColumn(ObjList<QwpEgressColumnDef> columns, String name, int type) {
        final QwpEgressColumnDef def = new QwpEgressColumnDef();
        def.of(name, type);
        columns.add(def);
    }

    private static void assertNoReallocOnRepeat(int rows, int batches) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final ObjList<QwpEgressColumnDef> columns = columns();
            try (
                    QwpResultBatchBuffer buffer = new QwpResultBatchBuffer();
                    QwpEgressConnSymbolDict dict = new QwpEgressConnSymbolDict()
            ) {
                // the first run grows the buffers; the second may trim what the first grew past
                // twice its peak
                runQuery(buffer, dict, columns, rows, batches);
                runQuery(buffer, dict, columns, rows, batches);
                final long before = Unsafe.getReallocCount();
                for (int i = 0; i < 5; i++) {
                    runQuery(buffer, dict, columns, rows, batches);
                }
                Assert.assertEquals("reallocations over 5 repeats of the same query", before, Unsafe.getReallocCount());
            }
        });
    }

    private static ObjList<QwpEgressColumnDef> columns() {
        final ObjList<QwpEgressColumnDef> columns = new ObjList<>();
        addColumn(columns, "ts", ColumnType.TIMESTAMP);
        addColumn(columns, "l", ColumnType.LONG);
        addColumn(columns, "d", ColumnType.DOUBLE);
        addColumn(columns, "i", ColumnType.INT);
        return columns;
    }

    // what the egress does per query: reset at its start, then batches, each shipped in full
    private static void runQuery(
            QwpResultBatchBuffer buffer,
            QwpEgressConnSymbolDict dict,
            ObjList<QwpEgressColumnDef> columns,
            int rows,
            int batches
    ) {
        buffer.resetForNewQuery();
        final RowRecord record = new RowRecord();
        final int perBatch = (rows + batches - 1) / batches;
        int row = 0;
        while (row < rows) {
            buffer.beginBatch(columns, null, dict);
            for (int end = Math.min(rows, row + perBatch); row < end; row++) {
                buffer.appendRow(record.of(row));
            }
            buffer.advanceStartRow(buffer.getRowCount());
        }
    }

    private static class RowRecord implements Record {
        private long row;

        @Override
        public double getDouble(int col) {
            return row * 0.5;
        }

        @Override
        public int getInt(int col) {
            return (int) row;
        }

        @Override
        public long getLong(int col) {
            return row;
        }

        @Override
        public long getTimestamp(int col) {
            return 1_700_000_000_000_000L + row;
        }

        RowRecord of(long row) {
            this.row = row;
            return this;
        }
    }
}
