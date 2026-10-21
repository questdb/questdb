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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A metadata change re-opens the columns it touched on a reader that already has the partition mapped.
 * That re-open must size the mapping the way every other open path does - by the partition's LIVE FILE
 * EXTENT, {@code max(rowOffset + rowCount)} over its pieces - and not by its live row count, which for a
 * composite partition sits BELOW the pieces a merge parked at the tail.
 * <p>
 * The fixture holds one relocated piece: 110 live rows at file offset 100, so the files reach file row 210
 * while the live count says 110. A re-open sized by the live count maps 110 rows, and the very first read
 * of the piece's tail runs off the end of the mapping - an assertion in {@code AbstractMemoryCR.addressOf}
 * under {@code -ea}, and a SIGSEGV in {@code PageFrameMemoryRecord.getSymA} without it.
 */
public class CompositeReaderMetadataTransitionTest extends AbstractCairoTest {
    private static final long DAY_TWO = MicrosTimestampDriver.floor("2024-01-02");

    @Before
    public void setUpMergeAppend() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
    }

    @Test
    public void testAddIndexKeepsCompositeMappingAtFileExtent() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = createCompositeQuotes();

            // Warm the pooled reader: the partition's columns are mapped here, at the right size.
            try (TableReader reader = engine.getReader(tt)) {
                reader.openPartition(0);
                assertSymbolColumnCoversExtent(reader, "before the metadata change");
            }

            // ADD INDEX changes the column's index type, which is what makes the transition treat it as a
            // NEW column and re-open its files on the reader that comes back out of the pool.
            execute("ALTER TABLE q ALTER COLUMN s ADD INDEX");
            drainWalQueue();

            try (TableReader reader = engine.getReader(tt)) {
                reader.openPartition(0);
                // The rows a short mapping cannot reach sit in its page-aligned tail, which reads as zeros
                // rather than faulting at this size, so they come back as the symbol with key 0 and as v=0.
                assertPartitionRows(reader);
                assertSymbolColumnCoversExtent(reader, "after ADD INDEX");
            }

            assertQuery("SELECT count() c, count(v) cv, sum(v) sv FROM q WHERE ts IN '2024-01-01'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\tcv\tsv\n110\t110\t8005105\n");
        });
    }

    private void assertPartitionRows(TableReader reader) {
        final int symbolIndex = reader.getMetadata().getColumnIndex("s");
        final int valueIndex = reader.getMetadata().getColumnIndex("v");
        long sum = 0;
        int kz = 0;
        int rows = 0;
        try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                if (record.getTimestamp(reader.getMetadata().getTimestampIndex()) >= DAY_TWO) {
                    continue;
                }
                rows++;
                sum += record.getInt(valueIndex);
                if (Chars.equals("kz", record.getSymA(symbolIndex))) {
                    kz++;
                }
            }
        }
        Assert.assertEquals("rows of 2024-01-01", 110, rows);
        Assert.assertEquals("sum of v over 2024-01-01", 8_005_105L, sum);
        Assert.assertEquals("rows of the merged-in batch", 10, kz);
    }

    private void assertSymbolColumnCoversExtent(TableReader reader, String when) {
        final int columnIndex = reader.getMetadata().getColumnIndex("s");
        final int columnBase = reader.getColumnBase(0);
        final long mappedBytes = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, columnIndex)).size();
        final long extent = reader.getGeometry().getLiveFileExtent(0);
        Assert.assertEquals(
                "symbol column mapping is short " + when + " [liveRows=" + reader.getTxFile().getPartitionSize(0) + ']',
                extent * Integer.BYTES,
                mappedBytes
        );
    }

    private TableToken createCompositeQuotes() throws Exception {
        execute("CREATE TABLE q AS (" +
                " SELECT x::INT v, ('k' || ((x % 2) + 1))::SYMBOL s," +
                " timestamp_sequence('2024-01-01', 60_000_000L) ts" +
                " FROM long_sequence(100)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO q VALUES (90_000, 'k1', '2024-01-03T00:00:00.000000Z')");
        drainWalQueue();
        execute("INSERT INTO q SELECT x::INT + 800_000, 'kz'," +
                " timestamp_sequence('2024-01-01T01:00:30', 60_000_000L) FROM long_sequence(10)");
        drainWalQueue();

        final TableToken tt = engine.verifyTableName("q");
        try (TableReader reader = engine.getReader(tt)) {
            Assert.assertTrue("2024-01-01 must be composite", reader.getTxFile().isPartitionComposite(0));
            Assert.assertEquals("live rows", 110, reader.getTxFile().getPartitionSize(0));
            Assert.assertEquals("mapped extent", 210, reader.getGeometry().getLiveFileExtent(0));
        }
        engine.releaseAllReaders();
        return tt;
    }
}
