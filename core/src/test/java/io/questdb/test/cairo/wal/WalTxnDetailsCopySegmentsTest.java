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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterSegmentCopyInfo;
import io.questdb.cairo.wal.WalTxnDetails;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class WalTxnDetailsCopySegmentsTest extends AbstractCairoTest {

    @Test
    public void testMultipleSegmentsInOrderDataReportNotInOrder() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createWalTable();
            try (
                    WalWriter wal1 = engine.getWalWriter(tableToken);
                    WalWriter wal2 = engine.getWalWriter(tableToken)
            ) {
                commit(wal1, "2024-05-01T10:00", "2024-05-01T10:04");
                commit(wal2, "2024-05-01T10:05", "2024-05-01T10:09");
            }
            // The data is globally ascending, yet the block spans two WALs. The copy loop walks
            // transactions in (walId, segmentId) order rather than time order, so a non-overlap
            // check would be meaningless here and the flag stays false.
            assertBlock(tableToken, 2, 2, false);
        });
    }

    @Test
    public void testMultipleSegmentsOverlappingDataReportNotInOrder() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createWalTable();
            try (
                    WalWriter wal1 = engine.getWalWriter(tableToken);
                    WalWriter wal2 = engine.getWalWriter(tableToken)
            ) {
                commit(wal1, "2024-05-01T10:00", "2024-05-01T10:10");
                commit(wal2, "2024-05-01T10:05", "2024-05-01T10:15");
            }
            assertBlock(tableToken, 2, 2, false);
        });
    }

    @Test
    public void testSegmentRolloverWithinOneWalReportsNotInOrder() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 2);
            TableToken tableToken = createWalTable();
            try (WalWriter wal = engine.getWalWriter(tableToken)) {
                commit(wal, "2024-05-01T10:00", "2024-05-01T10:04");
                // The rollover threshold moves this transaction into a second segment of the
                // same WAL.
                commit(wal, "2024-05-01T10:05", "2024-05-01T10:09");
            }
            assertBlock(tableToken, 2, 2, false);
        });
    }

    @Test
    public void testSingleSegmentInOrderMultiTimestampTxns() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createWalTable();
            try (WalWriter wal = engine.getWalWriter(tableToken)) {
                commit(wal, "2024-05-01T10:00", "2024-05-01T10:02", "2024-05-01T10:04");
                commit(wal, "2024-05-01T10:05", "2024-05-01T10:07", "2024-05-01T10:09");
                commit(wal, "2024-05-01T10:10", "2024-05-01T10:12", "2024-05-01T10:14");
            }
            // Every transaction spans a range of timestamps and the ranges do not overlap, so the
            // concatenated rows are sorted and the block can skip the O3 sort.
            assertBlock(tableToken, 3, 1, true);
        });
    }

    @Test
    public void testSingleSegmentOverlappingTxns() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createWalTable();
            try (WalWriter wal = engine.getWalWriter(tableToken)) {
                commit(wal, "2024-05-01T10:00", "2024-05-01T10:10");
                // Reaches back inside the previous transaction's range.
                commit(wal, "2024-05-01T10:05", "2024-05-01T10:15");
            }
            assertBlock(tableToken, 2, 1, false);
        });
    }

    @Test
    public void testSingleSegmentSingleTimestampTxns() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createWalTable();
            try (WalWriter wal = engine.getWalWriter(tableToken)) {
                commit(wal, "2024-05-01T10:00", "2024-05-01T10:00");
                commit(wal, "2024-05-01T10:01", "2024-05-01T10:01");
                commit(wal, "2024-05-01T10:02", "2024-05-01T10:02");
            }
            assertBlock(tableToken, 3, 1, true);
        });
    }

    @Test
    public void testSingleSegmentTouchingTxnBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createWalTable();
            try (WalWriter wal = engine.getWalWriter(tableToken)) {
                commit(wal, "2024-05-01T10:00", "2024-05-01T10:05");
                // Starts exactly where the previous transaction ended: duplicate timestamps keep
                // the range non-decreasing, which is all the copy path needs.
                commit(wal, "2024-05-01T10:05", "2024-05-01T10:10");
            }
            assertBlock(tableToken, 2, 1, true);
        });
    }

    @Test
    public void testSingleSegmentTxnDataOutOfOrder() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createWalTable();
            try (WalWriter wal = engine.getWalWriter(tableToken)) {
                commit(wal, "2024-05-01T10:00", "2024-05-01T10:05");
                // The transaction range does not overlap the previous one, so only the rows being
                // written in descending order can make the block unsorted.
                commit(wal, "2024-05-01T10:20", "2024-05-01T10:15", "2024-05-01T10:10");
            }
            assertBlock(tableToken, 2, 1, false);
        });
    }

    @Test
    public void testSingleSegmentTxnFullyBeforePrevious() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createWalTable();
            try (WalWriter wal = engine.getWalWriter(tableToken)) {
                commit(wal, "2024-05-01T10:00", "2024-05-01T10:05");
                commit(wal, "2024-05-01T09:00", "2024-05-01T09:30");
            }
            assertBlock(tableToken, 2, 1, false);
        });
    }

    private void assertBlock(TableToken tableToken, int expectedTxnCount, int expectedSegmentCount, boolean expectedInOrder) {
        try (
                TableWriter writer = getWriter(tableToken);
                TableWriterSegmentCopyInfo copyInfo = new TableWriterSegmentCopyInfo()
        ) {
            final long startSeqTxn = writer.getAppliedSeqTxn() + 1;
            try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(tableToken, writer.getAppliedSeqTxn())) {
                writer.readWalTxnDetails(cursor);
            }
            final WalTxnDetails walTxnDetails = writer.getWalTnxDetails();
            final int blockTransactionCount = (int) (walTxnDetails.getLastSeqTxn() - startSeqTxn + 1);
            Assert.assertEquals("transaction count", expectedTxnCount, blockTransactionCount);

            walTxnDetails.prepareCopySegments(startSeqTxn, blockTransactionCount, copyInfo, false);

            Assert.assertEquals("segment count", expectedSegmentCount, copyInfo.getSegmentCount());
            Assert.assertFalse("segment gaps", copyInfo.hasSegmentGaps());
            Assert.assertEquals("all txn data in order", expectedInOrder, copyInfo.getAllTxnDataInOrder());
        }
    }

    private void commit(WalWriter walWriter, String... timestamps) {
        for (String timestamp : timestamps) {
            TableWriter.Row row = walWriter.newRow(parseFloorPartialTimestamp(timestamp));
            row.append();
        }
        walWriter.commit();
    }

    private TableToken createWalTable() {
        TableModel model = new TableModel(configuration, testName.getMethodName(), PartitionBy.DAY)
                .timestamp("ts")
                .wal();
        return TestUtils.createTable(engine, model);
    }
}
