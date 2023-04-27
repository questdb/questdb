/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.wal;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.WalTxnDetails;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class WalTxnDetailsFuzzTest extends AbstractCairoTest {

    @Test
    public void testCalculateCommitTimestampWhenO3IsUnavoidable() {
        TableToken tableToken = createTable(testName.getMethodName());
        commitWalRows(tableToken, 2, "2022-02-24T02:00", "2022-02-24T12");
        drainWalQueue();

        commitWalRows(tableToken, 200, "2022-02-24T08:00", "2022-02-24T13");
        commitWalRows(tableToken, 200, "2022-02-24T09:00", "2022-02-24T13");
        commitWalPartitionDrop(tableToken, "2022-01-01");
        commitWalRows(tableToken, 200, "2022-02-24T10:00", "2022-02-24T15");
        commitWalRows(tableToken, 200, "2022-02-24T12:05", "2022-02-24T16");
        commitWalRows(tableToken, 200, "2022-02-24T13:00", "2022-02-24T18");

        try (TableWriter writer = getWriter(tableToken)) {
            try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(tableToken, writer.getAppliedSeqTxn())) {
                writer.readWalTxnDetails(cursor);
                int startTxn = (int) writer.getAppliedSeqTxn();
                WalTxnDetails walTnxDetails = writer.getWalTnxDetails();

                Assert.assertEquals(Long.MIN_VALUE, walTnxDetails.getCommitToTimestamp(startTxn + 1));
                Assert.assertEquals(Long.MAX_VALUE, walTnxDetails.getCommitToTimestamp(startTxn + 2));
                Assert.assertEquals(Long.MAX_VALUE, walTnxDetails.getCommitToTimestamp(startTxn + 3));
                assertTimestampEquals("2022-02-24T12:05", walTnxDetails.getCommitToTimestamp(startTxn + 4));
                assertTimestampEquals("2022-02-24T13:00", walTnxDetails.getCommitToTimestamp(startTxn + 5));
                Assert.assertEquals(Long.MAX_VALUE, walTnxDetails.getCommitToTimestamp(startTxn + 6));
            }
        }
    }

    @Test
    public void testCalculateMaxCommitTimestamp() {
        TableToken tableToken = createTable(testName.getMethodName());

        commitWalRows(tableToken, 2, "2022-02-24T02:00", "2022-02-24T12");
        commitWalRows(tableToken, 3, "2022-02-24T10:00", "2022-02-24T11");
        commitWalPartitionDrop(tableToken, "2022-01-01");
        commitWalRows(tableToken, 4, "2022-02-24T09:00", "2022-02-24T13"); // 4
        commitWalRows(tableToken, 5, "2022-02-24T10:00", "2022-02-24T12"); // 5
        commitWalRows(tableToken, 6, "2022-02-24T13:00", "2022-02-24T14"); // 6
        commitWalRows(tableToken, 7, "2022-02-24T13:00", "2022-02-24T13"); // 7
        commitWalRows(tableToken, 8, "2022-02-24T13:20", "2022-02-24T15"); // 8

        try (TableWriter writer = getWriter(tableToken)) {
            try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(tableToken, writer.getAppliedSeqTxn())) {
                writer.readWalTxnDetails(cursor);
                long startTxn = writer.getAppliedSeqTxn();

                WalTxnDetails walTnxDetails = writer.getWalTnxDetails();
                long commitTo = walTnxDetails.getCommitToTimestamp(startTxn + 1);
                assertTimestampEquals("2022-02-24T10", commitTo);

                commitTo = walTnxDetails.getCommitToTimestamp(startTxn + 2);
                Assert.assertEquals(Long.MAX_VALUE, commitTo);

                // value doesn't matter
                walTnxDetails.getCommitToTimestamp(startTxn + 3);

                commitTo = walTnxDetails.getCommitToTimestamp(startTxn + 4);
                assertTimestampEquals("2022-02-24T10:00", commitTo);

                commitTo = walTnxDetails.getCommitToTimestamp(startTxn + 5);
                assertTimestampEquals("2022-02-24T13:00", commitTo);

                commitTo = walTnxDetails.getCommitToTimestamp(startTxn + 6);
                assertTimestampEquals("2022-02-24T13:00", commitTo);

                commitTo = walTnxDetails.getCommitToTimestamp(startTxn + 7);
                assertTimestampEquals("2022-02-24T13:20", commitTo);

                commitTo = walTnxDetails.getCommitToTimestamp(startTxn + 8);
                Assert.assertEquals(Long.MAX_VALUE, commitTo);

                long fullyCommitted = walTnxDetails.getFullyCommittedTxn(startTxn, 8, parseFloorPartialTimestamp("2022-02-24T09:00"));
                Assert.assertEquals(startTxn, fullyCommitted);

                fullyCommitted = walTnxDetails.getFullyCommittedTxn(startTxn, 8, parseFloorPartialTimestamp("2022-02-24T12"));
                Assert.assertEquals(startTxn + 3, fullyCommitted);

                fullyCommitted = walTnxDetails.getFullyCommittedTxn(startTxn, 8, parseFloorPartialTimestamp("2022-02-24T13"));
                Assert.assertEquals(startTxn + 5, fullyCommitted);

                fullyCommitted = walTnxDetails.getFullyCommittedTxn(0, 1, parseFloorPartialTimestamp("2022-02-24T12"));
                Assert.assertEquals(1, fullyCommitted);
            }
        }
    }

    @Test
    public void testCalculateMaxCommitTimestampFuzz() {
        TableToken tableToken = createTable(testName.getMethodName());
        Rnd rnd = TestUtils.generateRandom(LOG);

        double ddlProb = rnd.nextDouble() * .3;
        int txnCount = rnd.nextInt(1000);
        long startTx = parseFloorPartialTimestamp("2022-02-24T02:00");
        long maxDuration = rnd.nextLong(Timestamps.DAY_MICROS);

        LongList minMaxTimestamps = new LongList();
        for (int i = 0; i < txnCount; i++) {
            boolean isDdl = rnd.nextDouble() < ddlProb;
            if (isDdl) {
                commitWalPartitionDrop(tableToken, "2022-01-01");
                minMaxTimestamps.add(Long.MAX_VALUE);
                minMaxTimestamps.add(Long.MAX_VALUE);
            } else {
                // data transaction
                long minTimestamp = rnd.nextLong(maxDuration) + startTx;
                long maxTimestamp = rnd.nextLong(maxDuration) + minTimestamp;
                commitWalRows(tableToken, 2, minTimestamp, maxTimestamp);
                minMaxTimestamps.add(minTimestamp);
                minMaxTimestamps.add(maxTimestamp);
            }
        }

        try (TableWriter writer = getWriter(tableToken)) {
            try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(tableToken, writer.getAppliedSeqTxn())) {
                writer.readWalTxnDetails(cursor);
                int startTxn = (int) writer.getAppliedSeqTxn();
                WalTxnDetails walTnxDetails = writer.getWalTnxDetails();
                for (int i = 0; i < startTxn + txnCount; i++) {

                    long commitTo = walTnxDetails.getCommitToTimestamp(i + startTxn + 1);

                    if (commitTo != Long.MAX_VALUE) {
                        long runningMin = Long.MAX_VALUE;
                        for (int j = i + 1; j < txnCount; j++) {
                            long minTimestamp = minMaxTimestamps.getQuick((j) * 2);
                            if (minTimestamp == Long.MAX_VALUE) {
                                break;
                            }

                            runningMin = Math.min(runningMin, minTimestamp);
                            if (commitTo > minTimestamp) {
                                Assert.fail("row=" + i + " commitTo=" + Timestamps.toString(commitTo) + ", row=" + j + " minTimestamp=" + Timestamps.toString(minTimestamp));
                            }
                        }

                        if (runningMin != commitTo) {
                            Assert.fail("row=" + i + " commitTo=" + Timestamps.toString(commitTo) + ", runningMin=" + Timestamps.toString(runningMin));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testLastCommitToTimestampIsUpdated() {
        TableToken tableToken = createTable(testName.getMethodName());
        commitWalRows(tableToken, 2, "2022-02-24T02:00", "2022-02-24T12");
        drainWalQueue();

        commitWalRows(tableToken, 200, "2022-02-24T08", "2022-02-24T13");
        commitWalRows(tableToken, 200, "2022-02-24T09", "2022-02-24T13");
        commitWalPartitionDrop(tableToken, "2022-01-01");
        commitWalRows(tableToken, 200, "2022-02-24T10", "2022-02-24T15");
        commitWalRows(tableToken, 200, "2022-02-24T12:05", "2022-02-24T16");
        commitWalRows(tableToken, 200, "2022-02-24T13", "2022-02-24T18");

        try (TableWriter writer = getWriter(tableToken)) {
            int startTxn = (int) writer.getAppliedSeqTxn();

            try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(tableToken, writer.getAppliedSeqTxn())) {
                writer.readWalTxnDetails(cursor);
                WalTxnDetails walTnxDetails = writer.getWalTnxDetails();

                Assert.assertEquals(Long.MIN_VALUE, walTnxDetails.getCommitToTimestamp(startTxn + 1));
                Assert.assertEquals(Long.MAX_VALUE, walTnxDetails.getCommitToTimestamp(startTxn + 2));
                Assert.assertEquals(Long.MAX_VALUE, walTnxDetails.getCommitToTimestamp(startTxn + 3));
                assertTimestampEquals("2022-02-24T12:05", walTnxDetails.getCommitToTimestamp(startTxn + 4));
                assertTimestampEquals("2022-02-24T13:00", walTnxDetails.getCommitToTimestamp(startTxn + 5));
                Assert.assertEquals(Long.MAX_VALUE, walTnxDetails.getCommitToTimestamp(startTxn + 6));
            }

            // Add one more commit
            commitWalRows(tableToken, 200, "2022-02-24T15", "2022-02-24T18");

            try (TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(tableToken, writer.getAppliedSeqTxn() + 5)) {
                writer.readWalTxnDetails(cursor);

                WalTxnDetails walTnxDetails = writer.getWalTnxDetails();
                assertTimestampEquals("2022-02-24T15", walTnxDetails.getCommitToTimestamp(startTxn + 6));
                Assert.assertEquals(Long.MAX_VALUE, walTnxDetails.getCommitToTimestamp(startTxn + 7));
            }
        }
    }

    private static void commitWalRows(TableToken tableToken, int rowCount, long from, long to) {
        long step = (to - from) / (rowCount - 1);

        try (WalWriter ww = engine.getWalWriter(tableToken)) {
            for (int i = 0; i < rowCount - 1; i++) {
                TableWriter.Row row = ww.newRow(from + i * step);
                row.append();
            }
            TableWriter.Row row = ww.newRow(to);
            row.append();
            ww.commit();
        }
    }

    @SuppressWarnings("resource")
    private static TableModel defaultModel(String tableName) {
        return new TableModel(configuration, tableName, PartitionBy.DAY)
                .timestamp("ts")
                .wal();
    }

    private static long parseFloorPartialTimestamp(String toTs) {
        try {
            return IntervalUtils.parseFloorPartialTimestamp(toTs);
        } catch (NumericException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertTimestampEquals(String expected, long actual) {
        long expectedTimestamp = parseFloorPartialTimestamp(expected);
        if (expectedTimestamp != actual) {
            Assert.assertEquals(expected, Timestamps.toString(actual));
        }
    }

    private void commitWalPartitionDrop(TableToken tableToken, String partition) {
        try (WalWriter ww = engine.getWalWriter(tableToken)) {
            AlterOperationBuilder builder = new AlterOperationBuilder();
            builder.ofDropPartition(0, tableToken, tableToken.getTableId())
                    .addPartitionToList(parseFloorPartialTimestamp(partition), 0);

            AlterOperation alterOp = builder.build();
            alterOp.withContext(new SqlExecutionContextImpl(engine, 1));
            ww.apply(alterOp, true);
        }
    }

    private void commitWalRows(TableToken tableToken, int rowCount, String fromTs, String toTs) {
        long from = parseFloorPartialTimestamp(fromTs);
        long to = parseFloorPartialTimestamp(toTs);
        assert to >= from;
        assert from == to || rowCount > 1;
        commitWalRows(tableToken, rowCount, from, to);
    }

    static TableToken createTable(String tableName) {
        try (TableModel model = defaultModel(tableName)) {
            return engine.createTable(
                    AllowAllSecurityContext.INSTANCE,
                    model.getMem(),
                    model.getPath(),
                    false,
                    model,
                    false
            );
        }
    }
}
