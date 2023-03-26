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

package io.questdb.cairo.wal;

import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.str.Path;

import static io.questdb.cairo.wal.WalTxnType.DATA;
import static io.questdb.cairo.wal.WalUtils.WAL_FORMAT_VERSION;
import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;

public class WalTxnDetails {
    public static final long FORCE_FULL_COMMIT = Long.MAX_VALUE;
    public static final long LAST_ROW_COMMIT = Long.MAX_VALUE - 1;
    public static final int TXN_METADATA_LONGS_SIZE = 3;
    private static final int MAX_TIMESTAMP_OFFSET = 2;
    private static final int MIN_TIMESTAMP_OFFSET = 1;
    private final LongList transactionMeta = new LongList();
    private final WalEventReader walEventReader;
    private long startSeqTxn = 0;

    public WalTxnDetails(FilesFacade ff) {
        walEventReader = new WalEventReader(ff);
    }

    public long getCommitToTimestamp(long seqTxn) {
        long value = transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE));
        return value == LAST_ROW_COMMIT ? FORCE_FULL_COMMIT : value;
    }

    public long getFullyCommittedTxn(long fromSeqTxn, long toSeqTxn, long maxCommittedTimestamp) {
        for (long seqTxn = fromSeqTxn + 1; seqTxn <= toSeqTxn; seqTxn++) {
            long maxTimestamp = getCommitMaxTimestamp(seqTxn);
            if (maxTimestamp > maxCommittedTimestamp) {
                return seqTxn - 1;
            }
        }
        return toSeqTxn;
    }

    public long getLastSeqTxn() {
        return startSeqTxn + transactionMeta.size() / TXN_METADATA_LONGS_SIZE - 1;
    }

    public boolean hasRecord(long seqTxn) {
        return (seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE < transactionMeta.size();
    }

    public void readObservableTxnMeta(
            final Path tempPath,
            final TransactionLogCursor transactionLogCursor,
            final int rootLen,
            final long committedSeqTxn,
            final long maxCommittedTimestamp
    ) {
        if (committedSeqTxn <= getLastSeqTxn()) {
            int shift = (int) (committedSeqTxn - startSeqTxn + 1);
            transactionMeta.removeIndexBlock(0, shift * TXN_METADATA_LONGS_SIZE);
            startSeqTxn = committedSeqTxn + 1;
            if (transactionMeta.size() > 0) {
                transactionMeta.set(0, -1);
            }
        } else {
            transactionMeta.clear();
            startSeqTxn = -1;
        }

        try (WalEventReader eventReader = walEventReader) {

            int prevWalId = Integer.MIN_VALUE;
            int prevSegmentId = Integer.MIN_VALUE;
            int prevSegmentTxn = Integer.MIN_VALUE;
            WalEventCursor walEventCursor = null;

            long runningMaxTimestamp = maxCommittedTimestamp;
            while (transactionLogCursor.hasNext()) {

                final int walId = transactionLogCursor.getWalId();
                final int segmentId = transactionLogCursor.getSegmentId();
                final int segmentTxn = transactionLogCursor.getSegmentTxn();
                if (startSeqTxn == -1) {
                    startSeqTxn = transactionLogCursor.getTxn();
                }
                assert startSeqTxn + transactionMeta.size() / TXN_METADATA_LONGS_SIZE == transactionLogCursor.getTxn();

                if (walId > 0) {
                    tempPath.trimTo(rootLen).concat(WAL_NAME_BASE).put(walId).slash().put(segmentId);

                    if (prevWalId != walId || prevSegmentId != segmentId || prevSegmentTxn + 1 != segmentTxn) {
                        walEventCursor = eventReader.of(tempPath, WAL_FORMAT_VERSION, segmentTxn);
                        prevWalId = walId;
                        prevSegmentId = segmentId;
                        prevSegmentTxn = segmentTxn;
                    } else {
                        // This is same WALE file, just read next txn transaction.
                        if (!walEventCursor.hasNext()) {
                            walEventCursor = eventReader.of(tempPath, WAL_FORMAT_VERSION, segmentTxn);
                        }
                    }

                    final byte walTxnType = walEventCursor.getType();
                    if (walTxnType == DATA) {
                        WalEventCursor.DataInfo commitInfo = walEventCursor.getDataInfo();
                        transactionMeta.add(-1); // commit to timestamp
                        transactionMeta.add(commitInfo.getMinTimestamp());
                        transactionMeta.add(commitInfo.getMaxTimestamp());
                        runningMaxTimestamp = Math.max(commitInfo.getMaxTimestamp(), runningMaxTimestamp);
                        continue;
                    }
                }
                // If there is ALTER or UPDATE, we have to flush everything without keeping anything in the lag.
                transactionMeta.add(FORCE_FULL_COMMIT); // commit to timestamp
                transactionMeta.add(runningMaxTimestamp); // min timestamp
                transactionMeta.add(runningMaxTimestamp); // max timestamp
            }
        } finally {
            tempPath.trimTo(rootLen);
        }

        // set commit to timestamp moving backwards
        long runningMinTimestamp = LAST_ROW_COMMIT;
        for (int i = transactionMeta.size() - TXN_METADATA_LONGS_SIZE; i > -1; i -= TXN_METADATA_LONGS_SIZE) {

            long commitToTimestamp = runningMinTimestamp;
            long currentMinTimestamp = transactionMeta.getQuick(i + MIN_TIMESTAMP_OFFSET);
            runningMinTimestamp = Math.min(runningMinTimestamp, currentMinTimestamp);

            if (transactionMeta.get(i) != FORCE_FULL_COMMIT) {
                transactionMeta.set(i, commitToTimestamp);
            } else {
                // Force full commit before this record
                runningMinTimestamp = FORCE_FULL_COMMIT;
            }
        }

        // Avoid O3 commits with existing data. Start from beginning and set commit to timestamp to be min infinity until
        // the all future min timestamp are greater than current max timestamp.
        for (int i = 0, n = transactionMeta.size(); i < n; i += TXN_METADATA_LONGS_SIZE) {

            long commitToTimestamp = transactionMeta.get(i);
            if (commitToTimestamp < maxCommittedTimestamp) {
                transactionMeta.set(i, Long.MIN_VALUE);
            }
        }

    }

    private long getCommitMaxTimestamp(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + MAX_TIMESTAMP_OFFSET));
    }
}
