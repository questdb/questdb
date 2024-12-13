/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;

import static io.questdb.cairo.wal.WalTxnType.DATA;
import static io.questdb.cairo.wal.WalUtils.*;

public class WalTxnDetails {
    public static final long FORCE_FULL_COMMIT = Long.MAX_VALUE;
    public static final long LAST_ROW_COMMIT = Long.MAX_VALUE - 1;
    private static final int SEQ_TXN_OFFSET = 0;
    private static final int COMMIT_TO_TIMESTAMP_OFFSET = SEQ_TXN_OFFSET + 1;
    private static final int MIN_TIMESTAMP_OFFSET = COMMIT_TO_TIMESTAMP_OFFSET + 1;
    private static final int MAX_TIMESTAMP_OFFSET = MIN_TIMESTAMP_OFFSET + 1;
    private static final int WAL_ID_SEG_ID_OFFSET = MAX_TIMESTAMP_OFFSET + 1;
    private static final int TXN_DETAIL_RECORD_SIZE = 4;
    private static final int WAL_TXN_ROW_LO_OFFSET = WAL_ID_SEG_ID_OFFSET + 1;
    private static final int WAL_TXN_ROW_HI_OFFSET = WAL_TXN_ROW_LO_OFFSET + 1;
    public static final int TXN_METADATA_LONGS_SIZE = WAL_TXN_ROW_HI_OFFSET + 1;
    private final LongHashSet futureWalSegments = new LongHashSet();
    private final int maxLookahead;
    private final LongList transactionMeta = new LongList();
    private final IntList txnDetails = new IntList();
    private final WalEventReader walEventReader;
    private long startSeqTxn = 0;

    public WalTxnDetails(FilesFacade ff, int maxLookahead) {
        walEventReader = new WalEventReader(ff);
        this.maxLookahead = maxLookahead * 10;
    }

    public long getCommitToTimestamp(long seqTxn) {
        long value = transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE) + COMMIT_TO_TIMESTAMP_OFFSET);
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

    public long getWalSegmentId(long seqTxn) {
        long value = transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_ID_SEG_ID_OFFSET));
        int walId = Numbers.decodeHighInt(value);
        int segmentId = Numbers.decodeLowInt(value);
        return Numbers.encodeLowHighInts(segmentId, Math.abs(walId));
    }

    public boolean hasRecord(long seqTxn) {
        return (seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE < transactionMeta.size();
    }

    public boolean isLastSegmentUsage(long seqTxn) {
        long value = transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_ID_SEG_ID_OFFSET));
        int walId = Numbers.decodeHighInt(value);
        return walId < 0;
    }

    public void readObservableTxnMeta(
            final Path tempPath,
            final TransactionLogCursor transactionLogCursor,
            final int rootLen,
            long appliedSeqTxn,
            final long maxCommittedTimestamp
    ) {
        final long lastSeqTxn = getLastSeqTxn();
        long loadFromSeqTxn = appliedSeqTxn + 1;

        transactionMeta.clear();
        this.startSeqTxn = loadFromSeqTxn;

//        if (lastSeqTxn <= loadFromSeqTxn) {
//            int shift = (int) (appliedSeqTxn - this.startSeqTxn + 1);
//            if (shift < 0) {
//                // This can happen after a rollback and lag txns being discarded.
//                // In this case we have to clear everything.
//                transactionMeta.clear();
//                this.startSeqTxn = loadFromSeqTxn;
//            } else {
//                int size = transactionMeta.size();
//                transactionMeta.removeIndexBlock(0, shift * TXN_METADATA_LONGS_SIZE);
//                this.startSeqTxn = appliedSeqTxn + 1;
//                if (transactionMeta.size() > 0) {
//                    transactionMeta.set(COMMIT_TO_TIMESTAMP_OFFSET, -1);
//                }
//            }
//        } else {
//            transactionMeta.clear();
//            this.startSeqTxn = loadFromSeqTxn;
//        }

        loadTransactionDetails(tempPath, transactionLogCursor, loadFromSeqTxn, rootLen, maxCommittedTimestamp);

        // set commit to timestamp moving backwards
        long runningMinTimestamp = LAST_ROW_COMMIT;
        futureWalSegments.clear();
        for (int i = transactionMeta.size() - TXN_METADATA_LONGS_SIZE; i > -1; i -= TXN_METADATA_LONGS_SIZE) {

            long commitToTimestamp = runningMinTimestamp;
            long currentMinTimestamp = transactionMeta.getQuick(i + MIN_TIMESTAMP_OFFSET);

            // Find out if the wal/segment is not used anymore for future transactions.
            // Since we're moving backwards, if this is the first time this combination occurs
            // it means that it's the last transaction from this wal/segment.
            long currentWalSegment = transactionMeta.getQuick(i + WAL_ID_SEG_ID_OFFSET);
            boolean isLastSegmentUsage = futureWalSegments.add(currentWalSegment);
            if (isLastSegmentUsage) {
                // Save a marker that this wal / segment combination will not be reused.
                // This will help TableWriter to cache the segment files FDs.
                int walId = Numbers.decodeHighInt(currentWalSegment);
                if (walId > -1) {
                    int segmentId = Numbers.decodeLowInt(currentWalSegment);
                    long finalWalSegment = Numbers.encodeLowHighInts(segmentId, -walId);
                    transactionMeta.set(i + WAL_ID_SEG_ID_OFFSET, finalWalSegment);
                }
            }
            runningMinTimestamp = Math.min(runningMinTimestamp, currentMinTimestamp);

            if (transactionMeta.get(i + COMMIT_TO_TIMESTAMP_OFFSET) != FORCE_FULL_COMMIT) {
                transactionMeta.set(i + COMMIT_TO_TIMESTAMP_OFFSET, commitToTimestamp);
            } else {
                // Force full commit before this record
                runningMinTimestamp = FORCE_FULL_COMMIT;
            }
        }

        // Avoid O3 commits with existing data. Start from beginning and set commit to timestamp to be min infinity until
        // the all future min timestamp are greater than current max timestamp.
        for (int i = 0, n = transactionMeta.size(); i < n; i += TXN_METADATA_LONGS_SIZE) {

            long commitToTimestamp = transactionMeta.get(i + COMMIT_TO_TIMESTAMP_OFFSET);
            if (commitToTimestamp < maxCommittedTimestamp) {
                transactionMeta.set(i + COMMIT_TO_TIMESTAMP_OFFSET, Long.MIN_VALUE);
            }
        }

    }

    private static WalEventCursor openWalEFile(Path tempPath, WalEventReader eventReader, int segmentTxn, long seqTxn) {
        WalEventCursor walEventCursor;
        try {
            walEventCursor = eventReader.of(tempPath, WAL_FORMAT_VERSION, segmentTxn);
        } catch (CairoException ex) {
            throw CairoException.critical(ex.getErrno()).put("cannot read WAL even file for seqTxn=").put(seqTxn)
                    .put(", ").put(ex.getFlyweightMessage()).put(']');
        }
        return walEventCursor;
    }

    private long getCommitMaxTimestamp(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + MAX_TIMESTAMP_OFFSET));
    }

    private void loadTransactionDetails(Path tempPath, TransactionLogCursor transactionLogCursor, long loadFromSeqTxn, int rootLen, long maxCommittedTimestamp) {
        transactionLogCursor.setPosition(loadFromSeqTxn - 1);

        try (WalEventReader eventReader = walEventReader) {

            int prevWalId = Integer.MIN_VALUE;
            int prevSegmentId = Integer.MIN_VALUE;
            int prevSegmentTxn = Integer.MIN_VALUE;
            WalEventCursor walEventCursor = null;

            txnDetails.clear();
            int txnsToLoad = (int) Math.min(maxLookahead, transactionLogCursor.getMaxTxn() - loadFromSeqTxn + 1) * TXN_DETAIL_RECORD_SIZE;
            txnDetails.checkCapacity(txnsToLoad);

            // Load the map of outstanding WAL transactions to load necessary details from WAL-E files efficiently.
            for (int i = 0; i < maxLookahead && transactionLogCursor.hasNext(); i++) {
                assert i + loadFromSeqTxn == transactionLogCursor.getTxn();
                txnDetails.add(transactionLogCursor.getWalId());
                txnDetails.add(transactionLogCursor.getSegmentId());
                txnDetails.add(transactionLogCursor.getSegmentTxn());
                txnDetails.add(i);
            }

            int lastWalId = -1;
            int lastSegmentId = -1;
            int lastSegmentTxn = -2;

            txnDetails.sortGroups(TXN_DETAIL_RECORD_SIZE);
            int incrementalLoadStartIndex = transactionMeta.size();

            for (int i = 0, size = txnDetails.size() / TXN_DETAIL_RECORD_SIZE; i < size; i++) {
                int walId = txnDetails.get(TXN_DETAIL_RECORD_SIZE * i);
                int segmentId = txnDetails.get(TXN_DETAIL_RECORD_SIZE * i + 1);
                int segmentTxn = txnDetails.get(TXN_DETAIL_RECORD_SIZE * i + 2);
                long segTxn = txnDetails.get(TXN_DETAIL_RECORD_SIZE * i + 3) + loadFromSeqTxn;

                if (walId > 0) {

                    // Switch to the WAL-E file or scroll to the transaction
                    if (lastWalId == walId && segmentId == lastSegmentId) {
                        assert segmentTxn > lastSegmentTxn;
                        //noinspection StatementWithEmptyBody
                        while (lastSegmentTxn++ < segmentTxn && walEventCursor.hasNext()) {
                            // Skip uncommitted transactions
                        }
                        if (lastSegmentTxn != segmentTxn) {
                            walEventCursor = openWalEFile(tempPath, eventReader, segmentTxn, segTxn);
                            lastSegmentTxn = segmentTxn;
                        }
                    } else {
                        tempPath.trimTo(rootLen).concat(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                        walEventCursor = openWalEFile(tempPath, eventReader, segmentTxn, segTxn);
                        lastWalId = walId;
                        lastSegmentId = segmentId;
                        lastSegmentTxn = segmentTxn;
                    }

                    final byte walTxnType = walEventCursor.getType();
                    if (walTxnType == DATA) {
                        WalEventCursor.DataInfo commitInfo = walEventCursor.getDataInfo();
                        transactionMeta.add(segTxn);
                        transactionMeta.add(-1); // commit to timestamp
                        transactionMeta.add(commitInfo.getMinTimestamp());
                        transactionMeta.add(commitInfo.getMaxTimestamp());
                        transactionMeta.add(Numbers.encodeLowHighInts(segmentId, walId));
                        transactionMeta.add(commitInfo.getStartRowID());
                        transactionMeta.add(commitInfo.getEndRowID());
                        continue;
                    }
                }
                // If there is ALTER or UPDATE, we have to flush everything without keeping anything in the lag.
                transactionMeta.add(segTxn);
                transactionMeta.add(FORCE_FULL_COMMIT); // commit to timestamp
                transactionMeta.add(-1); // min timestamp
                transactionMeta.add(-1); // max timestamp
                transactionMeta.add(Numbers.encodeLowHighInts(segmentId, walId));
                transactionMeta.add(-1);
                transactionMeta.add(-1);
            }

            transactionMeta.sortGroups(TXN_METADATA_LONGS_SIZE, incrementalLoadStartIndex, transactionMeta.size());
        } finally {
            tempPath.trimTo(rootLen);
        }
    }
}
