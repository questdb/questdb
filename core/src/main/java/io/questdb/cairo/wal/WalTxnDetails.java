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
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;

import static io.questdb.cairo.wal.WalTxnType.DATA;
import static io.questdb.cairo.wal.WalUtils.*;

public class WalTxnDetails {
    public static final long FORCE_FULL_COMMIT = Long.MAX_VALUE;
    public static final long LAST_ROW_COMMIT = Long.MAX_VALUE - 1;
    private static final int MIN_TIMESTAMP_OFFSET = 1;
    private static final int MAX_TIMESTAMP_OFFSET = MIN_TIMESTAMP_OFFSET + 1;
    private static final int WAL_ID_SEG_ID_OFFSET = MAX_TIMESTAMP_OFFSET + 1;
    public static final int TXN_METADATA_LONGS_SIZE = WAL_ID_SEG_ID_OFFSET + 1;
    private final LongHashSet futureWalSegments = new LongHashSet();
    private final int maxLookahead;
    private final LongList transactionMeta = new LongList();
    private final WalEventReader walEventReader;
    private long startSeqTxn = 0;

    public WalTxnDetails(FilesFacade ff, int maxLookahead) {
        walEventReader = new WalEventReader(ff);
        this.maxLookahead = maxLookahead * 10;
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
            final long committedSeqTxn,
            final long maxCommittedTimestamp
    ) {
        if (committedSeqTxn <= getLastSeqTxn()) {
            int shift = (int) (committedSeqTxn - startSeqTxn + 1);
            if (shift < 0) {
                // This can happen after a rollback. In this case we have to clear everything.
                transactionMeta.clear();
                startSeqTxn = -1;
            } else {
                transactionMeta.removeIndexBlock(0, shift * TXN_METADATA_LONGS_SIZE);
                startSeqTxn = committedSeqTxn + 1;
                if (transactionMeta.size() > 0) {
                    transactionMeta.set(0, -1);
                }
            }
        } else {
            transactionMeta.clear();
            startSeqTxn = -1;
        }

        if (transactionLogCursor.getVersion() == WAL_SEQUENCER_FORMAT_VERSION_V1) {
            loadTransactionDetailsV1(tempPath, transactionLogCursor, rootLen, maxCommittedTimestamp);
        } else {
            loadTransactionDetailsV2(transactionLogCursor, maxCommittedTimestamp);
        }

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

    private void loadTransactionDetailsV1(Path tempPath, TransactionLogCursor transactionLogCursor, int rootLen, long maxCommittedTimestamp) {
        try (WalEventReader eventReader = walEventReader) {

            int prevWalId = Integer.MIN_VALUE;
            int prevSegmentId = Integer.MIN_VALUE;
            int prevSegmentTxn = Integer.MIN_VALUE;
            WalEventCursor walEventCursor = null;

            long runningMaxTimestamp = maxCommittedTimestamp;
            int i = 0;
            while (i++ < maxLookahead && transactionLogCursor.hasNext()) {

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
                        walEventCursor = openWalEFile(tempPath, eventReader, segmentTxn, transactionLogCursor.getTxn());
                        prevWalId = walId;
                        prevSegmentId = segmentId;
                        prevSegmentTxn = segmentTxn;
                    } else {
                        // This is same WALE file, just read next txn transaction.
                        if (!walEventCursor.hasNext()) {
                            walEventCursor = openWalEFile(tempPath, eventReader, segmentTxn, transactionLogCursor.getTxn());
                        }
                    }

                    final byte walTxnType = walEventCursor.getType();
                    if (walTxnType == DATA) {
                        WalEventCursor.DataInfo commitInfo = walEventCursor.getDataInfo();
                        transactionMeta.add(-1); // commit to timestamp
                        transactionMeta.add(commitInfo.getMinTimestamp());
                        transactionMeta.add(commitInfo.getMaxTimestamp());
                        transactionMeta.add(Numbers.encodeLowHighInts(segmentId, walId));
                        runningMaxTimestamp = Math.max(commitInfo.getMaxTimestamp(), runningMaxTimestamp);
                        continue;
                    }
                }
                // If there is ALTER or UPDATE, we have to flush everything without keeping anything in the lag.
                transactionMeta.add(FORCE_FULL_COMMIT); // commit to timestamp
                transactionMeta.add(runningMaxTimestamp); // min timestamp
                transactionMeta.add(runningMaxTimestamp); // max timestamp
                transactionMeta.add(Numbers.encodeLowHighInts(segmentId, walId));
            }
        } finally {
            tempPath.trimTo(rootLen);
        }
    }

    private void loadTransactionDetailsV2(TransactionLogCursor transactionLogCursor, long maxCommittedTimestamp) {
        long runningMaxTimestamp = maxCommittedTimestamp;
        int i = 0;
        while (i++ < maxLookahead && transactionLogCursor.hasNext()) {

            final int walId = transactionLogCursor.getWalId();
            final int segmentId = transactionLogCursor.getSegmentId();
            if (startSeqTxn == -1) {
                startSeqTxn = transactionLogCursor.getTxn();
            }
            assert startSeqTxn + transactionMeta.size() / TXN_METADATA_LONGS_SIZE == transactionLogCursor.getTxn();

            if (transactionLogCursor.getTxnRowCount() > 0) {
                transactionMeta.add(-1); // commit to timestamp
                transactionMeta.add(transactionLogCursor.getTxnMinTimestamp());
                long txnMaxTimestamp = transactionLogCursor.getTxnMaxTimestamp();
                transactionMeta.add(txnMaxTimestamp);
                transactionMeta.add(Numbers.encodeLowHighInts(segmentId, walId));
                runningMaxTimestamp = Math.max(txnMaxTimestamp, runningMaxTimestamp);
                continue;
            }
            // If there is ALTER or UPDATE, we have to flush everything without keeping anything in the lag.
            transactionMeta.add(FORCE_FULL_COMMIT); // commit to timestamp
            transactionMeta.add(runningMaxTimestamp); // min timestamp
            transactionMeta.add(runningMaxTimestamp); // max timestamp
            transactionMeta.add(Numbers.encodeLowHighInts(segmentId, walId));
        }
    }
}
