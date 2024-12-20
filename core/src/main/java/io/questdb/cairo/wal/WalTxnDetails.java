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
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;

import static io.questdb.cairo.wal.WalTxnType.DATA;
import static io.questdb.cairo.wal.WalTxnType.NONE;
import static io.questdb.cairo.wal.WalUtils.*;

public class WalTxnDetails {
    private static final int FLAG_IS_LAST_SEGMENT_USAGE = 0x2;
    private static final int FLAG_IS_OOO = 0x1;

    public static final long FORCE_FULL_COMMIT = Long.MAX_VALUE;
    public static final long LAST_ROW_COMMIT = Long.MAX_VALUE - 1;
    private static final int SEQ_TXN_OFFSET = 0;
    private static final int COMMIT_TO_TIMESTAMP_OFFSET = SEQ_TXN_OFFSET + 1;
    private static final int WAL_TXN_ID_WAL_ID_OFFSET = COMMIT_TO_TIMESTAMP_OFFSET + 1;
    private static final int WAL_TXN_ID_SEG_ID_OFFSET = WAL_TXN_ID_WAL_ID_OFFSET + 1;
    private static final int WAL_TXN_MIN_TIMESTAMP_OFFSET = WAL_TXN_ID_SEG_ID_OFFSET + 1;
    private static final int WAL_TXN_MAX_TIMESTAMP_OFFSET = WAL_TXN_MIN_TIMESTAMP_OFFSET + 1;
    private static final int WAL_TXN_ROW_LO_OFFSET = WAL_TXN_MAX_TIMESTAMP_OFFSET + 1;
    private static final int WAL_TXN_ROW_HI_OFFSET = WAL_TXN_ROW_LO_OFFSET + 1;
    private static final int WAL_TXN_ROW_IN_ORDER_DATA_TYPE = WAL_TXN_ROW_HI_OFFSET + 1;
    private static final int WAL_TXN_SYMBOL_DIFF_OFFSET = WAL_TXN_ROW_IN_ORDER_DATA_TYPE + 1;
    public static final int TXN_METADATA_LONGS_SIZE = WAL_TXN_SYMBOL_DIFF_OFFSET + 1;
    private static final int TXN_DETAIL_RECORD_SIZE = 5;
    private final CharSequenceIntHashMap allSymbols = new CharSequenceIntHashMap();
    private final int maxLookahead;
    private final int symbolIndexStartOffset = 0;
    private final IntList symbolIndexes = new IntList();
    private final SymbolMapDiffCursorImpl symbolMapDiffCursor;
    private final LongList transactionMeta = new LongList();
    private final IntList txnDetails = new IntList();
    private final WalEventReader walEventReader;
    private long startSeqTxn = 0;

    public WalTxnDetails(FilesFacade ff, int maxLookahead) {
        walEventReader = new WalEventReader(ff);
        this.maxLookahead = maxLookahead * 10;
        symbolMapDiffCursor = new SymbolMapDiffCursorImpl(allSymbols, symbolIndexes);
    }

    public int calculateInsertTransactionBlock(long seqTxn, long maxBlockRecordCount) {
        int blockSize = 1;
        long lastSeqTxn = getLastSeqTxn();
        long totalRowCount = 0;
        for (long nextTxn = seqTxn; nextTxn < lastSeqTxn; nextTxn++) {
            long txnRowCount = getSegmentRowHi(nextTxn) - getSegmentRowLo(nextTxn);
            totalRowCount += txnRowCount;
            blockSize++;

            if (getCommitToTimestamp(nextTxn) == FORCE_FULL_COMMIT || totalRowCount > maxBlockRecordCount) {
                break;
            }
        }

        // TODO: support blocked transactions
//        return blockSize;
        return 1;
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

    public long getMaxTimestamp(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE) + WAL_TXN_MAX_TIMESTAMP_OFFSET);
    }

    public long getMinTimestamp(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE) + WAL_TXN_MIN_TIMESTAMP_OFFSET);
    }

    public long getSegmentRowHi(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE) + WAL_TXN_ROW_HI_OFFSET);
    }

    public long getSegmentRowLo(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE) + WAL_TXN_ROW_LO_OFFSET);
    }

    public long getStructureVersion(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_MIN_TIMESTAMP_OFFSET));
    }

    public boolean getTxnInOrder(long seqTxn) {
        int isOutOfOrder = Numbers.decodeLowInt(transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_ROW_IN_ORDER_DATA_TYPE)));
        return (isOutOfOrder & FLAG_IS_OOO) == 0;
    }

    public int getWalId(long seqTxn) {
        return (int) transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_ID_WAL_ID_OFFSET));
    }

    public int getWalSegmentId(long seqTxn) {
        return (int) transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_ID_SEG_ID_OFFSET));
    }

    public SymbolMapDiffCursor getWalSymbolDiffCursor(long seqTxn) {
        long offset = transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_SYMBOL_DIFF_OFFSET));
        symbolMapDiffCursor.of((int) (offset - symbolIndexStartOffset));
        return symbolMapDiffCursor;
    }

    public byte getWalTxnType(long seqTxn) {
        return (byte) Numbers.decodeHighInt(transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_ROW_IN_ORDER_DATA_TYPE)));
    }

    public boolean hasRecord(long seqTxn) {
        return (seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE < transactionMeta.size();
    }

    public boolean isLastSegmentUsage(long seqTxn) {
        int isOutOfOrder = Numbers.decodeLowInt(transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_ROW_IN_ORDER_DATA_TYPE)));
        return (isOutOfOrder & FLAG_IS_LAST_SEGMENT_USAGE) != 0;
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

        if (lastSeqTxn >= loadFromSeqTxn && startSeqTxn < loadFromSeqTxn) {
            int shift = (int) (loadFromSeqTxn - startSeqTxn);
            transactionMeta.removeIndexBlock(0, shift * TXN_METADATA_LONGS_SIZE);
            this.startSeqTxn = loadFromSeqTxn;
            loadFromSeqTxn = lastSeqTxn + 1;
        } else {
            transactionMeta.clear();
            symbolIndexes.clear();
            allSymbols.clear();
            this.startSeqTxn = loadFromSeqTxn;
        }

        loadTransactionDetails(tempPath, transactionLogCursor, loadFromSeqTxn, rootLen, maxCommittedTimestamp);

        // set commit to timestamp moving backwards
        long runningMinTimestamp = LAST_ROW_COMMIT;
        for (int i = transactionMeta.size() - TXN_METADATA_LONGS_SIZE; i > -1; i -= TXN_METADATA_LONGS_SIZE) {

            long commitToTimestamp = runningMinTimestamp;
            long currentMinTimestamp = transactionMeta.getQuick(i + WAL_TXN_MIN_TIMESTAMP_OFFSET);

            // Find out if the wal/segment is not used anymore for future transactions.
            // Since we're moving backwards, if this is the first time this combination occurs
            // it means that it's the last transaction from this wal/segment.
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

    public WalTxnDetailsSlice sortSliceBySegment(long startSeqTxn, int blockTransactionCount) {
        // TODO: implement
        return null;
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
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_MAX_TIMESTAMP_OFFSET));
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
            long initialStructureVersion = 0;
            for (int i = 0; i < maxLookahead && transactionLogCursor.hasNext(); i++) {
                assert i + loadFromSeqTxn == transactionLogCursor.getTxn();
                txnDetails.add(transactionLogCursor.getWalId());
                txnDetails.add(transactionLogCursor.getSegmentId());
                txnDetails.add(transactionLogCursor.getSegmentTxn());
                txnDetails.add(i);
                if (i == 0) {
                    initialStructureVersion = transactionLogCursor.getStructureVersion();
                }
                txnDetails.add((int) (transactionLogCursor.getStructureVersion() - initialStructureVersion));
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
                long seqTxn = txnDetails.get(TXN_DETAIL_RECORD_SIZE * i + 3) + loadFromSeqTxn;
                long structureVersion = txnDetails.get(TXN_DETAIL_RECORD_SIZE * i + 4) + initialStructureVersion;

                final byte walTxnType;
                if (walId > 0) {
                    // Switch to the WAL-E file or scroll to the transaction
                    if (lastWalId == walId && segmentId == lastSegmentId) {
                        assert segmentTxn > lastSegmentTxn;
                        //noinspection StatementWithEmptyBody
                        while (lastSegmentTxn++ < segmentTxn && walEventCursor.hasNext()) {
                            // Skip uncommitted transactions
                        }
                        if (lastSegmentTxn != segmentTxn) {
                            walEventCursor = openWalEFile(tempPath, eventReader, segmentTxn, seqTxn);
                            lastSegmentTxn = segmentTxn;
                        }
                    } else {
                        tempPath.trimTo(rootLen).concat(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                        walEventCursor = openWalEFile(tempPath, eventReader, segmentTxn, seqTxn);
                        lastWalId = walId;
                        lastSegmentId = segmentId;
                        lastSegmentTxn = segmentTxn;
                    }

                    walTxnType = walEventCursor.getType();
                    if (walTxnType == DATA) {
                        WalEventCursor.DataInfo commitInfo = walEventCursor.getDataInfo();
                        transactionMeta.add(seqTxn);
                        transactionMeta.add(-1); // commit to timestamp
                        transactionMeta.add(walId);
                        transactionMeta.add(segmentId);
                        transactionMeta.add(commitInfo.getMinTimestamp());
                        transactionMeta.add(commitInfo.getMaxTimestamp());
                        transactionMeta.add(commitInfo.getStartRowID());
                        transactionMeta.add(commitInfo.getEndRowID());
                        int flags = commitInfo.isOutOfOrder() ? FLAG_IS_OOO : 0x0;
                        // The records are sorted by WAL ID, segment ID.
                        // If the next record is not from the same segment it means it's the last txn from the segment.
                        if (i + 1 < size) {
                            int nextWalId = txnDetails.get(TXN_DETAIL_RECORD_SIZE * (i + 1));
                            int nextSegmentId = txnDetails.get(TXN_DETAIL_RECORD_SIZE * (i + 1) + 1);
                            if (nextSegmentId != segmentId || nextWalId != walId) {
                                flags |= FLAG_IS_LAST_SEGMENT_USAGE;
                            }
                        } else {
                            flags |= FLAG_IS_LAST_SEGMENT_USAGE;
                        }
                        transactionMeta.add(Numbers.encodeLowHighInts(flags, walTxnType));
                        transactionMeta.add(saveSymbols(symbolIndexStartOffset, commitInfo, seqTxn));
                        continue;
                    }
                } else {
                    walTxnType = NONE;
                }
                // If there is ALTER or UPDATE, we have to flush everything without keeping anything in the lag.
                transactionMeta.add(seqTxn);
                transactionMeta.add(FORCE_FULL_COMMIT); // commit to timestamp
                transactionMeta.add(walId);
                transactionMeta.add(segmentId);
                transactionMeta.add(structureVersion); // min timestamp but used as structure version
                transactionMeta.add(-1); // max timestamp
                transactionMeta.add(-1); // start row id
                transactionMeta.add(-1); // end row id
                transactionMeta.add(Numbers.encodeLowHighInts(0, walTxnType));
                transactionMeta.add(-1); // symbols diff offset
            }

            transactionMeta.sortGroups(TXN_METADATA_LONGS_SIZE, incrementalLoadStartIndex, transactionMeta.size());
        } finally {
            tempPath.trimTo(rootLen);
        }
    }

    private int saveSymbols(int symbolIndexStartOffset, SymbolMapDiffCursor commitInfo, long seqTxn) {
        SymbolMapDiff symbolMapDiff;
        int symbolCount = 0;
        int startOffset = symbolIndexes.size();

        // Length of record for this seqTxn
        symbolIndexes.add(-1);
        // Symbol count in this record
        symbolIndexes.add(-1);
        symbolIndexes.add(Numbers.decodeLowInt(seqTxn));
        symbolIndexes.add(Numbers.decodeHighInt(seqTxn));

        int totalSymbolsSaved = 0;
        while ((symbolMapDiff = commitInfo.nextSymbolMapDiff()) != null) {
            int cleanSymbolCount = symbolMapDiff.getCleanSymbolCount();

            SymbolMapDiffEntry entry;
            int entryBegins = symbolIndexes.size();

            // records per this symbol column
            symbolIndexes.add(-1);
            symbolIndexes.add(symbolMapDiff.getColumnIndex());
            symbolIndexes.add(symbolMapDiff.hasNullValue() ? 1 : 0);
            symbolIndexes.add(cleanSymbolCount);

            int count = 0;
            while ((entry = symbolMapDiff.nextEntry()) != null) {
                final int key = entry.getKey();
                symbolIndexes.add(key);
                if (key >= cleanSymbolCount) {
                    final CharSequence symbolValue = entry.getSymbol();
                    int keyIndex = allSymbols.keyIndex(symbolValue);
                    int valueOffset;
                    if (keyIndex < 0) {
                        valueOffset = allSymbols.valueAt(keyIndex);
                    } else {
                        valueOffset = allSymbols.keys().size();
                        allSymbols.putAt(keyIndex, symbolValue, valueOffset);
                    }
                    symbolIndexes.add(valueOffset);
                } else {
                    assert false;
                    symbolIndexes.add(-1);
                }
                count++;
            }

            // Update number of symbol column entries
            assert allSymbols.keys().size() >= count;
            symbolIndexes.set(entryBegins, count);
            symbolCount++;
            totalSymbolsSaved += count;
        }

        // Empty record, return -1, save space, don't serialize empty records.
        if (symbolCount == 0 || totalSymbolsSaved == 0) {
            symbolIndexes.setPos(startOffset);
            return -1;
        }

        // Set the record length
        symbolIndexes.set(startOffset, symbolIndexes.size() - startOffset);
        // Set the count of symbols
        symbolIndexes.set(startOffset + 1, symbolCount);


        return symbolIndexStartOffset + startOffset;
    }

    private static class SymbolMapDiffCursorImpl implements SymbolMapDiffCursor {
        private final IntList symbolIndexes;
        private final SymbolMapDiffColumnRecord symbolMapDiff;
        int nextRecordLen;
        private long hi;
        private int lo;
        private long seqTxn;
        private int symbolIndex;
        private int symbolsCount;

        public SymbolMapDiffCursorImpl(CharSequenceIntHashMap allSymbols, IntList symbolIndexes) {
            this.symbolIndexes = symbolIndexes;
            symbolMapDiff = new SymbolMapDiffColumnRecord(allSymbols, symbolIndexes);
        }

        @Override
        public SymbolMapDiff nextSymbolMapDiff() {
            if (symbolIndex++ < symbolsCount) {
                assert lo <= hi;
                symbolMapDiff.switchToColumnRecord(lo);
                lo += symbolMapDiff.getRecordSize();
                return symbolMapDiff;
            }
            return null;
        }

        public void of(int startOffset) {
            if (startOffset > -1) {
                this.lo = startOffset;
                int recordSize = symbolIndexes.get(startOffset);
                assert recordSize >= 4;
                this.hi = this.lo + recordSize;
                this.symbolsCount = symbolIndexes.get(startOffset + 1);
                this.seqTxn = Numbers.encodeLowHighInts(symbolIndexes.get(startOffset + 2), symbolIndexes.get(startOffset + 3));
                assert this.symbolsCount > -1;
                this.lo += 4;
                symbolIndex = 0;
            } else {
                this.symbolsCount = 0;
            }
        }

        private static class SymbolMapDiffColumnRecord implements SymbolMapDiff, SymbolMapDiffEntry {
            private final CharSequenceIntHashMap allSymbols;
            private final IntList symbolIndexes;
            private int cleanSymbolCount;
            private int columnIndex;
            private boolean containsNull;
            private int nextSymbolOffset;
            private int offsetHi;
            private int savedSymbolRecordCount;

            public SymbolMapDiffColumnRecord(CharSequenceIntHashMap allSymbols, IntList symbolIndexes) {
                this.allSymbols = allSymbols;
                this.symbolIndexes = symbolIndexes;
            }

            @Override
            public void drain() {
            }

            @Override
            public int getCleanSymbolCount() {
                return cleanSymbolCount;
            }

            @Override
            public int getColumnIndex() {
                return columnIndex;
            }

            @Override
            public int getKey() {
                return symbolIndexes.get(nextSymbolOffset);
            }

            @Override
            public int getRecordCount() {
                return savedSymbolRecordCount;
            }

            public int getRecordSize() {
                return 4 + savedSymbolRecordCount * 2;
            }

            @Override
            public CharSequence getSymbol() {
                int keyIndex = symbolIndexes.get(nextSymbolOffset + 1);
                return allSymbols.keys().get(keyIndex);
            }

            @Override
            public boolean hasNullValue() {
                return containsNull;
            }

            @Override
            public SymbolMapDiffEntry nextEntry() {
                nextSymbolOffset += 2;
                if (nextSymbolOffset < offsetHi) {
                    return this;
                } else {
                    return null;
                }
            }

            public void switchToColumnRecord(int lo) {
                this.savedSymbolRecordCount = symbolIndexes.get(lo);
                this.columnIndex = symbolIndexes.get(lo + 1);
                this.containsNull = symbolIndexes.get(lo + 2) > 0;
                this.cleanSymbolCount = symbolIndexes.get(lo + 3);
                this.nextSymbolOffset = lo + 4 - 2;
                this.offsetHi = lo + 4 + savedSymbolRecordCount * 2;
            }
        }
    }

    private class WalTxnDetailsSlice {
    }
}
