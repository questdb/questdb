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
import io.questdb.cairo.MapWriter;
import io.questdb.cairo.SegmentCopyInfo;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.wal.WalTxnType.DATA;
import static io.questdb.cairo.wal.WalTxnType.NONE;
import static io.questdb.cairo.wal.WalUtils.*;

public class WalTxnDetails implements QuietCloseable {
    public static final long FORCE_FULL_COMMIT = Long.MAX_VALUE;
    public static final long LAST_ROW_COMMIT = Long.MAX_VALUE - 1;
    private static final ThreadLocal<DirectString> DIRECT_STRING = new ThreadLocal<>(DirectString::new);
    private static final int FLAG_IS_LAST_SEGMENT_USAGE = 0x2;
    private static final int FLAG_IS_OOO = 0x1;
    private static final int SEQ_TXN_OFFSET = 0;
    private static final int COMMIT_TO_TIMESTAMP_OFFSET = SEQ_TXN_OFFSET + 1;
    private static final int WAL_TXN_ID_WAL_SEG_ID_OFFSET = COMMIT_TO_TIMESTAMP_OFFSET + 1;
    private static final int WAL_TXN_MIN_TIMESTAMP_OFFSET = WAL_TXN_ID_WAL_SEG_ID_OFFSET + 1;
    private static final int WAL_TXN_MAX_TIMESTAMP_OFFSET = WAL_TXN_MIN_TIMESTAMP_OFFSET + 1;
    private static final int WAL_TXN_ROW_LO_OFFSET = WAL_TXN_MAX_TIMESTAMP_OFFSET + 1;
    private static final int WAL_TXN_ROW_HI_OFFSET = WAL_TXN_ROW_LO_OFFSET + 1;
    private static final int WAL_TXN_ROW_IN_ORDER_DATA_TYPE = WAL_TXN_ROW_HI_OFFSET + 1;
    private static final int WAL_TXN_SYMBOL_DIFF_OFFSET = WAL_TXN_ROW_IN_ORDER_DATA_TYPE + 1;
    public static final int TXN_METADATA_LONGS_SIZE = WAL_TXN_SYMBOL_DIFF_OFFSET + 1;
    private static final int TXN_DETAIL_RECORD_SIZE = 5;
    private final int maxLookahead;
    private final SymbolMapDiffCursorImpl symbolMapDiffCursor = new SymbolMapDiffCursorImpl();
    private final LongList transactionMeta = new LongList();
    private final IntList txnDetails = new IntList();
    private final WalEventReader walEventReader;
    WalTxnDetailsSlice txnSlice = new WalTxnDetailsSlice();
    private long currentSymbolIndexesStartOffset = 0;
    private long currentSymbolStringMemStartOffset = 0;
    private long startSeqTxn = 0;
    // Stores all symbol metadata for the stored transactions.
    // The format is 4 int header
    // 4 bytes - record length
    // 4 bytes - symbol map count
    // 4 byte - low 32bits of seqTxn
    // 4 byte - high 32bits of seqTxn
    // Then for every symbol column 6 int record
    // 4 bytes - map records count
    // 4 bytes - symbol column index
    // 4 bytes - null flag
    // 4 bytes - clean symbol count
    // 4 bytes - low 32bits of offset into stored symbol strings in symbolStringsMem
    // 4 bytes - high 32bits of offset into stored symbol strings in symbolStringsMem
    private DirectIntList symbolIndexes = new DirectIntList(4, MemoryTag.NATIVE_TABLE_WRITER);

    // Stores all symbol strings for the stored transactions. The format is usula STRING format 4 bytes length + string in chars
    private MemoryCARW symbolStringsMem = null;

    public WalTxnDetails(FilesFacade ff, int maxLookahead) {
        walEventReader = new WalEventReader(ff);
        this.maxLookahead = maxLookahead * 10;
    }

    public boolean buildTxnSymbolMap(SegmentCopyInfo transactions, int columnIndex, MapWriter mapWriter, MemoryCARW outMem) {
        // Header, 2 ints per txn,
        // - clear symbol count
        // - offset of the map start for the transactions
        long txnCount = transactions.getTxnCount();
        int headerInts = 2;
        outMem.jumpTo(txnCount * headerInts * Integer.BYTES);
        DirectString directSymbolString = DIRECT_STRING.get();

        assert transactions.assertSeqTxnOrder();

        long startTxn = transactions.getStartTxn();
        // Add symbols in the order of commits to make the int symbol values independent of WAL apply method
        for (long seqTxn = startTxn, n = startTxn + txnCount; seqTxn < n; seqTxn++) {
            long txnSymbolOffset = getWalSymbolDiffCursorOffset(seqTxn) - currentSymbolIndexesStartOffset;

            boolean identical = true;
            long mapAppendOffset = outMem.getAppendOffset();
            int cleanSymbolCount = 0;

            if (txnSymbolOffset > -1) {
                long txnSymbolOffsetHi = txnSymbolOffset + symbolIndexes.get(txnSymbolOffset);
                int mapCount = symbolIndexes.get(txnSymbolOffset + 1);
                txnSymbolOffset += 4;

                // Find column map of the given column index
                boolean notFound = true;
                while (mapCount-- > 0 && (notFound = symbolIndexes.get(txnSymbolOffset + 1) != columnIndex)) {
                    txnSymbolOffset += 6;
                }

                if (!notFound) {
                    // Symbol map exists in the transaction
                    int recordCount = symbolIndexes.get(txnSymbolOffset);
                    int hasNulls = symbolIndexes.get(txnSymbolOffset + 2);
                    cleanSymbolCount = symbolIndexes.get(txnSymbolOffset + 3);
                    int symbolStringsOffsetLo = symbolIndexes.get(txnSymbolOffset + 4);
                    int symbolStringsOffsetHi = symbolIndexes.get(txnSymbolOffset + 5);
                    long symbolStringsOffset = Numbers.encodeLowHighInts(symbolStringsOffsetLo, symbolStringsOffsetHi)
                            - currentSymbolStringMemStartOffset;

                    if (hasNulls != 0) {
                        mapWriter.updateNullFlag(true);
                    }

                    for (int i = 0; i < recordCount; i++) {
                        int symbolLen = symbolStringsMem.getInt(symbolStringsOffset);
                        assert symbolLen >= 0 && symbolLen * 2L + symbolStringsOffset < symbolStringsMem.getAppendOffset();
                        long addressLo = symbolStringsMem.addressOf(symbolStringsOffset + Integer.BYTES);
                        directSymbolString.of(addressLo, addressLo + symbolLen * 2L);

                        int newKey = mapWriter.put(directSymbolString);
                        // Sometimes newKey can be greater than cleanSymbolCount
                        // This is because cleanSymbolCount is read from _txn file after copying symbol files
                        // and it can show more symbols stored at that point than what used during the symbol opening
                        identical &= newKey == i + cleanSymbolCount;
                        outMem.putInt(mapAppendOffset + ((long) i << 2), newKey);
                        symbolStringsOffset += Vm.getStorageLength(symbolLen);
                    }
                    outMem.jumpTo(mapAppendOffset + ((long) recordCount << 2));
                }
            }

            long txnIndex = transactions.getMappingOrder(seqTxn);
            assert txnIndex > -1 && txnIndex < txnCount;

            int newAppendOffset = (int) outMem.getAppendOffset();
            if (identical) {
                // If the symbol map is identical to the previous transaction, we don't need to save the map.
                // Instead, we save the clean symbol count is maximum possible int value.
                outMem.putInt(txnIndex * Integer.BYTES * headerInts, Integer.MAX_VALUE);
                outMem.putInt(txnIndex * Integer.BYTES * headerInts + Integer.BYTES, (int) (mapAppendOffset >> 2));
                outMem.jumpTo(mapAppendOffset);
            } else {
                outMem.putInt(txnIndex * Integer.BYTES * headerInts, cleanSymbolCount);
                outMem.putInt(txnIndex * Integer.BYTES * headerInts + Integer.BYTES, (int) (mapAppendOffset >> 2));
                outMem.jumpTo(newAppendOffset);
            }
        }

        // Return true if there are any sumbol maps created, e.g. mapping is not identical transformation
        return outMem.getAppendOffset() > (txnCount << 3);
    }

    public int calculateInsertTransactionBlock(long seqTxn, long maxBlockRecordCount) {
        int blockSize = 1;
        long lastSeqTxn = getLastSeqTxn();
        long totalRowCount = 0;
        for (long nextTxn = seqTxn; nextTxn < lastSeqTxn; nextTxn++) {
            if (getCommitToTimestamp(nextTxn) == FORCE_FULL_COMMIT || totalRowCount > maxBlockRecordCount) {
                break;
            }
            long txnRowCount = getSegmentRowHi(nextTxn) - getSegmentRowLo(nextTxn);
            totalRowCount += txnRowCount;
            blockSize++;
        }

//         TODO: support blocked transactions
//        return 1;
        return blockSize;
    }

    @Override
    public void close() {
        symbolIndexes = Misc.free(symbolIndexes);
        symbolStringsMem = Misc.free(symbolStringsMem);
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

    public long getSegTxn(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + SEQ_TXN_OFFSET));
    }

    public int getSegmentId(long seqTxn) {
        return Numbers.decodeLowInt(transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_ID_WAL_SEG_ID_OFFSET)));
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
        return Numbers.decodeHighInt(transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_ID_WAL_SEG_ID_OFFSET)));
    }

    @Nullable
    public SymbolMapDiff getWalSymbolColMap(long seqTxn, int columnIndex) {
        long offset = transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_SYMBOL_DIFF_OFFSET));
        if (offset < 0) {
            return null;
        }

        symbolMapDiffCursor.of((int) (offset - currentSymbolIndexesStartOffset));
        SymbolMapDiff symbolMapDiff;
        while ((symbolMapDiff = symbolMapDiffCursor.nextSymbolMapDiff()) != null) {
            if (symbolMapDiff.getColumnIndex() == columnIndex) {
                return symbolMapDiff;
            }
        }
        // Column added to the existing transaction
        // all values are null
        return null;
    }

    @Nullable
    public SymbolMapDiffCursor getWalSymbolDiffCursor(long seqTxn) {
        long offset = transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_SYMBOL_DIFF_OFFSET));
        if (offset < 0) {
            return null;
        }
        symbolMapDiffCursor.of((int) (offset - currentSymbolIndexesStartOffset));
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

    public void prepareCopySegments(long startSeqTxn, int blockTransactionCount, SegmentCopyInfo copyTasks, boolean hasSymbols) {
        copyTasks.initBlock(startSeqTxn, blockTransactionCount, hasSymbols);
        try (var sortedBySegmentTxnSlice = sortSliceByWalAndSegment(startSeqTxn, blockTransactionCount)) {
            int lastSegmentId = -1;
            int lastWalId = -1;
            long segmentLo = -1;

            int copyTaskCount = 0;
            long totalRowsToCopy = 0;
            boolean isLastSegmentUse = false;
            long roHi = 0;

            for (int i = 0; i < blockTransactionCount; i++) {
                int relativeSeqTxn = (int) (sortedBySegmentTxnSlice.getSeqTxn(i) - startSeqTxn);
                int segmentId = sortedBySegmentTxnSlice.getSegmentId(i);
                int walId = sortedBySegmentTxnSlice.getWalId(i);
                long roLo = sortedBySegmentTxnSlice.getRoLo(i);

                if (i > 0) {
                    if (lastWalId != walId || lastSegmentId != segmentId) {
                        // Prev min, max timestamps, roHi
                        copyTasks.addSegment(lastWalId, lastSegmentId, segmentLo, roHi, isLastSegmentUse);
                        copyTaskCount++;
                        segmentLo = roLo;

                        lastWalId = walId;
                        lastSegmentId = segmentId;
                    }
                } else {
                    segmentLo = roLo;
                    lastWalId = walId;
                    lastSegmentId = segmentId;
                }

                long minTimestamp = sortedBySegmentTxnSlice.getMinTimestamp(i);
                long maxTimestamp = sortedBySegmentTxnSlice.getMaxTimestamp(i);
                isLastSegmentUse = isLastSegmentUse | sortedBySegmentTxnSlice.isLastSegmentUse(i);
                roHi = sortedBySegmentTxnSlice.getRoHi(i);
                long committedRowsCount = roHi - roLo;
                copyTasks.addTxn(roLo, relativeSeqTxn, committedRowsCount, copyTaskCount, minTimestamp, maxTimestamp);
            }

            int lastIndex = blockTransactionCount - 1;
            int segmentId = sortedBySegmentTxnSlice.getSegmentId(lastIndex);
            int walId = sortedBySegmentTxnSlice.getWalId(lastIndex);

            copyTasks.addSegment(walId, segmentId, segmentLo, roHi, isLastSegmentUse);
        }
    }

    public void readObservableTxnMeta(
            final Path tempPath,
            final TransactionLogCursor transactionLogCursor,
            final int rootLen,
            long appliedSeqTxn,
            final long maxCommittedTimestamp
    ) {
        final long lastLoadedSeqTxn = getLastSeqTxn();
        long loadFromSeqTxn = appliedSeqTxn + 1;

        if (lastLoadedSeqTxn >= loadFromSeqTxn && startSeqTxn < loadFromSeqTxn) {
            int shift = (int) (loadFromSeqTxn - startSeqTxn);
            long newSymbolsOffset = getMinSymbolIndexOffsetFromTxn(loadFromSeqTxn);
            assert newSymbolsOffset >= currentSymbolIndexesStartOffset;

            if (newSymbolsOffset > currentSymbolIndexesStartOffset) {

                if (symbolStringsMem != null && symbolStringsMem.getAppendOffset() > 0) {
                    long symbolMapStartOffset = symbolIndexes.get(newSymbolsOffset - currentSymbolIndexesStartOffset);
                    long newSymbolStringMemStartOffset = findFirstSymbolStringMemOffset(newSymbolsOffset);

                    shiftSymbolStringsDataLeft(newSymbolStringMemStartOffset - currentSymbolStringMemStartOffset);
                    currentSymbolStringMemStartOffset = newSymbolStringMemStartOffset;
                }

                symbolIndexes.removeIndexBlock(0, newSymbolsOffset - currentSymbolIndexesStartOffset);
                currentSymbolIndexesStartOffset = newSymbolsOffset;
            }

            transactionMeta.removeIndexBlock(0, shift * TXN_METADATA_LONGS_SIZE);
            this.startSeqTxn = loadFromSeqTxn;
            loadFromSeqTxn = lastLoadedSeqTxn + 1;
        } else {
            transactionMeta.clear();
            symbolIndexes.clear();
            if (symbolStringsMem != null) {
                symbolStringsMem.truncate();
            }
            currentSymbolStringMemStartOffset = 0;
            currentSymbolIndexesStartOffset = 0;
            startSeqTxn = loadFromSeqTxn;
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

    private long findFirstSymbolStringMemOffset(long symbolsOffset) {
        int i = (int) (symbolsOffset - currentSymbolIndexesStartOffset), n = (int) symbolIndexes.size();
        if (i < n) {
            assert i + 4 + 6 <= n;

            int symbolColCount = symbolIndexes.get(i + 1);
            assert symbolColCount > 0;
            // See record format in the comments to symbolIndexes
            // We are reading lo, hi of the offset in symbolStringsMem
            int lo = symbolIndexes.get(i + 8);
            int hi = symbolIndexes.get(i + 9);
            return Numbers.encodeLowHighInts(lo, hi);
        }
        return currentSymbolStringMemStartOffset + symbolStringsMem.getAppendOffset();
    }

    private long getCommitMaxTimestamp(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_MAX_TIMESTAMP_OFFSET));
    }

    private long getMinSymbolIndexOffsetFromTxn(long seqTxn) {
        // Find the minimum symbol index offset from all the transactions from seqTxn
        // Because the transactions are loaded by segments and then sorted by seqTxn
        // the first transaction does not have the minimum offset.
        long n = getLastSeqTxn() + 1;
        long minOffset = symbolIndexes.size() + currentSymbolIndexesStartOffset;

        for (long txn = seqTxn; txn < n; txn++) {
            long offset = getWalSymbolDiffCursorOffset(txn);
            if (offset > -1) {
                minOffset = Math.min(minOffset, offset);
            }
        }
        return minOffset;
    }

    private MemoryCARW getSymbolMem() {
        if (symbolStringsMem == null) {
            symbolStringsMem = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_TABLE_WRITER);
        }
        return symbolStringsMem;
    }

    private long getWalSymbolDiffCursorOffset(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_SYMBOL_DIFF_OFFSET));
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

            int txnCount = txnDetails.size() / TXN_DETAIL_RECORD_SIZE;
            for (int i = 0; i < txnCount; i++) {
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
                        transactionMeta.add(Numbers.encodeLowHighInts(segmentId, walId));
                        transactionMeta.add(commitInfo.getMinTimestamp());
                        transactionMeta.add(commitInfo.getMaxTimestamp());
                        transactionMeta.add(commitInfo.getStartRowID());
                        transactionMeta.add(commitInfo.getEndRowID());
                        int flags = commitInfo.isOutOfOrder() ? FLAG_IS_OOO : 0x0;
                        // The records are sorted by WAL ID, segment ID.
                        // If the next record is not from the same segment it means it's the last txn from the segment.
                        if (i + 1 < txnCount) {
                            int nextWalId = txnDetails.get(TXN_DETAIL_RECORD_SIZE * (i + 1));
                            int nextSegmentId = txnDetails.get(TXN_DETAIL_RECORD_SIZE * (i + 1) + 1);
                            if (nextSegmentId != segmentId || nextWalId != walId) {
                                flags |= FLAG_IS_LAST_SEGMENT_USAGE;
                            }
                        } else {
                            flags |= FLAG_IS_LAST_SEGMENT_USAGE;
                        }
                        transactionMeta.add(Numbers.encodeLowHighInts(flags, walTxnType));
                        transactionMeta.add(saveSymbols(commitInfo, seqTxn));
                        continue;
                    }
                } else {
                    walTxnType = NONE;
                }
                // If there is ALTER or UPDATE, we have to flush everything without keeping anything in the lag.
                transactionMeta.add(seqTxn);
                transactionMeta.add(FORCE_FULL_COMMIT); // commit to timestamp
                transactionMeta.add(Numbers.encodeLowHighInts(segmentId, walId));
                transactionMeta.add(structureVersion); // min timestamp but used as structure version
                transactionMeta.add(-1); // max timestamp
                transactionMeta.add(-1); // start row id
                transactionMeta.add(-1); // end row id
                transactionMeta.add(Numbers.encodeLowHighInts(0, walTxnType));
                transactionMeta.add(-1); // symbols diff offset
            }

            transactionMeta.sortGroupsByElement(
                    TXN_METADATA_LONGS_SIZE,
                    0,
                    incrementalLoadStartIndex / TXN_METADATA_LONGS_SIZE,
                    incrementalLoadStartIndex / TXN_METADATA_LONGS_SIZE + txnCount
            );
        } finally {
            tempPath.trimTo(rootLen);
        }
    }

    private long saveSymbols(SymbolMapDiffCursor commitInfo, long seqTxn) {
        var symbolMem = getSymbolMem();
        SymbolMapDiff symbolMapDiff;
        int symbolCount = 0;
        int startOffset = (int) symbolIndexes.size();

        // Length of record for this seqTxn
        symbolIndexes.add(-1);
        // Symbol count in this record
        symbolIndexes.add(-1);
        symbolIndexes.add(Numbers.decodeLowInt(seqTxn));
        symbolIndexes.add(Numbers.decodeHighInt(seqTxn));

        int totalSymbolsSaved = 0;
        boolean hasNull = false;
        while ((symbolMapDiff = commitInfo.nextSymbolMapDiff()) != null) {
            int cleanSymbolCount = symbolMapDiff.getCleanSymbolCount();

            SymbolMapDiffEntry entry;
            int entryBegins = (int) symbolIndexes.size();

            // records per this symbol column
            symbolIndexes.add(-1);
            symbolIndexes.add(symbolMapDiff.getColumnIndex());
            symbolIndexes.add(symbolMapDiff.hasNullValue() ? 1 : 0);
            hasNull = hasNull || symbolMapDiff.hasNullValue();
            symbolIndexes.add(cleanSymbolCount);

            int symbolIndex = 0;
            long symbolValueOffset = symbolMem.getAppendOffset() + currentSymbolStringMemStartOffset;

            while ((entry = symbolMapDiff.nextEntry()) != null) {
                final int key = entry.getKey() - cleanSymbolCount;
                assert key == symbolIndex;
                symbolIndex++;
                entry.appendSymbolTo(symbolMem);
            }

            symbolIndexes.add(Numbers.decodeLowInt(symbolValueOffset));
            symbolIndexes.add(Numbers.decodeHighInt(symbolValueOffset));

            int count = symbolIndex;
            // Update number of symbol column entries
            symbolIndexes.set(entryBegins, count);
            symbolCount++;
            totalSymbolsSaved += count;
        }

        // Empty record, return -1, save space, don't serialize empty records.
        if ((symbolCount == 0 || totalSymbolsSaved == 0) && !hasNull) {
            symbolIndexes.setPos(startOffset);
            return -1 - (currentSymbolIndexesStartOffset + startOffset);
        }

        // Set the record length
        symbolIndexes.set(startOffset, (int) symbolIndexes.size() - startOffset);
        // Set the count of symbols
        symbolIndexes.set(startOffset + 1, symbolCount);

        return currentSymbolIndexesStartOffset + startOffset;
    }

    private void shiftSymbolStringsDataLeft(long shiftBytes) {
        final long size = symbolStringsMem.getAppendOffset();
        assert size >= shiftBytes;
        if (size > shiftBytes) {
            long address = symbolStringsMem.getPageAddress(0);
            Vect.memcpy(address, address + shiftBytes, size - shiftBytes);
            symbolStringsMem.jumpTo(size - shiftBytes);
        } else {
            symbolStringsMem.truncate();
        }
    }

    private WalTxnDetailsSlice sortSliceByWalAndSegment(long startSeqTxn, int blockTransactionCount) {
        assert blockTransactionCount > 1;
        return txnSlice.of(startSeqTxn, blockTransactionCount);
    }

    private class SymbolMapDiffCursorImpl implements SymbolMapDiffCursor {
        private final SymbolMapDiffColumnRecord symbolMapDiff = new SymbolMapDiffColumnRecord();
        int nextRecordLen;
        private long hi;
        private int lo;
        private int symbolIndex;
        private int symbolsCount;

        @Override
        public SymbolMapDiff nextSymbolMapDiff() {
            if (symbolIndex++ < symbolsCount) {
                assert lo <= hi;
                symbolMapDiff.switchToColumnRecord(lo);
                lo += 6;
                return symbolMapDiff;
            }
            return null;
        }

        public void of(int startOffset) {
            if (startOffset > -1) {
                this.lo = startOffset;
                int recordSize = WalTxnDetails.this.symbolIndexes.get(startOffset);
                assert recordSize >= 4;
                this.hi = this.lo + recordSize;
                this.symbolsCount = symbolIndexes.get(startOffset + 1);
                assert this.symbolsCount > -1;
                this.lo += 4;
                symbolIndex = 0;
            } else {
                this.symbolsCount = 0;
            }
        }

        private class SymbolMapDiffColumnRecord implements SymbolMapDiff, SymbolMapDiffEntry {
            private int cleanSymbolCount;
            private int columnIndex;
            private boolean containsNull;
            private long currentSymbolMemOffset;
            private int index = 0;
            private long nextSymbolMemOffset;
            private int offsetHi;
            private int savedSymbolRecordCount;

            @Override
            public void appendSymbolTo(MemoryARW symbolMem) {
                throw new UnsupportedOperationException();
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
                return index + cleanSymbolCount - 1;
            }

            @Override
            public int getRecordCount() {
                return savedSymbolRecordCount;
            }

            @Override
            public CharSequence getSymbol() {
                return symbolStringsMem.getStrA(currentSymbolMemOffset);
            }

            @Override
            public boolean hasNullValue() {
                return containsNull;
            }

            @Override
            public SymbolMapDiffEntry nextEntry() {
                if (index < savedSymbolRecordCount) {
                    if (index > 0) {
                        currentSymbolMemOffset += Vm.getStorageLength(symbolStringsMem.getInt(currentSymbolMemOffset));
                    }
                    index++;
                    return this;
                } else {
                    return null;
                }
            }

            public void switchToColumnRecord(int lo) {
                this.savedSymbolRecordCount = symbolIndexes.get(lo++);
                this.columnIndex = symbolIndexes.get(lo++);
                this.containsNull = symbolIndexes.get(lo++) > 0;
                this.cleanSymbolCount = symbolIndexes.get(lo++);
                int nextSymbolMemOffsetLo = symbolIndexes.get(lo++);
                int nextSymbolMemOffsetHi = symbolIndexes.get(lo);

                this.currentSymbolMemOffset =
                        Numbers.encodeLowHighInts(nextSymbolMemOffsetLo, nextSymbolMemOffsetHi)
                                - WalTxnDetails.this.currentSymbolStringMemStartOffset;
                this.index = 0;
            }
        }
    }

    public class WalTxnDetailsSlice implements QuietCloseable {
        private long hi;
        private long lo;

        @Override
        public void close() {
            // Sort the data back to original order
            transactionMeta.sortGroupsByElement(
                    TXN_METADATA_LONGS_SIZE,
                    SEQ_TXN_OFFSET,
                    (int) (lo - startSeqTxn),
                    (int) (hi - startSeqTxn)
            );
        }

        public long getMaxTimestamp(int txn) {
            return WalTxnDetails.this.getMaxTimestamp(lo + txn);
        }

        public long getMinTimestamp(int txn) {
            return WalTxnDetails.this.getMinTimestamp(lo + txn);
        }

        public long getRoHi(int txn) {
            return WalTxnDetails.this.getSegmentRowHi(lo + txn);
        }

        public long getRoLo(int txn) {
            return WalTxnDetails.this.getSegmentRowLo(lo + txn);
        }

        public int getSegmentId(int txn) {
            return WalTxnDetails.this.getSegmentId(lo + txn);
        }

        public long getSeqTxn(int txn) {
            return WalTxnDetails.this.getSegTxn(lo + txn);
        }

        public int getWalId(int txn) {
            return WalTxnDetails.this.getWalId(lo + txn);
        }

        public boolean isLastSegmentUse(int txn) {
            return WalTxnDetails.this.isLastSegmentUsage(lo + txn);
        }

        public WalTxnDetailsSlice of(long lo, long count) {
            this.lo = lo;
            this.hi = lo + count;
            transactionMeta.sortGroupsBy2Elements(
                    TXN_METADATA_LONGS_SIZE,
                    WAL_TXN_ID_WAL_SEG_ID_OFFSET,
                    SEQ_TXN_OFFSET,
                    (int) (lo - startSeqTxn),
                    (int) (hi - startSeqTxn)
            );
            return this;
        }
    }
}
