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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MapWriter;
import io.questdb.cairo.TableWriterSegmentCopyInfo;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.FilesFacade;
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
    private static final int WAL_TXN_MAT_VIEW_REFRESH_TXN = WAL_TXN_SYMBOL_DIFF_OFFSET + 1;
    private static final int WAL_TXN_MAT_VIEW_REFRESH_TS = WAL_TXN_MAT_VIEW_REFRESH_TXN + 1;
    private static final int WAL_TXN_REPLACE_RANGE_TS_LOW = WAL_TXN_MAT_VIEW_REFRESH_TS + 1;
    private static final int WAL_TXN_REPLACE_RANGE_TS_HI = WAL_TXN_REPLACE_RANGE_TS_LOW + 1;
    private static final int WAL_TXN_MAT_VIEW_PERIOD_HI = WAL_TXN_REPLACE_RANGE_TS_HI + 1;
    public static final int TXN_METADATA_LONGS_SIZE = WAL_TXN_MAT_VIEW_PERIOD_HI + 1;
    private static final int SYMBOL_MAP_COLUMN_RECORD_HEADER_INTS = 6;
    private static final int SYMBOL_MAP_RECORD_HEADER_INTS = 4;
    private final CairoConfiguration config;
    private final long maxLookaheadRows;
    private final SymbolMapDiffCursorImpl symbolMapDiffCursor = new SymbolMapDiffCursorImpl();
    private final LongList transactionMeta = new LongList();
    private final WalEventReader walEventReader;
    WalTxnDetailsSlice txnSlice = new WalTxnDetailsSlice();
    private long currentSymbolIndexesStartOffset = 0;
    private long currentSymbolStringMemStartOffset = 0;
    private long startSeqTxn = 0;
    // Stores all symbol metadata for the stored transactions.
    // The format is 4 int header / SYMBOL_MAP_RECORD_HEADER_INTS
    // 4 bytes - record length
    // 4 bytes - symbol map count
    // 4 byte - low 32bits of seqTxn
    // 4 byte - high 32bits of seqTxn
    // Then for every symbol column 6 int record / SYMBOL_MAP_COLUMN_RECORD_HEADER_INTS
    // 4 bytes - map records count
    // 4 bytes - symbol column index
    // 4 bytes - null flag
    // 4 bytes - clean symbol count
    // 4 bytes - low 32bits of offset into stored symbol strings in symbolStringsMem
    // 4 bytes - high 32bits of offset into stored symbol strings in symbolStringsMem
    private DirectIntList symbolIndexes = new DirectIntList(4, MemoryTag.NATIVE_TABLE_WRITER);
    // Stores all symbol strings for the stored transactions. The format is STRING format (4 bytes length + string in chars)
    private MemoryCARW symbolStringsMem = null;
    private long totalRowsLoadedToApply = 0;
    private DirectLongList txnOrder = new DirectLongList(10 * 4L, MemoryTag.NATIVE_TABLE_WRITER);

    public WalTxnDetails(FilesFacade ff, CairoConfiguration configuration, long maxLookaheadRows) {
        walEventReader = new WalEventReader(ff);
        this.config = configuration;
        this.maxLookaheadRows = maxLookaheadRows;
    }

    public static int loadTxns(TransactionLogCursor transactionLogCursor, int txnCount, DirectLongList txnList) {
        txnList.setCapacity(txnCount * 4L);

        // Load the map of outstanding WAL transactions to load necessary details from WAL-E files efficiently.
        long max = Long.MIN_VALUE, min = Long.MAX_VALUE;
        int txn = 0;
        for (; txn < txnCount && transactionLogCursor.hasNext(); txn++) {
            long long1 = Numbers.encodeLowHighInts(transactionLogCursor.getSegmentId(), transactionLogCursor.getWalId() - MIN_WAL_ID);
            max = Math.max(max, long1);
            min = Math.min(min, long1);
            txnList.add(long1);
            txnList.add(Numbers.encodeLowHighInts(transactionLogCursor.getSegmentTxn(), txn));
        }
        assert txn > 0;
        Vect.radixSortLongIndexAscChecked(
                txnList.getAddress(),
                txn,
                txnList.getAddress() + txn * 2L * Long.BYTES,
                min,
                max
        );
        return txn;
    }

    public static WalEventCursor openWalEFile(Path tempPath, WalEventReader eventReader, int segmentTxn, long seqTxn) {
        WalEventCursor walEventCursor;
        try {
            walEventCursor = eventReader.of(tempPath, segmentTxn);
        } catch (CairoException ex) {
            throw CairoException.critical(ex.getErrno()).put("cannot read WAL event file for seqTxn=").put(seqTxn)
                    .put(", ").put(ex.getFlyweightMessage()).put(']');
        }
        return walEventCursor;
    }

    /**
     * Creates symbol map for multiple transactions. It is used in {@link io.questdb.cairo.TableWriter} during
     * WAL transaction block application
     *
     * @param transactions - transactions to build symbol map for
     * @param columnIndex  - symbol column index
     * @param mapWriter    - map writer to write symbols to
     * @param outMem       - memory to store symbol map
     * @return true if symbol map is created, false if the symbol map is identical, no remapping needed
     */
    public boolean buildTxnSymbolMap(TableWriterSegmentCopyInfo transactions, int columnIndex, MapWriter mapWriter, MemoryCARW outMem) {
        // Output format, for every transaction
        // Header, 2 ints per txn,
        // - clear symbol count
        // - offset of the map start in the output buffer
        // Map format
        // 4 bytes with the final symbol key for every symbol key in the WAL column shifted by clear symbol count

        // Clear symbol count is the number of symbol keys that do not to be re-mapped since they were taken
        // directly from the table symbol map during WAL writing.
        long txnCount = transactions.getTxnCount();
        int headerInts = 2;
        outMem.jumpTo(txnCount * headerInts * Integer.BYTES);
        DirectString directSymbolString = DIRECT_STRING.get();

        long startTxn = transactions.getStartTxn();
        // Add symbols in the order of commits to make the int symbol values independent of WAL apply method
        for (long seqTxn = startTxn, n = startTxn + txnCount; seqTxn < n; seqTxn++) {
            long txnSymbolOffset = getWalSymbolDiffCursorOffset(seqTxn) - currentSymbolIndexesStartOffset;

            boolean identical = true;
            long mapAppendOffset = outMem.getAppendOffset();
            int cleanSymbolCount = 0;

            if (txnSymbolOffset > -1) {
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

        // Return true if there are any symbol maps created, e.g. mapping is not identical transformation
        return outMem.getAppendOffset() > (txnCount << 3);
    }

    /**
     * Calculates the count of transactions to apply in one go in {@link io.questdb.cairo.TableWriter}.
     * Applying many transactions is also referenced as wrting a block of transactions.
     *
     * @param seqTxn              - seqTxn to start the block from
     * @param pressureControl     - pressure control to calculate the block size, pressure control can limit the block size
     * @param maxBlockRecordCount - maximum number of transactions in the block
     * @return - the number of transactions to apply in one go, e.g. the block size
     */
    public int calculateInsertTransactionBlock(long seqTxn, TableWriterPressureControl pressureControl, long maxBlockRecordCount) {
        int blockSize = 1;
        long lastSeqTxn = getLastSeqTxn();
        long totalRowCount = getSegmentRowHi(seqTxn) - getSegmentRowLo(seqTxn);
        maxBlockRecordCount = Math.min(maxBlockRecordCount, pressureControl.getMaxBlockRowCount() - 1);

        long lastWalSegment = getWalSegment(seqTxn);
        boolean allInOrder = getTxnInOrder(seqTxn);
        long minTs = Long.MAX_VALUE;
        long maxTs = Long.MIN_VALUE;

        if (getCommitToTimestamp(seqTxn) != FORCE_FULL_COMMIT && getDedupMode(seqTxn) != WAL_DEDUP_MODE_REPLACE_RANGE) {
            for (long nextTxn = seqTxn + 1; nextTxn <= lastSeqTxn; nextTxn++) {
                long currentWalSegment = getWalSegment(nextTxn);
                if (allInOrder) {
                    if (currentWalSegment != lastWalSegment) {
                        if (totalRowCount >= maxBlockRecordCount / 10) {
                            // Big enough chunk of all in order data in same segment
                            break;
                        }
                        allInOrder = false;
                    } else {
                        allInOrder = getTxnInOrder(nextTxn) && maxTs <= getMinTimestamp(nextTxn);
                        minTs = Math.min(minTs, getMinTimestamp(nextTxn));
                        maxTs = Math.max(maxTs, getMaxTimestamp(nextTxn));
                    }
                }
                totalRowCount += getSegmentRowHi(nextTxn) - getSegmentRowLo(nextTxn);
                lastWalSegment = currentWalSegment;

                if (getCommitToTimestamp(nextTxn) == FORCE_FULL_COMMIT || totalRowCount > maxBlockRecordCount) {
                    break;
                }
                if (getDedupMode(nextTxn) == WAL_DEDUP_MODE_REPLACE_RANGE) {
                    // If there is a deduplication mode, we need to commit the transaction one by one
                    break;
                }
                blockSize++;
            }
        }

        // Find reasonable block size that will not cause O3
        // Here we are trying to find the block size that is in the row count range of [maxBlockRecordCount / 2; maxBlockRecordCount]
        // And the commit to timestamp includes the last transaction in the block
        // This is very basic heuristic and needs some read time testing to come with a more robust solution
        long lastTxn = seqTxn + blockSize - 1;
        if (blockSize > 1 && getCommitToTimestamp(lastTxn) != FORCE_FULL_COMMIT) {
            while (blockSize > 1 && getCommitToTimestamp(lastTxn) < getMaxTimestamp(lastTxn) && totalRowCount > maxBlockRecordCount / 2) {
                blockSize--;
                lastTxn--;
                totalRowCount -= getSegmentRowHi(lastTxn) - getSegmentRowLo(lastTxn);
            }
        }

        // to force switch to 1 by 1 txn commit, uncomment the following line
        // return 1;
        pressureControl.updateInflightTxnBlockLength(blockSize, totalRowCount);
        return blockSize;
    }

    @Override
    public void close() {
        symbolIndexes = Misc.free(symbolIndexes);
        symbolStringsMem = Misc.free(symbolStringsMem);
        txnOrder = Misc.free(txnOrder);
    }

    /**
     * Calculate a heuristic timestamp to make the data visible to the readers. The purpose is to avoid future
     * O3 commits and partition rewrites.
     *
     * @param seqTxn - seqTxn to calculate the commit to timestamp for
     * @return - the commit to timestamp
     */
    public long getCommitToTimestamp(long seqTxn) {
        long value = transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE) + COMMIT_TO_TIMESTAMP_OFFSET);
        return value == LAST_ROW_COMMIT ? FORCE_FULL_COMMIT : value;
    }

    public byte getDedupMode(long seqTxn) {
        int flags = Numbers.decodeLowInt(transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_ROW_IN_ORDER_DATA_TYPE)));
        return (byte) (flags >> 24);
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

    public long getMatViewPeriodHi(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE) + WAL_TXN_MAT_VIEW_PERIOD_HI);
    }

    public long getMatViewRefreshTimestamp(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE) + WAL_TXN_MAT_VIEW_REFRESH_TS);
    }

    public long getMatViewRefreshTxn(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE) + WAL_TXN_MAT_VIEW_REFRESH_TXN);
    }

    public long getMaxTimestamp(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE) + WAL_TXN_MAX_TIMESTAMP_OFFSET);
    }

    public long getMinTimestamp(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE) + WAL_TXN_MIN_TIMESTAMP_OFFSET);
    }

    public long getReplaceRangeTsHi(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE) + WAL_TXN_REPLACE_RANGE_TS_HI);
    }

    public long getReplaceRangeTsLow(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE) + WAL_TXN_REPLACE_RANGE_TS_LOW);
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

    public boolean getTxnInOrder(long seqTxn) {
        int isOutOfOrder = Numbers.decodeLowInt(transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_ROW_IN_ORDER_DATA_TYPE)));
        return (isOutOfOrder & FLAG_IS_OOO) == 0;
    }

    public int getWalId(long seqTxn) {
        return Numbers.decodeHighInt(transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_ID_WAL_SEG_ID_OFFSET)));
    }

    public long getWalSegment(long seqTxn) {
        return transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_ID_WAL_SEG_ID_OFFSET));
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

    public boolean isLastSegmentUsage(long seqTxn) {
        int isOutOfOrder = Numbers.decodeLowInt(transactionMeta.get((int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_ROW_IN_ORDER_DATA_TYPE)));
        return (isOutOfOrder & FLAG_IS_LAST_SEGMENT_USAGE) != 0;
    }

    public void prepareCopySegments(long startSeqTxn, int blockTransactionCount, TableWriterSegmentCopyInfo copyTasks, boolean hasSymbols) {
        copyTasks.initBlock(startSeqTxn, blockTransactionCount, hasSymbols);
        var sortedBySegmentTxnSlice = sortSliceByWalAndSegment(startSeqTxn, blockTransactionCount);
        int lastSegmentId = -1;
        int lastWalId = -1;
        long segmentLo = -1;

        int copyTaskCount = 0;
        boolean isLastSegmentUse = false;
        long roHi = 0;
        long prevRoHi = -1;
        boolean allInOrder = true;

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
                    prevRoHi = -1;
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
            allInOrder = allInOrder && minTimestamp >= copyTasks.getMaxTimestamp() && sortedBySegmentTxnSlice.isTxnDataInOrder(i);

            if (prevRoHi != -1 && prevRoHi != roLo) {
                // In theory it's possible but in practice it should not happen
                // This means that some of the segment rows are not committed
                // If this happens some optimisations will not be possible
                // and the commit should fall back to txn by txn commit
                copyTasks.setSegmentGap(true);
            }
            prevRoHi = roHi;
        }

        int lastIndex = blockTransactionCount - 1;
        int segmentId = sortedBySegmentTxnSlice.getSegmentId(lastIndex);
        int walId = sortedBySegmentTxnSlice.getWalId(lastIndex);

        copyTasks.addSegment(walId, segmentId, segmentLo, roHi, isLastSegmentUse);
        copyTasks.setAllTxnDataInOrder(allInOrder);
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

        if (lastLoadedSeqTxn >= loadFromSeqTxn && startSeqTxn <= loadFromSeqTxn) {
            int shift = (int) (loadFromSeqTxn - startSeqTxn);
            long newSymbolsOffset = getMinSymbolIndexOffsetFromTxn(loadFromSeqTxn);
            assert newSymbolsOffset >= currentSymbolIndexesStartOffset;

            if (newSymbolsOffset > currentSymbolIndexesStartOffset) {
                if (symbolStringsMem != null && symbolStringsMem.getAppendOffset() > 0) {
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
            totalRowsLoadedToApply = 0;
        }

        int maxLookaheadTxn = config.getWalApplyLookAheadTransactionCount();
        int txnLoadCount = maxLookaheadTxn;
        long rowsToLoad;

        int initialSize = transactionMeta.size();
        do {
            long rowsLoaded = loadTransactionDetails(tempPath, transactionLogCursor, loadFromSeqTxn, rootLen, txnLoadCount);
            totalRowsLoadedToApply += rowsLoaded;
            loadFromSeqTxn = getLastSeqTxn() + 1;

            rowsToLoad = maxLookaheadRows - totalRowsLoadedToApply;
            if (totalRowsLoadedToApply > 0 && rowsToLoad > 0) {
                // Estimate how many transactions to load to reach maxLookaheadRows
                long loadedTxn = transactionMeta.size() / TXN_METADATA_LONGS_SIZE;
                txnLoadCount = (int) Math.max(
                        rowsToLoad * loadedTxn / totalRowsLoadedToApply,
                        maxLookaheadTxn
                );
            }
        } while (rowsToLoad > 0 && getLastSeqTxn() < transactionLogCursor.getMaxTxn());

        if (transactionMeta.size() == initialSize) {
            // No transactions loaded, no need to do anything
            return;
        }

        // set commit to timestamp moving backwards
        long runningMinTimestamp = LAST_ROW_COMMIT;
        for (int i = transactionMeta.size() - TXN_METADATA_LONGS_SIZE; i > -1; i -= TXN_METADATA_LONGS_SIZE) {

            long commitToTimestamp = runningMinTimestamp;
            long currentMinTimestamp = transactionMeta.getQuick(i + WAL_TXN_MIN_TIMESTAMP_OFFSET);
            runningMinTimestamp = Math.min(runningMinTimestamp, currentMinTimestamp);

            if (transactionMeta.get(i + COMMIT_TO_TIMESTAMP_OFFSET) != FORCE_FULL_COMMIT) {
                transactionMeta.set(i + COMMIT_TO_TIMESTAMP_OFFSET, commitToTimestamp);
            } else {
                // Force full commit before this record
                // when this is newly loaded record with FORCE_FULL_COMMIT value
                if (i >= initialSize) {
                    runningMinTimestamp = FORCE_FULL_COMMIT;
                }
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

    public void setIncrementRowsCommitted(long rowsCommitted) {
        // This tracks how many transactions rows are loaded from sequencer and WAL-e files
        // to load incrementally just enough to go forward
        totalRowsLoadedToApply -= rowsCommitted;
        assert totalRowsLoadedToApply >= 0;
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

    private long loadTransactionDetails(Path tempPath, TransactionLogCursor transactionLogCursor, long loadFromSeqTxn, int rootLen, int maxLoadTxnCount) {
        transactionLogCursor.setPosition(loadFromSeqTxn - 1);
        long totalRowsLoaded = 0;

        try (WalEventReader eventReader = walEventReader) {
            WalEventCursor walEventCursor = null;

            txnOrder.clear();
            int txnCount = (int) Math.min(maxLoadTxnCount, transactionLogCursor.getMaxTxn() - loadFromSeqTxn + 1);
            if (txnCount > 0) {
                txnCount = loadTxns(transactionLogCursor, txnCount, txnOrder);

                int lastWalId = -1;
                int lastSegmentId = -1;
                int lastSegmentTxn = -2;

                int incrementalLoadStartIndex = transactionMeta.size();

                transactionMeta.setPos(incrementalLoadStartIndex + txnCount * TXN_METADATA_LONGS_SIZE);

                for (int i = 0; i < txnCount; i++) {
                    long long1 = txnOrder.get(2L * i);
                    long long2 = txnOrder.get(2L * i + 1);

                    long seqTxn = Numbers.decodeHighInt(long2) + loadFromSeqTxn;
                    int walId = Numbers.decodeHighInt(long1) + MIN_WAL_ID;
                    int segmentId = Numbers.decodeLowInt(long1);
                    int segmentTxn = Numbers.decodeLowInt(long2);
                    int txnMetaOffset = (int) ((seqTxn - startSeqTxn) * TXN_METADATA_LONGS_SIZE);

                    final byte walTxnType;
                    if (walId > 0) {
                        // Switch to the WAL-E file or scroll to the transaction
                        if (lastWalId == walId && segmentId == lastSegmentId) {
                            assert segmentTxn > lastSegmentTxn;
                            while (lastSegmentTxn < segmentTxn && walEventCursor.hasNext()) {
                                // Skip uncommitted transactions
                                lastSegmentTxn++;
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
                        if (WalTxnType.isDataType(walTxnType)) {
                            WalEventCursor.DataInfo commitInfo = walEventCursor.getDataInfo();
                            transactionMeta.set(txnMetaOffset + SEQ_TXN_OFFSET, seqTxn);
                            transactionMeta.set(txnMetaOffset + COMMIT_TO_TIMESTAMP_OFFSET, -1); // commit to timestamp
                            transactionMeta.set(txnMetaOffset + WAL_TXN_ID_WAL_SEG_ID_OFFSET, Numbers.encodeLowHighInts(segmentId, walId));
                            transactionMeta.set(txnMetaOffset + WAL_TXN_MIN_TIMESTAMP_OFFSET, commitInfo.getMinTimestamp());
                            transactionMeta.set(txnMetaOffset + WAL_TXN_MAX_TIMESTAMP_OFFSET, commitInfo.getMaxTimestamp());
                            transactionMeta.set(txnMetaOffset + WAL_TXN_ROW_LO_OFFSET, commitInfo.getStartRowID());
                            transactionMeta.set(txnMetaOffset + WAL_TXN_ROW_HI_OFFSET, commitInfo.getEndRowID());
                            totalRowsLoaded += commitInfo.getEndRowID() - commitInfo.getStartRowID();
                            int flags = commitInfo.isOutOfOrder() ? FLAG_IS_OOO : 0x0;
                            // The records are sorted by WAL ID, segment ID.
                            // If the next record is not from the same segment it means it's the last txn from the segment.
                            if (i + 1 < txnCount) {
                                int nextWalId = Numbers.decodeHighInt(txnOrder.get(2L * (i + 1))) + MIN_WAL_ID;
                                int nextSegmentId = Numbers.decodeLowInt(txnOrder.get(2L * (i + 1)));
                                if (nextSegmentId != segmentId || nextWalId != walId) {
                                    flags |= FLAG_IS_LAST_SEGMENT_USAGE;
                                }
                            } else {
                                flags |= FLAG_IS_LAST_SEGMENT_USAGE;
                            }
                            flags |= (commitInfo.getDedupMode() << 24);
                            transactionMeta.set(txnMetaOffset + WAL_TXN_ROW_IN_ORDER_DATA_TYPE, Numbers.encodeLowHighInts(flags, walTxnType));
                            transactionMeta.set(txnMetaOffset + WAL_TXN_SYMBOL_DIFF_OFFSET, saveSymbols(commitInfo, seqTxn));
                            if (walTxnType == WalTxnType.MAT_VIEW_DATA) {
                                WalEventCursor.MatViewDataInfo matViewDataInfo = walEventCursor.getMatViewDataInfo();
                                transactionMeta.set(txnMetaOffset + WAL_TXN_MAT_VIEW_REFRESH_TXN, matViewDataInfo.getLastRefreshBaseTableTxn());
                                transactionMeta.set(txnMetaOffset + WAL_TXN_MAT_VIEW_REFRESH_TS, matViewDataInfo.getLastRefreshTimestampUs());
                                transactionMeta.set(txnMetaOffset + WAL_TXN_MAT_VIEW_PERIOD_HI, matViewDataInfo.getLastPeriodHi());
                            } else {
                                transactionMeta.set(txnMetaOffset + WAL_TXN_MAT_VIEW_REFRESH_TXN, -1);
                                transactionMeta.set(txnMetaOffset + WAL_TXN_MAT_VIEW_REFRESH_TS, -1);
                                transactionMeta.set(txnMetaOffset + WAL_TXN_MAT_VIEW_PERIOD_HI, -1);
                            }
                            transactionMeta.set(txnMetaOffset + WAL_TXN_REPLACE_RANGE_TS_LOW, commitInfo.getReplaceRangeTsLow());
                            transactionMeta.set(txnMetaOffset + WAL_TXN_REPLACE_RANGE_TS_HI, commitInfo.getReplaceRangeTsHi());
                            if (commitInfo.getDedupMode() != WAL_DEDUP_MODE_DEFAULT) {
                                // If it is a replace range commit, we need to commit everything and not store it in the lag.
                                transactionMeta.set(txnMetaOffset + COMMIT_TO_TIMESTAMP_OFFSET, FORCE_FULL_COMMIT);
                            }
                            continue;
                        }
                    } else {
                        walTxnType = NONE;
                    }
                    // If there is ALTER or UPDATE, we have to flush everything without keeping anything in the lag.
                    transactionMeta.set(txnMetaOffset + SEQ_TXN_OFFSET, seqTxn);
                    transactionMeta.set(txnMetaOffset + COMMIT_TO_TIMESTAMP_OFFSET, FORCE_FULL_COMMIT); // commit to timestamp
                    transactionMeta.set(txnMetaOffset + WAL_TXN_ID_WAL_SEG_ID_OFFSET, Numbers.encodeLowHighInts(segmentId, walId));
                    transactionMeta.set(txnMetaOffset + WAL_TXN_MIN_TIMESTAMP_OFFSET, -1); // min timestamp
                    transactionMeta.set(txnMetaOffset + WAL_TXN_MAX_TIMESTAMP_OFFSET, -1); // max timestamp
                    transactionMeta.set(txnMetaOffset + WAL_TXN_ROW_LO_OFFSET, -1); // start row id
                    transactionMeta.set(txnMetaOffset + WAL_TXN_ROW_HI_OFFSET, -1); // end row id
                    transactionMeta.set(txnMetaOffset + WAL_TXN_ROW_IN_ORDER_DATA_TYPE, Numbers.encodeLowHighInts(0, walTxnType));
                    transactionMeta.set(txnMetaOffset + WAL_TXN_SYMBOL_DIFF_OFFSET, -1); // symbols diff offset
                    transactionMeta.set(txnMetaOffset + WAL_TXN_MAT_VIEW_REFRESH_TXN, -1); // mat view refresh txn
                    transactionMeta.set(txnMetaOffset + WAL_TXN_MAT_VIEW_REFRESH_TS, -1); // mat view refresh timestamp
                    transactionMeta.set(txnMetaOffset + WAL_TXN_REPLACE_RANGE_TS_LOW, -1); // replace range low boundary
                    transactionMeta.set(txnMetaOffset + WAL_TXN_REPLACE_RANGE_TS_HI, -1); // replace range high boundary
                    transactionMeta.set(txnMetaOffset + WAL_TXN_MAT_VIEW_PERIOD_HI, -1); // mat view last period high boundary
                }
            }
        } finally {
            tempPath.trimTo(rootLen);
            txnOrder.resetCapacity();
        }
        return totalRowsLoaded;
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
        private int offsetIndex;
        private long recordIndexHi;
        private int symbolIndex;
        private int symbolsCount;

        @Override
        public SymbolMapDiff nextSymbolMapDiff() {
            if (symbolIndex++ < symbolsCount) {
                assert offsetIndex <= recordIndexHi;
                symbolMapDiff.switchToColumnRecord(offsetIndex);
                offsetIndex += SYMBOL_MAP_COLUMN_RECORD_HEADER_INTS;
                return symbolMapDiff;
            }
            return null;
        }

        public void of(int startOffset) {
            if (startOffset > -1) {
                offsetIndex = startOffset;
                int recordLength = WalTxnDetails.this.symbolIndexes.get(startOffset);
                assert recordLength >= SYMBOL_MAP_RECORD_HEADER_INTS;
                recordIndexHi = this.offsetIndex + recordLength;
                symbolsCount = symbolIndexes.get(startOffset + 1);
                assert this.symbolsCount > -1;
                offsetIndex += SYMBOL_MAP_RECORD_HEADER_INTS;
                symbolIndex = 0;
            } else {
                symbolsCount = 0;
            }
        }

        private class SymbolMapDiffColumnRecord implements SymbolMapDiff, SymbolMapDiffEntry {
            private int cleanSymbolCount;
            private int columnIndex;
            private boolean containsNull;
            private long currentSymbolMemOffset;
            private int savedSymbolRecordCount;
            private int symbolIndex = 0;

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
                return symbolIndex + cleanSymbolCount - 1;
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
                if (symbolIndex < savedSymbolRecordCount) {
                    if (symbolIndex > 0) {
                        currentSymbolMemOffset += Vm.getStorageLength(symbolStringsMem.getInt(currentSymbolMemOffset));
                    }
                    symbolIndex++;
                    return this;
                } else {
                    return null;
                }
            }

            public void switchToColumnRecord(int lo) {
                savedSymbolRecordCount = symbolIndexes.get(lo++);
                columnIndex = symbolIndexes.get(lo++);
                containsNull = symbolIndexes.get(lo++) > 0;
                cleanSymbolCount = symbolIndexes.get(lo++);
                int nextSymbolMemOffsetIndexLo = symbolIndexes.get(lo++);
                int nextSymbolMemOffsetIndexHi = symbolIndexes.get(lo);

                currentSymbolMemOffset =
                        Numbers.encodeLowHighInts(nextSymbolMemOffsetIndexLo, nextSymbolMemOffsetIndexHi)
                                - WalTxnDetails.this.currentSymbolStringMemStartOffset;
                symbolIndex = 0;
            }
        }
    }

    public class WalTxnDetailsSlice {

        public long getMaxTimestamp(int txn) {
            return WalTxnDetails.this.getMaxTimestamp(getSeqTxn(txn));
        }

        public long getMinTimestamp(int txn) {
            return WalTxnDetails.this.getMinTimestamp(getSeqTxn(txn));
        }

        public long getRoHi(int txn) {
            return WalTxnDetails.this.getSegmentRowHi(getSeqTxn(txn));
        }

        public long getRoLo(int txn) {
            return WalTxnDetails.this.getSegmentRowLo(getSeqTxn(txn));
        }

        public int getSegmentId(int txn) {
            return Numbers.decodeLowInt(txnOrder.get(2L * txn));
        }

        public long getSeqTxn(int txn) {
            return txnOrder.get(2L * txn + 1);
        }

        public int getWalId(int txn) {
            return Numbers.decodeHighInt(txnOrder.get(2L * txn));
        }

        public boolean isLastSegmentUse(int txn) {
            return WalTxnDetails.this.isLastSegmentUsage(getSeqTxn(txn));
        }

        public boolean isTxnDataInOrder(int txn) {
            return WalTxnDetails.this.getTxnInOrder(getSeqTxn(txn));
        }

        public WalTxnDetailsSlice of(long lo, int count) {
            txnOrder.clear();
            // Reserve double capacity to use with radix sort
            txnOrder.setCapacity(count * 4L);

            long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
            for (long i = lo, n = lo + count; i < n; i++) {
                long segWalId = transactionMeta.get((int) ((i - startSeqTxn) * TXN_METADATA_LONGS_SIZE + WAL_TXN_ID_WAL_SEG_ID_OFFSET));
                min = Math.min(min, segWalId);
                max = Math.max(max, segWalId);
                txnOrder.add(segWalId);
                txnOrder.add(i);
            }

            Vect.radixSortLongIndexAscChecked(
                    txnOrder.getAddress(),
                    count,
                    txnOrder.getAddress() + count * 2L * Long.BYTES,
                    min,
                    max
            );

            return this;
        }
    }
}
