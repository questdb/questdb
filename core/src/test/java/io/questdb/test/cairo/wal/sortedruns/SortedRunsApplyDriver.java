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

package io.questdb.test.cairo.wal.sortedruns;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.cairo.wal.sortedruns.SortedRunsFormat;
import io.questdb.cairo.wal.sortedruns.SortedRunsIndex;
import io.questdb.cairo.wal.sortedruns.SortedRunsReader;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Test-only WAL apply driver that writes data in the new
 * indexed-sorted-runs storage format. Replaces {@code ApplyWal2TableJob}
 * for tests that need sorted-runs partitions without depending on the
 * production write PR. Each pending WAL transaction becomes one sorted
 * run appended to the destination partition's column tails, plus one
 * CommitStat record appended to the partition's {@code _sortedruns}
 * sidecar.
 * <p>
 * Scope limits in this initial cut:
 * <ul>
 *   <li>Primitive fixed-size columns only. Throws clearly if the table
 *       has SYMBOL or variable-length columns.</li>
 *   <li>Single-partition transactions only. Throws if a transaction spans
 *       multiple partitions.</li>
 *   <li>No dedup. Every row in every commit is appended; dedup is a read
 *       and compaction concern in this storage scheme.</li>
 * </ul>
 */
public final class SortedRunsApplyDriver implements QuietCloseable {
    private static final int SORT_ENTRY_BYTES = 16;
    private final MemoryCMARW commitsMem = Vm.getCMARWInstance();
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final MemoryCMARW idxMem = Vm.getCMARWInstance();
    private final MemoryCMARW partitionColMem = Vm.getCMARWInstance();
    private final Path partitionPath = new Path();
    private final Path walSegmentPath = new Path();
    private long sortScratchAddr;
    private long sortScratchBytes;

    public SortedRunsApplyDriver(CairoEngine engine) {
        this.engine = engine;
        this.ff = engine.getConfiguration().getFilesFacade();
    }

    public void apply(TableToken token) {
        try (TableWriter writer = engine.getWriter(token, "sorted-runs-apply")) {
            applyAll(writer);
        }
    }

    @Override
    public void close() {
        if (sortScratchAddr != 0) {
            Unsafe.free(sortScratchAddr, sortScratchBytes, MemoryTag.NATIVE_DEFAULT);
            sortScratchAddr = 0;
            sortScratchBytes = 0;
        }
        Misc.free(commitsMem);
        Misc.free(idxMem);
        Misc.free(partitionColMem);
        Misc.free(walSegmentPath);
        Misc.free(partitionPath);
    }

    private void appendCommitStat(
            long physRowStart,
            int rowCount,
            int flags,
            long minTs,
            long maxTs,
            long commitsFileSizeBefore
    ) {
        // partitionPath must point at the partition directory when this is called.
        final int partitionPathLen = partitionPath.size();
        partitionPath.concat(SortedRunsFormat.FILE_NAME);
        try {
            final LPSZ commitsFile = partitionPath.$();
            commitsMem.of(
                    ff,
                    commitsFile,
                    ff.getPageSize(),
                    -1L,
                    MemoryTag.MMAP_DEFAULT,
                    0
            );
            if (commitsFileSizeBefore == 0) {
                commitsMem.jumpTo(0);
                commitsMem.putInt(SortedRunsFormat.MAGIC);
                commitsMem.putShort((short) SortedRunsFormat.VERSION_1);
                commitsMem.putShort((short) 0);
                // Pad to header size (skip 56 more bytes).
                for (int i = 0; i < SortedRunsFormat.HEADER_SIZE_BYTES - 8; i++) {
                    commitsMem.putByte((byte) 0);
                }
            } else {
                commitsMem.jumpTo(commitsFileSizeBefore);
            }
            commitsMem.putLong(physRowStart);
            commitsMem.putInt(rowCount);
            commitsMem.putShort((short) flags);
            commitsMem.putShort((short) 0); // reserved
            commitsMem.putLong(minTs);
            commitsMem.putLong(maxTs);
            commitsMem.putInt(0);           // extLength
            commitsMem.sync(false);
        } finally {
            partitionPath.trimTo(partitionPathLen);
        }
    }

    private void appendSortedRunToPartition(
            TableWriter writer,
            long partitionRowCountBefore,
            long segmentRowLo,
            int rowCount
    ) {
        final TableRecordMetadata md = writer.getMetadata();
        final int columnCount = md.getColumnCount();
        final int timestampIndex = md.getTimestampIndex();
        final int walSegPathLen = walSegmentPath.size();
        final int partitionPathLen = partitionPath.size();
        final long sortBase = sortScratchAddr;

        for (int colIdx = 0; colIdx < columnCount; colIdx++) {
            final int colType = md.getColumnType(colIdx);
            if (colType < 0) {
                continue;
            }
            final CharSequence colName = md.getColumnName(colIdx);
            final int elementSize = ColumnType.sizeOf(colType);
            if (elementSize <= 0) {
                throw CairoException.nonCritical()
                        .put("SortedRunsApplyDriver: variable-length column not supported [col=")
                        .put(colName).put(", type=").put(ColumnType.nameOf(colType)).put(']');
            }
            if (ColumnType.tagOf(colType) == ColumnType.SYMBOL) {
                throw CairoException.nonCritical()
                        .put("SortedRunsApplyDriver: SYMBOL column not supported [col=").put(colName).put(']');
            }

            // The designated timestamp column is stored as 16-byte
            // (timestamp, segmentRowId) tuples in WAL segments (putLong128) but
            // as 8-byte timestamps in the partition. For all other columns the
            // WAL and partition strides match the element size.
            final boolean isDesignatedTs = (colIdx == timestampIndex);
            final int walStride = isDesignatedTs ? 16 : elementSize;

            walSegmentPath.trimTo(walSegPathLen);
            final LPSZ walColFile = TableUtils.dFile(walSegmentPath, colName);
            try (MemoryCMR walColMem = Vm.getCMRInstance()) {
                final long walExpectedSize = (segmentRowLo + rowCount) * (long) walStride;
                walColMem.of(ff, walColFile, 0, walExpectedSize, MemoryTag.MMAP_DEFAULT);

                partitionPath.trimTo(partitionPathLen);
                final LPSZ partColFile = TableUtils.dFile(partitionPath, colName);
                partitionColMem.of(
                        ff,
                        partColFile,
                        ff.getPageSize(),
                        -1L,
                        MemoryTag.MMAP_DEFAULT,
                        0
                );
                final long tailOffset = partitionRowCountBefore * (long) elementSize;
                partitionColMem.jumpTo(tailOffset);
                final long srcBase = walColMem.addressOf(0);
                for (int i = 0; i < rowCount; i++) {
                    final long segRowOffset = Unsafe.getUnsafe().getLong(sortBase + (long) i * SORT_ENTRY_BYTES + 8L);
                    final long srcAddr = srcBase + (segmentRowLo + segRowOffset) * (long) walStride;
                    copyFixedSize(partitionColMem, srcAddr, elementSize);
                }
                partitionColMem.sync(false);
            }
        }
        walSegmentPath.trimTo(walSegPathLen);
        partitionPath.trimTo(partitionPathLen);
    }

    private void applyAll(TableWriter writer) {
        validateColumns(writer.getMetadata());
        try (WalEventReader walEventReader = new WalEventReader(engine.getConfiguration());
             TransactionLogCursor txnCursor = engine.getTableSequencerAPI()
                     .getCursor(writer.getTableToken(), writer.getAppliedSeqTxn())) {
            while (txnCursor.hasNext()) {
                applyOne(writer, txnCursor, walEventReader);
            }
        }
        // TxWriter requires this call before commit so that transientRowCount and
        // fixedRowCount sync with the attachedPartitions contents. Without it
        // TxReader.unsafeLoadPartitions clobbers the last partition's size with
        // the stale (zero) transientRowCount on the next read.
        writer.getTxWriter().finishPartitionSizeUpdate();
        writer.commitSeqTxn();
    }

    private void applyOne(TableWriter writer, TransactionLogCursor txnCursor, WalEventReader walEventReader) {
        final int walId = txnCursor.getWalId();
        final int segmentId = txnCursor.getSegmentId();
        final long segmentTxn = txnCursor.getSegmentTxn();
        final long seqTxn = txnCursor.getTxn();

        if (walId <= 0) {
            writer.setSeqTxn(seqTxn);
            return;
        }

        final TableToken token = writer.getTableToken();
        walSegmentPath.of(engine.getConfiguration().getDbRoot())
                .concat(token)
                .slash()
                .putAscii(WalUtils.WAL_NAME_BASE).put(walId)
                .slash().put(segmentId);

        // WalEventReader.of() positions the cursor AT segmentTxn and reads its event;
        // the cursor's getType()/getDataInfo() are immediately valid (no hasNext() needed).
        final WalEventCursor walEventCursor = walEventReader.of(walSegmentPath, segmentTxn);
        if (!WalTxnType.isDataType(walEventCursor.getType())) {
            writer.setSeqTxn(seqTxn);
            return;
        }
        final WalEventCursor.DataInfo dataInfo = walEventCursor.getDataInfo();
        final long startRow = dataInfo.getStartRowID();
        final long endRow = dataInfo.getEndRowID();
        final long minTs = dataInfo.getMinTimestamp();
        final long maxTs = dataInfo.getMaxTimestamp();
        final int rowCount = (int) (endRow - startRow);
        if (rowCount <= 0) {
            writer.setSeqTxn(seqTxn);
            return;
        }

        final TableRecordMetadata md = writer.getMetadata();
        final int timestampIndex = md.getTimestampIndex();
        final int partitionBy = writer.getPartitionBy();
        final int timestampType = writer.getTimestampType();
        final TimestampDriver tsDriver = ColumnType.getTimestampDriver(timestampType);
        final TimestampDriver.TimestampFloorMethod floor = tsDriver.getPartitionFloorMethod(partitionBy);
        final long partitionTimestamp = floor.floor(minTs);
        if (floor.floor(maxTs) != partitionTimestamp) {
            throw CairoException.nonCritical()
                    .put("SortedRunsApplyDriver: transaction spans multiple partitions [minTs=")
                    .put(minTs).put(", maxTs=").put(maxTs).put(']');
        }

        // The designated timestamp column is written by WalWriter as 16-byte
        // (timestamp, segmentRowId) tuples via putLong128. The partition stores
        // only the 8-byte timestamp value. Read stride 16 from WAL, write stride
        // 8 to partition.
        final int walTsStride = 16;

        // Read timestamps from WAL segment and sort (ts, segRowOffset) tuples.
        final int walSegPathLen = walSegmentPath.size();
        final CharSequence tsColName = md.getColumnName(timestampIndex);
        ensureSortScratchCapacity(rowCount);
        final long sortBase = sortScratchAddr;
        try (MemoryCMR walTsMem = Vm.getCMRInstance()) {
            walSegmentPath.trimTo(walSegPathLen);
            final LPSZ walTsFile = TableUtils.dFile(walSegmentPath, tsColName);
            walTsMem.of(ff, walTsFile, 0, endRow * (long) walTsStride, MemoryTag.MMAP_DEFAULT);
            final long tsBase = walTsMem.addressOf(0);
            for (int i = 0; i < rowCount; i++) {
                final long ts = Unsafe.getUnsafe().getLong(tsBase + (startRow + i) * (long) walTsStride);
                Unsafe.getUnsafe().putLong(sortBase + (long) i * SORT_ENTRY_BYTES, ts);
                Unsafe.getUnsafe().putLong(sortBase + (long) i * SORT_ENTRY_BYTES + 8L, (long) i);
            }
            Vect.radixSortLongIndexAscInPlace(sortBase, rowCount, sortBase + (long) rowCount * SORT_ENTRY_BYTES);
        }
        walSegmentPath.trimTo(walSegPathLen);

        // Find or open destination partition.
        final TxWriter txWriter = writer.getTxWriter();
        final long existingRowCount = txWriter.getPartitionRowCountByTimestamp(partitionTimestamp);
        final long partitionRowCountBefore = existingRowCount >= 0 ? existingRowCount : 0L;
        final long nameTxn = existingRowCount >= 0
                ? txWriter.getPartitionNameTxn(findPartitionIndex(txWriter, partitionTimestamp))
                : txWriter.getTxn() - 1;

        partitionPath.of(engine.getConfiguration().getDbRoot()).concat(token);
        TableUtils.setPathForNativePartition(partitionPath, timestampType, partitionBy, partitionTimestamp, nameTxn);
        if (!ff.exists(partitionPath.$())) {
            if (ff.mkdirs(partitionPath.slash(), engine.getConfiguration().getMkDirMode()) != 0) {
                throw CairoException.critical(ff.errno()).put("could not mkdir partition path [path=")
                        .put(partitionPath).put(']');
            }
        }

        // Copy sorted-run rows into partition column files.
        appendSortedRunToPartition(writer, partitionRowCountBefore, startRow, rowCount);

        // Determine current sorted-runs file size.
        long commitsFileSizeBefore = 0L;
        if (existingRowCount >= 0) {
            final int partitionIndex = findPartitionIndex(txWriter, partitionTimestamp);
            if (txWriter.isPartitionSortedRuns(partitionIndex)) {
                commitsFileSizeBefore = txWriter.getPartitionSortedRunsFileSize(partitionIndex);
            }
        }

        // Append CommitStat.
        appendCommitStat(
                partitionRowCountBefore,
                rowCount,
                1 << SortedRunsFormat.FLAG_SORTED_BY_TS,
                minTs,
                maxTs,
                commitsFileSizeBefore
        );
        final long commitsFileSizeAfter = (commitsFileSizeBefore == 0
                ? SortedRunsFormat.HEADER_SIZE_BYTES
                : commitsFileSizeBefore) + SortedRunsFormat.RECORD_HEADER_SIZE_BYTES;

        // Update _txn.
        final long newPartitionRowCount = partitionRowCountBefore + rowCount;
        txWriter.updatePartitionSizeByTimestamp(partitionTimestamp, newPartitionRowCount, nameTxn);
        txWriter.setPartitionSortedRunsFileSize(partitionTimestamp, commitsFileSizeAfter);
        if (txWriter.getMinTimestamp() == Long.MAX_VALUE || minTs < txWriter.getMinTimestamp()) {
            txWriter.setMinTimestamp(minTs);
        }
        if (maxTs > txWriter.getMaxTimestamp()) {
            txWriter.updateMaxTimestamp(maxTs);
        }

        // Rewrite _sortedruns.idx from scratch using K-way merge over the
        // partition's runs. The test driver does this on every commit; the
        // production write path will append+remerge incrementally.
        rewriteSortedRunsIndex(writer, md, timestampIndex, commitsFileSizeAfter, newPartitionRowCount);

        writer.setSeqTxn(seqTxn);
    }

    private void copyFixedSize(MemoryCMARW dst, long srcAddr, int byteCount) {
        switch (byteCount) {
            case 1:
                dst.putByte(Unsafe.getUnsafe().getByte(srcAddr));
                break;
            case 2:
                dst.putShort(Unsafe.getUnsafe().getShort(srcAddr));
                break;
            case 4:
                dst.putInt(Unsafe.getUnsafe().getInt(srcAddr));
                break;
            case 8:
                dst.putLong(Unsafe.getUnsafe().getLong(srcAddr));
                break;
            case 16:
                dst.putLong(Unsafe.getUnsafe().getLong(srcAddr));
                dst.putLong(Unsafe.getUnsafe().getLong(srcAddr + 8L));
                break;
            case 32:
                dst.putLong(Unsafe.getUnsafe().getLong(srcAddr));
                dst.putLong(Unsafe.getUnsafe().getLong(srcAddr + 8L));
                dst.putLong(Unsafe.getUnsafe().getLong(srcAddr + 16L));
                dst.putLong(Unsafe.getUnsafe().getLong(srcAddr + 24L));
                break;
            default:
                throw CairoException.nonCritical().put("SortedRunsApplyDriver: unsupported element size ").put(byteCount);
        }
    }

    private void ensureSortScratchCapacity(int rowCount) {
        // 16 bytes per (ts, idx) entry, plus another 16 bytes per entry for radix temp area = 32 bytes/row.
        final long required = (long) rowCount * (SORT_ENTRY_BYTES * 2L);
        if (required > sortScratchBytes) {
            long newCap = Math.max(required, 64L);
            if (sortScratchAddr != 0) {
                Unsafe.free(sortScratchAddr, sortScratchBytes, MemoryTag.NATIVE_DEFAULT);
            }
            sortScratchAddr = Unsafe.malloc(newCap, MemoryTag.NATIVE_DEFAULT);
            sortScratchBytes = newCap;
        }
    }

    /**
     * Rewrites the partition's {@code _sortedruns.idx} from scratch. After
     * the per-commit data file (ts column) and runs metadata file
     * ({@code _sortedruns}) have been written for this transaction, this
     * helper opens a {@link SortedRunsReader} over the runs metadata,
     * mmaps the partition's ts column, K-way merges over all runs once,
     * and writes the resulting partitionRowCount * 8 byte permutation
     * to {@code _sortedruns.idx}. Read-side cursors mmap this file and
     * dereference the address directly - no merge or scratch needed at
     * query time.
     */
    private void rewriteSortedRunsIndex(
            TableWriter writer,
            TableRecordMetadata md,
            int timestampIndex,
            long commitsFileSize,
            long partitionRowCount
    ) {
        // partitionPath points at the partition directory at this point.
        final int partitionPathLen = partitionPath.size();
        final CharSequence tsColName = md.getColumnName(timestampIndex);
        final long idxByteCount = partitionRowCount * Long.BYTES;
        final long scratchAddr = Unsafe.malloc(idxByteCount, MemoryTag.NATIVE_DEFAULT);
        try (
                MemoryCMR tsColRead = Vm.getCMRInstance();
                SortedRunsReader runsReader = new SortedRunsReader()
        ) {
            // mmap the partition's ts column (8 bytes per row in partition layout).
            partitionPath.trimTo(partitionPathLen);
            final LPSZ tsColFile = TableUtils.dFile(partitionPath, tsColName);
            tsColRead.of(ff, tsColFile, 0, partitionRowCount * Long.BYTES, MemoryTag.MMAP_DEFAULT);

            // Open SortedRunsReader on the runs metadata file.
            partitionPath.trimTo(partitionPathLen);
            partitionPath.concat(SortedRunsFormat.FILE_NAME);
            runsReader.of(ff, partitionPath.$(), commitsFileSize);
            partitionPath.trimTo(partitionPathLen);

            // K-way merge once into off-heap scratch.
            runsReader.materialiseSortedSlice(
                    0L,
                    partitionRowCount,
                    tsColRead.addressOf(0),
                    Long.BYTES,
                    scratchAddr
            );

            // Rewrite _sortedruns.idx from scratch. The file is opened fresh,
            // truncated, and re-grown via putLong so the on-disk size is
            // exactly partitionRowCount * 8.
            partitionPath.concat(SortedRunsIndex.FILE_NAME);
            final LPSZ idxFile = partitionPath.$();
            idxMem.of(ff, idxFile, ff.getPageSize(), -1L, MemoryTag.MMAP_DEFAULT, 0);
            idxMem.truncate();
            idxMem.jumpTo(0);
            for (long i = 0; i < partitionRowCount; i++) {
                idxMem.putLong(Unsafe.getUnsafe().getLong(scratchAddr + i * Long.BYTES));
            }
            idxMem.sync(false);
        } finally {
            Unsafe.free(scratchAddr, idxByteCount, MemoryTag.NATIVE_DEFAULT);
            partitionPath.trimTo(partitionPathLen);
        }
    }

    private int findPartitionIndex(TxWriter txWriter, long partitionTimestamp) {
        for (int i = 0, n = txWriter.getPartitionCount(); i < n; i++) {
            if (txWriter.getPartitionTimestampByIndex(i) == partitionTimestamp) {
                return i;
            }
        }
        return -1;
    }

    private void validateColumns(TableRecordMetadata md) {
        for (int i = 0, n = md.getColumnCount(); i < n; i++) {
            final int type = md.getColumnType(i);
            if (type < 0) continue;
            if (ColumnType.tagOf(type) == ColumnType.SYMBOL) {
                throw CairoException.nonCritical()
                        .put("SortedRunsApplyDriver: SYMBOL column not supported [col=")
                        .put(md.getColumnName(i)).put(']');
            }
            if (ColumnType.sizeOf(type) <= 0) {
                throw CairoException.nonCritical()
                        .put("SortedRunsApplyDriver: variable-length column not supported [col=")
                        .put(md.getColumnName(i))
                        .put(", type=").put(ColumnType.nameOf(type)).put(']');
            }
        }
    }
}
