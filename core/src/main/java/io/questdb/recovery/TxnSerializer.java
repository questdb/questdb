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

package io.questdb.recovery;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;

/**
 * Serializes a {@link TxnState} to the binary {@code _txn} file format.
 * Follows the same layout as {@link io.questdb.cliutil.TxSerializer} but
 * works directly from {@link TxnState} rather than a JSON intermediary.
 */
public final class TxnSerializer {

    private TxnSerializer() {
    }

    public static void write(TxnState state, LPSZ path, FilesFacade ff) {
        final int mapWriterCount = Math.max(0, state.getMapWriterCount());
        final int partitionCount = state.getPartitions().size();
        final int symbolsSize = mapWriterCount * Long.BYTES;
        final int partitionSegmentSize = partitionCount * TableUtils.LONGS_PER_TX_ATTACHED_PARTITION * Long.BYTES;
        final long fileSize = TableUtils.TX_BASE_HEADER_SIZE
                + TableUtils.calculateTxRecordSize(symbolsSize, partitionSegmentSize);

        try (MemoryCMARW mem = Vm.getSmallCMARWInstance(ff, path, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
            mem.jumpTo(fileSize);
            Vect.memset(mem.addressOf(0), fileSize, 0);
            mem.setTruncateSize(fileSize);

            final long version = state.getBaseVersion();
            final boolean isA = (version & 1L) == 0L;
            final int baseOffset = (int) TableUtils.TX_BASE_HEADER_SIZE;

            // base header (64 bytes)
            mem.putLong(TableUtils.TX_BASE_OFFSET_VERSION_64, version);
            mem.putInt(isA ? TableUtils.TX_BASE_OFFSET_A_32 : TableUtils.TX_BASE_OFFSET_B_32, baseOffset);
            mem.putInt(
                    isA ? TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_B_32,
                    symbolsSize
            );
            mem.putInt(
                    isA ? TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_B_32,
                    partitionSegmentSize
            );

            // record header
            mem.putLong(baseOffset + TableUtils.TX_OFFSET_TXN_64, state.getTxn());
            mem.putLong(baseOffset + TableUtils.TX_OFFSET_TRANSIENT_ROW_COUNT_64, state.getTransientRowCount());
            mem.putLong(baseOffset + TableUtils.TX_OFFSET_FIXED_ROW_COUNT_64, state.getFixedRowCount());
            mem.putLong(baseOffset + TableUtils.TX_OFFSET_MIN_TIMESTAMP_64, state.getMinTimestamp());
            mem.putLong(baseOffset + TableUtils.TX_OFFSET_MAX_TIMESTAMP_64, state.getMaxTimestamp());
            mem.putLong(baseOffset + TableUtils.TX_OFFSET_STRUCT_VERSION_64, state.getStructureVersion());
            mem.putLong(baseOffset + TableUtils.TX_OFFSET_DATA_VERSION_64, state.getDataVersion());
            mem.putLong(baseOffset + TableUtils.TX_OFFSET_PARTITION_TABLE_VERSION_64, state.getPartitionTableVersion());
            mem.putLong(baseOffset + TableUtils.TX_OFFSET_COLUMN_VERSION_64, state.getColumnVersion());
            mem.putLong(baseOffset + TableUtils.TX_OFFSET_TRUNCATE_VERSION_64, state.getTruncateVersion());
            mem.putLong(baseOffset + TableUtils.TX_OFFSET_SEQ_TXN_64, state.getSeqTxn());
            mem.putInt(baseOffset + TableUtils.TX_OFFSET_CHECKSUM_32, state.getLagChecksum());
            mem.putInt(baseOffset + TableUtils.TX_OFFSET_LAG_TXN_COUNT_32, state.getLagTxnCount());
            mem.putInt(baseOffset + TableUtils.TX_OFFSET_LAG_ROW_COUNT_32, state.getLagRowCount());
            mem.putLong(baseOffset + TableUtils.TX_OFFSET_LAG_MIN_TIMESTAMP_64, state.getLagMinTimestamp());
            mem.putLong(baseOffset + TableUtils.TX_OFFSET_LAG_MAX_TIMESTAMP_64, state.getLagMaxTimestamp());
            mem.putInt(baseOffset + TableUtils.TX_OFFSET_MAP_WRITER_COUNT_32, mapWriterCount);

            // symbol segment
            ObjList<TxnSymbolState> symbols = state.getSymbols();
            for (int i = 0, n = symbols.size(); i < n; i++) {
                TxnSymbolState sym = symbols.getQuick(i);
                long offset = baseOffset + TableUtils.getSymbolWriterIndexOffset(i);
                mem.putInt(offset, sym.getCount());
                mem.putInt(offset + Integer.BYTES, sym.getTransientCount());
            }

            // partition segment size (4 bytes) + partition entries
            long partitionTableOffset = TableUtils.getPartitionTableSizeOffset(mapWriterCount);
            mem.putInt(baseOffset + partitionTableOffset, partitionSegmentSize);

            // Partition entries: each partition is LONGS_PER_TX_ATTACHED_PARTITION longs (4 * 8 = 32 bytes).
            // getPartitionTableIndexOffset indexes by LONG, not by partition entry, so we compute
            // the base offset once (index=0) and stride by partition entry size, mirroring BoundedTxnReader.
            ObjList<TxnPartitionState> partitions = state.getPartitions();
            final long partitionDataOffset = baseOffset
                    + TableUtils.getPartitionTableIndexOffset(partitionTableOffset, 0);
            final int entryBytes = TableUtils.LONGS_PER_TX_ATTACHED_PARTITION * Long.BYTES;

            for (int i = 0; i < partitionCount; i++) {
                TxnPartitionState part = partitions.getQuick(i);
                long entryOffset = partitionDataOffset + (long) i * entryBytes;
                mem.putLong(entryOffset, part.getTimestampLo());
                long maskedSize = part.getRowCount()
                        | ((long) part.getSquashCount() << 44)
                        | (part.isParquetFormat() ? (1L << 61) : 0)
                        | (part.isReadOnly() ? (1L << 62) : 0);
                mem.putLong(entryOffset + Long.BYTES, maskedSize);
                mem.putLong(entryOffset + 2L * Long.BYTES, part.getNameTxn());
                mem.putLong(entryOffset + 3L * Long.BYTES, part.getParquetFileSize());
            }
        }
    }
}
