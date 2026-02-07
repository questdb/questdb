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

import io.questdb.cairo.TableUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;

public class BoundedTxnReader {
    public static final int DEFAULT_MAX_PARTITIONS = 100_000;
    public static final int DEFAULT_MAX_SYMBOLS = 10_000;
    private static final int PARTITION_FLAG_PARQUET_BIT_OFFSET = 61;
    private static final int PARTITION_FLAG_READ_ONLY_BIT_OFFSET = 62;
    private static final int PARTITION_SIZE_BIT_WIDTH = 44;
    private static final long PARTITION_SIZE_MASK = (1L << PARTITION_SIZE_BIT_WIDTH) - 1;
    private final FilesFacade ff;
    private final int maxPartitions;
    private final int maxSymbols;

    public BoundedTxnReader(FilesFacade ff) {
        this(ff, DEFAULT_MAX_SYMBOLS, DEFAULT_MAX_PARTITIONS);
    }

    public BoundedTxnReader(FilesFacade ff, int maxSymbols, int maxPartitions) {
        this.ff = ff;
        this.maxSymbols = Math.max(1, maxSymbols);
        this.maxPartitions = Math.max(1, maxPartitions);
    }

    public TxnState read(LPSZ txnPath) {
        final TxnState txnState = new TxnState();
        final String txnPathStr = txnPath.asAsciiCharSequence().toString();
        txnState.setTxnPath(txnPathStr);

        final long fd = ff.openRO(txnPath);
        if (fd < 0) {
            addFileOpenFailure(txnPath, txnPathStr, txnState);
            return txnState;
        }

        try {
            final long fileSize = ff.length(fd);
            txnState.setFileSize(fileSize);

            if (fileSize < TableUtils.TX_BASE_HEADER_SIZE) {
                txnState.addIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.SHORT_FILE,
                        "_txn file is shorter than header [path=" + txnPathStr + ", size=" + fileSize + ']'
                );
                return txnState;
            }

            long scratch = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                readTxn(fd, fileSize, scratch, txnState);
            } finally {
                Unsafe.free(scratch, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            ff.close(fd);
        }

        return txnState;
    }

    private static boolean isBitSet(long value, int bit) {
        return ((value >>> bit) & 1L) == 1L;
    }

    private void addFileOpenFailure(LPSZ txnPath, String txnPathStr, TxnState txnState) {
        if (ff.exists(txnPath)) {
            txnState.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.IO_ERROR,
                    "cannot open _txn file [path=" + txnPathStr + ", errno=" + ff.errno() + ']'
            );
        } else {
            txnState.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.MISSING_FILE,
                    "_txn file does not exist [path=" + txnPathStr + ']'
            );
        }
    }

    private void addShortReadIssue(
            TxnState txnState,
            RecoveryIssueCode code,
            String detail,
            long offset,
            int width,
            long fileSize
    ) {
        txnState.addIssue(
                RecoveryIssueSeverity.ERROR,
                code,
                detail + " [offset=" + offset + ", width=" + width + ", fileSize=" + fileSize + ']'
        );
    }

    private boolean isRangeReadable(long offset, int width, long fileSize) {
        return offset >= 0 && width >= 0 && fileSize >= 0 && offset <= fileSize - width;
    }

    private void readPartitions(
            long fd,
            long fileSize,
            long scratch,
            TxnState txnState,
            int baseOffset,
            int symbolsSegmentCount,
            int partitionSegmentSize
    ) {
        if (partitionSegmentSize == 0) {
            return;
        }

        if (partitionSegmentSize < 0) {
            txnState.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.INVALID_COUNT,
                    "partition segment size is negative [value=" + partitionSegmentSize + ']'
            );
            return;
        }

        if (partitionSegmentSize % Long.BYTES != 0) {
            txnState.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.INVALID_COUNT,
                    "partition segment size is not long aligned [value=" + partitionSegmentSize + ']'
            );
        }

        final int partitionLongCount = partitionSegmentSize / Long.BYTES;
        if (partitionLongCount % TableUtils.LONGS_PER_TX_ATTACHED_PARTITION != 0) {
            txnState.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.INVALID_COUNT,
                    "partition segment long count is not partition-entry aligned [value=" + partitionLongCount + ']'
            );
        }

        int partitionCount = partitionLongCount / TableUtils.LONGS_PER_TX_ATTACHED_PARTITION;
        if (partitionCount > maxPartitions) {
            txnState.addIssue(
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.TRUNCATED_OUTPUT,
                    "partition list is capped [requested=" + partitionCount + ", cap=" + maxPartitions + ']'
            );
            partitionCount = maxPartitions;
        }

        final long partitionDataOffset = baseOffset + TableUtils.getPartitionTableIndexOffset(
                TableUtils.getPartitionTableSizeOffset(symbolsSegmentCount),
                0
        );

        for (int i = 0; i < partitionCount; i++) {
            final long entryOffset = partitionDataOffset + (long) i * TableUtils.LONGS_PER_TX_ATTACHED_PARTITION * Long.BYTES;
            if (!isRangeReadable(entryOffset, TableUtils.LONGS_PER_TX_ATTACHED_PARTITION * Long.BYTES, fileSize)) {
                addShortReadIssue(
                        txnState,
                        RecoveryIssueCode.PARTIAL_READ,
                        "partition entry points outside file",
                        entryOffset,
                        TableUtils.LONGS_PER_TX_ATTACHED_PARTITION * Long.BYTES,
                        fileSize
                );
                return;
            }

            int issuesBefore = txnState.getIssues().size();
            long timestampLo = readLongValue(fd, fileSize, scratch, txnState, entryOffset, "partition.ts");
            if (txnState.getIssues().size() > issuesBefore) {
                return;
            }
            long maskedSize = readLongValue(fd, fileSize, scratch, txnState, entryOffset + Long.BYTES, "partition.maskedSize");
            if (txnState.getIssues().size() > issuesBefore) {
                return;
            }
            long nameTxn = readLongValue(fd, fileSize, scratch, txnState, entryOffset + 2L * Long.BYTES, "partition.nameTxn");
            if (txnState.getIssues().size() > issuesBefore) {
                return;
            }
            long parquetFileSize = readLongValue(fd, fileSize, scratch, txnState, entryOffset + 3L * Long.BYTES, "partition.parquetFileSize");
            if (txnState.getIssues().size() > issuesBefore) {
                return;
            }

            final long rowCount = maskedSize & PARTITION_SIZE_MASK;
            txnState.getPartitions().add(
                    new TxnPartitionState(
                            i,
                            timestampLo,
                            maskedSize,
                            rowCount,
                            nameTxn,
                            parquetFileSize,
                            isBitSet(maskedSize, PARTITION_FLAG_PARQUET_BIT_OFFSET),
                            isBitSet(maskedSize, PARTITION_FLAG_READ_ONLY_BIT_OFFSET)
                    )
            );
        }
    }

    private void readSymbols(
            long fd,
            long fileSize,
            long scratch,
            TxnState txnState,
            int baseOffset,
            int symbolsSegmentSize
    ) {
        if (symbolsSegmentSize == 0) {
            return;
        }

        if (symbolsSegmentSize < 0) {
            txnState.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.INVALID_COUNT,
                    "symbol segment size is negative [value=" + symbolsSegmentSize + ']'
            );
            return;
        }

        if (symbolsSegmentSize % Long.BYTES != 0) {
            txnState.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.INVALID_COUNT,
                    "symbol segment size is not long aligned [value=" + symbolsSegmentSize + ']'
            );
        }

        int symbolCount = symbolsSegmentSize / Long.BYTES;
        if (symbolCount > maxSymbols) {
            txnState.addIssue(
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.TRUNCATED_OUTPUT,
                    "symbol list is capped [requested=" + symbolCount + ", cap=" + maxSymbols + ']'
            );
            symbolCount = maxSymbols;
        }

        final long symbolDataOffset = baseOffset + TableUtils.getSymbolWriterIndexOffset(0);
        for (int i = 0; i < symbolCount; i++) {
            final long symbolOffset = symbolDataOffset + (long) i * Long.BYTES;
            if (!isRangeReadable(symbolOffset, Long.BYTES, fileSize)) {
                addShortReadIssue(
                        txnState,
                        RecoveryIssueCode.PARTIAL_READ,
                        "symbol entry points outside file",
                        symbolOffset,
                        Long.BYTES,
                        fileSize
                );
                return;
            }

            int issuesBefore = txnState.getIssues().size();
            int count = readIntValue(fd, fileSize, scratch, txnState, symbolOffset, "symbol.count");
            if (txnState.getIssues().size() > issuesBefore) {
                return;
            }
            int transientCount = readIntValue(fd, fileSize, scratch, txnState, symbolOffset + Integer.BYTES, "symbol.transientCount");
            if (txnState.getIssues().size() > issuesBefore) {
                return;
            }
            txnState.getSymbols().add(new TxnSymbolState(i, count, transientCount));
        }
    }

    private void readTxn(long fd, long fileSize, long scratch, TxnState txnState) {
        int issuesBefore = txnState.getIssues().size();

        final long baseVersion = readLongValue(fd, fileSize, scratch, txnState, TableUtils.TX_BASE_OFFSET_VERSION_64, "base.version");
        if (txnState.getIssues().size() > issuesBefore) {
            return;
        }
        txnState.setBaseVersion(baseVersion);

        final boolean isA = (baseVersion & 1L) == 0L;
        final int baseOffset = readIntValue(
                fd,
                fileSize,
                scratch,
                txnState,
                isA ? TableUtils.TX_BASE_OFFSET_A_32 : TableUtils.TX_BASE_OFFSET_B_32,
                "base.offset"
        );
        if (txnState.getIssues().size() > issuesBefore) {
            return;
        }
        txnState.setRecordBaseOffset(baseOffset);

        final int symbolsSegmentSize = readIntValue(
                fd,
                fileSize,
                scratch,
                txnState,
                isA ? TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_B_32,
                "base.symbolsSegmentSize"
        );
        if (txnState.getIssues().size() > issuesBefore) {
            return;
        }
        txnState.setSymbolsSegmentSize(symbolsSegmentSize);

        final int partitionSegmentSize = readIntValue(
                fd,
                fileSize,
                scratch,
                txnState,
                isA ? TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_B_32,
                "base.partitionSegmentSize"
        );
        if (txnState.getIssues().size() > issuesBefore) {
            return;
        }
        txnState.setPartitionSegmentSize(partitionSegmentSize);

        if (baseOffset < TableUtils.TX_BASE_HEADER_SIZE) {
            txnState.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.INVALID_OFFSET,
                    "record base offset points into tx header [value=" + baseOffset + ']'
            );
            return;
        }

        final int recordSize = TableUtils.calculateTxRecordSize(Math.max(0, symbolsSegmentSize), Math.max(0, partitionSegmentSize));
        if (!isRangeReadable(baseOffset, recordSize, fileSize)) {
            addShortReadIssue(
                    txnState,
                    RecoveryIssueCode.PARTIAL_READ,
                    "record area points outside file",
                    baseOffset,
                    recordSize,
                    fileSize
            );
            return;
        }

        if (!readHeader(fd, fileSize, scratch, txnState, baseOffset)) {
            return;
        }

        final int symbolsSegmentCount = symbolsSegmentSize > 0 ? symbolsSegmentSize / Long.BYTES : 0;
        if (txnState.getMapWriterCount() != TxnState.UNSET_INT
                && symbolsSegmentCount >= 0
                && txnState.getMapWriterCount() != symbolsSegmentCount) {
            txnState.addIssue(
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.META_COLUMN_COUNT_MISMATCH,
                    "map writer count does not match symbol segment size [mapWriterCount="
                            + txnState.getMapWriterCount()
                            + ", symbolCountFromSegment="
                            + symbolsSegmentCount
                            + ']'
            );
        }

        readSymbols(fd, fileSize, scratch, txnState, baseOffset, symbolsSegmentSize);
        readPartitions(fd, fileSize, scratch, txnState, baseOffset, symbolsSegmentCount, partitionSegmentSize);
    }

    private boolean readHeader(long fd, long fileSize, long scratch, TxnState txnState, int baseOffset) {
        int issuesBefore = txnState.getIssues().size();

        txnState.setTxn(readLongValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_TXN_64, "txn"));
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }

        txnState.setTransientRowCount(readLongValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_TRANSIENT_ROW_COUNT_64, "transientRowCount"));
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }

        txnState.setFixedRowCount(readLongValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_FIXED_ROW_COUNT_64, "fixedRowCount"));
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }

        txnState.setMinTimestamp(readLongValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_MIN_TIMESTAMP_64, "minTimestamp"));
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }

        txnState.setMaxTimestamp(readLongValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_MAX_TIMESTAMP_64, "maxTimestamp"));
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }

        txnState.setStructureVersion(readLongValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_STRUCT_VERSION_64, "structureVersion"));
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }

        txnState.setDataVersion(readLongValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_DATA_VERSION_64, "dataVersion"));
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }

        txnState.setPartitionTableVersion(readLongValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_PARTITION_TABLE_VERSION_64, "partitionTableVersion"));
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }

        txnState.setColumnVersion(readLongValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_COLUMN_VERSION_64, "columnVersion"));
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }

        txnState.setTruncateVersion(readLongValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_TRUNCATE_VERSION_64, "truncateVersion"));
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }

        txnState.setSeqTxn(readLongValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_SEQ_TXN_64, "seqTxn"));
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }

        txnState.setLagTxnCount(readIntValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_LAG_TXN_COUNT_32, "lagTxnCount"));
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }

        txnState.setLagRowCount(readIntValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_LAG_ROW_COUNT_32, "lagRowCount"));
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }

        txnState.setLagMinTimestamp(readLongValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_LAG_MIN_TIMESTAMP_64, "lagMinTimestamp"));
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }

        txnState.setLagMaxTimestamp(readLongValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_LAG_MAX_TIMESTAMP_64, "lagMaxTimestamp"));
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }

        final int mapWriterCount = readIntValue(fd, fileSize, scratch, txnState, baseOffset + TableUtils.TX_OFFSET_MAP_WRITER_COUNT_32, "mapWriterCount");
        if (txnState.getIssues().size() > issuesBefore) {
            return false;
        }
        txnState.setMapWriterCount(mapWriterCount);
        if (mapWriterCount < 0) {
            txnState.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.INVALID_COUNT,
                    "map writer count is negative [value=" + mapWriterCount + ']'
            );
        }

        return true;
    }

    private int readIntValue(long fd, long fileSize, long scratch, TxnState txnState, long offset, String fieldName) {
        if (!isRangeReadable(offset, Integer.BYTES, fileSize)) {
            addShortReadIssue(
                    txnState,
                    RecoveryIssueCode.OUT_OF_RANGE,
                    "field is outside file: " + fieldName,
                    offset,
                    Integer.BYTES,
                    fileSize
            );
            return TxnState.UNSET_INT;
        }

        final long bytesRead = ff.read(fd, scratch, Integer.BYTES, offset);
        if (bytesRead != Integer.BYTES) {
            addShortReadIssue(
                    txnState,
                    bytesRead < 0 ? RecoveryIssueCode.IO_ERROR : RecoveryIssueCode.SHORT_FILE,
                    "cannot read int field: " + fieldName,
                    offset,
                    Integer.BYTES,
                    fileSize
            );
            return TxnState.UNSET_INT;
        }

        return Unsafe.getUnsafe().getInt(scratch);
    }

    private long readLongValue(long fd, long fileSize, long scratch, TxnState txnState, long offset, String fieldName) {
        if (!isRangeReadable(offset, Long.BYTES, fileSize)) {
            addShortReadIssue(
                    txnState,
                    RecoveryIssueCode.OUT_OF_RANGE,
                    "field is outside file: " + fieldName,
                    offset,
                    Long.BYTES,
                    fileSize
            );
            return TxnState.UNSET_LONG;
        }

        final long bytesRead = ff.read(fd, scratch, Long.BYTES, offset);
        if (bytesRead != Long.BYTES) {
            addShortReadIssue(
                    txnState,
                    bytesRead < 0 ? RecoveryIssueCode.IO_ERROR : RecoveryIssueCode.SHORT_FILE,
                    "cannot read long field: " + fieldName,
                    offset,
                    Long.BYTES,
                    fileSize
            );
            return TxnState.UNSET_LONG;
        }

        return Unsafe.getUnsafe().getLong(scratch);
    }
}

