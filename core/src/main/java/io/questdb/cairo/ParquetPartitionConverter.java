/*+*****************************************************************************
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

package io.questdb.cairo;

import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.PartitionFrameEncoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Path;

/**
 * Writes the live rows of one logical partition - every folder of it, plain or composite, main directory or split -
 * into a single parquet file, straight off a {@link TableReader} snapshot and holding no writer. Each live piece of
 * each folder is handed to {@link PartitionFrameEncoder} as one frame, in {@code _txn} order and then piece order,
 * which is timestamp order; the encoder cuts row groups across the frames. Nothing is first folded into a native
 * directory.
 * <p>
 * The result holds every column from row 0: a frame's column top is written as nulls, so the caller publishes the
 * partition with every column top zero. Not thread-safe.
 */
final class ParquetPartitionConverter implements QuietCloseable {
    // Longs per column of a frame: column top, primary address and size, secondary address and size, symbol
    // offsets address and count - see PartitionFrameEncoder.
    private static final int FRAME_LONGS_PER_COLUMN = 7;
    private static final Log LOG = LogFactory.getLog(ParquetPartitionConverter.class);
    private final DirectIntList bloomFilterIndexes = new DirectIntList(8, MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
    private final DirectLongList columnMetadata = new DirectLongList(32, MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
    private final DirectUtf8Sink columnNames = new DirectUtf8Sink(256, true, MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final DirectLongList frameData = new DirectLongList(64, MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
    private long rowCount;

    ParquetPartitionConverter(CairoConfiguration configuration, FilesFacade ff) {
        this.configuration = configuration;
        this.ff = ff;
    }

    @Override
    public void close() {
        Misc.free(bloomFilterIndexes);
        Misc.free(columnMetadata);
        Misc.free(columnNames);
        Misc.free(frameData);
    }

    /**
     * Writes {@code data.parquet} and {@code _pm} of the folders {@code [lo, hi)} of {@code reader}'s snapshot into
     * {@code dstPartitionDir}, which has to exist.
     *
     * @param seqTxn the version the footer is stamped with
     * @return the parquet file size; {@link #getRowCount()} is the row count it holds
     */
    long convert(TableReader reader, int lo, int hi, Path dstPartitionDir, long seqTxn) {
        final TableMetadata metadata = reader.getMetadata();
        final TxReader txFile = reader.getTxFile();
        final PartitionGeometry geometry = reader.getGeometry();
        final int columnCount = metadata.getColumnCount();
        final int dstDirLen = dstPartitionDir.size();

        int activeColumnCount = 0;
        int timestampIndex = -1;
        columnNames.clear();
        columnMetadata.clear();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            int columnType = metadata.getColumnType(columnIndex);
            if (columnType <= 0) {
                continue;
            }
            if (columnIndex == metadata.getTimestampIndex()) {
                timestampIndex = activeColumnCount;
            }
            if (ColumnType.isSymbol(columnType) && !hasNulls(reader, lo, hi, columnIndex)) {
                // The encoder's fast path for a symbol column that holds no null.
                columnType |= Integer.MIN_VALUE;
            }
            final TableColumnMetadata columnMetadata = metadata.getColumnMetadata(columnIndex);
            final int nameStart = columnNames.size();
            columnNames.put(metadata.getColumnName(columnIndex));
            this.columnMetadata.add(columnNames.size() - nameStart);
            // The file keys each column by its ORIGINAL writer index, so a column re-keyed by ALTER COLUMN TYPE is
            // still found under the id the rest of the parquet code looks it up by.
            this.columnMetadata.add((long) columnMetadata.getOriginalWriterIndex() << 32 | (columnType & 0xFFFFFFFFL));
            this.columnMetadata.add(columnMetadata.getParquetEncodingConfig());
            activeColumnCount++;
        }
        bloomFilterIndexes.clear();
        TableUtils.deriveBloomFilterColumnIndexes(metadata, bloomFilterIndexes);

        long parquetMetaFd = -1;
        long writer = 0;
        try {
            dstPartitionDir.trimTo(dstDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
            parquetMetaFd = TableUtils.openRW(ff, dstPartitionDir.$(), LOG, configuration.getWriterFileOpenOpts());
            dstPartitionDir.trimTo(dstDirLen).concat(TableUtils.PARQUET_PARTITION_NAME).$();
            writer = PartitionFrameEncoder.create(
                    activeColumnCount,
                    columnNames.ptr(),
                    columnNames.size(),
                    columnMetadata.getAddress(),
                    timestampIndex,
                    dstPartitionDir.ptr(),
                    dstPartitionDir.size(),
                    ParquetCompression.packCompressionCodecLevel(
                            configuration.getPartitionEncoderParquetCompressionCodec(),
                            configuration.getPartitionEncoderParquetCompressionLevel()
                    ),
                    configuration.isPartitionEncoderParquetStatisticsEnabled(),
                    configuration.isPartitionEncoderParquetRawArrayEncoding(),
                    configuration.getPartitionEncoderParquetRowGroupSize(),
                    configuration.getPartitionEncoderParquetDataPageSize(),
                    configuration.getPartitionEncoderParquetVersion(),
                    bloomFilterIndexes.size() > 0 ? bloomFilterIndexes.getAddress() : 0,
                    (int) bloomFilterIndexes.size(),
                    configuration.getPartitionEncoderParquetBloomFilterFpp(),
                    configuration.getPartitionEncoderParquetMinCompressionRatio(),
                    Files.toOsFd(parquetMetaFd),
                    -1L,
                    seqTxn
            );

            rowCount = 0;
            for (int partitionIndex = lo; partitionIndex < hi; partitionIndex++) {
                if (txFile.getPartitionSize(partitionIndex) == 0) {
                    continue;
                }
                reader.openPartition(partitionIndex);
                if (txFile.isPartitionComposite(partitionIndex)) {
                    geometry.resolve(partitionIndex);
                    for (int p = 0, n = geometry.getPieceCount(partitionIndex); p < n; p++) {
                        final long pieceRows = geometry.getPieceRowCount(partitionIndex, p);
                        if (pieceRows > 0) {
                            writeFrame(writer, reader, partitionIndex, geometry.getPieceRowOffset(partitionIndex, p), pieceRows);
                        }
                    }
                } else {
                    writeFrame(writer, reader, partitionIndex, 0, txFile.getPartitionSize(partitionIndex));
                }
            }
            // The reader keeps every folder mapped until it is closed, so the frames stay valid up to the finish.
            return PartitionFrameEncoder.finish(writer, configuration.getCommitMode() != CommitMode.NOSYNC);
        } finally {
            dstPartitionDir.trimTo(dstDirLen);
            if (writer != 0) {
                PartitionFrameEncoder.destroy(writer);
            }
            if (parquetMetaFd > -1) {
                ff.close(parquetMetaFd);
            }
        }
    }

    long getRowCount() {
        return rowCount;
    }

    /**
     * Whether any row of the symbol column is null in the folders {@code [lo, hi)}: a column top makes one, and so does
     * a null value the symbol table holds.
     */
    private static boolean hasNulls(TableReader reader, int lo, int hi, int columnIndex) {
        if (reader.getSymbolMapReader(columnIndex).containsNullValue()) {
            return true;
        }
        for (int partitionIndex = lo; partitionIndex < hi; partitionIndex++) {
            if (reader.getTxFile().getPartitionSize(partitionIndex) == 0) {
                continue;
            }
            reader.openPartition(partitionIndex);
            final int columnBase = reader.getColumnBase(partitionIndex);
            if (reader.getColumnTop(columnBase, columnIndex) != 0
                    || reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, columnIndex)) == NullMemoryCMR.INSTANCE) {
                return true;
            }
        }
        return false;
    }

    /**
     * Hands the encoder the FILE rows {@code [rowLo, rowLo + rows)} of folder {@code partitionIndex} as one frame. A
     * column's top is in file rows too: rows below it are null and its files start at it.
     */
    private void writeFrame(long writer, TableReader reader, int partitionIndex, long rowLo, long rows) {
        final TableMetadata metadata = reader.getMetadata();
        final int columnBase = reader.getColumnBase(partitionIndex);
        frameData.clear();
        for (int columnIndex = 0, n = metadata.getColumnCount(); columnIndex < n; columnIndex++) {
            final int columnType = metadata.getColumnType(columnIndex);
            if (columnType <= 0) {
                continue;
            }
            final int primaryIndex = TableReader.getPrimaryColumnIndex(columnBase, columnIndex);
            final MemoryR primary = reader.getColumn(primaryIndex);
            // A column the folder does not hold at all reads as null for every row, whatever its recorded top.
            final long columnTop = primary == NullMemoryCMR.INSTANCE
                    ? Long.MAX_VALUE
                    : Math.max(0, reader.getColumnTop(columnBase, columnIndex));
            final long frameTop = columnTop - rowLo >= rows ? rows : Math.max(0, columnTop - rowLo);
            final long dataRows = rows - frameTop;
            // The first file row of the column the frame reads.
            final long fileRowLo = dataRows > 0 ? Math.max(rowLo, columnTop) - columnTop : 0;
            frameData.add(frameTop);
            if (dataRows == 0) {
                frameData.add(0L);
                frameData.add(0L);
                if (ColumnType.isSymbol(columnType)) {
                    addSymbolTable(reader, columnIndex);
                } else {
                    frameData.add(0L);
                    frameData.add(0L);
                    frameData.add(0L);
                    frameData.add(0L);
                }
                continue;
            }
            if (ColumnType.isVarSize(columnType)) {
                final ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                final MemoryR aux = reader.getColumn(primaryIndex + 1);
                // The aux vector holds offsets into the whole data vector, so the data goes in from its start.
                frameData.add(primary.size() > 0 ? primary.addressOf(0) : 0L);
                frameData.add(primary.size());
                frameData.add(aux.addressOf(driver.getAuxVectorOffset(fileRowLo)));
                frameData.add(driver.getAuxVectorSize(dataRows));
                frameData.add(0L);
                frameData.add(0L);
            } else {
                final int shl = ColumnType.pow2SizeOf(columnType);
                frameData.add(primary.addressOf(fileRowLo << shl));
                frameData.add(dataRows << shl);
                if (ColumnType.isSymbol(columnType)) {
                    addSymbolTable(reader, columnIndex);
                } else {
                    frameData.add(0L);
                    frameData.add(0L);
                    frameData.add(0L);
                    frameData.add(0L);
                }
            }
        }
        assert frameData.size() % FRAME_LONGS_PER_COLUMN == 0;
        PartitionFrameEncoder.writeFrame(writer, frameData.getAddress(), rows);
        rowCount += rows;
    }

    private void addSymbolTable(TableReader reader, int columnIndex) {
        final SymbolMapReader symbolMapReader = reader.getSymbolMapReader(columnIndex);
        final MemoryR symbolValues = symbolMapReader.getSymbolValuesColumn();
        final MemoryR symbolOffsets = symbolMapReader.getSymbolOffsetsColumn();
        frameData.add(symbolValues.addressOf(0));
        frameData.add(symbolValues.size());
        frameData.add(symbolOffsets.addressOf(SymbolMapWriter.HEADER_SIZE));
        frameData.add(symbolMapReader.getSymbolCount());
    }
}
