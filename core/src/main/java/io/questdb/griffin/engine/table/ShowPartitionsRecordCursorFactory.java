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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoKeywords;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.str.SizePrettyFunctionFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;
import java.util.Comparator;

public class ShowPartitionsRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final Comparator<String> CHAR_COMPARATOR = Chars::compare;
    private static final Log LOG = LogFactory.getLog(ShowPartitionsRecordCursor.class);
    private static final RecordMetadata METADATA_TIMESTAMP;
    private static final RecordMetadata METADATA_TIMESTAMP_NS;
    private final ShowPartitionsRecordCursor cursor = new ShowPartitionsRecordCursor();
    private final Path path = new Path();
    private final TableToken tableToken;
    private CairoConfiguration cairoConfig;
    private SqlExecutionContext executionContext;
    private FilesFacade ff;

    public ShowPartitionsRecordCursorFactory(TableToken tableToken, int timestampType) {
        super(ColumnType.isTimestampMicro(timestampType) ? METADATA_TIMESTAMP : METADATA_TIMESTAMP_NS);
        this.tableToken = tableToken;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        this.executionContext = executionContext;
        this.cairoConfig = executionContext.getCairoEngine().getConfiguration();
        this.ff = cairoConfig.getFilesFacade();
        return cursor.initialize();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show_partitions").meta("of").val(tableToken.getTableName());
    }

    @Override
    protected void _close() {
        Misc.free(path);
        Misc.free(cursor);
        executionContext = null;
        cairoConfig = null;
        ff = null;
    }

    private enum Column {
        PARTITION_INDEX(0, "index", ColumnType.INT),
        PARTITION_BY(1, "partitionBy", ColumnType.STRING),
        PARTITION_NAME(2, "name", ColumnType.STRING),
        MIN_TIMESTAMP(3, "minTimestamp", ColumnType.TIMESTAMP_MICRO),
        MAX_TIMESTAMP(4, "maxTimestamp", ColumnType.TIMESTAMP_MICRO),
        MIN_TIMESTAMP_NS(3, "minTimestamp", ColumnType.TIMESTAMP_NANO),
        MAX_TIMESTAMP_NS(4, "maxTimestamp", ColumnType.TIMESTAMP_NANO),
        NUM_ROWS(5, "numRows", ColumnType.LONG),
        DISK_SIZE(6, "diskSize", ColumnType.LONG),
        DISK_SIZE_HUMAN(7, "diskSizeHuman", ColumnType.STRING),
        IS_READ_ONLY(8, "readOnly", ColumnType.BOOLEAN),
        IS_ACTIVE(9, "active", ColumnType.BOOLEAN),
        IS_ATTACHED(10, "attached", ColumnType.BOOLEAN),
        IS_DETACHED(11, "detached", ColumnType.BOOLEAN),
        IS_ATTACHABLE(12, "attachable", ColumnType.BOOLEAN),
        HAS_PARQUET_GENERATED(13, "hasParquetGenerated", ColumnType.BOOLEAN),
        IS_PARQUET(14, "isParquet", ColumnType.BOOLEAN),
        PARQUET_FILE_SIZE(15, "parquetFileSize", ColumnType.LONG);

        private final int idx;
        private final TableColumnMetadata metadata;

        Column(int idx, String name, int type) {
            this.idx = idx;
            this.metadata = new TableColumnMetadata(name, type);
        }

        boolean is(int idx) {
            return this.idx == idx;
        }

        TableColumnMetadata metadata() {
            return metadata;
        }
    }

    private class ShowPartitionsRecordCursor implements NoRandomAccessRecordCursor {
        private final ObjList<String> attachablePartitions = new ObjList<>(4);
        private final ObjList<String> detachedPartitions = new ObjList<>(8);
        private final StringSink partitionName = new StringSink();
        private final PartitionsRecord partitionRecord = new PartitionsRecord();
        private final StringSink partitionSizeSink = new StringSink();
        private TableReaderMetadata detachedMetaReader;
        private TxReader detachedTxReader;
        private int dynamicPartitionIndex = -1;
        private boolean hasParquetGenerated;
        private boolean isActive;
        private boolean isAttachable;
        private boolean isDetached;
        private boolean isParquet;
        private boolean isReadOnly;
        private int limit; // partitionCount + detached + attachable
        private long maxTimestamp = Long.MIN_VALUE;
        private long minTimestamp = Numbers.LONG_NULL; // so that in absence of metadata is NaN
        private long numRows = -1L;
        private long parquetFileSize;
        private ParquetMetaFileReader parquetMetaReader;
        private int partitionBy = -1;
        private int partitionIndex = -1;
        private long partitionSize = -1L;
        private int rootLen;
        private TableReader tableReader;
        private int timestampType;
        private CharSequence tsColName;

        @Override
        public void close() {
            closeParquetMeta();
            detachedMetaReader = Misc.free(detachedMetaReader);
            detachedTxReader = Misc.free(detachedTxReader);
            attachablePartitions.clear();
            detachedPartitions.clear();
            partitionName.clear();
            partitionRecord.close();
            tableReader = Misc.free(tableReader);
            partitionSizeSink.clear();
        }

        @Override
        public Record getRecord() {
            return partitionRecord;
        }

        @Override
        public boolean hasNext() {
            if (++partitionIndex < limit) {
                loadNextPartition();
                return true;
            }
            --partitionIndex;
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return limit;
        }

        @Override
        public void toTop() {
            partitionIndex = -1;
        }

        private void closeParquetMeta() {
            if (parquetMetaReader != null) {
                // Capture before clear() zeros the fields so we can munmap.
                final long parquetMetaAddr = parquetMetaReader.getAddr();
                final long parquetMetaSize = parquetMetaReader.getFileSize();
                parquetMetaReader.clear();
                if (parquetMetaAddr != 0) {
                    ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                }
            }
        }

        private ShowPartitionsRecordCursor initialize() {
            if (tableReader != null) {//
                // this call is idempotent
                return this;
            }
            tsColName = null;
            tableReader = executionContext.getReader(tableToken);
            timestampType = tableReader.getMetadata().getTimestampType();
            partitionBy = tableReader.getPartitionedBy();
            if (PartitionBy.isPartitioned(partitionBy)) {
                TableReaderMetadata meta = tableReader.getMetadata();
                tsColName = meta.getColumnName(meta.getTimestampIndex());
            }
            path.of(cairoConfig.getDbRoot()).concat(tableToken).$();
            rootLen = path.size();
            scanDetachedAndAttachablePartitions();
            limit = tableReader.getTxFile().getPartitionCount() +
                    attachablePartitions.size() +
                    detachedPartitions.size();
            toTop();
            return this;
        }

        /**
         * Mmaps the _pm file, initializes the lazy ParquetMetaFileReader, and extracts
         * the parquet file size. The reader stays open so that min/max timestamps
         * can be read later in the same loadNextPartition() call. The mmap is
         * released when the reader is cleared after timestamp extraction.
         */
        private void loadNextPartition() {
            // Ensure any _pm mmap from a previous iteration is released.
            closeParquetMeta();
            isReadOnly = false;
            isActive = false;
            isDetached = false;
            isAttachable = false;
            isParquet = false;
            hasParquetGenerated = false;
            parquetFileSize = -1L;
            minTimestamp = Numbers.LONG_NULL; // so that in absence of metadata is NaN
            maxTimestamp = Long.MIN_VALUE;
            numRows = -1L;
            partitionSize = -1L;
            partitionName.clear();
            dynamicPartitionIndex = partitionIndex;
            CharSequence dynamicTsColName = tsColName;
            path.trimTo(rootLen).$();

            TxReader tableTxReader = tableReader.getTxFile();
            int partitionCount = tableTxReader.getPartitionCount();
            if (partitionIndex < partitionCount) {
                // we are within the partition table
                isReadOnly = tableTxReader.isPartitionReadOnly(partitionIndex);
                hasParquetGenerated = tableTxReader.isPartitionParquetGenerated(partitionIndex);
                isParquet = tableTxReader.isPartitionParquet(partitionIndex);
                long timestamp = tableTxReader.getPartitionTimestampByIndex(partitionIndex);
                isActive = timestamp == tableTxReader.getLastPartitionTimestamp();
                PartitionBy.setSinkForPartition(partitionName, timestampType, partitionBy, timestamp);
                TableUtils.setPathForNativePartition(path, timestampType, partitionBy, timestamp, tableTxReader.getPartitionNameTxn(partitionIndex));
                if (hasParquetGenerated || isParquet) {
                    openParquetMeta(path, tableTxReader.getPartitionParquetFileSize(partitionIndex));
                }
                numRows = tableTxReader.getPartitionSize(partitionIndex);
            } else {
                // partition table is over, we will iterate over detached and attachable partitions
                isDetached = true;
                int idx = partitionIndex - partitionCount; // index in detachedPartitions
                int n = detachedPartitions.size();
                if (idx < n) {
                    // is detached
                    partitionName.put(detachedPartitions.get(idx));
                } else {
                    idx -= n; // index in attachablePartitions
                    if (idx < attachablePartitions.size()) {
                        // is attachable, also detached
                        partitionName.put(attachablePartitions.get(idx));
                        isAttachable = true;
                    }
                }
                assert !partitionName.isEmpty();

                // open detached meta files (_meta, _txn) if they exist
                dynamicPartitionIndex = Numbers.INT_NULL; // so that in absence of metadata is NaN
                if (ff.exists(path.concat(partitionName).concat(TableUtils.META_FILE_NAME).$())) {
                    try {
                        if (detachedMetaReader == null) {
                            detachedMetaReader = new TableReaderMetadata(cairoConfig);
                        }
                        detachedMetaReader.loadMetadata(path.$());
                        if (tableToken.getTableId() == detachedMetaReader.getTableId() && partitionBy == detachedMetaReader.getPartitionBy()) {
                            if (ff.exists(path.parent().concat(TableUtils.TXN_FILE_NAME).$())) {
                                try {
                                    if (detachedTxReader == null) {
                                        detachedTxReader = new TxReader(FilesFacadeImpl.INSTANCE);
                                    }
                                    detachedTxReader.ofRO(path.$(), timestampType, partitionBy);
                                    detachedTxReader.unsafeLoadAll();
                                    int length = partitionName.indexOf(".");
                                    if (length < 0) {
                                        length = partitionName.length();
                                    }
                                    long timestamp = PartitionBy.parsePartitionDirName(partitionName, timestampType, partitionBy, 0, length);
                                    int pIndex = detachedTxReader.getPartitionIndex(timestamp);
                                    // could set dynamicPartitionIndex to -pIndex
                                    numRows = detachedTxReader.getPartitionSize(pIndex);
                                    if (PartitionBy.isPartitioned(partitionBy) && numRows > 0L) {
                                        int tsIndex = detachedMetaReader.getTimestampIndex();
                                        dynamicTsColName = detachedMetaReader.getColumnName(tsIndex);
                                    }
                                } finally {
                                    if (detachedTxReader != null) {
                                        detachedTxReader.clear();
                                    }
                                }
                            } else {
                                LOG.error().$("detached partition does not have meta file [path=").$(path).I$();
                            }
                        } else {
                            LOG.error().$("detached partition meta does not match [path=").$(path).I$();
                        }
                    } finally {
                        if (detachedMetaReader != null) {
                            detachedMetaReader.clear();
                        }
                    }
                } else {
                    LOG.error().$("detached partition does not have meta file [path=").$(path).I$();
                }
                path.parent();
            }

            partitionSize = ff.getDirSize(path);
            partitionSizeSink.clear();
            SizePrettyFunctionFactory.toSizePretty(partitionSizeSink, partitionSize);
            if (PartitionBy.isPartitioned(partitionBy) && numRows > 0L) {
                if (isParquet && parquetMetaReader != null && parquetMetaReader.isOpen()) {
                    int tsIndex = parquetMetaReader.getDesignatedTimestampColumnIndex();
                    int rowGroupCount = parquetMetaReader.getRowGroupCount();
                    if (tsIndex >= 0 && rowGroupCount > 0) {
                        minTimestamp = parquetMetaReader.getRowGroupMinTimestamp(0, tsIndex);
                        maxTimestamp = parquetMetaReader.getRowGroupMaxTimestamp(rowGroupCount - 1, tsIndex);
                    }
                    closeParquetMeta();
                } else if (!isParquet) {
                    TableUtils.dFile(path.slash(), dynamicTsColName, TableUtils.COLUMN_NAME_TXN_NONE);
                    long fd = -1;
                    try {
                        fd = TableUtils.openRO(ff, path.$(), LOG);
                        long lastOffset = (numRows - 1) * Long.BYTES; // timestamp size
                        minTimestamp = ff.readNonNegativeLong(fd, 0);
                        maxTimestamp = ff.readNonNegativeLong(fd, lastOffset);
                    } catch (CairoException e) {
                        dynamicPartitionIndex = Numbers.INT_NULL;
                        LOG.error().$("no file found for designated timestamp column [path=").$(path).I$();
                    } finally {
                        if (fd != -1) {
                            ff.close(fd);
                        }
                    }
                } else {
                    minTimestamp = Long.MIN_VALUE;
                    maxTimestamp = Long.MIN_VALUE;
                }
            }
        }

        /**
         * Mmaps the _pm file, initializes the lazy ParquetMetaFileReader, and extracts
         * the parquet file size. The reader stays open so that min/max timestamps
         * can be read later in the same loadNextPartition() call.
         * Call {@link #closeParquetMeta()} to release the mmap.
         */
        private void openParquetMeta(Path partitionDirPath, long parquetFileSize) {
            this.parquetFileSize = parquetFileSize;
            if (parquetFileSize <= 0) {
                return;
            }
            int dirLen = partitionDirPath.size();
            partitionDirPath.concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
            try {
                if (parquetMetaReader == null) {
                    parquetMetaReader = new ParquetMetaFileReader();
                }
                ParquetMetaFileReader.openAndMapRO(ff, partitionDirPath.$(), parquetMetaReader);
                if (parquetMetaReader.getAddr() == 0 || !parquetMetaReader.resolveFooter(parquetFileSize)) {
                    throw CairoException.critical(0)
                            .put("could not resolve expected footer");
                }
            } catch (Throwable e) {
                LOG.error().$("could not read parquet metadata [path=").$(partitionDirPath).$(", error=").$(e.getMessage()).I$();
                closeParquetMeta();
            } finally {
                partitionDirPath.trimTo(dirLen);
            }
        }

        private void scanDetachedAndAttachablePartitions() {
            long pFind = ff.findFirst(path.$());
            if (pFind > 0L) {
                try {
                    attachablePartitions.clear();
                    detachedPartitions.clear();
                    do {
                        partitionName.clear();
                        long name = ff.findName(pFind);
                        Utf8s.utf8ToUtf16Z(name, partitionName);
                        int type = ff.findType(pFind);
                        if ((type == Files.DT_LNK || type == Files.DT_DIR) && Chars.endsWith(partitionName, TableUtils.ATTACHABLE_DIR_MARKER)) {
                            attachablePartitions.add(Chars.toString(partitionName));
                        } else if (type == Files.DT_DIR && CairoKeywords.isDetachedDirMarker(name)) {
                            detachedPartitions.add(Chars.toString(partitionName));
                        }
                    } while (ff.findNext(pFind) > 0);
                    attachablePartitions.sort(CHAR_COMPARATOR);
                    detachedPartitions.sort(CHAR_COMPARATOR);
                } finally {
                    ff.findClose(pFind);
                }
            }
        }

        private class PartitionsRecord implements Record, Closeable {
            @Override
            public void close() {
                // no-op
            }

            @Override
            public boolean getBool(int col) {
                return switch (col) {
                    case 8 -> isReadOnly;
                    case 9 -> isActive;
                    case 10 -> isReadOnly || !isDetached;
                    case 11 -> isDetached;
                    case 12 -> isAttachable;
                    case 13 -> hasParquetGenerated;
                    case 14 -> isParquet;
                    default -> throw new UnsupportedOperationException();
                };
            }

            @Override
            public int getInt(int col) {
                if (Column.PARTITION_INDEX.is(col)) {
                    return dynamicPartitionIndex;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public long getLong(int col) {
                return switch (col) {
                    case 3 -> minTimestamp;
                    case 4 -> maxTimestamp;
                    case 5 -> numRows;
                    case 6 -> partitionSize;
                    case 15 -> parquetFileSize;
                    default -> throw new UnsupportedOperationException();
                };
            }

            @Override
            public CharSequence getStrA(int col) {
                return switch (col) {
                    case 1 -> PartitionBy.toString(partitionBy);
                    case 2 -> partitionName;
                    case 7 -> partitionSizeSink;
                    default -> throw new UnsupportedOperationException();
                };
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStrA(col);
            }

            @Override
            public int getStrLen(int col) {
                return TableUtils.lengthOf(getStrA(col));
            }

            @Override
            public long getTimestamp(int col) {
                return switch (col) {
                    case 3 -> minTimestamp;
                    case 4 -> maxTimestamp;
                    default -> throw new UnsupportedOperationException();
                };
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(Column.PARTITION_INDEX.metadata());
        metadata.add(Column.PARTITION_BY.metadata());
        metadata.add(Column.PARTITION_NAME.metadata());
        metadata.add(Column.MIN_TIMESTAMP.metadata());
        metadata.add(Column.MAX_TIMESTAMP.metadata());
        metadata.add(Column.NUM_ROWS.metadata());
        metadata.add(Column.DISK_SIZE.metadata());
        metadata.add(Column.DISK_SIZE_HUMAN.metadata());
        metadata.add(Column.IS_READ_ONLY.metadata());
        metadata.add(Column.IS_ACTIVE.metadata());
        metadata.add(Column.IS_ATTACHED.metadata());
        metadata.add(Column.IS_DETACHED.metadata());
        metadata.add(Column.IS_ATTACHABLE.metadata());
        metadata.add(Column.HAS_PARQUET_GENERATED.metadata());
        metadata.add(Column.IS_PARQUET.metadata());
        metadata.add(Column.PARQUET_FILE_SIZE.metadata());
        METADATA_TIMESTAMP = metadata;
        final GenericRecordMetadata metadataNs = new GenericRecordMetadata();
        metadataNs.add(Column.PARTITION_INDEX.metadata());
        metadataNs.add(Column.PARTITION_BY.metadata());
        metadataNs.add(Column.PARTITION_NAME.metadata());
        metadataNs.add(Column.MIN_TIMESTAMP_NS.metadata());
        metadataNs.add(Column.MAX_TIMESTAMP_NS.metadata());
        metadataNs.add(Column.NUM_ROWS.metadata());
        metadataNs.add(Column.DISK_SIZE.metadata());
        metadataNs.add(Column.DISK_SIZE_HUMAN.metadata());
        metadataNs.add(Column.IS_READ_ONLY.metadata());
        metadataNs.add(Column.IS_ACTIVE.metadata());
        metadataNs.add(Column.IS_ATTACHED.metadata());
        metadataNs.add(Column.IS_DETACHED.metadata());
        metadataNs.add(Column.IS_ATTACHABLE.metadata());
        metadataNs.add(Column.HAS_PARQUET_GENERATED.metadata());
        metadataNs.add(Column.IS_PARQUET.metadata());
        metadataNs.add(Column.PARQUET_FILE_SIZE.metadata());
        METADATA_TIMESTAMP_NS = metadataNs;
    }
}
