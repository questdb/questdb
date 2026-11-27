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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.str.SizePrettyFunctionFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

import java.util.Comparator;

public class AllTablesPartitionsFunctionFactory implements FunctionFactory {

    private enum Column {
        TABLE_NAME(0, "tableName", ColumnType.STRING),
        PARTITION_INDEX(1, "index", ColumnType.INT),
        PARTITION_BY(2, "partitionBy", ColumnType.STRING),
        PARTITION_NAME(3, "name", ColumnType.STRING),
        MIN_TIMESTAMP(4, "minTimestamp", ColumnType.TIMESTAMP_MICRO),
        MAX_TIMESTAMP(5, "maxTimestamp", ColumnType.TIMESTAMP_MICRO),
        NUM_ROWS(6, "numRows", ColumnType.LONG),
        DISK_SIZE(7, "diskSize", ColumnType.LONG),
        DISK_SIZE_HUMAN(8, "diskSizeHuman", ColumnType.STRING),
        IS_READ_ONLY(9, "readOnly", ColumnType.BOOLEAN),
        IS_ACTIVE(10, "active", ColumnType.BOOLEAN),
        IS_ATTACHED(11, "attached", ColumnType.BOOLEAN),
        IS_DETACHED(12, "detached", ColumnType.BOOLEAN),
        IS_ATTACHABLE(13, "attachable", ColumnType.BOOLEAN),
        HAS_PARQUET_GENERATED(14, "hasParquetGenerated", ColumnType.BOOLEAN),
        IS_PARQUET(15, "isParquet", ColumnType.BOOLEAN),
        PARQUET_FILE_SIZE(16, "parquetFileSize", ColumnType.LONG);

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

    private static final RecordMetadata METADATA;

    @Override
    public String getSignature() {
        return "all_tables_partitions()";
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new CursorFunction(new AllTablesPartitionsRecordCursorFactory(sqlExecutionContext.getCairoEngine())) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    public static class AllTablesPartitionsRecordCursorFactory extends AbstractRecordCursorFactory {
        private static final Log LOG = LogFactory.getLog(AllTablesPartitionsRecordCursorFactory.class);
        private final CairoEngine engine;
        private final AllTablesPartitionsRecordCursor cursor = new AllTablesPartitionsRecordCursor();
        private final Path path = new Path();
        private SqlExecutionContext executionContext;
        private FilesFacade ff;

        public AllTablesPartitionsRecordCursorFactory(CairoEngine engine) {
            super(METADATA);
            this.engine = engine;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            this.executionContext = executionContext;
            this.ff = executionContext.getCairoEngine().getConfiguration().getFilesFacade();
            return cursor.initialize(engine, executionContext, ff, path);
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("all_tables_partitions()");
        }

        @Override
        protected void _close() {
            Misc.free(path);
            Misc.free(cursor);
            executionContext = null;
            ff = null;
        }
    }

    private static class AllTablesPartitionsRecordCursor implements NoRandomAccessRecordCursor {
        private static final Comparator<String> CHAR_COMPARATOR = Chars::compare;
        private static final Log LOG = LogFactory.getLog(AllTablesPartitionsRecordCursor.class);

        private final ObjList<String> attachablePartitions = new ObjList<>(4);
        private final ObjHashSet<TableToken> tableBucket = new ObjHashSet<>();
        private final ObjList<String> detachedPartitions = new ObjList<>(8);
        private final PartitionRecord partitionRecord = new PartitionRecord();
        private final StringSink partitionName = new StringSink();
        private final StringSink partitionSizeSink = new StringSink();
        private TableReaderMetadata detachedMetaReader;
        private TxReader detachedTxReader;
        private int dynamicPartitionIndex = -1;
        private CairoEngine engine;
        private SqlExecutionContext executionContext;
        private FilesFacade ff;
        private boolean hasParquetGenerated;
        private boolean isActive;
        private boolean isAttachable;
        private boolean isDetached;
        private boolean isParquet;
        private boolean isReadOnly;
        private int limit;
        private long maxTimestamp = Long.MIN_VALUE;
        private long minTimestamp = Numbers.LONG_NULL;
        private long numRows = -1L;
        private long parquetFileSize;
        private ParquetMetaFileReader parquetMetaReader;
        private int partitionBy = -1;
        private int partitionIndex = -1;
        private long partitionSize = -1L;
        private Path path;
        private int rootLen;
        private TableReader tableReader;
        private int tableIndex = -1;
        private int timestampType;
        private CharSequence tsColName;

        @Override
        public void close() {
            closeCurrentTable();
            tableBucket.clear();
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
            while (tableIndex < tableBucket.size()) {
                TableToken token = tableBucket.get(tableIndex++);
                if (token.isSystem()) {
                    continue;
                }
                if (openTable(token)) {
                    partitionIndex = -1;
                    if (hasNext()) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            tableIndex = 0;
            partitionIndex = -1;
            closeCurrentTable();
        }

        private void closeCurrentTable() {
            closeParquetMeta();
            detachedMetaReader = Misc.free(detachedMetaReader);
            detachedTxReader = Misc.free(detachedTxReader);
            attachablePartitions.clear();
            detachedPartitions.clear();
            tableReader = Misc.free(tableReader);
            limit = 0;
        }

        private void closeParquetMeta() {
            if (parquetMetaReader != null) {
                final long parquetMetaAddr = parquetMetaReader.getAddr();
                final long parquetMetaSize = parquetMetaReader.getFileSize();
                parquetMetaReader.clear();
                if (parquetMetaAddr != 0) {
                    ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                }
            }
        }

        private AllTablesPartitionsRecordCursor initialize(
                CairoEngine engine,
                SqlExecutionContext executionContext,
                FilesFacade ff,
                Path path
        ) {
            this.engine = engine;
            this.executionContext = executionContext;
            this.ff = ff;
            this.path = path;
            tableBucket.clear();
            engine.getTableTokens(tableBucket, false);
            tableIndex = 0;
            partitionIndex = -1;
            return this;
        }

        private boolean openTable(TableToken token) {
            closeCurrentTable();
            try {
                tableReader = executionContext.getReader(token);
            } catch (CairoException e) {
                LOG.error().$("could not open table [table=").$(token.getTableName()).$(", error=").$(e.getFlyweightMessage()).I$();
                return false;
            }
            timestampType = tableReader.getMetadata().getTimestampType();
            partitionBy = tableReader.getPartitionedBy();
            tsColName = null;
            if (PartitionBy.isPartitioned(partitionBy)) {
                TableReaderMetadata meta = tableReader.getMetadata();
                tsColName = meta.getColumnName(meta.getTimestampIndex());
            }
            path.of(engine.getConfiguration().getDbRoot()).concat(token).$();
            rootLen = path.size();
            scanDetachedAndAttachablePartitions();
            limit = tableReader.getTxFile().getPartitionCount() +
                    attachablePartitions.size() +
                    detachedPartitions.size();
            partitionIndex = -1;
            return limit > 0;
        }

        private void loadNextPartition() {
            closeParquetMeta();
            isReadOnly = false;
            isActive = false;
            isDetached = false;
            isAttachable = false;
            isParquet = false;
            hasParquetGenerated = false;
            parquetFileSize = -1L;
            minTimestamp = Numbers.LONG_NULL;
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
                isDetached = true;
                int idx = partitionIndex - partitionCount;
                int n = detachedPartitions.size();
                if (idx < n) {
                    partitionName.put(detachedPartitions.get(idx));
                } else {
                    idx -= n;
                    if (idx < attachablePartitions.size()) {
                        partitionName.put(attachablePartitions.get(idx));
                        isAttachable = true;
                    }
                }
                assert !partitionName.isEmpty();

                dynamicPartitionIndex = Numbers.INT_NULL;
                if (ff.exists(path.concat(partitionName).concat(TableUtils.META_FILE_NAME).$())) {
                    try {
                        if (detachedMetaReader == null) {
                            detachedMetaReader = new TableReaderMetadata(engine.getConfiguration());
                        }
                        detachedMetaReader.loadMetadata(path.$());
                        TableToken currentToken = tableReader.getTableToken();
                        if (currentToken.getTableId() == detachedMetaReader.getTableId() && partitionBy == detachedMetaReader.getPartitionBy()) {
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
                        long lastOffset = (numRows - 1) * Long.BYTES;
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

        private class PartitionRecord implements Record {
            @Override
            public boolean getBool(int col) {
                return switch (col) {
                    case 9 -> isReadOnly;
                    case 10 -> isActive;
                    case 11 -> isReadOnly || !isDetached;
                    case 12 -> isDetached;
                    case 13 -> isAttachable;
                    case 14 -> hasParquetGenerated || isParquet;
                    case 15 -> isParquet;
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
                    case 4 -> minTimestamp;
                    case 5 -> maxTimestamp;
                    case 6 -> numRows;
                    case 7 -> partitionSize;
                    case 16 -> parquetFileSize;
                    default -> throw new UnsupportedOperationException();
                };
            }

            @Override
            public CharSequence getStrA(int col) {
                return switch (col) {
                    case 0 -> tableReader.getTableToken().getTableName();
                    case 2 -> PartitionBy.toString(partitionBy);
                    case 3 -> partitionName;
                    case 8 -> partitionSizeSink;
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
                    case 4 -> minTimestamp;
                    case 5 -> maxTimestamp;
                    default -> throw new UnsupportedOperationException();
                };
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(Column.TABLE_NAME.metadata());
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
        METADATA = metadata;
    }
}
