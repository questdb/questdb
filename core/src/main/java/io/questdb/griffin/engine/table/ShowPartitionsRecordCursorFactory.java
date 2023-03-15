/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.str.SizePrettyFunctionFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

public class ShowPartitionsRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final Log LOG = LogFactory.getLog(ShowPartitionsRecordCursor.class);
    private static final RecordMetadata METADATA;

    private final ShowPartitionsRecordCursor cursor = new ShowPartitionsRecordCursor();
    private final TableToken tableToken;

    public ShowPartitionsRecordCursorFactory(TableToken tableToken) {
        super(METADATA);
        this.tableToken = tableToken;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        return cursor.of(executionContext, tableToken);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show_partitions").meta("of").val(tableToken);
    }

    private enum Column {
        PARTITION_INDEX(0, "index", ColumnType.INT),
        PARTITION_BY(1, "partitionBy", ColumnType.STRING),
        PARTITION_NAME(2, "name", ColumnType.STRING),
        MIN_TIMESTAMP(3, "minTimestamp", ColumnType.TIMESTAMP),
        MAX_TIMESTAMP(4, "maxTimestamp", ColumnType.TIMESTAMP),
        NUM_ROWS(5, "numRows", ColumnType.LONG),
        DISK_SIZE(6, "diskSize", ColumnType.LONG),
        DISK_SIZE_HUMAN(7, "diskSizeHuman", ColumnType.STRING),
        IS_READ_ONLY(8, "readOnly", ColumnType.BOOLEAN),
        IS_ACTIVE(9, "active", ColumnType.BOOLEAN),
        IS_ATTACHED(10, "attached", ColumnType.BOOLEAN),
        IS_DETACHED(11, "detached", ColumnType.BOOLEAN),
        IS_ATTACHABLE(12, "attachable", ColumnType.BOOLEAN);

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

    private static class ShowPartitionsRecordCursor implements RecordCursor {
        private final ObjList<String> attachablePartitions = new ObjList<>(4);
        private final ObjList<String> detachedPartitions = new ObjList<>(8);
        private final PartitionsRecord partitionRecord = new PartitionsRecord();
        private final StringSink sink = new StringSink();
        private final TxReader tableTxReader = new TxReader(FilesFacadeImpl.INSTANCE);
        private CairoConfiguration cairoConfig;
        private int dynamicPartitionIndex = -1;
        private int limit; // partitionCount + detached + attachable
        private int partitionBy = -1;
        private int partitionIndex = -1;
        private TableToken tableToken;
        private CharSequence tsColName;

        @Override
        public void close() {
            Misc.free(tableTxReader);
            Misc.free(partitionRecord);
            attachablePartitions.clear();
            detachedPartitions.clear();
            sink.clear();
            tableToken = null;
            cairoConfig = null;
            tsColName = null;
            partitionIndex = -1;
            limit = 0;
            partitionBy = -1;
        }

        @Override
        public Record getRecord() {
            return partitionRecord;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            if (++partitionIndex < limit) {
                partitionRecord.loadNextPartition();
                return true;
            }
            --partitionIndex;
            return false;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            return limit;
        }

        @Override
        public void toTop() {
            partitionIndex = -1;
        }

        private void findDetachedAndAttachablePartitions(Path path) {
            FilesFacade ff = cairoConfig.getFilesFacade();
            long pFind = ff.findFirst(path);
            if (pFind > 0L) {
                try {
                    attachablePartitions.clear();
                    detachedPartitions.clear();
                    do {
                        sink.clear();
                        Chars.utf8DecodeZ(ff.findName(pFind), sink);
                        int type = ff.findType(pFind);
                        if ((type == Files.DT_LNK || type == Files.DT_DIR) && Chars.endsWith(sink, TableUtils.ATTACHABLE_DIR_MARKER)) {
                            attachablePartitions.add(Chars.toString(sink));
                        } else if (type == Files.DT_DIR && Chars.endsWith(sink, TableUtils.DETACHED_DIR_MARKER)) {
                            detachedPartitions.add(Chars.toString(sink));
                        }
                    } while (ff.findNext(pFind) > 0);
                    attachablePartitions.sort(Chars::compare);
                    detachedPartitions.sort(Chars::compare);
                } finally {
                    ff.findClose(pFind);
                }
            }
        }

        private ShowPartitionsRecordCursor of(SqlExecutionContext executionContext, TableToken tableToken) {
            int partitionBy;
            String tsColName = null;
            try (TableReader reader = executionContext.getReader(tableToken)) {
                partitionBy = reader.getPartitionedBy();
                if (PartitionBy.isPartitioned(partitionBy)) {
                    TableReaderMetadata meta = reader.getMetadata();
                    tsColName = meta.getColumnName(meta.getTimestampIndex());
                }
            }
            cairoConfig = executionContext.getCairoEngine().getConfiguration();
            this.partitionBy = partitionBy;
            this.tsColName = tsColName;
            this.tableToken = tableToken;
            Path path = Path.getThreadLocal(cairoConfig.getRoot()).concat(tableToken.getDirName()).$();
            findDetachedAndAttachablePartitions(path);
            path.concat(TableUtils.TXN_FILE_NAME).$();
            tableTxReader.ofRO(path, partitionBy);
            tableTxReader.unsafeLoadAll();
            limit = tableTxReader.getPartitionCount() + attachablePartitions.size() + detachedPartitions.size();
            toTop();
            return this;
        }

        private class PartitionsRecord implements Record, Closeable {
            private TableReaderMetadata detachedMetaReader;
            private TxReader detachedTxReader;
            private boolean isActive;
            private boolean isAttachable;
            private boolean isDetached;
            private boolean isReadOnly;
            private long maxTimestamp = Long.MIN_VALUE;
            private long minTimestamp = Long.MIN_VALUE; // so that in absence of metadata is NaN
            private long numRows = -1L;
            private String partitionName;
            private long partitionSize = -1L;

            @Override
            public void close() {
                detachedMetaReader = Misc.free(detachedMetaReader);
                detachedTxReader = Misc.free(detachedTxReader);
            }

            @Override
            public boolean getBool(int col) {
                if (Column.IS_READ_ONLY.is(col)) {
                    return isReadOnly;
                }
                if (Column.IS_ACTIVE.is(col)) {
                    return isActive;
                }
                if (Column.IS_ATTACHED.is(col)) {
                    return isReadOnly || !isDetached;
                }
                if (Column.IS_DETACHED.is(col)) {
                    return isDetached;
                }
                if (Column.IS_ATTACHABLE.is(col)) {
                    return isAttachable;
                }
                throw new UnsupportedOperationException();
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
                if (Column.NUM_ROWS.is(col)) {
                    return numRows;
                }
                if (Column.DISK_SIZE.is(col)) {
                    return partitionSize;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence getStr(int col) {
                if (Column.PARTITION_BY.is(col)) {
                    return PartitionBy.toString(partitionBy);
                }
                if (Column.DISK_SIZE_HUMAN.is(col)) {
                    return SizePrettyFunctionFactory.toSizePretty(partitionSize);
                }
                if (Column.PARTITION_NAME.is(col)) {
                    return partitionName;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStr(col);
            }

            @Override
            public int getStrLen(int col) {
                CharSequence s = getStr(col);
                return s != null ? s.length() : TableUtils.NULL_LEN;
            }

            @Override
            public long getTimestamp(int col) {
                if (Column.MIN_TIMESTAMP.is(col)) {
                    return minTimestamp;
                }
                if (Column.MAX_TIMESTAMP.is(col)) {
                    return maxTimestamp;
                }
                throw new UnsupportedOperationException();
            }

            private void loadNextPartition() {
                isReadOnly = false;
                isActive = false;
                isDetached = false;
                isAttachable = false;
                minTimestamp = Long.MIN_VALUE; // so that in absence of metadata is NaN
                maxTimestamp = Long.MIN_VALUE;
                numRows = -1L;
                partitionName = null;
                partitionSize = -1L;
                dynamicPartitionIndex = partitionIndex;
                CharSequence dynamicTsColName = tsColName;
                FilesFacade ff = cairoConfig.getFilesFacade();
                Path path = Path.getThreadLocal(cairoConfig.getRoot()).concat(tableToken.getDirName()).$();
                int partitionCount = tableTxReader.getPartitionCount();
                if (partitionIndex < partitionCount) {
                    // we are within the partition table
                    isReadOnly = tableTxReader.isPartitionReadOnly(partitionIndex);
                    long timestamp = tableTxReader.getPartitionTimestamp(partitionIndex);
                    isActive = timestamp == tableTxReader.getLastPartitionTimestamp();
                    sink.clear();
                    PartitionBy.setSinkForPartition(sink, partitionBy, timestamp, false);
                    TableUtils.txnPartitionConditionally(path.concat(sink), tableTxReader.getPartitionNameTxn(partitionIndex));
                    partitionName = Chars.toString(sink);
                    numRows = tableTxReader.getPartitionSize(partitionIndex);
                } else {
                    // partition table is over
                    // we will iterate over detached and attachable
                    isDetached = true;
                    int idx = partitionIndex - partitionCount; // index in detachedPartitions
                    int n = detachedPartitions.size();
                    if (idx < n) {
                        // is detached
                        partitionName = detachedPartitions.get(idx);
                    } else {
                        idx -= n; // index in attachablePartitions
                        if (idx < attachablePartitions.size()) {
                            // is attachable, also detached
                            partitionName = attachablePartitions.get(idx);
                            isAttachable = true;
                        }
                    }
                    assert partitionName != null;
                    // open meta files if they exist
                    dynamicPartitionIndex = Integer.MIN_VALUE; // so that in absence of metadata is NaN
                    path.concat(partitionName).concat(TableUtils.META_FILE_NAME).$();
                    if (ff.exists(path)) {
                        if (detachedMetaReader == null) {
                            detachedMetaReader = new TableReaderMetadata(cairoConfig);
                        }
                        detachedMetaReader.load(path);
                        if (tableToken.getTableId() == detachedMetaReader.getTableId() && partitionBy == detachedMetaReader.getPartitionBy()) {
                            path.parent().concat(TableUtils.TXN_FILE_NAME).$();
                            if (ff.exists(path)) {
                                if (detachedTxReader == null) {
                                    detachedTxReader = new TxReader(FilesFacadeImpl.INSTANCE);
                                }
                                detachedTxReader.ofRO(path, partitionBy);
                                detachedTxReader.unsafeLoadAll();
                                long timestamp = PartitionBy.parsePartitionDirName(partitionName, partitionBy);
                                int pIndex = detachedTxReader.getPartitionIndex(timestamp);
                                numRows = detachedTxReader.getPartitionSize(pIndex);
                                if (PartitionBy.isPartitioned(partitionBy) && numRows > 0L) {
                                    int tsIndex = detachedMetaReader.getTimestampIndex();
                                    dynamicTsColName = detachedMetaReader.getColumnName(tsIndex);
                                }
                                // could set dynamicPartitionIndex to -pIndex
                            } else {
                                LOG.error().$("detached partition does not have meta file [path=").$(path).I$();
                            }
                        } else {
                            LOG.error().$("detached partition meta does not match [path=").$(path).I$();
                        }
                    } else {
                        LOG.error().$("detached partition does not have meta file [path=").$(path).I$();
                    }
                    path.parent();
                }
                partitionSize = ff.getDirectoryContentSize(path.$());
                if (PartitionBy.isPartitioned(partitionBy) && numRows > 0L) {
                    TableUtils.dFile(path.slash$(), dynamicTsColName, TableUtils.COLUMN_NAME_TXN_NONE);
                    int fd = -1;
                    try {
                        fd = TableUtils.openRO(ff, path, LOG);
                        long lastOffset = (numRows - 1) * ColumnType.sizeOf(ColumnType.TIMESTAMP);
                        minTimestamp = ff.readNonNegativeLong(fd, 0);
                        maxTimestamp = ff.readNonNegativeLong(fd, lastOffset);
                    } catch (CairoException e) {
                        if (partitionIndex < partitionCount) {
                            throw CairoException.critical(ff.errno()).put("no file found for designated timestamp column [path=").put(path).put(']');
                        }
                        dynamicPartitionIndex = Integer.MIN_VALUE;
                        LOG.error().$("no file found for designated timestamp column [path=").$(path).I$();
                    } finally {
                        ff.close(fd);
                    }
                }
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
        METADATA = metadata;
    }
}
