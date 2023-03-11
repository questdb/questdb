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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.util.Formatter;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static io.questdb.cairo.TableUtils.dFile;

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
        sink.type("show_partitions");
        sink.meta("of").val(tableToken);
    }

    private static class ShowPartitionsRecordCursor implements RecordCursor {
        private int columnIndex;
        private final ObjHashSet<String> detachedPartitions = new ObjHashSet<>(8);
        private final ObjHashSet<String> attachablePartitions = new ObjHashSet<>(4);
        private FilesFacade ff;
        private final Formatter formatter = new Formatter(new Appendable() {
            @Override
            public Appendable append(CharSequence csq) {
                sink.put(csq);
                return this;
            }

            @Override
            public Appendable append(CharSequence csq, int start, int end) {
                sink.put(csq, start, end);
                return this;
            }

            @Override
            public Appendable append(char c) {
                sink.put(c);
                return this;
            }
        });
        private int partitionBy;
        private int partitionIndex;
        private final ShowPartitionsRecord record = new ShowPartitionsRecord();
        private CharSequence root;
        private final StringSink sink = new StringSink();
        private TableToken tableToken;
        private CharSequence tsColName;
        private final TxReader txReader = new TxReader(FilesFacadeImpl.INSTANCE);

        @Override
        public void close() {
            Misc.free(txReader);
            attachablePartitions.clear();
            detachedPartitions.clear();
            sink.clear();
            tableToken = null;
            tsColName = null;
            root = null;
            columnIndex = 0;
            partitionIndex = 0;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            if (partitionIndex < size() && columnIndex < METADATA.getColumnCount()) {
                columnIndex++;
                return true;
            }
            return false;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void toTop() {
            columnIndex = 0;
            partitionIndex = 0;
        }

        @Override
        public long size() {
            return txReader.getPartitionCount() + attachablePartitions.size() + detachedPartitions.size();
        }

        private ShowPartitionsRecordCursor of(SqlExecutionContext executionContext, TableToken tableToken) {
            try (TableReader reader = executionContext.getReader(tableToken)) {
                TableReaderMetadata meta = reader.getMetadata();
                partitionBy = meta.getPartitionBy();
                if (PartitionBy.isPartitioned(partitionBy)) {
                    tsColName = meta.getColumnName(meta.getTimestampIndex());
                }
            }
            this.tableToken = tableToken;
            CairoConfiguration cairoConfig = executionContext.getCairoEngine().getConfiguration();
            ff = cairoConfig.getFilesFacade();
            root = cairoConfig.getRoot();
            Path path = Path.PATH2.get().of(root).concat(tableToken.getDirName()).$();
            findDetachedAndAttachablePartitions(path);
            path.concat(TableUtils.TXN_FILE_NAME).$();
            txReader.ofRO(path, partitionBy);
            txReader.unsafeLoadAll();
            partitionIndex = 0;
            toTop();
            return this;
        }

        private void findDetachedAndAttachablePartitions(Path path) {
            long pFind = Files.findFirst(path);
            if (pFind > 0L) {
                try {
                    attachablePartitions.clear();
                    detachedPartitions.clear();
                    while (Files.findNext(pFind) > 0) {
                        sink.clear();
                        Chars.utf8DecodeZ(Files.findName(pFind), sink);
                        int type = Files.findType(pFind);
                        if (type == Files.DT_LNK && Chars.endsWith(sink, TableUtils.ATTACHABLE_DIR_MARKER)) {
                            attachablePartitions.add(Chars.toString(sink));
                        } else if (type == Files.DT_DIR && Chars.endsWith(sink, TableUtils.DETACHED_DIR_MARKER)) {
                            detachedPartitions.add(Chars.toString(sink));
                        }
                    }
                } finally {
                    Files.findClose(pFind);
                }
            }
        }

        private class ShowPartitionsRecord implements Record {
            private boolean isActive;
            private boolean isDetached;
            private boolean isAttachable;
            private boolean isReadOnly;
            private long minTimestamp = Long.MAX_VALUE;
            private long maxTimestamp = Long.MIN_VALUE;
            private long partitionSize = -1L;

            @Override
            public boolean getBool(int col) {
                if (Column.IS_READ_ONLY.is(col)) {
                    // if it is read only, it has been attached via soft link
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
                    // First column
                    assert col == 0;
                    int partitionCount = txReader.getPartitionCount();
                    Path path = Path.PATH2.get().of(root).concat(tableToken);
                    if (partitionIndex < partitionCount) {
                        // we are within the partition table
                        isReadOnly = txReader.isPartitionReadOnly(partitionIndex);
                        isActive = txReader.getPartitionTimestamp(partitionIndex) == txReader.getLastPartitionTimestamp();
                        isDetached = false;
                        isAttachable = false;
                        TableUtils.setPathForPartition(
                                path.slash$(),
                                path.length(),
                                partitionBy,
                                txReader.getPartitionTimestamp(partitionIndex),
                                txReader.getPartitionNameTxn(partitionIndex));
                        path.$();
                    } else {
                        // partition table is over, we will iterate over detached and attachable
                        isReadOnly = false; // it cannot be read at all
                        isActive = false; // not attached
                        isDetached = true;
                        isAttachable = false;
                        int pIdx = partitionIndex - partitionCount; // index in detachedPartitions
                        int n = detachedPartitions.size();
                        String partitionName = null;
                        if (pIdx < n) {
                            partitionName = detachedPartitions.get(pIdx);
                        } else {
                            pIdx -= n; // index in attachablePartitions
                            if (pIdx < attachablePartitions.size()) {
                                partitionName = attachablePartitions.get(pIdx);
                                isAttachable = true;
                            }
                        }
                        assert partitionName != null;
                        path.concat(partitionName).$();
                    }

                    partitionSize = ff.getDirectoryContentSize(path);
                    if (PartitionBy.isPartitioned(partitionBy)) {
                        dFile(path.slash$(), tsColName, COLUMN_NAME_TXN_NONE);
                        int fd = TableUtils.openRO(ff, path, LOG);
                        minTimestamp = Long.MAX_VALUE;
                        maxTimestamp = Long.MIN_VALUE;
                        try {
                            minTimestamp = ff.readNonNegativeLong(fd, 0);
                            long lastOffset = (txReader.getPartitionSize(partitionIndex) - 1) * ColumnType.sizeOf(ColumnType.TIMESTAMP);
                            maxTimestamp = ff.readNonNegativeLong(fd, lastOffset);
                        } finally {
                            ff.close(fd);
                        }
                    }
                    return partitionIndex;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public long getLong(int col) {
                if (Column.NUM_ROWS.is(col)) {
                    return txReader.getPartitionSize(partitionIndex);
                }
                if (Column.DISK_SIZE.is(col)) {
                    return partitionSize;
                }
                if (Column.TXN.is(col)) {
                    return txReader.getPartitionNameTxn(partitionIndex);
                }
                if (Column.CV.is(col)) {
                    assert Column.CV.idx == Column.values().length - 1;
                    // Final column, increment partition
                    long cv = txReader.getPartitionColumnVersion(partitionIndex);
                    partitionIndex++;
                    return cv;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence getStr(int col) {
                if (Column.PARTITION_BY.is(col)) {
                    return PartitionBy.toString(partitionBy);
                }
                if (Column.DISK_SIZE_HUMAN.is(col)) {
                    int z = Numbers.msb(partitionSize) / 10;
                    sink.clear(); // ' KMGTPEZ': _, Kilo, Mega, Giga, Tera, Peta, Exa, Zetta
                    formatter.format("%.1f %sB", (float) partitionSize / (1L << z * 10), " KMGTPEZ".charAt(z));
                    return Chars.toString(sink);
                }
                if (Column.PARTITION_NAME.is(col)) {
                    sink.clear();
                    PartitionBy.setSinkForPartition(
                            sink,
                            partitionBy,
                            txReader.getPartitionTimestamp(partitionIndex),
                            false);
                    return Chars.toString(sink);
                }
                if (Column.LOCATION.is(col)) {
                    assert Column.LOCATION.idx > Column.PARTITION_NAME.idx;
                    return tableToken.getDirName();
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStr(col);
            }

            @Override
            public int getStrLen(int col) {
                return getStr(col).length();
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
        }
    }

    private enum Column {
        PARTITION_INDEX(0, "index", ColumnType.INT),
        PARTITION_BY(1, "partitionBy", ColumnType.STRING),
        PARTITION_NAME(2, "name", ColumnType.STRING),
        MIN_TIMESTAMP(3, "minTimestamp", ColumnType.TIMESTAMP),
        MAX_TIMESTAMP(4, "maxTimestamp", ColumnType.TIMESTAMP),
        NUM_ROWS(5, "numRows", ColumnType.LONG),
        DISK_SIZE(6, "diskSize (bytes)", ColumnType.LONG),
        DISK_SIZE_HUMAN(7, "diskSizeHuman", ColumnType.STRING),
        IS_READ_ONLY(8, "readOnly", ColumnType.BOOLEAN),
        IS_ACTIVE(9, "active", ColumnType.BOOLEAN),
        IS_ATTACHED(10, "attached", ColumnType.BOOLEAN),
        IS_DETACHED(11, "detached", ColumnType.BOOLEAN),
        IS_ATTACHABLE(12, "attachable", ColumnType.BOOLEAN),
        LOCATION(13, "location", ColumnType.STRING),
        TXN(14, "txn", ColumnType.LONG),
        CV(15, "cv", ColumnType.LONG);

        private final TableColumnMetadata metadata;
        private final int idx;

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
        metadata.add(Column.LOCATION.metadata());
        metadata.add(Column.TXN.metadata());
        metadata.add(Column.CV.metadata());
        METADATA = metadata;
    }
}
