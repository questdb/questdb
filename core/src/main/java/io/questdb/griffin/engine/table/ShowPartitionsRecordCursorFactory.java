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
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
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
        private CairoConfiguration cairoConfig;
        private int columnIndex;
        private int partitionIndex;
        TableToken tableToken;
        private TableReader reader;
        private final ShowPartitionsRecord record = new ShowPartitionsRecord();
        private TxReader txReader;
        private final StringSink sink = new StringSink();
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

        @Override
        public void close() {
            if (null != reader) {
                reader = Misc.free(reader);
                txReader = Misc.free(txReader);
                tableToken = null;
                columnIndex = 0;
                partitionIndex = 0;
                sink.clear();
            }
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
            if (partitionIndex < txReader.getPartitionCount() && columnIndex < METADATA.getColumnCount()) {
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
            return -1;
        }

        private ShowPartitionsRecordCursor of(SqlExecutionContext executionContext, TableToken tableToken) {
            this.tableToken = tableToken;
            cairoConfig = executionContext.getCairoEngine().getConfiguration();
            reader = executionContext.getReader(tableToken);
            TableReaderMetadata readerMetadata = reader.getMetadata();
            FilesFacade ff = cairoConfig.getFilesFacade();
            Path path = Path.PATH2.get().of(cairoConfig.getRoot()).concat(tableToken.getDirName());
            txReader = new TxReader(ff);
            path.concat(TableUtils.TXN_FILE_NAME).$();
            txReader.ofRO(path, readerMetadata.getPartitionBy());
            txReader.unsafeLoadAll();
            partitionIndex = 0;
            toTop();
            return this;
        }

        private class ShowPartitionsRecord implements Record {
            private boolean isActive;
            private boolean isReadOnly;
            private long minTimestamp = Long.MAX_VALUE;
            private long maxTimestamp = Long.MIN_VALUE;

            private long partitionSize = -1L;

            @Override
            public boolean getBool(int col) {
                if (Column.IS_READ_ONLY.is(col)) {
                    return isReadOnly;
                }
                if (Column.IS_ACTIVE.is(col)) {
                    return isActive;
                }
                if (Column.IS_ATTACHED.is(col)) {
                    return true; // TODO check detached folder for partitions
                }
                if (Column.IS_DETACHED.is(col)) {
                    return false; // TODO check detached folder for partitions
                }
                if (Column.IS_ATTACHABLE.is(col)) {
                    return false; // TODO check whether is soft link
                }
                if (Column.IS_WAL.is(col)) {
                    return reader.getMetadata().isWalEnabled();
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public int getInt(int col) {
                assert col == 0;
                // First column
                if (Column.PARTITION_INDEX.is(col)) {
                    isReadOnly = txReader.isPartitionReadOnly(partitionIndex);
                    isActive = txReader.getPartitionTimestamp(partitionIndex) == txReader.getLastPartitionTimestamp();
                    Path path = Path.PATH2.get().of(cairoConfig.getRoot()).concat(tableToken).slash$();
                    TableReaderMetadata meta = reader.getMetadata();
                    TableUtils.setPathForPartition(
                            path,
                            path.length(),
                            meta.getPartitionBy(),
                            txReader.getPartitionTimestamp(partitionIndex),
                            txReader.getPartitionNameTxn(partitionIndex));
                    path.$();
                    int pathLen = path.length();
                    FilesFacade ff = cairoConfig.getFilesFacade();
                    partitionSize = ff.getDirectoryContentSize(path);
                    if (PartitionBy.isPartitioned(meta.getPartitionBy())) {
                        dFile(path.trimTo(pathLen).slash$(), meta.getColumnName(meta.getTimestampIndex()), COLUMN_NAME_TXN_NONE);
                        int fd = TableUtils.openRO(ff, path, LOG);
                        try {
                            minTimestamp = ff.readNonNegativeLong(fd, 0);
                            maxTimestamp = ff.readNonNegativeLong(
                                    fd,
                                    (txReader.getPartitionSize(partitionIndex) - 1) * ColumnType.sizeOf(ColumnType.TIMESTAMP));
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
                    long cv = txReader.getPartitionColumnVersion(partitionIndex);
                    // Final column, increment partition
                    partitionIndex++;
                    return cv;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence getStr(int col) {
                if (Column.PARTITION_BY.is(col)) {
                    return PartitionBy.toString(reader.getMetadata().getPartitionBy());
                }
                if (Column.DISK_SIZE_HUMAN.is(col)) {
                    int z = Numbers.msb(partitionSize);
                    sink.clear();
                    formatter.format("%.1f %sB", (float) partitionSize / (1L << z), " KMGTPE".charAt(z / 10));
                    return Chars.toString(sink);
                }
                if (Column.TIMESTAMP.is(col)) {
                    sink.clear();
                    PartitionBy.setSinkForPartition(
                            sink,
                            reader.getMetadata().getPartitionBy(),
                            txReader.getPartitionTimestamp(partitionIndex),
                            false);
                    return Chars.toString(sink);
                }
                if (Column.LOCATION.is(col)) {
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
                // values retrieved when accessing the first column "index"
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
        // index: index of the partition 0..n-1, where n is the number of attached partitions, -1 otherwise
        PARTITION_INDEX(0, "index", ColumnType.INT),
        // partitionBy: YEAR, MONTH, DAY, HOUR, WEEK, NONE
        PARTITION_BY(1, "partitionBy", ColumnType.STRING),
        // timestamp: timestamp of the partition in human readable form
        TIMESTAMP(2, "timestamp", ColumnType.STRING),
        // minTimestamp: min timestamp in the partition
        MIN_TIMESTAMP(3, "minTimestamp", ColumnType.TIMESTAMP),
        // maxTimestamp: max timestamp in the partition
        MAX_TIMESTAMP(4, "maxTimestamp", ColumnType.TIMESTAMP),
        // numRows: size
        NUM_ROWS(5, "numRows", ColumnType.LONG),
        // diskSize: size in bytes in the file system
        DISK_SIZE(6, "diskSize", ColumnType.LONG),
        DISK_SIZE_HUMAN(7, "diskSizeHuman", ColumnType.STRING),
        // readOnly: can it be written to, or is it read only
        IS_READ_ONLY(8, "readOnly", ColumnType.BOOLEAN),
        // active: is it the partition we are currently appending to
        IS_ACTIVE(9, "active", ColumnType.BOOLEAN),
        // attached: is it attached, or not (table folder, and additional detached path)
        IS_ATTACHED(10, "attached", ColumnType.BOOLEAN),
        // detached: is it detached
        IS_DETACHED(11, "detached", ColumnType.BOOLEAN),
        // attachable: is it detacched and attachable
        IS_ATTACHABLE(12, "attachable", ColumnType.BOOLEAN),
        // wal: is it a wal table
        IS_WAL(13, "wal", ColumnType.BOOLEAN),
        // location: path to the parent folder of the partition in disk
        LOCATION(14, "location", ColumnType.STRING),
        // txn: current transaction version, available from _txn, if attached
        TXN(15, "txn", ColumnType.LONG),
        // cv: current column version, available from _txn, if attached
        CV(16, "cv", ColumnType.LONG);

        private final TableColumnMetadata metadata;
        private final int idx;

        Column(int idx, String name, int type) {
            this.idx = idx;
            this.metadata = new TableColumnMetadata(name, type);
        }

        public boolean is(int idx) {
            return this.idx == idx;
        }

        public TableColumnMetadata metadata() {
            return metadata;
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(Column.PARTITION_INDEX.metadata());
        metadata.add(Column.PARTITION_BY.metadata());
        metadata.add(Column.TIMESTAMP.metadata());
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
        metadata.add(Column.IS_WAL.metadata());
        metadata.add(Column.LOCATION.metadata());
        metadata.add(Column.TXN.metadata());
        metadata.add(Column.CV.metadata());
        METADATA = metadata;
    }
}
