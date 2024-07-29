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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class ShowAllTablePartitionsCursoryFactory extends AbstractRecordCursorFactory {
    private static final int TABLE_NAME;
    private static final int WAL_ENABLED;
    private static final int PARTITION_BY;
    private static final int PARTITION_COUNT;
    private static final int ROW_COUNT;
    private static final int DISK_SIZE;
    private static final RecordMetadata METADATA;
    private SqlExecutionContext executionContext;
    private final AllTablePartitionListRecordCursor cursor = new AllTablePartitionListRecordCursor();
    private CairoConfiguration cairoConfig;
    private FilesFacade ff;


    public ShowAllTablePartitionsCursoryFactory() {
        super(METADATA);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        this.executionContext = executionContext;
        this.cairoConfig = executionContext.getCairoEngine().getConfiguration();
        this.ff = cairoConfig.getFilesFacade();
        return cursor.initialize();
    }

    @Override
    public void _close() {
        cursor.close();
        executionContext = null;
        ff = null;
        cairoConfig = null;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("all_table_partition_storage");
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    private class AllTablePartitionListRecordCursor implements NoRandomAccessRecordCursor {
        private final ObjHashSet<TableToken> tableBucket = new ObjHashSet<>();
        private Path path = new Path();
        private int tableIndex = -1;
        private final AllPartitionsRecord record = new AllPartitionsRecord();

        private AllTablePartitionListRecordCursor initialize() {
            executionContext.getCairoEngine().getTableTokens(tableBucket, false);
            toTop();
            return this;
        }

        @Override
        public void close() {
            tableBucket.clear();
            Misc.free(path);
            record.close();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() throws DataUnavailableException {
            tableIndex++;
            int n = tableBucket.size();
            for (; tableIndex < n; tableIndex++) {
                TableToken tableToken = tableBucket.get(tableIndex);
                if (tableToken.isSystem())
                    continue;
                record.loadPartitionDetailsForCurrentTable(tableToken);
                return true;
            }
            return false;
        }

        @Override
        public long size() throws DataUnavailableException {
            return tableBucket.size();
        }

        @Override
        public void toTop() {
            tableIndex = -1;
        }

        private class AllPartitionsRecord implements Record, Closeable {
            private CharSequence tableName;
            private int partitionBy;
            private long partitionCount;
            private long rowCount;
            private long sizeB;
            private boolean walEnabled;

            @Override
            public void close() {

            }

            @Override
            public long getLong(int col) {
                switch (col) {
                    case 3:
                        return partitionCount;
                    case 4:
                        return rowCount;
                    case 5:
                        return sizeB;
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            @Override
            public boolean getBool(int col) {
                switch (col) {
                    case 1:
                        return walEnabled;
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            @Override
            public @Nullable CharSequence getStrA(int col) {
                switch (col) {
                    case 0:
                        return tableName;
                    case 2:
                        return PartitionBy.toString(partitionBy);
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            @Override
            public @Nullable CharSequence getStrB(int col) {
                switch (col) {
                    case 0:
                        return tableName;
                    case 2:
                        return PartitionBy.toString(partitionBy);
                    default:
                        throw new UnsupportedOperationException();
                }
            }


            private void loadPartitionDetailsForCurrentTable(TableToken tableToken) {
                rowCount = 0;
                partitionCount = 0;
                sizeB = 0;
                TableReader tableReader = executionContext.getReader(tableToken);
                TxReader tableTxReader = tableReader.getTxFile();
                int partitionIndex = 0;
                tableName = tableToken.getTableName();
                walEnabled = tableToken.isWal();
                partitionBy = tableReader.getPartitionedBy();
                path.of(cairoConfig.getRoot()).concat(tableToken).$();
                int rootLen = path.size();
                int attachedPartitions = tableTxReader.getPartitionCount();
                partitionCount = attachedPartitions;
                while (partitionIndex < partitionCount) {
                    path.trimTo(rootLen).$();
                    if (partitionIndex < attachedPartitions) {
                        long timestamp = tableTxReader.getPartitionTimestampByIndex(partitionIndex);
                        TableUtils.setPathForPartition
                                (path, partitionBy, timestamp, tableTxReader.getPartitionNameTxn(partitionIndex));
                        rowCount += tableTxReader.getPartitionSize(partitionIndex);
                    }
                    sizeB += ff.getDirSize(path);
                    partitionIndex++;
                    tableReader = Misc.free(tableReader);
                }
            }
        }
    }

    static {
        TABLE_NAME = 0;
        WAL_ENABLED = 1;
        PARTITION_BY = 2;
        PARTITION_COUNT = 3;
        ROW_COUNT = 4;
        DISK_SIZE = 5;

        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("tableName", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("walEnabled", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("partitionBy", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("partitionCount", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("rowCount", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("diskSize", ColumnType.LONG));
        METADATA = metadata;
    }

}

