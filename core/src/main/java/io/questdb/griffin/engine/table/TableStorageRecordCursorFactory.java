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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Files;
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class TableStorageRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final int DISK_SIZE = 5;
    private static final RecordMetadata METADATA;
    private static final int PARTITION_BY = 2;
    private static final int PARTITION_COUNT = 3;
    private static final int ROW_COUNT = 4;
    private static final int TABLE_NAME = 0;
    private static final int WAL_ENABLED = 1;
    private final TableStorageRecordCursor cursor = new TableStorageRecordCursor();
    private final Path path = new Path();
    private CairoConfiguration configuration;
    private SqlExecutionContext executionContext;
    private TxReader reader;


    public TableStorageRecordCursorFactory() {
        super(METADATA);
    }

    @Override
    public void _close() {
        cursor.close();
        executionContext = null;
        configuration = null;
        path.close();
        reader.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        this.executionContext = executionContext;
        this.configuration = executionContext.getCairoEngine().getConfiguration();
        reader = new TxReader(configuration.getFilesFacade());
        return cursor.initialize();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("table_storage()");
    }

    private class TableStorageRecordCursor implements NoRandomAccessRecordCursor {
        private final TableStorageRecord record = new TableStorageRecord();
        private final ObjHashSet<TableToken> tableBucket = new ObjHashSet<>();
        private int tableIndex = -1;

        @Override
        public void close() {
            tableBucket.clear();
            record.close();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() throws DataUnavailableException {
            ++tableIndex;
            int n = tableBucket.size();

            if (tableIndex >= n) {
                return false;
            }

            TableToken token;
            do {
                token = tableBucket.get(tableIndex);
                if (!token.isSystem()) {
                    record.getTableStats(token);
                    return true;
                }
                tableIndex++;
            } while (tableIndex < n);

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

        private TableStorageRecordCursor initialize() {
            executionContext.getCairoEngine().getTableTokens(tableBucket, false);
            toTop();
            return this;
        }

        private class TableStorageRecord implements Record, Closeable {
            private long diskSize;
            private int partitionBy;
            private long partitionCount;
            private long rowCount;
            private CharSequence tableName;
            private boolean walEnabled;

            @Override
            public void close() {

            }

            @Override
            public boolean getBool(int col) {
                if (col == WAL_ENABLED) {
                    return walEnabled;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public long getLong(int col) {
                switch (col) {
                    case PARTITION_COUNT:
                        return partitionCount;
                    case ROW_COUNT:
                        return rowCount;
                    case DISK_SIZE:
                        return diskSize;
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            @Override
            public @Nullable CharSequence getStrA(int col) {
                switch (col) {
                    case TABLE_NAME:
                        return tableName;
                    case PARTITION_BY:
                        return PartitionBy.toString(partitionBy);
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            @Override
            public @Nullable CharSequence getStrB(int col) {
                switch (col) {
                    case TABLE_NAME:
                        return tableName;
                    case PARTITION_BY:
                        return PartitionBy.toString(partitionBy);
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            private void getTableStats(@NotNull TableToken token) {
                reset();
                walEnabled = token.isWal();
                tableName = token.getTableName();

                // Metadata
                TableMetadata tm = executionContext.getMetadataForRead(token);
                partitionBy = tm.getPartitionBy();
                tm.close();

                // Path
                TableUtils.setPathTable(path, configuration, token);
                diskSize = Files.getDirSize(path);

                // TxReader
                TableUtils.setTxReaderPath(reader, path, partitionBy); // modifies path
                rowCount = reader.unsafeLoadRowCount();
                partitionCount = reader.getPartitionCount();
                reader.close();
            }

            private void reset() {
                rowCount = -1;
                partitionCount = -1;
                diskSize = -1;
                path.close();
                reader.close();
            }
        }
    }

    static {
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

