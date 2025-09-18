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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TableStorageRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final int DISK_SIZE = 5;
    private static final RecordMetadata METADATA;
    private static final int PARTITION_BY = 2;
    private static final int PARTITION_COUNT = 3;
    private static final int ROW_COUNT = 4;
    private static final int TABLE_NAME = 0;
    private static final int WAL_ENABLED = 1;
    private final CairoConfiguration configuration;
    private final TableStorageRecordCursor cursor = new TableStorageRecordCursor();
    private final CairoEngine engine;
    private final TxReader txReader;

    public TableStorageRecordCursorFactory(CairoEngine engine) {
        super(METADATA);
        this.configuration = engine.getConfiguration();
        this.engine = engine;
        this.txReader = new TxReader(configuration.getFilesFacade());
    }

    @Override
    public void _close() {
        Misc.free(cursor);
        Misc.free(txReader);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
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
            txReader.clear();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
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
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return tableBucket.size();
        }

        @Override
        public void toTop() {
            tableIndex = -1;
        }

        private TableStorageRecordCursor initialize() {
            engine.getTableTokens(tableBucket, false);
            toTop();
            return this;
        }

        private class TableStorageRecord implements Record {
            private long diskSize;
            private int partitionBy;
            private long partitionCount;
            private long rowCount;
            private CharSequence tableName;
            private boolean walEnabled;

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
                return getStrA(col);
            }

            @Override
            public int getStrLen(int col) {
                return TableUtils.lengthOf(getStrA(col));
            }

            private void getTableStats(@NotNull TableToken token) {
                walEnabled = token.isWal();
                tableName = token.getTableName();
                int timestampType;
                try (TableMetadata tm = engine.getTableMetadata(token)) {
                    partitionBy = tm.getPartitionBy();
                    timestampType = tm.getTimestampType();
                }

                final Path path = Path.getThreadLocal(configuration.getDbRoot()).concat(token.getDirName());
                diskSize = Files.getDirSize(path);

                // TxReader
                TableUtils.setTxReaderPath(txReader, path, timestampType, partitionBy); // modifies path
                rowCount = txReader.unsafeLoadRowCount();
                partitionCount = txReader.getPartitionCount();
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