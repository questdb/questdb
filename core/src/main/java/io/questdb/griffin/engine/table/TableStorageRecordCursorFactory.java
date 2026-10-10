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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableDiskSizeCache;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Backs {@code table_storage()}: one row per user table with its partitioning, partition count,
 * row count and on-disk size.
 * <p>
 * The record loads each value on first access, so a query reads only the files its projection
 * and filter need: the table name and the WAL flag come from the table token, the partitioning
 * from the table metadata, the counts from {@code _txn}, and the disk size from the engine's
 * {@link TableDiskSizeCache}. {@code SELECT tableName, rowCount FROM table_storage()} never walks
 * a table directory, and a filter on the table name walks only the directories of the tables
 * that pass it.
 * <p>
 * A table dropped or renamed after the cursor listed it keeps its row, with NULL in the columns
 * derived from its files.
 */
public class TableStorageRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final int DISK_SIZE = 5;
    private static final int LOADED_DISK_SIZE = 4;
    private static final int LOADED_METADATA = 1;
    private static final int LOADED_TXN = 2;
    private static final int LOADED_ALL = LOADED_METADATA | LOADED_TXN | LOADED_DISK_SIZE;
    private static final RecordMetadata METADATA;
    private static final int PARTITION_BY = 2;
    private static final int PARTITION_COUNT = 3;
    private static final int ROW_COUNT = 4;
    private static final int TABLE_NAME = 0;
    private static final int WAL_ENABLED = 1;
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private TableStorageRecordCursor cursor = new TableStorageRecordCursor();
    private Path path;
    private TxReader txReader;

    public TableStorageRecordCursorFactory(CairoEngine engine) {
        super(METADATA);
        this.configuration = engine.getConfiguration();
        this.engine = engine;
        this.txReader = new TxReader(configuration.getFilesFacade());
        this.path = new Path();
    }

    @Override
    public void _close() {
        // Close the cursor before nulling txReader: TableStorageRecordCursor.close()
        // clears the outer txReader field, so the field must stay set until the cursor
        // close attempt completes. freeBestEffort() never throws, so the fields are
        // always nulled even when a close fails.
        Throwable failure = Misc.freeBestEffort(null, cursor);
        this.cursor = null;
        failure = Misc.freeBestEffort(failure, txReader);
        this.txReader = null;
        failure = Misc.freeBestEffort(failure, path);
        this.path = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        return cursor.of(executionContext.getCircuitBreaker());
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
        private final ObjList<TableToken> tables = new ObjList<>();
        private SqlExecutionCircuitBreaker circuitBreaker;
        private int tableIndex = -1;

        @Override
        public void close() {
            tableBucket.clear();
            tables.clear();
            // The factory nulls txReader once it is freed; a cursor closed after the
            // factory (late close on an error path) must not dereference it.
            final TxReader txReader = TableStorageRecordCursorFactory.this.txReader;
            if (txReader != null) {
                txReader.clear();
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            if (tableIndex + 1 < tables.size()) {
                record.of(tables.getQuick(++tableIndex));
                return true;
            }
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return tables.size();
        }

        @Override
        public void toTop() {
            tableIndex = -1;
        }

        private TableStorageRecordCursor of(SqlExecutionCircuitBreaker circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
            engine.getTableTokens(tableBucket, false);
            // drop system tables up front, so that size() matches the rows hasNext() returns
            tables.clear();
            for (int i = 0, n = tableBucket.size(); i < n; i++) {
                final TableToken token = tableBucket.get(i);
                if (!token.isSystem()) {
                    tables.add(token);
                }
            }
            toTop();
            return this;
        }

        private class TableStorageRecord implements Record {
            private long diskSize;
            private boolean isTableGone;
            // bit set of LOADED_* flags for the current row
            private int loaded;
            private int partitionBy;
            private long partitionCount;
            private long rowCount;
            private int timestampType;
            private TableToken token;

            @Override
            public boolean getBool(int col) {
                if (col == WAL_ENABLED) {
                    return token.isWal();
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public long getLong(int col) {
                return switch (col) {
                    case PARTITION_COUNT -> {
                        loadTxn();
                        yield partitionCount;
                    }
                    case ROW_COUNT -> {
                        loadTxn();
                        yield rowCount;
                    }
                    case DISK_SIZE -> {
                        loadDiskSize();
                        yield diskSize;
                    }
                    default -> throw new UnsupportedOperationException();
                };
            }

            @Override
            public @Nullable CharSequence getStrA(int col) {
                return switch (col) {
                    case TABLE_NAME -> token.getTableName();
                    case PARTITION_BY -> {
                        loadMetadata();
                        yield isTableGone ? null : PartitionBy.toString(partitionBy);
                    }
                    default -> throw new UnsupportedOperationException();
                };
            }

            @Override
            public @Nullable CharSequence getStrB(int col) {
                return getStrA(col);
            }

            @Override
            public int getStrLen(int col) {
                return TableUtils.lengthOf(getStrA(col));
            }

            // Checks whether the table was dropped or renamed after the cursor listed it.
            private boolean isTokenStale() {
                final TableToken current = engine.getTableTokenIfExists(token.getTableName());
                return current == null || !current.equals(token);
            }

            private void loadDiskSize() {
                loadTxn();
                if ((loaded & LOADED_DISK_SIZE) == 0) {
                    loaded |= LOADED_DISK_SIZE;
                    // uses the _txn snapshot that loadTxn() left in txReader
                    diskSize = engine.getTableDiskSizeCache().getDiskSize(
                            token,
                            txReader,
                            timestampType,
                            partitionBy,
                            path,
                            circuitBreaker
                    );
                }
            }

            private void loadMetadata() {
                if ((loaded & LOADED_METADATA) == 0) {
                    loaded |= LOADED_METADATA;
                    try (TableMetadata metadata = engine.getTableMetadata(token)) {
                        partitionBy = metadata.getPartitionBy();
                        timestampType = metadata.getTimestampType();
                    } catch (CairoException | TableReferenceOutOfDateException e) {
                        if (!isTokenStale()) {
                            throw e;
                        }
                        setTableGone();
                    }
                }
            }

            private void loadTxn() {
                loadMetadata();
                if ((loaded & LOADED_TXN) == 0) {
                    loaded |= LOADED_TXN;
                    try {
                        path.of(configuration.getDbRoot()).concat(token.getDirName());
                        TableUtils.setTxReaderPath(txReader, path, timestampType, partitionBy);
                        TableUtils.safeReadTxn(txReader, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());
                        rowCount = txReader.getRowCount();
                        partitionCount = txReader.getPartitionCount();
                    } catch (CairoException e) {
                        if (!isTokenStale()) {
                            throw e;
                        }
                        setTableGone();
                    }
                }
            }

            private void of(@NotNull TableToken token) {
                this.token = token;
                this.loaded = 0;
                this.isTableGone = false;
            }

            private void setTableGone() {
                isTableGone = true;
                partitionCount = Numbers.LONG_NULL;
                rowCount = Numbers.LONG_NULL;
                diskSize = Numbers.LONG_NULL;
                loaded = LOADED_ALL;
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
