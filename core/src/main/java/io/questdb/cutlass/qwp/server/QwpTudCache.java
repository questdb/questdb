/*+****************************************************************************
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

package io.questdb.cutlass.qwp.server;

import io.questdb.Telemetry;
import io.questdb.TelemetryEvent;
import io.questdb.TelemetryOrigin;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitFailedException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.QwpWalAppender;
import io.questdb.cutlass.line.tcp.SymbolCache;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpArrayColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseUtf8SequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.WeakClosableObjectPool;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.TelemetryTask;

/**
 * Cache for table update details in QWP v1 processing.
 */
public class QwpTudCache implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(QwpTudCache.class);
    private final boolean autoCreateNewColumns;
    private final boolean autoCreateNewTables;
    private final long commitInterval;
    private final DefaultColumnTypes defaultColumnTypes;
    private final int defaultPartitionBy;
    private final CairoEngine engine;
    private final long maxUncommittedRows;
    private final StringSink tableNameUtf16 = new StringSink();
    private final LowerCaseUtf8SequenceObjHashMap<WalTableUpdateDetails> tableUpdateDetails = new LowerCaseUtf8SequenceObjHashMap<>();
    private final Telemetry<TelemetryTask> telemetry;
    private MemoryMARW ddlMem;
    private boolean isDistressed = false;
    private Path path;
    private WeakClosableObjectPool<SymbolCache> symbolCachePool;

    public QwpTudCache(
            CairoEngine engine,
            boolean autoCreateNewColumns,
            boolean autoCreateNewTables,
            DefaultColumnTypes defaultColumnTypes,
            int defaultPartitionBy
    ) {
        this(
                engine,
                autoCreateNewColumns,
                autoCreateNewTables,
                defaultColumnTypes,
                defaultPartitionBy,
                -1,
                Long.MAX_VALUE
        );
    }

    public QwpTudCache(
            CairoEngine engine,
            boolean autoCreateNewColumns,
            boolean autoCreateNewTables,
            DefaultColumnTypes defaultColumnTypes,
            int defaultPartitionBy,
            long commitInterval,
            long maxUncommittedRows
    ) {
        try {
            this.ddlMem = Vm.getCMARWInstance();
            this.path = new Path();
            this.engine = engine;
            this.telemetry = engine.getTelemetry();
            this.autoCreateNewColumns = autoCreateNewColumns;
            this.autoCreateNewTables = autoCreateNewTables;
            this.commitInterval = commitInterval;
            this.defaultColumnTypes = defaultColumnTypes;
            this.defaultPartitionBy = defaultPartitionBy;
            this.maxUncommittedRows = maxUncommittedRows;
            this.symbolCachePool = new WeakClosableObjectPool<>(
                    () -> new SymbolCache(engine.getConfiguration().getMicrosecondClock(), 10_000),
                    5
            );
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    public void clear() {
        ObjList<Utf8Sequence> keys = tableUpdateDetails.keys();
        if (!isDistressed) {
            for (int i = 0, n = keys.size(); i < n; i++) {
                Utf8Sequence tableName = keys.get(i);
                WalTableUpdateDetails tud = tableUpdateDetails.get(tableName);
                try {
                    tud.rollback();
                } catch (Throwable th) {
                    LOG.error().$("could not rollback [table=").$(tableName).$(", e=").$(th).I$();
                    isDistressed = true;
                }
            }
        }
        if (isDistressed) {
            for (int i = 0, n = keys.size(); i < n; i++) {
                Utf8Sequence tableName = keys.get(i);
                WalTableUpdateDetails tud = tableUpdateDetails.get(tableName);
                Misc.free(tud);
            }
            tableUpdateDetails.clear();
            isDistressed = false;
        }
    }

    @Override
    public void close() {
        reset();
        tableUpdateDetails.clear();
        ddlMem = Misc.free(ddlMem);
        path = Misc.free(path);
        symbolCachePool = Misc.free(symbolCachePool);
    }

    /**
     * Commits all cached tables. Aborts on the first non-dropped-table commit
     * failure, leaving the remaining tables uncommitted. Suitable for callers
     * that propagate the error to the client (e.g. HTTP/WebSocket).
     */
    public void commitAll() throws Throwable {
        boolean droppedTableFound;
        do {
            droppedTableFound = false;
            ObjList<Utf8Sequence> keys = tableUpdateDetails.keys();
            for (int i = 0, n = keys.size(); i < n; i++) {
                Utf8Sequence tableName = tableUpdateDetails.keys().get(i);
                WalTableUpdateDetails tud = tableUpdateDetails.get(tableName);
                try {
                    if (!tud.isDropped()) {
                        tud.commit(false);
                    }
                } catch (CommitFailedException e) {
                    if (!e.isTableDropped()) {
                        throw e.getReason();
                    } else {
                        tud.setIsDropped();
                    }
                }

                if (tud.isDropped()) {
                    tableUpdateDetails.remove(tableName);
                    Misc.free(tud);
                    droppedTableFound = true;
                    break;
                }
            }
        } while (droppedTableFound);
    }

    /**
     * Commits all cached tables, continuing past per-table failures so that
     * one table's error does not prevent the remaining tables from being
     * committed. Errors are logged. Suitable for fire-and-forget callers
     * (e.g. UDP receivers) where there is no client to report the error to.
     */
    public void commitAllBestEffort() {
        boolean droppedTableFound;
        do {
            droppedTableFound = false;
            ObjList<Utf8Sequence> keys = tableUpdateDetails.keys();
            for (int i = 0, n = keys.size(); i < n; i++) {
                Utf8Sequence tableName = tableUpdateDetails.keys().get(i);
                WalTableUpdateDetails tud = tableUpdateDetails.get(tableName);
                try {
                    if (!tud.isDropped()) {
                        tud.commit(false);
                    }
                } catch (CommitFailedException e) {
                    if (e.isTableDropped()) {
                        tud.setIsDropped();
                    } else {
                        LOG.error().$("commit error [table=").$(tableName).$(", e=").$(e.getReason()).I$();
                    }
                } catch (Throwable t) {
                    LOG.error().$("commit error [table=").$(tableName).$(", e=").$(t.getMessage()).I$();
                }

                if (tud.isDropped()) {
                    tableUpdateDetails.remove(tableName);
                    Misc.free(tud);
                    droppedTableFound = true;
                    break;
                }
            }
        } while (droppedTableFound);
    }

    public long commitWalTables(long wallClockMillis) {
        long minTableNextCommitTime = Long.MAX_VALUE;
        boolean droppedTableFound;
        do {
            droppedTableFound = false;
            ObjList<Utf8Sequence> keys = tableUpdateDetails.keys();
            for (int i = 0, n = keys.size(); i < n; i++) {
                Utf8Sequence tableName = tableUpdateDetails.keys().get(i);
                WalTableUpdateDetails tud = tableUpdateDetails.get(tableName);
                try {
                    if (!tud.isDropped()) {
                        long tableNextCommitTime = tud.commitIfIntervalElapsed(wallClockMillis);
                        wallClockMillis = tud.getMillisecondClock().getTicks();
                        if (tableNextCommitTime < minTableNextCommitTime) {
                            minTableNextCommitTime = tableNextCommitTime;
                        }
                    }
                } catch (CommitFailedException e) {
                    if (e.isTableDropped()) {
                        tud.setIsDropped();
                    } else {
                        LOG.error().$("commit error [table=").$(tableName).$(", e=").$(e.getReason()).I$();
                    }
                } catch (Throwable t) {
                    LOG.error().$("commit error [table=").$(tableName).$(", e=").$(t.getMessage()).I$();
                }

                if (tud.isDropped()) {
                    tableUpdateDetails.remove(tableName);
                    Misc.free(tud);
                    droppedTableFound = true;
                    break;
                }
            }
        } while (droppedTableFound);
        return minTableNextCommitTime;
    }

    public WalTableUpdateDetails getTableUpdateDetails(
            SecurityContext securityContext,
            Utf8Sequence tableNameUtf8,
            ObjList<QwpColumnDef> schema,
            QwpTableBlockCursor cursor,
            int maxTables
    ) {
        int key = tableUpdateDetails.keyIndex(tableNameUtf8);
        if (key < 0) {
            return tableUpdateDetails.valueAt(key);
        }

        if (tableUpdateDetails.size() >= maxTables) {
            throw CairoException.nonCritical()
                    .put("too many distinct tables, limit: ").put(maxTables);
        }

        tableNameUtf16.clear();
        Utf8s.utf8ToUtf16(tableNameUtf8, tableNameUtf16);
        TableToken tableToken = getOrCreateTable(securityContext, tableNameUtf16, schema, cursor);
        if (tableToken == null) {
            return null;
        }

        if (!engine.isWalTable(tableToken)) {
            throw CairoException.nonCritical().put("cannot insert into non-WAL table: ").put(tableNameUtf16);
        }

        TelemetryTask.store(telemetry, TelemetryOrigin.ILP_TCP, TelemetryEvent.ILP_RESERVE_WRITER);
        path.of(engine.getConfiguration().getDbRoot());

        // Copy table name to heap - needed for WalTableUpdateDetails and cache key
        Utf8String tableNameCopy = Utf8String.newInstance(tableNameUtf8);

        TableWriterAPI walWriter = engine.getWalWriter(tableToken);
        WalTableUpdateDetails tud = null;
        try {
            tud = new WalTableUpdateDetails(
                    engine,
                    securityContext,
                    walWriter,
                    defaultColumnTypes,
                    tableNameCopy,
                    symbolCachePool,
                    commitInterval,
                    false,
                    maxUncommittedRows
            );
            tableUpdateDetails.putAt(key, tableNameCopy, tud);
            return tud;
        } catch (Throwable th) {
            Misc.free(tud != null ? tud : walWriter);
            throw th;
        }
    }

    public void reset() {
        ObjList<Utf8Sequence> keys = tableUpdateDetails.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            Utf8Sequence tableName = tableUpdateDetails.keys().get(i);
            WalTableUpdateDetails tud = tableUpdateDetails.get(tableName);
            Misc.free(tud);
        }
        tableUpdateDetails.clear();
    }

    public void setDistressed() {
        this.isDistressed = true;
    }

    private static boolean isValidQwpSchemaColumnName(QwpColumnDef columnDef, int maxFileNameLength) {
        final String columnName = columnDef.getName();
        if (columnName.isEmpty()) {
            final byte typeCode = columnDef.getTypeCode();
            return typeCode == QwpConstants.TYPE_TIMESTAMP || typeCode == QwpConstants.TYPE_TIMESTAMP_NANOS;
        }
        return TableUtils.isValidColumnName(columnName, maxFileNameLength);
    }

    private TableToken getOrCreateTable(SecurityContext securityContext, StringSink tableNameUtf16,
                                        ObjList<QwpColumnDef> schema, QwpTableBlockCursor cursor) {
        int maxFileNameLength = engine.getConfiguration().getMaxFileNameLength();
        if (!TableUtils.isValidTableName(tableNameUtf16, maxFileNameLength)) {
            return null;
        }
        TableToken tableToken = engine.getTableTokenIfExists(tableNameUtf16);
        int status = engine.getTableStatus(path, tableToken);
        if (status != TableUtils.TABLE_EXISTS) {
            if (!autoCreateNewTables) {
                return null;
            }
            if (!autoCreateNewColumns) {
                return null;
            }

            for (int i = 0; i < schema.size(); i++) {
                if (!isValidQwpSchemaColumnName(schema.getQuick(i), maxFileNameLength)) {
                    return null;
                }
            }

            // Create table using QWP v1 schema
            QwpTableStructureAdapter tsa = new QwpTableStructureAdapter(
                    engine.getConfiguration(),
                    tableNameUtf16.toString(),
                    schema,
                    cursor,
                    defaultPartitionBy
            );

            for (int i = 0, n = tsa.getColumnCount(); i < n; i++) {
                CharSequence columnName = tsa.getColumnName(i);
                if (!TableUtils.isValidColumnName(columnName, maxFileNameLength)) {
                    return null;
                }
            }
            tableToken = engine.createTable(securityContext, ddlMem, path, true, tsa, false, TableUtils.TABLE_KIND_REGULAR_TABLE);
        }
        if (tableToken != null && (tableToken.isView() || tableToken.isMatView())) {
            return null;
        }
        return tableToken;
    }

    /**
     * Table structure adapter for QWP v1 schema.
     * <p>
     * When no timestamp column is provided in the schema, this adapter automatically
     * adds a "timestamp" column as the designated timestamp. This matches the behavior
     * of the old ILP text protocol.
     */
    private static class QwpTableStructureAdapter implements TableStructure {
        private static final String DEFAULT_TIMESTAMP_FIELD = "timestamp";
        private final IntList columnTypes = new IntList();
        private final CairoConfiguration configuration;
        private final QwpTableBlockCursor cursor;
        private final IntList includedSchemaIndexes = new IntList();
        private final int outputTimestampIndex;
        private final int partitionBy;
        private final ObjList<QwpColumnDef> schema;
        private final String tableName;
        private int timestampSchemaIndex = -1;

        QwpTableStructureAdapter(CairoConfiguration configuration, String tableName, ObjList<QwpColumnDef> schema,
                                 QwpTableBlockCursor cursor, int partitionBy) {
            this.configuration = configuration;
            this.tableName = tableName;
            this.schema = schema;
            this.cursor = cursor;
            this.partitionBy = partitionBy;

            // Find designated timestamp column - empty name with TIMESTAMP or TIMESTAMP_NANOS type
            for (int i = 0, n = schema.size(); i < n; i++) {
                byte typeCode = schema.getQuick(i).getTypeCode();
                if (schema.getQuick(i).getName().isEmpty() &&
                        (typeCode == QwpConstants.TYPE_TIMESTAMP || typeCode == QwpConstants.TYPE_TIMESTAMP_NANOS)) {
                    timestampSchemaIndex = i;
                    break;
                }
            }
            for (int i = 0; i < schema.size(); i++) {
                final int columnType = getSchemaColumnType(i);
                if (columnType == ColumnType.UNDEFINED) {
                    continue;
                }
                includedSchemaIndexes.add(i);
                columnTypes.add(columnType);
            }
            outputTimestampIndex = timestampSchemaIndex == -1 ? includedSchemaIndexes.size() : includedSchemaIndexes.binarySearchUniqueList(timestampSchemaIndex);
            // If no designated timestamp found, we'll add one automatically (see getColumnCount)
        }

        @Override
        public int getColumnCount() {
            // If no timestamp column in schema, add one automatically
            return timestampSchemaIndex == -1 ? includedSchemaIndexes.size() + 1 : includedSchemaIndexes.size();
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            // If this is the auto-added timestamp column (no designated timestamp in schema)
            if (columnIndex == getTimestampIndex() && timestampSchemaIndex == -1) {
                return DEFAULT_TIMESTAMP_FIELD;
            }
            // If this is the designated timestamp column from schema, use default name
            // (the schema column name is empty for TYPE_DESIGNATED_TIMESTAMP)
            if (columnIndex == outputTimestampIndex) {
                return DEFAULT_TIMESTAMP_FIELD;
            }
            return schema.getQuick(includedSchemaIndexes.get(columnIndex)).getName();
        }

        @Override
        public int getColumnType(int columnIndex) {
            // If this is the auto-added timestamp column
            if (columnIndex == getTimestampIndex() && timestampSchemaIndex == -1) {
                return ColumnType.TIMESTAMP;
            }
            return columnTypes.get(columnIndex);
        }

        @Override
        public int getIndexBlockCapacity(int columnIndex) {
            return 0;
        }

        @Override
        public int getMaxUncommittedRows() {
            return configuration.getMaxUncommittedRows();
        }

        @Override
        public long getO3MaxLag() {
            return configuration.getO3MaxLag();
        }

        @Override
        public int getPartitionBy() {
            return partitionBy;
        }

        @Override
        public boolean getSymbolCacheFlag(int columnIndex) {
            return configuration.getDefaultSymbolCacheFlag();
        }

        @Override
        public int getSymbolCapacity(int columnIndex) {
            return configuration.getDefaultSymbolCapacity();
        }

        @Override
        public CharSequence getTableName() {
            return tableName;
        }

        @Override
        public int getTimestampIndex() {
            // If no timestamp column in schema, it's the auto-added one at the end
            return outputTimestampIndex;
        }

        @Override
        public boolean isDedupKey(int columnIndex) {
            return false;
        }

        @Override
        public boolean isIndexed(int columnIndex) {
            return false;
        }

        @Override
        public boolean isWalEnabled() {
            return true; // QWP v1 uses WAL
        }

        private static int getArrayBatchDimensionality(
                QwpArrayColumnCursor cursor,
                int rowCount,
                CharSequence columnName
        ) {
            int batchDims = -1;
            cursor.resetRowPosition();
            for (int row = 0; row < rowCount; row++) {
                cursor.advanceRow();
                if (cursor.isNull()) {
                    continue;
                }

                final int rowDims = cursor.getNDims();
                if (batchDims == -1) {
                    batchDims = rowDims;
                } else if (batchDims != rowDims) {
                    throw CairoException.nonCritical()
                            .put("array dimensionality mismatch in QWP batch [column=")
                            .put(columnName)
                            .put(", expectedDims=")
                            .put(batchDims)
                            .put(", actualDims=")
                            .put(rowDims)
                            .put(']');
                }
            }
            cursor.resetRowPosition();
            return batchDims;
        }

        private int getSchemaColumnType(int schemaIndex) {
            final byte typeCode = schema.getQuick(schemaIndex).getTypeCode();
            if (typeCode == QwpConstants.TYPE_DECIMAL64 ||
                    typeCode == QwpConstants.TYPE_DECIMAL128 ||
                    typeCode == QwpConstants.TYPE_DECIMAL256) {
                final int scale = cursor.getDecimalColumn(schemaIndex).getScale() & 0xFF;
                final int tag = QwpWalAppender.mapQwpTypeToQuestDB(typeCode);
                final int precision = Decimals.getDecimalTagPrecision(tag);
                return ColumnType.getDecimalType(tag, precision, scale);
            }
            if (typeCode == QwpConstants.TYPE_DOUBLE_ARRAY) {
                final int nDims = getArrayBatchDimensionality(
                        cursor.getArrayColumn(schemaIndex),
                        cursor.getRowCount(),
                        schema.getQuick(schemaIndex).getName()
                );
                if (nDims < 1) {
                    return ColumnType.UNDEFINED;
                }
                return ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
            }
            return QwpWalAppender.mapQwpTypeToQuestDB(typeCode);
        }
    }
}
