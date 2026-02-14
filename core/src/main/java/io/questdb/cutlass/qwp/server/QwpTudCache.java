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

package io.questdb.cutlass.qwp.server;

import io.questdb.Telemetry;
import io.questdb.TelemetryOrigin;
import io.questdb.TelemetryEvent;
import io.questdb.cairo.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.QwpWalAppender;
import io.questdb.cutlass.line.tcp.SymbolCache;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpTableBlockCursor;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.TelemetryTask;

/**
 * Cache for table update details in ILP v4 processing.
 */
public class QwpTudCache implements QuietCloseable {
    private final boolean autoCreateNewColumns;
    private final boolean autoCreateNewTables;
    private final MemoryMARW ddlMem = Vm.getCMARWInstance();
    private final DefaultColumnTypes defaultColumnTypes;
    private final CairoEngine engine;
    private final Path path = new Path();
    private final StringSink tableNameUtf16 = new StringSink();
    private final int defaultPartitionBy;
    private final LowerCaseUtf8SequenceObjHashMap<WalTableUpdateDetails> tableUpdateDetails = new LowerCaseUtf8SequenceObjHashMap<>();
    private final Telemetry<TelemetryTask> telemetry;
    private final WeakClosableObjectPool<SymbolCache> symbolCachePool;
    private boolean distressed = false;

    public QwpTudCache(
            CairoEngine engine,
            boolean autoCreateNewColumns,
            boolean autoCreateNewTables,
            DefaultColumnTypes defaultColumnTypes,
            int defaultPartitionBy
    ) {
        this.engine = engine;
        this.telemetry = engine.getTelemetry();
        this.autoCreateNewColumns = autoCreateNewColumns;
        this.autoCreateNewTables = autoCreateNewTables;
        this.defaultColumnTypes = defaultColumnTypes;
        this.defaultPartitionBy = defaultPartitionBy;
        this.symbolCachePool = new WeakClosableObjectPool<>(
                () -> new SymbolCache(engine.getConfiguration().getMicrosecondClock(), 10_000),
                5
        );
    }

    public void clear() {
        ObjList<Utf8Sequence> keys = tableUpdateDetails.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            Utf8Sequence tableName = tableUpdateDetails.keys().get(i);
            WalTableUpdateDetails tud = tableUpdateDetails.get(tableName);
            if (distressed) {
                Misc.free(tud);
            } else {
                tud.rollback();
            }
        }
        if (distressed) {
            tableUpdateDetails.clear();
        }
    }

    @Override
    public void close() {
        ObjList<Utf8Sequence> keys = tableUpdateDetails.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            Utf8Sequence tableName = tableUpdateDetails.keys().get(i);
            WalTableUpdateDetails tud = tableUpdateDetails.get(tableName);
            Misc.free(tud);
        }
        tableUpdateDetails.clear();
        Misc.free(ddlMem);
        Misc.free(path);
        Misc.free(symbolCachePool);
    }

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

    public WalTableUpdateDetails getTableUpdateDetails(
            SecurityContext securityContext,
            Utf8Sequence tableNameUtf8,
            QwpColumnDef[] schema,
            QwpTableBlockCursor cursor
    ) {
        int key = tableUpdateDetails.keyIndex(tableNameUtf8);
        if (key < 0) {
            return tableUpdateDetails.valueAt(key);
        }

        tableNameUtf16.clear();
        Utf8s.utf8ToUtf16(tableNameUtf8, tableNameUtf16);
        TableToken tableToken = getOrCreateTable(securityContext, tableNameUtf16, schema, cursor);
        if (tableToken == null) {
            return null;
        }

        if (!engine.isWalTable(tableToken)) {
            // Cannot insert into non-WAL table
            return null;
        }

        TelemetryTask.store(telemetry, TelemetryOrigin.ILP_TCP, TelemetryEvent.ILP_RESERVE_WRITER);
        path.of(engine.getConfiguration().getDbRoot());

        // Copy table name to heap - needed for WalTableUpdateDetails and cache key
        Utf8String tableNameCopy = Utf8String.newInstance(tableNameUtf8);

        WalTableUpdateDetails tud = new WalTableUpdateDetails(
                engine,
                securityContext,
                engine.getWalWriter(tableToken),
                defaultColumnTypes,
                tableNameCopy,
                symbolCachePool,
                -1,
                false,
                Long.MAX_VALUE
        );

        tableUpdateDetails.putAt(key, tableNameCopy, tud);
        return tud;
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
        this.distressed = true;
    }

    private TableToken getOrCreateTable(SecurityContext securityContext, StringSink tableNameUtf16,
                                        QwpColumnDef[] schema, QwpTableBlockCursor cursor) {
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

            // Create table using ILP v4 schema
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
     * Table structure adapter for ILP v4 schema.
     * <p>
     * When no timestamp column is provided in the schema, this adapter automatically
     * adds a "timestamp" column as the designated timestamp. This matches the behavior
     * of the old ILP text protocol.
     */
    private static class QwpTableStructureAdapter implements TableStructure {
        private static final String DEFAULT_TIMESTAMP_FIELD = "timestamp";
        private final CairoConfiguration configuration;
        private final String tableName;
        private final QwpColumnDef[] schema;
        private final QwpTableBlockCursor cursor;
        private final int partitionBy;
        private int timestampIndex = -1;

        QwpTableStructureAdapter(CairoConfiguration configuration, String tableName, QwpColumnDef[] schema,
                                   QwpTableBlockCursor cursor, int partitionBy) {
            this.configuration = configuration;
            this.tableName = tableName;
            this.schema = schema;
            this.cursor = cursor;
            this.partitionBy = partitionBy;

            // Find designated timestamp column - empty name with TIMESTAMP or TIMESTAMP_NANOS type
            for (int i = 0; i < schema.length; i++) {
                byte typeCode = schema[i].getTypeCode();
                if (schema[i].getName().isEmpty() &&
                        (typeCode == QwpConstants.TYPE_TIMESTAMP || typeCode == QwpConstants.TYPE_TIMESTAMP_NANOS)) {
                    timestampIndex = i;
                    break;
                }
            }
            // If no designated timestamp found, we'll add one automatically (see getColumnCount)
        }

        @Override
        public int getColumnCount() {
            // If no timestamp column in schema, add one automatically
            return timestampIndex == -1 ? schema.length + 1 : schema.length;
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            // If this is the auto-added timestamp column (no designated timestamp in schema)
            if (columnIndex == getTimestampIndex() && timestampIndex == -1) {
                return DEFAULT_TIMESTAMP_FIELD;
            }
            // If this is the designated timestamp column from schema, use default name
            // (the schema column name is empty for TYPE_DESIGNATED_TIMESTAMP)
            if (columnIndex == timestampIndex) {
                return DEFAULT_TIMESTAMP_FIELD;
            }
            return schema[columnIndex].getName();
        }

        @Override
        public int getColumnType(int columnIndex) {
            // If this is the auto-added timestamp column
            if (columnIndex == getTimestampIndex() && timestampIndex == -1) {
                return ColumnType.TIMESTAMP;
            }
            byte typeCode = schema[columnIndex].getTypeCode();
            // For decimal types, get the scale from the cursor
            if (typeCode == QwpConstants.TYPE_DECIMAL64 ||
                    typeCode == QwpConstants.TYPE_DECIMAL128 ||
                    typeCode == QwpConstants.TYPE_DECIMAL256) {
                int scale = cursor.getDecimalColumn(columnIndex).getScale() & 0xFF;
                int tag = QwpWalAppender.mapQwpTypeToQuestDB(typeCode);
                int precision = Decimals.getDecimalTagPrecision(tag);
                return ColumnType.getDecimalType(tag, precision, scale);
            }
            return QwpWalAppender.mapQwpTypeToQuestDB(typeCode);
        }

        @Override
        public int getIndexBlockCapacity(int columnIndex) {
            return 0;
        }

        @Override
        public boolean isIndexed(int columnIndex) {
            return false;
        }

        @Override
        public boolean isDedupKey(int columnIndex) {
            return false;
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
            return timestampIndex == -1 ? schema.length : timestampIndex;
        }

        @Override
        public boolean isWalEnabled() {
            return true; // ILP v4 uses WAL
        }
    }
}
