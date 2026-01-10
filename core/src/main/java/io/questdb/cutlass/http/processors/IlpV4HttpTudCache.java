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

package io.questdb.cutlass.http.processors;

import io.questdb.Telemetry;
import io.questdb.TelemetryOrigin;
import io.questdb.TelemetrySystemEvent;
import io.questdb.cairo.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.IlpV4WalAppender;
import io.questdb.cutlass.line.tcp.SymbolCache;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.cutlass.line.tcp.v4.IlpV4ColumnDef;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.tasks.TelemetryTask;

/**
 * Cache for table update details in ILP v4 HTTP processing.
 */
public class IlpV4HttpTudCache implements QuietCloseable {
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

    public IlpV4HttpTudCache(
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
            String tableName,
            IlpV4ColumnDef[] schema
    ) {
        Utf8String tableNameUtf8 = new Utf8String(tableName);
        int key = tableUpdateDetails.keyIndex(tableNameUtf8);
        if (key < 0) {
            return tableUpdateDetails.valueAt(key);
        }

        tableNameUtf16.clear();
        tableNameUtf16.put(tableName);
        TableToken tableToken = getOrCreateTable(securityContext, tableNameUtf16, schema);
        if (tableToken == null) {
            return null;
        }

        if (!engine.isWalTable(tableToken)) {
            // Cannot insert into non-WAL table
            return null;
        }

        TelemetryTask.store(telemetry, TelemetryOrigin.ILP_TCP, TelemetrySystemEvent.ILP_RESERVE_WRITER);
        path.of(engine.getConfiguration().getDbRoot());

        WalTableUpdateDetails tud = new WalTableUpdateDetails(
                engine,
                securityContext,
                engine.getWalWriter(tableToken),
                defaultColumnTypes,
                tableNameUtf8,
                symbolCachePool,
                -1,
                false,
                Long.MAX_VALUE
        );

        tableUpdateDetails.putAt(key, tableNameUtf8, tud);
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

    private TableToken getOrCreateTable(SecurityContext securityContext, StringSink tableNameUtf16, IlpV4ColumnDef[] schema) {
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
            IlpV4TableStructureAdapter tsa = new IlpV4TableStructureAdapter(
                    engine.getConfiguration(),
                    tableNameUtf16.toString(),
                    schema,
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
     */
    private static class IlpV4TableStructureAdapter implements TableStructure {
        private final CairoConfiguration configuration;
        private final String tableName;
        private final IlpV4ColumnDef[] schema;
        private final int partitionBy;
        private int timestampIndex = -1;

        IlpV4TableStructureAdapter(CairoConfiguration configuration, String tableName, IlpV4ColumnDef[] schema, int partitionBy) {
            this.configuration = configuration;
            this.tableName = tableName;
            this.schema = schema;
            this.partitionBy = partitionBy;

            // Find timestamp column
            for (int i = 0; i < schema.length; i++) {
                if ("timestamp".equals(schema[i].getName()) &&
                        IlpV4WalAppender.mapIlpV4TypeToQuestDB(schema[i].getTypeCode()) == ColumnType.TIMESTAMP) {
                    timestampIndex = i;
                    break;
                }
            }
        }

        @Override
        public int getColumnCount() {
            return schema.length;
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            return schema[columnIndex].getName();
        }

        @Override
        public int getColumnType(int columnIndex) {
            return IlpV4WalAppender.mapIlpV4TypeToQuestDB(schema[columnIndex].getTypeCode());
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
            return timestampIndex;
        }

        @Override
        public boolean isWalEnabled() {
            return true; // ILP v4 uses WAL
        }
    }
}
