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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitFailedException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.line.tcp.DefaultColumnTypes;
import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.cutlass.line.tcp.SymbolCache;
import io.questdb.cutlass.line.tcp.TableStructureAdapter;
import io.questdb.cutlass.line.tcp.WalTableUpdateDetails;
import io.questdb.std.LowerCaseUtf8SequenceObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Pool;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.NotNull;

public class LineHttpTudCache implements QuietCloseable {
    private final boolean autoCreateNewColumns;
    private final boolean autoCreateNewTables;
    private final MemoryMARW ddlMem = Vm.getCMARWInstance();
    private final DefaultColumnTypes defaultColumnTypes;
    private final CairoEngine engine;
    private final TableCreateException parseException = new TableCreateException();
    private final Path path = new Path();
    private final StringSink tableNameUtf16 = new StringSink();
    private final TableStructureAdapter tableStructureAdapter;
    private final LowerCaseUtf8SequenceObjHashMap<WalTableUpdateDetails> tableUpdateDetails = new LowerCaseUtf8SequenceObjHashMap<>();
    private final Telemetry<TelemetryTask> telemetry;
    private boolean distressed = false;

    public LineHttpTudCache(
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
        this.tableStructureAdapter = new TableStructureAdapter(engine.getConfiguration(), this.defaultColumnTypes, defaultPartitionBy, true);
    }

    public void clear() {
        ObjList<Utf8String> keys = tableUpdateDetails.keys();
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
        // Close happens when HTTP connection is closed
        ObjList<Utf8String> keys = tableUpdateDetails.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            Utf8Sequence tableName = tableUpdateDetails.keys().get(i);
            WalTableUpdateDetails tud = tableUpdateDetails.get(tableName);
            Misc.free(tud);
        }
        tableUpdateDetails.clear();
        Misc.free(ddlMem);
        Misc.free(path);
    }

    public void commitAll() throws Throwable {
        boolean droppedTableFound;
        do {
            droppedTableFound = false;
            ObjList<Utf8String> keys = tableUpdateDetails.keys();
            for (int i = 0, n = keys.size(); i < n; i++) {
                Utf8Sequence tableName = tableUpdateDetails.keys().get(i);
                WalTableUpdateDetails tud = tableUpdateDetails.get(tableName);
                try {
                    if (!tud.isDropped()) {
                        tud.commit(false);
                    }
                } catch (CommitFailedException e) {
                    // We can ignore dropped tables and simply roll back the transaction
                    if (!e.isTableDropped()) {
                        // This real commit error, abort the batch commit
                        throw e.getReason();
                    } else {
                        // Remove dropped table TUD from cache
                        // and repeat the commit procedure
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
            @NotNull LineTcpParser parser,
            Pool<SymbolCache> symbolCachePool
    ) throws TableCreateException {
        int key = tableUpdateDetails.keyIndex(parser.getMeasurementName());
        if (key < 0) {
            return tableUpdateDetails.valueAt(key);
        }

        tableNameUtf16.clear();
        Utf8s.utf8ToUtf16(parser.getMeasurementName(), tableNameUtf16);
        TableToken tableToken = getOrCreateTable(securityContext, parser, tableNameUtf16);
        if (!engine.isWalTable(tableToken)) {
            throw parseException.of("cannot insert in non-WAL table", null);
        }

        TelemetryTask.store(telemetry, TelemetryOrigin.ILP_TCP, TelemetrySystemEvent.ILP_RESERVE_WRITER);
        // check if table on disk is WAL
        path.of(engine.getConfiguration().getDbRoot());
        Utf8String nameUtf8 = Utf8String.newInstance(parser.getMeasurementName());
        WalTableUpdateDetails tud = new WalTableUpdateDetails(
                engine,
                securityContext,
                engine.getWalWriter(tableToken),
                defaultColumnTypes,
                nameUtf8,
                symbolCachePool,
                -1,
                false,
                Long.MAX_VALUE
        );

        tableUpdateDetails.putAt(key, nameUtf8, tud);
        return tud;
    }

    public void reset() {
        ObjList<Utf8String> keys = tableUpdateDetails.keys();
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

    private TableToken getOrCreateTable(SecurityContext securityContext, @NotNull LineTcpParser parser, StringSink tableNameUtf16) throws TableCreateException {
        int maxFileNameLength = engine.getConfiguration().getMaxFileNameLength();
        if (!TableUtils.isValidTableName(tableNameUtf16, maxFileNameLength)) {
            throw parseException.of("invalid table name", null);
        }
        TableToken tableToken = engine.getTableTokenIfExists(tableNameUtf16);
        int status = engine.getTableStatus(path, tableToken);
        if (status != TableUtils.TABLE_EXISTS) {
            if (!autoCreateNewTables) {
                throw parseException.of("table does not exist, creating new tables is disabled", null);
            }
            if (!autoCreateNewColumns) {
                throw parseException.of("table does not exist, cannot create table, creating new columns is disabled", null);
            }
            // validate that parser entities do not contain NULLs
            TableStructureAdapter tsa = tableStructureAdapter.of(tableNameUtf16, parser);

            for (int i = 0, n = tsa.getColumnCount(); i < n; i++) {
                CharSequence columnName = tsa.getColumnNameNoValidation(i);
                if (!TableUtils.isValidColumnName(columnName, maxFileNameLength)) {
                    throw parseException.of("invalid column name", columnName);
                }
                final int columnType = tsa.getColumnType(i);
                if (columnType == LineTcpParser.ENTITY_TYPE_NULL) {
                    throw parseException.of("invalid column type", columnName);
                } else if (columnType == ColumnType.DECIMAL) {
                    throw parseException.of("decimal columns must be created manually", columnName);
                }
            }
            tableToken = engine.createTable(securityContext, ddlMem, path, true, tsa, false, TableUtils.TABLE_KIND_REGULAR_TABLE);
        }
        if (tableToken != null && tableToken.isMatView()) {
            throw parseException.of("cannot modify materialized view", tableToken.getTableName());
        }
        return tableToken;
    }

    public static class TableCreateException extends Exception {
        private String msg;
        private CharSequence token;

        public String getMsg() {
            return msg;
        }

        public CharSequence getToken() {
            return token;
        }

        TableCreateException of(String message, CharSequence token) {
            this.msg = message;
            this.token = token;
            return this;
        }
    }
}
