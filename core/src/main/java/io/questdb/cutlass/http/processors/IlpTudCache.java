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

package io.questdb.cutlass.http.processors;

import io.questdb.Telemetry;
import io.questdb.TelemetryOrigin;
import io.questdb.TelemetrySystemEvent;
import io.questdb.cairo.*;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.line.tcp.*;
import io.questdb.std.*;
import io.questdb.std.str.*;
import io.questdb.tasks.TelemetryTask;
import org.jetbrains.annotations.NotNull;

public class IlpTudCache implements QuietCloseable {
    private final boolean autoCreateNewColumns;
    private final boolean autoCreateNewTables;
    private final MemoryMARW ddlMem = Vm.getMARWInstance();
    private final DefaultColumnTypes defaultColumnTypes;
    private final CairoEngine engine;
    private final Path path = new Path();
    private final StringSink tableNameUtf16 = new StringSink();
    private final TableStructureAdapter tableStructureAdapter;
    private final Utf8SequenceObjHashMap<WalTableUpdateDetails> tableUpdateDetails = new Utf8SequenceObjHashMap<>();
    private final Telemetry<TelemetryTask> telemetry;

    public IlpTudCache(
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
            tud.rollback();
            tud.close();
        }
        tableUpdateDetails.clear();
    }

    @Override
    public void close() {
        Misc.free(ddlMem);
        Misc.free(path);
    }

    public void commitAll() {
        ObjList<Utf8String> keys = tableUpdateDetails.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            Utf8Sequence tableName = tableUpdateDetails.keys().get(i);
            WalTableUpdateDetails tud = tableUpdateDetails.get(tableName);
            // Close will commit
            tud.close();
        }
        tableUpdateDetails.clear();
    }

    public WalTableUpdateDetails getTableUpdateDetails(
            SecurityContext securityContext,
            @NotNull LineTcpParser parser,
            Pool<SymbolCache> symbolCachePool
    ) {
        int key = tableUpdateDetails.keyIndex(parser.getMeasurementName());
        if (key < 0) {
            return tableUpdateDetails.valueAt(key);
        }

        tableNameUtf16.clear();
        Utf8s.utf8ToUtf16(parser.getMeasurementName(), tableNameUtf16);
        TableToken tableToken = getOrCreateTable(securityContext, parser, tableNameUtf16);
        if (!engine.isWalTable(tableToken)) {
            throw CairoException.nonCritical().put("table is not WAL [table=").put(tableNameUtf16).put(']');
        }

        TelemetryTask.store(telemetry, TelemetryOrigin.ILP_TCP, TelemetrySystemEvent.ILP_RESERVE_WRITER);
        // check if table on disk is WAL
        path.of(engine.getConfiguration().getRoot());
        Utf8String nameUtf8 = Utf8String.newInstance(parser.getMeasurementName());
        WalTableUpdateDetails tud = new WalTableUpdateDetails(
                engine,
                securityContext,
                engine.getWalWriter(tableToken),
                defaultColumnTypes,
                nameUtf8,
                symbolCachePool,
                -1
        );

        tableUpdateDetails.putAt(key, nameUtf8, tud);
        return tud;
    }

    private TableToken getOrCreateTable(SecurityContext securityContext, @NotNull LineTcpParser parser, StringSink tableNameUtf16) {
        TableToken tableToken = engine.getTableTokenIfExists(tableNameUtf16);
        int status = engine.getTableStatus(path, tableToken);
        if (status != TableUtils.TABLE_EXISTS) {
            if (!autoCreateNewTables) {
                throw CairoException.nonCritical()
                        .put("table does not exist, creating new tables is disabled [table=").put(tableNameUtf16)
                        .put(']');
            }
            if (!autoCreateNewColumns) {
                throw CairoException.nonCritical()
                        .put("table does not exist, cannot create table, creating new columns is disabled [table=").put(tableNameUtf16)
                        .put(']');
            }
            // validate that parser entities do not contain NULLs
            TableStructureAdapter tsa = tableStructureAdapter.of(tableNameUtf16, parser);

            for (int i = 0, n = tsa.getColumnCount(); i < n; i++) {
                if (tsa.getColumnType(i) == LineTcpParser.ENTITY_TYPE_NULL) {
                    throw CairoException.nonCritical().put("unknown column type [columnName=").put(tsa.getColumnName(i)).put(']');
                }
            }
            tableToken = engine.createTable(securityContext, ddlMem, path, true, tsa, false);
        }
        return tableToken;
    }

}
