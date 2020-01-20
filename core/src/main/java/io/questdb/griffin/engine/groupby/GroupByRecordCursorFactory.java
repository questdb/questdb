/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.*;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class GroupByRecordCursorFactory implements RecordCursorFactory {

    protected final RecordCursorFactory base;
    private final Map dataMap;
    private final VirtualFunctionSkewedSymbolRecordCursor cursor;
    private final ObjList<Function> recordFunctions;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final RecordSink mapSink;
    // this sink is used to copy recordKeyMap keys to dataMap
    private final RecordMetadata metadata;

    public GroupByRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @Transient @NotNull QueryModel model,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull FunctionParser functionParser,
            @Transient @NotNull SqlExecutionContext executionContext,
            @Transient @NotNull BytecodeAssembler asm,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes
    ) throws SqlException {
        final int columnCount = model.getColumns().size();
        final RecordMetadata metadata = base.getMetadata();
        this.groupByFunctions = new ObjList<>(columnCount);
        GroupByUtils.prepareGroupByFunctions(
                model,
                metadata,
                functionParser,
                executionContext,
                groupByFunctions,
                valueTypes
        );

        this.recordFunctions = new ObjList<>(columnCount);
        final GenericRecordMetadata groupByMetadata = new GenericRecordMetadata();
        final IntIntHashMap symbolTableSkewIndex = new IntIntHashMap();

        GroupByUtils.prepareGroupByRecordFunctions(
                model,
                metadata,
                listColumnFilter,
                groupByFunctions,
                recordFunctions,
                groupByMetadata,
                keyTypes,
                valueTypes.getColumnCount(),
                symbolTableSkewIndex,
                true
        );

        // sink will be storing record columns to map key
        this.mapSink = RecordSinkFactory.getInstance(asm, metadata, listColumnFilter, false);
        this.dataMap = MapFactory.createMap(configuration, keyTypes, valueTypes);
        this.base = base;
        this.metadata = groupByMetadata;
        this.cursor = new VirtualFunctionSkewedSymbolRecordCursor(recordFunctions, symbolTableSkewIndex);
    }

    @Override
    public Record newRecord() {
        return cursor.newRecord();
    }

    @Override
    public void close() {
        for (int i = 0, n = recordFunctions.size(); i < n; i++) {
            recordFunctions.getQuick(i).close();
        }
        dataMap.close();
        base.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        dataMap.clear();
        final RecordCursor baseCursor = base.getCursor(executionContext);

        try {
            final Record baseRecord = baseCursor.getRecord();
            final int n = groupByFunctions.size();
            while (baseCursor.hasNext()) {
                final MapKey key = dataMap.withKey();
                mapSink.copy(baseRecord, key);
                MapValue value = key.createValue();
                GroupByUtils.updateFunctions(groupByFunctions, n, value, baseRecord);
            }
            cursor.of(baseCursor, dataMap.getCursor());
            // init all record function for this cursor, in case functions require metadata and/or symbol tables
            for (int i = 0, m = recordFunctions.size(); i < m; i++) {
                recordFunctions.getQuick(i).init(cursor, executionContext);
            }
            return cursor;
        } catch (CairoException e) {
            baseCursor.close();
            throw e;
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean isRandomAccessCursor() {
        return true;
    }

}
