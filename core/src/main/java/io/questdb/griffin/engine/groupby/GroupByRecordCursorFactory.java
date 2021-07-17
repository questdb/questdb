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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
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
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull BytecodeAssembler asm,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions
    ) {
        // sink will be storing record columns to map key
        try {
            this.dataMap = MapFactory.createMap(configuration, keyTypes, valueTypes);
            this.mapSink = RecordSinkFactory.getInstance(asm, base.getMetadata(), listColumnFilter, false);
            this.base = base;
            this.metadata = groupByMetadata;
            this.groupByFunctions = groupByFunctions;
            this.recordFunctions = recordFunctions;
            this.cursor = new VirtualFunctionSkewedSymbolRecordCursor(recordFunctions);
        } catch (Throwable e) {
            Misc.freeObjList(recordFunctions);
            throw e;
        }
    }

    @Override
    public void close() {
        Misc.freeObjList(recordFunctions);
        Misc.free(dataMap);
        Misc.free(base);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        dataMap.clear();
        final RecordCursor baseCursor = base.getCursor(executionContext);

        try {
            final Record baseRecord = baseCursor.getRecord();
            final int n = groupByFunctions.size();
            while (baseCursor.hasNext()) {
                executionContext.getSqlExecutionInterruptor().checkInterrupted();
                final MapKey key = dataMap.withKey();
                mapSink.copy(baseRecord, key);
                MapValue value = key.createValue();
                GroupByUtils.updateFunctions(groupByFunctions, n, value, baseRecord);
            }
            cursor.of(baseCursor, dataMap.getCursor());
            // init all record function for this cursor, in case functions require metadata and/or symbol tables
            Function.init(recordFunctions, baseCursor, executionContext);
            return cursor;
        } catch (Throwable e) {
            baseCursor.close();
            throw e;
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }
}
