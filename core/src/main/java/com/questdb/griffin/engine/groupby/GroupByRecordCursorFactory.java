/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.groupby;

import com.questdb.cairo.*;
import com.questdb.cairo.map.Map;
import com.questdb.cairo.map.MapFactory;
import com.questdb.cairo.map.MapKey;
import com.questdb.cairo.map.MapValue;
import com.questdb.cairo.sql.*;
import com.questdb.griffin.FunctionParser;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.griffin.engine.functions.GroupByFunction;
import com.questdb.griffin.model.QueryModel;
import com.questdb.std.*;
import org.jetbrains.annotations.NotNull;

public class GroupByRecordCursorFactory implements RecordCursorFactory {

    protected final RecordCursorFactory base;
    private final Map dataMap;
    private final GroupByRecordCursor cursor;
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
        final IntIntHashMap symbolTableIndex = new IntIntHashMap();

        GroupByUtils.prepareGroupByRecordFunctions(
                model,
                metadata,
                listColumnFilter,
                groupByFunctions,
                recordFunctions,
                groupByMetadata,
                keyTypes,
                valueTypes,
                symbolTableIndex,
                true
        );

        // sink will be storing record columns to map key
        this.mapSink = RecordSinkFactory.getInstance(asm, metadata, listColumnFilter, false);
        this.dataMap = MapFactory.createMap(configuration, keyTypes, valueTypes);
        this.base = base;
        this.metadata = groupByMetadata;
        this.cursor = new GroupByRecordCursor(recordFunctions, symbolTableIndex);
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
            return initFunctionsAndCursor(executionContext, dataMap.getCursor(), baseCursor);
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

    @NotNull
    protected RecordCursor initFunctionsAndCursor(
            SqlExecutionContext executionContext,
            RecordCursor mapCursor,
            RecordCursor baseCursor
    ) {
        cursor.of(mapCursor, baseCursor);
        // init all record function for this cursor, in case functions require metadata and/or symbol tables
        for (int i = 0, m = recordFunctions.size(); i < m; i++) {
            recordFunctions.getQuick(i).init(cursor, executionContext);
        }
        return cursor;
    }

    private static class GroupByRecordCursor implements RecordCursor {
        private final VirtualRecord functionRecord;
        private final IntIntHashMap symbolTableIndex;
        private RecordCursor mapCursor;
        private RecordCursor baseCursor;

        public GroupByRecordCursor(ObjList<Function> functions, IntIntHashMap symbolTableIndex) {
            this.functionRecord = new VirtualRecord(functions);
            this.symbolTableIndex = symbolTableIndex;
        }

        @Override
        public void close() {
            Misc.free(mapCursor);
            Misc.free(baseCursor);
        }

        @Override
        public Record getRecord() {
            return functionRecord;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(symbolTableIndex.get(columnIndex));
        }

        @Override
        public boolean hasNext() {
            return mapCursor.hasNext();
        }

        @Override
        public Record newRecord() {
            VirtualRecord record = new VirtualRecord(functionRecord.getFunctions());
            record.of(mapCursor.newRecord());
            return record;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            assert record instanceof VirtualRecord;
            mapCursor.recordAt(((VirtualRecord) record).getBaseRecord(), atRowId);
        }

        @Override
        public void recordAt(long rowId) {
            mapCursor.recordAt(functionRecord.getBaseRecord(), rowId);
        }

        @Override
        public void toTop() {
            mapCursor.toTop();
        }

        public void of(RecordCursor mapCursor, RecordCursor baseCursor) {
            this.mapCursor = mapCursor;
            this.baseCursor = baseCursor;
            functionRecord.of(mapCursor.getRecord());
        }
    }
}
