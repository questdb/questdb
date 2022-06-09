/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class DistinctRecordCursorFactory extends AbstractRecordCursorFactory {

    private final RecordCursorFactory base;
    private final Map dataMap;
    private final DistinctRecordCursor cursor;
    // this sink is used to copy recordKeyMap keys to dataMap
    private final RecordSink mapSink;

    public DistinctRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @Transient @NotNull EntityColumnFilter columnFilter,
            @Transient @NotNull BytecodeAssembler asm
    ) {
        super(base.getMetadata());
        final RecordMetadata metadata = base.getMetadata();
        // sink will be storing record columns to map key
        columnFilter.of(metadata.getColumnCount());
        this.mapSink = RecordSinkFactory.getInstance(asm, metadata, columnFilter, false);
        this.dataMap = MapFactory.createMap(configuration, metadata);
        this.base = base;
        this.cursor = new DistinctRecordCursor();
    }

    @Override
    protected void _close() {
        dataMap.close();
        base.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        dataMap.clear();
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, dataMap, mapSink, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable e) {
            baseCursor.close();
            throw e;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    private static class DistinctRecordCursor implements RecordCursor {
        private RecordCursor baseCursor;
        private Map dataMap;
        private RecordSink recordSink;
        private Record record;
        private SqlExecutionCircuitBreaker circuitBreaker;

        public DistinctRecordCursor() {
        }

        @Override
        public void close() {
            Misc.free(baseCursor);
            dataMap.restoreInitialCapacity();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                MapKey key = dataMap.withKey();
                recordSink.copy(record, key);
                if (key.create()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Record getRecordB() {
            return baseCursor.getRecordB();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            baseCursor.recordAt(record, atRowId);
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            dataMap.clear();
        }

        public void of(RecordCursor baseCursor, Map dataMap, RecordSink recordSink, SqlExecutionCircuitBreaker circuitBreaker) {
            this.baseCursor = baseCursor;
            this.dataMap = dataMap;
            this.recordSink = recordSink;
            this.record = baseCursor.getRecord();
            this.circuitBreaker = circuitBreaker;
        }

        @Override
        public long size() {
            return -1;
        }
    }
}
