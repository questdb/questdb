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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class DistinctRecordCursorFactory extends AbstractRecordCursorFactory {

    private final RecordCursorFactory base;
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
        this.base = base;
        try {
            final RecordMetadata metadata = base.getMetadata();
            // sink will be storing record columns to map key
            columnFilter.of(metadata.getColumnCount());
            mapSink = RecordSinkFactory.getInstance(asm, metadata, columnFilter);
            cursor = new DistinctRecordCursor(configuration, metadata);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, mapSink, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable e) {
            cursor.close();
            throw e;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Distinct");
        sink.attr("keys").val(getMetadata());
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(cursor);
    }

    private static class DistinctRecordCursor implements NoRandomAccessRecordCursor {
        private final IntList columnIndex = new IntList();
        private final Map dataMap;
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMapBuilt;
        private boolean isOpen;
        private RecordCursor mapCursor;
        private MapRecord recordA;
        private RecordSink recordSink;

        public DistinctRecordCursor(CairoConfiguration configuration, RecordMetadata metadata) {
            this.isOpen = true;
            this.dataMap = MapFactory.createOrderedMap(configuration, metadata);
            // entity column index because distinct SQL has the same metadata as the base SQL
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                columnIndex.add(i);
            }
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                baseCursor = Misc.free(baseCursor);
                Misc.free(dataMap);
            }
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            buildMap();
            return mapCursor.hasNext();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        public void of(RecordCursor baseCursor, RecordSink recordSink, SqlExecutionCircuitBreaker circuitBreaker) {
            this.baseCursor = baseCursor;
            if (!isOpen) {
                isOpen = true;
                dataMap.reopen();
            }
            this.isMapBuilt = false;
            this.recordA = dataMap.getRecord();
            this.recordA.setSymbolTableResolver(baseCursor, columnIndex);
            this.recordSink = recordSink;
            this.circuitBreaker = circuitBreaker;
        }

        @Override
        public long preComputedStateSize() {
            return dataMap.size();
        }

        @Override
        public long size() {
            buildMap();
            return dataMap.size();
        }

        @Override
        public void toTop() {
            if (isMapBuilt && mapCursor != null) {
                mapCursor.toTop();
            }
        }

        private void buildMap() {
            if (!isMapBuilt) {
                buildMapSlow();
            }
        }

        private void buildMapSlow() {
            Record record = baseCursor.getRecord();
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                MapKey key = dataMap.withKey();
                recordSink.copy(record, key);
                key.create();
            }
            mapCursor = dataMap.getCursor();
            isMapBuilt = true;
        }
    }
}
