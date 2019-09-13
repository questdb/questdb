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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.*;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class DistinctRecordCursorFactory implements RecordCursorFactory {

    protected final RecordCursorFactory base;
    private final Map dataMap;
    private final DistinctRecordCursor cursor;
    private final RecordSink mapSink;
    // this sink is used to copy recordKeyMap keys to dataMap
    private final RecordMetadata metadata;

    public DistinctRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @Transient @NotNull EntityColumnFilter columnFilter,
            @Transient @NotNull BytecodeAssembler asm
    ) {
        final RecordMetadata metadata = base.getMetadata();
        // sink will be storing record columns to map key
        columnFilter.of(metadata.getColumnCount());
        this.mapSink = RecordSinkFactory.getInstance(asm, metadata, columnFilter, false);
        this.dataMap = MapFactory.createMap(configuration, metadata);
        this.base = base;
        this.metadata = metadata;
        this.cursor = new DistinctRecordCursor();
    }

    @Override
    public void close() {
        dataMap.close();
        base.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        dataMap.clear();
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, dataMap, mapSink);
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
        return base.isRandomAccessCursor();
    }

    private static class DistinctRecordCursor implements RecordCursor {
        private RecordCursor baseCursor;
        private Map dataMap;
        private RecordSink recordSink;
        private Record record;

        public DistinctRecordCursor() {
        }

        @Override
        public void close() {
            Misc.free(baseCursor);
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
        public boolean hasNext() {
            while (baseCursor.hasNext()) {
                MapKey key = dataMap.withKey();
                recordSink.copy(record, key);
                if (key.create()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Record newRecord() {
            return baseCursor.newRecord();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            baseCursor.recordAt(record, atRowId);
        }

        @Override
        public void recordAt(long rowId) {
            baseCursor.recordAt(rowId);
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            dataMap.clear();
        }

        public void of(RecordCursor baseCursor, Map dataMap, RecordSink recordSink) {
            this.baseCursor = baseCursor;
            this.dataMap = dataMap;
            this.recordSink = recordSink;
            this.record = baseCursor.getRecord();
        }

        @Override
        public long size() {
            return -1;
        }
    }
}
