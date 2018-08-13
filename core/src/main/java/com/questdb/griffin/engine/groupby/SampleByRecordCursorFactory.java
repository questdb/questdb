/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.cairo.AbstractRecordCursorFactory;
import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.ColumnTypes;
import com.questdb.cairo.map.Map;
import com.questdb.cairo.map.MapFactory;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class SampleByRecordCursorFactory extends AbstractRecordCursorFactory {

    private final Map map;
    private final RecordCursorFactory base;
    private final SampleByRecordCursor cursor;

    public SampleByRecordCursorFactory(
            RecordMetadata metadata,
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @NotNull ColumnTypes valueTypes,
            RecordCursorFactory base) {
        super(metadata);
        this.map = MapFactory.createMap(configuration, keyTypes, valueTypes);
        this.base = base;
        this.cursor = new SampleByRecordCursor(map);
    }

    @Override
    public RecordCursor getCursor(BindVariableService bindVariableService) {
        return null;
    }

    @Override
    public boolean isRandomAccessCursor() {
        return false;
    }

    private static class SampleByRecordCursor implements RecordCursor {
        private final Map map;
        private RecordCursor base;

        public SampleByRecordCursor(Map map) {
            this.map = map;
        }

        @Override
        public void close() {

        }

        @Override
        public Record getRecord() {
            return null;
        }

        @Override
        public Record newRecord() {
            return null;
        }

        @Override
        public Record recordAt(long rowId) {
            return null;
        }

        @Override
        public void recordAt(Record record, long atRowId) {

        }

        @Override
        public void toTop() {

        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Record next() {
            return null;
        }

        void of(RecordCursor base) {
            this.base = base;
        }
    }
}
