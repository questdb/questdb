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

package com.questdb.griffin.engine.join;

import com.questdb.cairo.AbstractRecordCursorFactory;
import com.questdb.cairo.sql.*;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.griffin.engine.EmptyTableRecordCursor;
import com.questdb.std.Misc;

public class CrossJoinRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final HashJoinRecordCursor cursor;

    public CrossJoinRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit

    ) {
        super(metadata);
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        this.cursor = new HashJoinRecordCursor(columnSplit);
    }

    @Override
    public void close() {
        ((JoinRecordMetadata) getMetadata()).close();
        masterFactory.close();
        slaveFactory.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        final RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        RecordCursor slaveCursor = slaveFactory.getCursor(executionContext);
        if (masterCursor.hasNext()) {
            cursor.of(masterCursor, slaveCursor);
            return cursor;
        }

        return EmptyTableRecordCursor.INSTANCE;
    }

    @Override
    public boolean isRandomAccessCursor() {
        return false;
    }

    private static class HashJoinRecordCursor implements NoRandomAccessRecordCursor {
        private final JoinRecord record;
        private final int columnSplit;
        private RecordCursor masterCursor;
        private RecordCursor slaveCursor;

        public HashJoinRecordCursor(int columnSplit) {
            this.record = new JoinRecord(columnSplit);
            this.columnSplit = columnSplit;
        }

        @Override
        public void close() {
            masterCursor = Misc.free(masterCursor);
            slaveCursor = Misc.free(slaveCursor);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                return masterCursor.getSymbolTable(columnIndex);
            }
            return slaveCursor.getSymbolTable(columnIndex - columnSplit);
        }

        @Override
        public boolean hasNext() {

            if (slaveCursor.hasNext()) {
                return true;
            }

            if (masterCursor.hasNext()) {
                slaveCursor.toTop();
                return slaveCursor.hasNext();
            }

            return false;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            masterCursor.hasNext();
            slaveCursor.toTop();
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor) {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            record.of(masterCursor.getRecord(), slaveCursor.getRecord());
        }
    }
}
