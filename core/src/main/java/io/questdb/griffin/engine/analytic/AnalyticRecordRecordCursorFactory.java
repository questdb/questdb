/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package io.questdb.griffin.engine.analytic;


import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class AnalyticRecordRecordCursorFactory implements RecordCursorFactory {

    private final RecordCursorFactory base;
    private final ObjList<AnalyticFunction> functions;
    private final RecordMetadata metadata;
    private final AnalyticRecordCursor cursor;

    public AnalyticRecordRecordCursorFactory(
            RecordCursorFactory base,
            ObjList<AnalyticFunction> functionGroups
    ) {
        this.base = base;

        // create our metadata and also flatten functions for our record representation
        GenericRecordMetadata funcMetadata = new GenericRecordMetadata();
        this.functions = functionGroups;
        for (int j = 0; j < functionGroups.size(); j++) {
            funcMetadata.add(functionGroups.getQuick(j).getMetadata());
        }

        this.metadata = new SplitRecordMetadata(base.getMetadata(), funcMetadata);
        int split = base.getMetadata().getColumnCount();
        this.cursor = new AnalyticRecordCursor(functions, split);
    }

    @Override
    public void close() {
        Misc.free(base);
        for (int i = 0, n = functions.size(); i < n; i++) {
            Misc.free(functions.getQuick(i));
        }
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        this.cursor.of(base.getCursor(executionContext));
        return cursor;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    private static class AnalyticRecordCursor implements NoRandomAccessRecordCursor {
        private final AnalyticRecord record;
        private final ObjList<AnalyticFunction> functions;
        private final int functionCount;
        private final int split;
        private RecordCursor baseCursor;

        public AnalyticRecordCursor(ObjList<AnalyticFunction> functions, int split) {
            this.record = new AnalyticRecord(split, functions);
            this.functions = functions;
            this.functionCount = functions.size();
            this.split = split;
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
            if (columnIndex < split) {
                return baseCursor.getSymbolTable(columnIndex);
            }
            return functions.get(columnIndex - split).getSymbolTable();
        }

        @Override
        public boolean hasNext() {
            if (baseCursor.hasNext()) {
                for (int i = 0; i < functionCount; i++) {
                    functions.getQuick(i).prepareFor(record);
                }
                return true;
            }
            return false;
        }

        @Override
        public void toTop() {
            this.baseCursor.toTop();
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).toTop();
            }
        }

        @Override
        public long size() {
            return baseCursor.size();
        }

        public void of(RecordCursor cursor) {
            this.baseCursor = cursor;
            this.record.of(cursor.getRecord());
            for (int i = 0; i < functionCount; i++) {
                final AnalyticFunction f = functions.getQuick(i);
                f.reset();
                f.prepare(baseCursor);
            }
        }
    }
}
