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

package com.questdb.griffin.engine;

import com.questdb.cairo.AbstractRecordCursorFactory;
import com.questdb.cairo.sql.*;
import com.questdb.griffin.engine.functions.bind.BindVariableService;

public class LimitRecordCustorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final LimitRecordCursor cursor;

    public LimitRecordCustorFactory(RecordCursorFactory base, Function loFunction, Function hiFunction) {
        super(base.getMetadata());
        this.base = base;
        this.cursor = new LimitRecordCursor(loFunction, hiFunction);
    }

    @Override
    public RecordCursor getCursor(BindVariableService bindVariableService) {
        cursor.of(base.getCursor(bindVariableService));
        return cursor;
    }

    @Override
    public boolean isRandomAccessCursor() {
        return base.isRandomAccessCursor();
    }

    private static class LimitRecordCursor implements DelegatingRecordCursor {
        private final Function loFunction;
        private final Function hiFunction;
        private RecordCursor base;
        private long limit;

        public LimitRecordCursor(Function loFunction, Function hiFunction) {
            this.loFunction = loFunction;
            this.hiFunction = hiFunction;
        }

        @Override
        public void close() {
            base.close();
        }

        @Override
        public Record getRecord() {
            return base.getRecord();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return base.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            return limit-- > 0 && base.hasNext();
        }

        @Override
        public Record newRecord() {
            return base.newRecord();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            base.recordAt(record, atRowId);
        }

        @Override
        public void recordAt(long rowId) {
            base.recordAt(rowId);
        }

        @Override
        public void toTop() {
            base.toTop();
            long lo = loFunction.getLong(null);
            if (lo < 0 && hiFunction == null) {
                // last N rows
                long count = 0;
                while (base.hasNext()) {
                    count++;
                }

                base.toTop();
                // lo is negative, -5 for example
                // if we have 12 records we need to skip 12-5 = 7
                // if we have 4 records = return all of them
                if (count > -lo) {
                    count += lo;
                    while (count-- > 0 && base.hasNext()) ;
                }
                // set limit to return remaining rows
                limit = -lo;
            } else if (lo > -1 && hiFunction == null) {
                // first N rows
                limit = lo;
            } else {
                // at this stage we have 'hi'
                long hi = hiFunction.getLong(null);
                if (lo < 0) {
                    // right, here we are looking for something like
                    // -10,-5 five rows away from tail

                    long count = 0;
                    while (base.hasNext()) {
                        count++;
                    }

                    // when count < -hi we have empty cursor
                    if (count >= -hi) {
                        base.toTop();

                        if (count < -lo) {
                            // if we asked for -9,-4 but there are 7 records in cursor
                            // we would first ignore last 4 and return first 3
                            limit = count + hi;
                        } else {
                            count += lo;
                            while (count-- > 0 && base.hasNext()) ;
                            limit = -lo + hi;
                        }
                    }
                } else {
                    if (lo > 0) {
                        long count = lo;
                        while (--count > 0 && base.hasNext()) ;
                    }
                    limit = hi - lo + 1;
                }
            }
        }

        @Override
        public void of(RecordCursor base) {
            this.base = base;
            toTop();
        }
    }
}
