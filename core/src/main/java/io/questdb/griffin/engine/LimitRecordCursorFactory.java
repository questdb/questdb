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

package io.questdb.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import org.jetbrains.annotations.Nullable;

public class LimitRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final LimitRecordCursor cursor;

    public LimitRecordCursorFactory(RecordCursorFactory base, Function loFunction, @Nullable Function hiFunction) {
        super(base.getMetadata());
        this.base = base;
        this.cursor = new LimitRecordCursor(loFunction, hiFunction);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(base.getCursor(executionContext), executionContext);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void close() {
        base.close();
    }

    private static class LimitRecordCursor implements RecordCursor {
        private final Function loFunction;
        private final Function hiFunction;
        private RecordCursor base;
        private long limit;
        private long size;

        public LimitRecordCursor(Function loFunction, Function hiFunction) {
            this.loFunction = loFunction;
            this.hiFunction = hiFunction;
        }

        @Override
        public void close() {
            base.close();
        }

        @Override
        public long size() {
            if (size > -1) {
                return size;
            }
            return -1;
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
        public Record getRecordB() {
            return base.getRecordB();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            base.recordAt(record, atRowId);
        }

        public void of(RecordCursor base, SqlExecutionContext executionContext) throws SqlException {
            this.base = base;
            loFunction.init(base, executionContext);
            if (hiFunction != null) {
                hiFunction.init(base, executionContext);
            }
            toTop();
        }

        @Override
        public void toTop() {
            base.toTop();
            long lo = loFunction.getLong(null);
            if (lo < 0 && hiFunction == null) {
                // last N rows
                long count = countRows();

                base.toTop();
                // lo is negative, -5 for example
                // if we have 12 records we need to skip 12-5 = 7
                // if we have 4 records = return all of them
                if (count > -lo) {
                    skipTo(count + lo);
                }
                // set limit to return remaining rows
                limit = -lo;
                size = -lo;
            } else if (lo > -1 && hiFunction == null) {
                // first N rows
                limit = lo;
                size = lo;
            } else {
                // at this stage we have 'hi'
                long hi = hiFunction.getLong(null);
                if (lo < 0) {
                    // right, here we are looking for something like
                    // -10,-5 five rows away from tail

                    if (lo < hi) {
                        long count = countRows();
                        // when count < -hi we have empty cursor
                        if (count >= -hi) {
                            base.toTop();

                            if (count < -lo) {
                                // if we asked for -9,-4 but there are 7 records in cursor
                                // we would first ignore last 4 and return first 3
                                limit = count + hi;
                            } else {
                                skipTo(count + lo);
                                limit = -lo + hi;
                            }
                            size = limit;
                        }
                    } else {
                        // this is invalid bottom range, for example -3, -10
                        limit = 0;
                        size = 0;
                    }
                } else {
                    if (hi < 0) {
                        limit = countRows() - lo + hi;
                        size = limit;
                        base.toTop();
                    } else {
                        limit = Math.max(0, hi - lo);
                        size = limit;
                    }

                    if (lo > 0 && limit > 0) {
                        skipTo(lo);
                    }
                }
            }
        }

        private long countRows() {
            long count = base.size();
            if (count > -1L) {
                return count;
            }

            count = 0L;
            while (base.hasNext()) {
                count++;
            }
            return count;
        }

        private void skipTo(long count) {
            //noinspection StatementWithEmptyBody
            while (count-- > 0 && base.hasNext()) ;
        }
    }
}
