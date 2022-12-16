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

package io.questdb.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.Record;
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
        boolean preTouchEnabled = executionContext.isColumnPreTouchEnabled();
        // Forcefully disable column pre-touch for LIMIT K,N queries for all downstream
        // async filtered factories to avoid redundant disk reads.
        executionContext.setColumnPreTouchEnabled(preTouchEnabled && cursor.hiFunction == null);
        try {
            cursor.of(base.getCursor(executionContext), executionContext);
        } finally {
            executionContext.setColumnPreTouchEnabled(preTouchEnabled);
        }
        return cursor;
    }

    @Override
    public boolean implementsLimit() {
        return true;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    protected void _close() {
        base.close();
    }

    private static class LimitRecordCursor implements RecordCursor {
        private final Function hiFunction;
        private final Function loFunction;
        private RecordCursor base;
        private long hi;
        private boolean isLimitCounted;
        private long limit;
        private long lo;
        private long rowCount;
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
        public Record getRecord() {
            return base.getRecord();
        }

        @Override
        public Record getRecordB() {
            return base.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return base.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            // TODO(puzpuzpuz): test suspendability
            if (!isLimitCounted) {
                countLimit();
                isLimitCounted = true;
            }
            if (limit <= 0) {
                return false;
            }
            if (base.hasNext()) {
                limit--;
                return true;
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return base.newSymbolTable(columnIndex);
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
        public void recordAt(Record record, long atRowId) {
            base.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return size > -1 ? size : -1;
        }

        public void skipTo(long rowCount) {
            base.skipTo(Math.max(0, rowCount));
        }

        @Override
        public void toTop() {
            base.toTop();
            rowCount = 0;
            size = -1;
            lo = loFunction.getLong(null);
            hi = hiFunction != null ? hiFunction.getLong(null) : -1;
            isLimitCounted = false;
        }

        private void countLimit() {
            // TODO(puzpuzpuz): this is non-suspendable (skipTo calls without previous countRows)
            if (lo < 0 && hiFunction == null) {
                // last N rows
                countRows();

                base.toTop();
                // lo is negative, -5 for example
                // if we have 12 records we need to skip 12-5 = 7
                // if we have 4 records = return all of them
                if (rowCount > -lo) {
                    skipTo(rowCount + lo);
                }
                // set limit to return remaining rows
                limit = Math.min(rowCount, -lo);
                size = limit;
            } else if (lo > -1 && hiFunction == null) {
                // first N rows
                long baseRowCount = base.size();
                if (baseRowCount > -1L) { // we don't want to cause a pass-through whole data set
                    limit = Math.min(baseRowCount, lo);
                } else {
                    limit = lo;
                }
                size = limit;
            } else {
                // at this stage we have 'hi'
                if (lo < 0) {
                    // right, here we are looking for something like
                    // -10,-5 five rows away from tail

                    if (lo < hi) {
                        countRows();
                        // when count < -hi we have empty cursor
                        if (rowCount >= -hi) {
                            base.toTop();

                            if (rowCount < -lo) {
                                // if we asked for -9,-4 but there are 7 records in cursor
                                // we would first ignore last 4 and return first 3
                                limit = rowCount + hi;
                            } else {
                                skipTo(rowCount + lo);
                                limit = Math.min(rowCount, -lo + hi);
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
                        countRows();
                        limit = Math.max(rowCount - lo + hi, 0);
                        size = limit;
                        base.toTop();
                    } else {
                        long baseRowCount = base.size();
                        if (baseRowCount > -1L) { // we don't want to cause a pass-through whole data set
                            limit = Math.max(0, Math.min(baseRowCount, hi) - lo);
                        } else {
                            limit = Math.max(0, hi - lo); // doesn't handle hi exceeding number of rows
                        }
                        size = limit;
                    }

                    if (lo > 0 && limit > 0) {
                        skipTo(lo);
                    }
                }
            }
        }

        private void countRows() {
            rowCount = base.size();
            if (rowCount > -1L) {
                return;
            }

            rowCount = 0L;
            while (base.hasNext()) {
                rowCount++;
            }
        }
    }
}
