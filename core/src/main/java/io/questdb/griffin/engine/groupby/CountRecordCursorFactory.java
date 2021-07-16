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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

public class CountRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final GenericRecordMetadata DEFAULT_COUNT_METADATA = new GenericRecordMetadata();
    private final RecordCursorFactory base;
    private final CountRecordCursor cursor = new CountRecordCursor();

    public CountRecordCursorFactory(RecordMetadata metadata, RecordCursorFactory base) {
        super(metadata);
        this.base = base;
    }

    @Override
    public void close() {
        base.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        try (RecordCursor baseCursor = base.getCursor(executionContext)) {
            final long size = baseCursor.size();
            if (size < 0) {
                long count = 0;
                while (baseCursor.hasNext()) {
                    count++;
                }
                cursor.of(count);
            } else {
                cursor.of(size);
            }
            return cursor;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    private static class CountRecordCursor implements NoRandomAccessRecordCursor {
        private final CountRecord countRecord = new CountRecord();
        private boolean hasNext = true;
        private long count;

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return countRecord;
        }

        @Override
        public boolean hasNext() {
            if (hasNext) {
                hasNext = false;
                return true;
            }
            return false;
        }

        @Override
        public void toTop() {
            hasNext = true;
        }

        @Override
        public long size() {
            return 1;
        }

        private void of(long count) {
            this.count = count;
            toTop();
        }

        private class CountRecord implements Record {
            @Override
            public long getLong(int col) {
                return count;
            }
        }
    }

    static {
        DEFAULT_COUNT_METADATA.add(new TableColumnMetadata("count", ColumnType.LONG, null));
    }
}
