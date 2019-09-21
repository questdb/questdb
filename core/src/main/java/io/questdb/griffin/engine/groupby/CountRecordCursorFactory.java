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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.*;
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
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
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
    public boolean isRandomAccessCursor() {
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
        DEFAULT_COUNT_METADATA.add(new TableColumnMetadata("count", ColumnType.LONG));
    }
}
