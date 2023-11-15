/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;

public class ShowServerVersionCursorFactory extends AbstractRecordCursorFactory {

    public static final String SERVER_VERSION = Constants.PG_COMPATIBLE_VERSION + " (questdb)";

    private static final int SIZE = 1;
    private static final GenericRecordMetadata METADATA = new GenericRecordMetadata();
    private final ShowServerVersionRecordCursor cursor = new ShowServerVersionRecordCursor();

    public ShowServerVersionCursorFactory() {
        super(METADATA);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show_server_version");
    }

    private static class ShowServerVersionRecordCursor implements RecordCursor {
        private final Record record = new Record() {
            @Override
            public CharSequence getStr(int col) {
                return col == 0 ? SERVER_VERSION : null;
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStr(col);
            }

            @Override
            public int getStrLen(int col) {
                CharSequence s = getStr(col);
                return s != null ? s.length() : -1;
            }
        };
        private int idx = -1;

        @Override
        public void close() {
            idx = -1;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public Record getRecordB() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            return ++idx < SIZE;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long size() {
            return SIZE;
        }

        @Override
        public void toTop() {
            close();
        }
    }

    static {
        METADATA.add(new TableColumnMetadata("server_version", ColumnType.STRING));
    }
}
