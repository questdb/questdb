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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.Misc;

public class RecordAsAFieldRecordCursorFactory implements RecordCursorFactory {
    private final RecordCursorFactory base;
    private final GenericRecordMetadata metadata;
    private final RecordAsAFieldRecordCursor cursor;

    public RecordAsAFieldRecordCursorFactory(RecordCursorFactory base, CharSequence columnAlias) {
        this.base = base;
        this.cursor = new RecordAsAFieldRecordCursor();
        this.metadata = new GenericRecordMetadata();
        this.metadata.add(new TableColumnMetadata(Chars.toString(columnAlias), ColumnType.RECORD, base.getMetadata()));
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(base.getCursor(executionContext), executionContext);
        return cursor;
    }

    @Override
    public void close() {
        Misc.free(base);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    private static final class RecordAsAFieldRecord implements Record {

        private Record base;

        @Override
        public Record getRecord(int col) {
            assert col == 0;
            return base;
        }
    }

    private static final class RecordAsAFieldRecordCursor implements DelegatingRecordCursor {
        private final RecordAsAFieldRecord record = new RecordAsAFieldRecord();
        private final RecordAsAFieldRecord recordB = new RecordAsAFieldRecord();
        private RecordCursor base;

        @Override
        public void close() {
            Misc.free(base);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            return base.hasNext();
        }

        @Override
        public Record getRecordB() {
            return recordB;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            base.recordAt(((RecordAsAFieldRecord) record).base, atRowId);
        }

        @Override
        public void toTop() {
            base.toTop();
        }

        @Override
        public long size() {
            return base.size();
        }

        @Override
        public void of(RecordCursor base, SqlExecutionContext executionContext) {
            this.base = base;
            record.base = base.getRecord();
//            recordB.base = base.getRecordB();
        }
    }
}
