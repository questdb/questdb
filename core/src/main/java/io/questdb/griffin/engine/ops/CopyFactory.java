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

package io.questdb.griffin.engine.ops;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.SingleValueRecordCursor;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;

public class CopyFactory extends AbstractRecordCursorFactory {
    private final static GenericRecordMetadata METADATA = new GenericRecordMetadata();
    private final ImportIdRecord record = new ImportIdRecord();
    private final SingleValueRecordCursor cursor = new SingleValueRecordCursor(record);

    public CopyFactory(long importId) {
        super(METADATA);
        StringSink importIdSink = new StringSink();
        Numbers.appendHex(importIdSink, importId, true);
        record.setValue(importIdSink);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        cursor.toTop();
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    private static class ImportIdRecord implements Record {
        private CharSequence value;

        public void setValue(CharSequence value) {
            this.value = value;
        }

        @Override
        public CharSequence getStr(int col) {
            return value;
        }

        @Override
        public int getStrLen(int col) {
            return value.length();
        }

        @Override
        public CharSequence getStrB(int col) {
            // the sink is immutable
            return getStr(col);
        }
    }

    static {
        METADATA.add(new TableColumnMetadata("id", 1, ColumnType.STRING));
    }
}
