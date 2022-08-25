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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;

final class StringLongTuplesRecordCursor implements RecordCursor {
    private final TableWriterMetricsRecord record = new TableWriterMetricsRecord();
    private String[] keys;
    private long[] values;
    private int pos;

    public void of(String[] keys, long[] values) {
        assert keys.length == values.length;
        this.keys = keys;
        this.values = values;
        toTop();
    }

    @Override
    public void close() {

    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public boolean hasNext() {
        if (keys.length > pos + 1) {
            pos++;
            return true;
        }
        return false;
    }

    @Override
    public Record getRecordB() {
        throw new UnsupportedOperationException("RecordB not supported");
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        throw new UnsupportedOperationException("random access not supported");
    }

    @Override
    public void toTop() {
        pos = -1;
    }

    @Override
    public long size() {
        return keys.length;
    }

    private class TableWriterMetricsRecord implements Record {
        @Override
        public CharSequence getStr(int col) {
            if (col != 0) {
                throw CairoException.instance(0).put("unsupported string column number [column=").put(col).put("]");
            }
            return keys[pos];
        }

        @Override
        public CharSequence getStrB(int col) {
            return getStr(col);
        }

        @Override
        public int getStrLen(int col) {
            if (col != 0) {
                throw CairoException.instance(0).put("unsupported string column number [column=").put(col).put("]");
            }
            return getStr(col).length();
        }

        @Override
        public long getLong(int col) {
            if (col != 1) {
                throw CairoException.instance(0).put("unsupported long column number [column=").put(col).put("]");
            }
            return values[pos];
        }
    }
}
