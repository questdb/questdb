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

final class SingleRowRecordCursor implements RecordCursor {
    private final TableWriterMetricsRecord record = new TableWriterMetricsRecord();
    private boolean recordEmitted;
    private Object[] data;

    public void of(Object[] data) {
        this.data = data;
        recordEmitted = false;
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
        return !recordEmitted && (recordEmitted = true);
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
        recordEmitted = false;
    }

    @Override
    public long size() {
        return 1;
    }

    private class TableWriterMetricsRecord implements Record {
        @Override
        public long getLong(int col) {
            if (col < 0 || col >= data.length) {
                throw CairoException.instance(0).put("unsupported column number. [column=").put(col).put("]");
            }
            Object o = data[col];
            if (o == null) {
                throw CairoException.instance(0).put("long column cannot be null. [column=").put(col).put("]");
            }
            if (!(o instanceof Long)) {
                throw CairoException.instance(0).put("unsupported column type. [column=").put(col).put(", expected-type=").put("Long, actual-type=").put(o.getClass().getSimpleName()).put("]");
            }
            return (long) o;
        }
    }
}
