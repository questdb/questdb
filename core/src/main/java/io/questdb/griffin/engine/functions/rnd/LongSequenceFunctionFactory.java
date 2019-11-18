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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.GenericRecordCursorFactory;
import io.questdb.std.ObjList;

public class LongSequenceFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
        METADATA = metadata;
    }

    @Override
    public String getSignature() {
        return "long_sequence(l)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {

        final long recordCount = args.getQuick(0).getLong(null);

        return new CursorFunction(
                position,
                new GenericRecordCursorFactory(
                        METADATA,
                        new LongSequenceRecordCursor(Math.max(0L, recordCount)
                        ),
                        true
                )
        );
    }

    static class LongSequenceRecordCursor implements RecordCursor {

        private final long recordCount;
        private final LongSequenceRecord record = new LongSequenceRecord();

        public LongSequenceRecordCursor(long recordCount) {
            this.recordCount = recordCount;
            this.record.of(0);
        }

        @Override
        public void close() {
        }

        @Override
        public long size() {
            return recordCount;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (record.getValue() < recordCount) {
                record.next();
                return true;
            }
            return false;
        }

        @Override
        public Record newRecord() {
            return new LongSequenceRecord();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((LongSequenceRecord) record).of(atRowId);
        }

        @Override
        public void recordAt(long rowId) {
            record.of(rowId);
        }

        @Override
        public void toTop() {
            record.of(0);
        }
    }

    static class LongSequenceRecord implements Record {
        private long value;

        @Override
        public long getLong(int col) {
            return value;
        }

        @Override
        public long getRowId() {
            return value;
        }

        long getValue() {
            return value;
        }

        void next() {
            value++;
        }

        void of(long value) {
            this.value = value;
        }
    }
}
