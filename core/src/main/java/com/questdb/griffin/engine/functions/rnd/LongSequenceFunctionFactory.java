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

package com.questdb.griffin.engine.functions.rnd;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.GenericRecordMetadata;
import com.questdb.cairo.TableColumnMetadata;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.engine.functions.CursorFunction;
import com.questdb.griffin.engine.functions.GenericRecordCursorFactory;
import com.questdb.std.ObjList;

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
