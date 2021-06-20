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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class LongSequenceFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;

    @Override
    public String getSignature() {
        return "long_sequence(v)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        Function countFunc;
        final Function seedLoFunc;
        final Function seedHiFunc;
        final int argCount = args.size();
        if (argCount == 1 && SqlCompiler.isAssignableFrom(ColumnType.LONG, (countFunc = args.getQuick(0)).getType())) {
            return new CursorFunction(
                    new LongSequenceCursorFactory(METADATA, countFunc.getLong(null))
            );
        }

        if (
                argCount > 2
                        && SqlCompiler.isAssignableFrom(ColumnType.LONG, (countFunc = args.getQuick(0)).getType())
                        && SqlCompiler.isAssignableFrom(ColumnType.LONG, (seedLoFunc = args.getQuick(1)).getType())
                        && SqlCompiler.isAssignableFrom(ColumnType.LONG, (seedHiFunc = args.getQuick(2)).getType())
        ) {
            return new CursorFunction(
                    new SeedingLongSequenceCursorFactory(
                            METADATA,
                            countFunc.getLong(null),
                            seedLoFunc.getLong(null),
                            seedHiFunc.getLong(null)
                    )
            );
        }

        throw SqlException.position(position).put("invalid arguments");
    }

    private static class LongSequenceCursorFactory extends AbstractRecordCursorFactory {
        private final RecordCursor cursor;

        public LongSequenceCursorFactory(RecordMetadata metadata, long recordCount) {
            super(metadata);
            this.cursor = new LongSequenceRecordCursor(Math.max(0L, recordCount));
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }
    }

    private static class SeedingLongSequenceCursorFactory extends AbstractRecordCursorFactory {
        private final RecordCursor cursor;
        private final Rnd rnd;
        private final long seedLo;
        private final long seedHi;

        public SeedingLongSequenceCursorFactory(RecordMetadata metadata, long recordCount, long seedLo, long seedHi) {
            super(metadata);
            this.cursor = new LongSequenceRecordCursor(Math.max(0L, recordCount));
            this.rnd = new Rnd(this.seedLo = seedLo, this.seedHi = seedHi);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            rnd.reset(this.seedLo, this.seedHi);
            executionContext.setRandom(rnd);
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }
    }


    static class LongSequenceRecordCursor implements RecordCursor {

        private final long recordCount;
        private final LongSequenceRecord recordA = new LongSequenceRecord();
        private final LongSequenceRecord recordB = new LongSequenceRecord();

        public LongSequenceRecordCursor(long recordCount) {
            this.recordCount = recordCount;
            this.recordA.of(0);
        }

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public boolean hasNext() {
            if (recordA.getValue() < recordCount) {
                recordA.next();
                return true;
            }
            return false;
        }

        @Override
        public Record getRecordB() {
            return recordB;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((LongSequenceRecord) record).of(atRowId);
        }

        @Override
        public void toTop() {
            recordA.of(0);
        }

        @Override
        public long size() {
            return recordCount;
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

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("x", ColumnType.LONG, null));
        METADATA = metadata;
    }
}
