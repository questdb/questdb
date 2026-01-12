/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Transient;

public class LongSequenceFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;

    @Override
    public String getSignature() {
        return "long_sequence(v)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function countFunc;
        final Function seedLoFunc;
        final Function seedHiFunc;
        if (args != null) {
            final int argCount = args.size();
            countFunc = args.getQuick(0);

            if (argCount == 1 && ColumnType.isConvertibleFrom(countFunc.getType(), ColumnType.LONG)) {
                try {
                    return new CursorFunction(
                            new LongSequenceCursorFactory(METADATA, countFunc.getLong(null))
                    );
                } catch (UnsupportedOperationException ex) {
                    throw SqlException.position(position).put("argument type ")
                            .put(ColumnType.nameOf(countFunc.getType())).put(" is not supported");
                }
            }

            if (
                    argCount > 2
                            && ColumnType.isSameOrBuiltInWideningCast((countFunc = args.getQuick(0)).getType(), ColumnType.LONG)
                            && ColumnType.isSameOrBuiltInWideningCast((seedLoFunc = args.getQuick(1)).getType(), ColumnType.LONG)
                            && ColumnType.isSameOrBuiltInWideningCast((seedHiFunc = args.getQuick(2)).getType(), ColumnType.LONG)
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
        }
        throw SqlException.position(position).put("invalid arguments");
    }

    private static class LongSequenceCursorFactory extends AbstractRecordCursorFactory {
        private final LongSequenceRecordCursor cursor;

        public LongSequenceCursorFactory(RecordMetadata metadata, long recordCount) {
            super(metadata);
            this.cursor = new LongSequenceRecordCursor(Math.max(0L, recordCount));
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("long_sequence");
            sink.meta("count").val(cursor.recordCount);
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

    static class LongSequenceRecordCursor implements RecordCursor {

        private final LongSequenceRecord recordA = new LongSequenceRecord();
        private final LongSequenceRecord recordB = new LongSequenceRecord();
        private final long recordCount;

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
        public Record getRecordB() {
            return recordB;
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
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            ((LongSequenceRecord) record).of(atRowId);
        }

        @Override
        public long size() {
            return recordCount;
        }

        @Override
        public void toTop() {
            recordA.of(0);
        }
    }

    private static class SeedingLongSequenceCursorFactory extends AbstractRecordCursorFactory {
        private final LongSequenceRecordCursor cursor;
        private final Rnd rnd;
        private final long seedHi;
        private final long seedLo;

        public SeedingLongSequenceCursorFactory(RecordMetadata metadata, long recordCount, long seedLo, long seedHi) {
            super(metadata);
            this.cursor = new LongSequenceRecordCursor(Math.max(0L, recordCount));
            this.rnd = new Rnd(this.seedLo = seedLo, this.seedHi = seedHi);
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            rnd.reset(this.seedLo, this.seedHi);
            executionContext.setRandom(rnd);
            cursor.toTop();
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("long_sequence");
            sink.meta("count").val(cursor.recordCount);
            sink.meta("seedLo").val(seedLo);
            sink.meta("seedHi").val(seedHi);
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
        METADATA = metadata;
    }
}
