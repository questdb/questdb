/*+*****************************************************************************
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
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
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
                if (countFunc.isConstant()) {
                    try {
                        return new CursorFunction(
                                new LongSequenceCursorFactory(METADATA, countFunc.getLong(null))
                        );
                    } catch (UnsupportedOperationException ex) {
                        throw unsupportedArgumentType(position, countFunc);
                    }
                }
                // A runtime constant (bind variable) is read at cursor open. DOUBLE and FLOAT pass
                // isConvertibleFrom() as narrowing casts, but their getLong() throws, so reject them now.
                final short countTypeTag = ColumnType.tagOf(countFunc.getType());
                if (countTypeTag == ColumnType.DOUBLE || countTypeTag == ColumnType.FLOAT) {
                    throw unsupportedArgumentType(position, countFunc);
                }
                return new CursorFunction(new LongSequenceCursorFactory(METADATA, countFunc));
            }

            if (
                    argCount > 2
                            && ColumnType.isSameOrBuiltInWideningCast((countFunc = args.getQuick(0)).getType(), ColumnType.LONG)
                            && ColumnType.isSameOrBuiltInWideningCast((seedLoFunc = args.getQuick(1)).getType(), ColumnType.LONG)
                            && ColumnType.isSameOrBuiltInWideningCast((seedHiFunc = args.getQuick(2)).getType(), ColumnType.LONG)
            ) {
                if (countFunc.isConstant() && seedLoFunc.isConstant() && seedHiFunc.isConstant()) {
                    return new CursorFunction(
                            new SeedingLongSequenceCursorFactory(
                                    METADATA,
                                    countFunc.getLong(null),
                                    seedLoFunc.getLong(null),
                                    seedHiFunc.getLong(null)
                            )
                    );
                }
                return new CursorFunction(
                        new SeedingLongSequenceCursorFactory(METADATA, countFunc, seedLoFunc, seedHiFunc)
                );
            }
        }
        throw SqlException.position(position).put("invalid arguments");
    }

    // Untyped bind variables become LONG rather than the default STRING, which the seeded
    // arm's widening check would reject.
    @Override
    public int resolvePreferredVariadicType(int sqlPos, int argPos, ObjList<Function> args) {
        return ColumnType.LONG;
    }

    private static SqlException unsupportedArgumentType(int position, Function countFunc) {
        return SqlException.position(position).put("argument type ")
                .put(ColumnType.nameOf(countFunc.getType())).put(" is not supported");
    }

    private static class LongSequenceCursorFactory extends AbstractRecordCursorFactory {
        // null when the count is a compile-time constant; otherwise read at every cursor open
        private final Function countFunc;
        private final LongSequenceRecordCursor cursor = new LongSequenceRecordCursor();

        public LongSequenceCursorFactory(RecordMetadata metadata, long recordCount) {
            super(metadata);
            this.countFunc = null;
            cursor.of(recordCount);
        }

        public LongSequenceCursorFactory(RecordMetadata metadata, Function countFunc) {
            super(metadata);
            this.countFunc = countFunc;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            if (countFunc != null) {
                countFunc.init(null, executionContext);
                cursor.of(countFunc.getLong(null));
            }
            cursor.circuitBreaker = executionContext.getCircuitBreaker();
            cursor.toTop();
            return cursor;
        }

        // The produced relation is always 1..N; with a constant N it is deterministic by
        // construction, while a bind variable N can change between opens.
        @Override
        public boolean isNonDeterministic() {
            return countFunc != null;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("long_sequence");
            if (countFunc != null) {
                sink.meta("count").val(countFunc);
            } else {
                sink.meta("count").val(cursor.recordCount);
            }
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
        private SqlExecutionCircuitBreaker circuitBreaker = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
        private long recordCount;

        public LongSequenceRecordCursor() {
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
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
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

        // Clamps a negative or NULL (Long.MIN_VALUE) count to an empty sequence.
        void of(long recordCount) {
            this.recordCount = Math.max(0L, recordCount);
        }
    }

    private static class SeedingLongSequenceCursorFactory extends AbstractRecordCursorFactory {
        // null when every argument is a compile-time constant; otherwise read at every cursor open
        private final Function countFunc;
        private final LongSequenceRecordCursor cursor = new LongSequenceRecordCursor();
        private final Rnd rnd = new Rnd();
        private final Function seedHiFunc;
        private final Function seedLoFunc;
        private long seedHi;
        private long seedLo;

        public SeedingLongSequenceCursorFactory(RecordMetadata metadata, long recordCount, long seedLo, long seedHi) {
            super(metadata);
            this.countFunc = null;
            this.seedLoFunc = null;
            this.seedHiFunc = null;
            cursor.of(recordCount);
            this.seedLo = seedLo;
            this.seedHi = seedHi;
        }

        public SeedingLongSequenceCursorFactory(RecordMetadata metadata, Function countFunc, Function seedLoFunc, Function seedHiFunc) {
            super(metadata);
            this.countFunc = countFunc;
            this.seedLoFunc = seedLoFunc;
            this.seedHiFunc = seedHiFunc;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            if (countFunc != null) {
                countFunc.init(null, executionContext);
                seedLoFunc.init(null, executionContext);
                seedHiFunc.init(null, executionContext);
                cursor.of(countFunc.getLong(null));
                seedLo = seedLoFunc.getLong(null);
                seedHi = seedHiFunc.getLong(null);
            }
            rnd.reset(seedLo, seedHi);
            executionContext.setRandom(rnd);
            cursor.circuitBreaker = executionContext.getCircuitBreaker();
            cursor.toTop();
            return cursor;
        }

        // The produced relation is always 1..N and the rnd seeds reset on every open, so with
        // constant arguments even downstream seeded rnd_* draws are reproducible. A bind variable
        // argument can change between opens.
        @Override
        public boolean isNonDeterministic() {
            return countFunc != null;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("long_sequence");
            if (countFunc != null) {
                sink.meta("count").val(countFunc);
                sink.meta("seedLo").val(seedLoFunc);
                sink.meta("seedHi").val(seedHiFunc);
            } else {
                sink.meta("count").val(cursor.recordCount);
                sink.meta("seedLo").val(seedLo);
                sink.meta("seedHi").val(seedHi);
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
        METADATA = metadata;
    }
}
