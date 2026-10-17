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
import io.questdb.std.Misc;
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
        if (args != null) {
            final int argCount = args.size();
            final Function countFunc = args.getQuick(0);

            if (argCount == 1 && ColumnType.isConvertibleFrom(countFunc.getType(), ColumnType.LONG)) {
                try {
                    return new CursorFunction(new LongSequenceCursorFactory(METADATA, countFunc));
                } catch (UnsupportedOperationException ex) {
                    throw SqlException.position(position).put("argument type ")
                            .put(ColumnType.nameOf(countFunc.getType())).put(" is not supported");
                }
            }

            final Function seedLoFunc;
            final Function seedHiFunc;
            if (
                    argCount > 2
                            && ColumnType.isSameOrBuiltInWideningCast(countFunc.getType(), ColumnType.LONG)
                            && ColumnType.isSameOrBuiltInWideningCast((seedLoFunc = args.getQuick(1)).getType(), ColumnType.LONG)
                            && ColumnType.isSameOrBuiltInWideningCast((seedHiFunc = args.getQuick(2)).getType(), ColumnType.LONG)
            ) {
                final Function function = new CursorFunction(
                        new SeedingLongSequenceCursorFactory(METADATA, countFunc, seedLoFunc, seedHiFunc)
                );
                for (int i = 3; i < argCount; i++) {
                    Misc.free(args.getQuick(i));
                }
                return function;
            }
        }
        throw SqlException.position(position).put("invalid arguments");
    }

    private static class LongSequenceCursorFactory extends AbstractRecordCursorFactory {
        private final Function countFunc;
        private final LongSequenceRecordCursor cursor = new LongSequenceRecordCursor();

        public LongSequenceCursorFactory(RecordMetadata metadata, Function countFunc) {
            super(metadata);
            this.countFunc = countFunc;
            if (countFunc.isConstant()) {
                cursor.of(countFunc.getLong(null));
            }
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            countFunc.init(null, executionContext);
            cursor.of(countFunc.getLong(null));
            cursor.circuitBreaker = executionContext.getCircuitBreaker();
            cursor.toTop();
            return cursor;
        }

        // The produced relation is always 1..N; deterministic by construction.
        @Override
        public boolean isNonDeterministic() {
            return false;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("long_sequence");
            sink.meta("count");
            // a runtime-constant count, such as a scalar sub-query, takes its value when the cursor opens
            if (countFunc.isConstant()) {
                sink.val(cursor.recordCount);
            } else {
                sink.val(countFunc);
            }
        }

        @Override
        protected void _close() {
            Misc.free(countFunc);
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

        void of(long recordCount) {
            this.recordCount = Math.max(0L, recordCount);
        }
    }

    private static class SeedingLongSequenceCursorFactory extends LongSequenceCursorFactory {
        private final Rnd rnd = new Rnd();
        private final Function seedHiFunc;
        private final Function seedLoFunc;

        public SeedingLongSequenceCursorFactory(RecordMetadata metadata, Function countFunc, Function seedLoFunc, Function seedHiFunc) {
            super(metadata, countFunc);
            this.seedLoFunc = seedLoFunc;
            this.seedHiFunc = seedHiFunc;
        }

        // The rnd seeds are reset on every open, so even downstream seeded rnd_* draws are reproducible.
        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            final RecordCursor cursor = super.getCursor(executionContext);
            seedLoFunc.init(null, executionContext);
            seedHiFunc.init(null, executionContext);
            rnd.reset(seedLoFunc.getLong(null), seedHiFunc.getLong(null));
            executionContext.setRandom(rnd);
            return cursor;
        }

        @Override
        public void toPlan(PlanSink sink) {
            super.toPlan(sink);
            sink.meta("seedLo");
            seedToPlan(sink, seedLoFunc);
            sink.meta("seedHi");
            seedToPlan(sink, seedHiFunc);
        }

        @Override
        protected void _close() {
            super._close();
            Misc.free(seedLoFunc);
            Misc.free(seedHiFunc);
        }

        private static void seedToPlan(PlanSink sink, Function seedFunc) {
            if (seedFunc.isConstant()) {
                sink.val(seedFunc.getLong(null));
            } else {
                sink.val(seedFunc);
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
        METADATA = metadata;
    }
}
