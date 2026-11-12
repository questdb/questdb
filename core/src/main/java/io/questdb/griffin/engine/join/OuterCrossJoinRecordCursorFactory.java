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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.Misc;
import org.jetbrains.annotations.Nullable;

/**
 * Joins a key-less CROSS input to the rows of a RIGHT or FULL join whose master side held the input: each row the
 * join NULL-extends on its master side returns once, with the input's columns NULL, and every other row returns once
 * per row of the input. The join's master side must hold no row when the input holds none, so the factory opens the
 * input before the join and publishes whether it has rows through {@link SlaveRows}, which gates the join's master.
 * The master's record is the join's {@link RightOuterJoinRecord}, read directly or through a filter.
 */
public class OuterCrossJoinRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final SlaveRows slaveRows;
    private OuterCrossJoinRecordCursor cursor;

    /**
     * Consumes both factories on entry, including failure. {@code slaveRows} gates the master side of the RIGHT or
     * FULL join under {@code masterFactory}, which owns it.
     */
    public OuterCrossJoinRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            SlaveRows slaveRows
    ) {
        super(metadata, null, masterFactory, slaveFactory);
        this.slaveRows = slaveRows;
        try {
            this.cursor = new OuterCrossJoinRecordCursor(columnSplit, NullRecordFactory.getInstance(slaveFactory.getMetadata()));
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public boolean followedOrderByAdvice() {
        return masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor slaveCursor = slaveFactory.getCursor(executionContext);
        RecordCursor masterCursor = null;
        try {
            slaveCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
            final boolean hasSlaveRows = slaveCursor.hasNext();
            slaveCursor.toTop();
            slaveRows.of(hasSlaveRows);
            masterCursor = masterFactory.getCursor(executionContext);
            cursor.of(masterCursor, slaveCursor, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable th) {
            Misc.free(masterCursor);
            Misc.free(slaveCursor);
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return masterFactory.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return masterFactory.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Outer Cross Join");
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        final Throwable failure = closeJoinOwnersBestEffort();
        cursor = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Whether the slave of an {@link OuterCrossJoinRecordCursorFactory} has rows in the current execution, as the
     * filter of the gate over the master side of the RIGHT or FULL join under it; the gate owns it.
     */
    public static final class SlaveRows extends BooleanFunction {
        private final @Nullable CharSequence alias;
        private boolean hasRows;

        public SlaveRows(@Nullable CharSequence alias) {
            this.alias = alias;
        }

        @Override
        public boolean getBool(Record rec) {
            return hasRows;
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("exists(");
            if (alias != null) {
                sink.val(alias);
            }
            sink.val(')');
        }

        private void of(boolean hasRows) {
            this.hasRows = hasRows;
        }
    }

    private static class OuterCrossJoinRecordCursor extends AbstractJoinCursor {
        private final OuterJoinRecord record;
        private final Counter slaveCounter = new Counter();
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMasterPending;
        private RightOuterJoinRecord masterRecord;
        private long slaveSize;

        OuterCrossJoinRecordCursor(int columnSplit, Record slaveNullRecord) {
            super(columnSplit);
            this.record = new OuterJoinRecord(columnSplit, slaveNullRecord);
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (!isMasterPending) {
                slaveCursor.calculateSize(circuitBreaker, counter);
                isMasterPending = true;
            }
            if (slaveSize < 0) {
                slaveCursor.toTop();
                slaveSize = slaveCursor.size();
                if (slaveSize < 0) {
                    slaveCounter.clear();
                    slaveCursor.calculateSize(circuitBreaker, slaveCounter);
                    slaveSize = slaveCounter.get();
                }
            }
            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                counter.add(masterRecord.hasMaster() ? slaveSize : 1);
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (isMasterPending) {
                    if (!masterCursor.hasNext()) {
                        return false;
                    }
                    if (!masterRecord.hasMaster()) {
                        record.hasSlave(false);
                        return true;
                    }
                    record.hasSlave(true);
                    isMasterPending = false;
                }
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                if (slaveCursor.hasNext()) {
                    return true;
                }
                slaveCursor.toTop();
                isMasterPending = true;
            }
        }

        @Override
        public long preComputedStateSize() {
            return masterCursor.preComputedStateSize() + slaveCursor.preComputedStateSize();
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            slaveCursor.toTop();
            isMasterPending = true;
            record.hasSlave(true);
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            assert masterCursor.getRecord() instanceof RightOuterJoinRecord : "outer cross join master is not a RIGHT or FULL join";
            final RightOuterJoinRecord masterRecord = (RightOuterJoinRecord) masterCursor.getRecord();
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            this.masterRecord = masterRecord;
            this.circuitBreaker = circuitBreaker;
            record.of(masterRecord, slaveCursor.getRecord());
            isMasterPending = true;
            slaveSize = -1;
        }
    }
}
