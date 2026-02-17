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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

// This exec plan is filter-less Nested Loop
public class CrossJoinRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final CrossJoinRecordCursor cursor;

    public CrossJoinRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit
    ) {
        super(metadata, null, masterFactory, slaveFactory);
        this.cursor = new CrossJoinRecordCursor(columnSplit);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        RecordCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getCursor(executionContext);
            cursor.of(masterCursor, slaveCursor, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable ex) {
            Misc.free(masterCursor);
            Misc.free(slaveCursor);
            throw ex;
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
        sink.type("Cross Join");
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
    }

    private static class CrossJoinRecordCursor extends AbstractJoinCursor {
        private final JoinRecord record;
        private final RecordCursor.Counter tmpCounter;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMasterHasNextPending;
        private boolean isMasterSizeCalculated;
        private boolean isSlavePartialSizeCalculated;
        private boolean isSlaveReset;
        private boolean isSlaveSizeCalculated;
        private boolean masterHasNext;
        private long masterSize;
        private long slavePartialSize;
        private long slaveSize;

        public CrossJoinRecordCursor(int columnSplit) {
            super(columnSplit);
            this.record = new JoinRecord(columnSplit);
            tmpCounter = new Counter();
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
            if (!isMasterHasNextPending && !masterHasNext) {
                return;
            }

            if (!isSlavePartialSizeCalculated) {
                slaveCursor.calculateSize(circuitBreaker, tmpCounter);
                slavePartialSize = tmpCounter.get();
                isSlavePartialSizeCalculated = true;
                tmpCounter.clear();
            }
            if (!isMasterSizeCalculated) {
                masterCursor.calculateSize(circuitBreaker, tmpCounter);
                masterSize = tmpCounter.get();
                isMasterSizeCalculated = true;
                tmpCounter.clear();
            }

            if (masterSize == 0) {
                if (!isMasterHasNextPending) {
                    counter.add(slavePartialSize);
                }
            } else {
                if (!isSlaveSizeCalculated) {
                    if (!isSlaveReset) {
                        slaveCursor.toTop();
                        isSlaveReset = true;
                    }
                    slaveCursor.calculateSize(circuitBreaker, tmpCounter);
                    slaveSize = tmpCounter.get();
                    isSlaveSizeCalculated = true;
                }

                long size = masterSize * slaveSize;
                if (!isMasterHasNextPending) {
                    size += slavePartialSize; // slave cursor was in the middle of the data set
                }
                counter.add(size);
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (isMasterHasNextPending) {
                    masterHasNext = masterCursor.hasNext();
                    isMasterHasNextPending = false;
                }

                if (!masterHasNext) {
                    return false;
                }

                circuitBreaker.statefulThrowExceptionIfTripped();
                if (slaveCursor.hasNext()) {
                    return true;
                }

                slaveCursor.toTop();
                isMasterHasNextPending = true;
            }
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            long sizeA = masterCursor.size();
            long sizeB = slaveCursor.size();
            if (sizeA == -1 || sizeB == -1) {
                return -1;
            }
            final long result = sizeA * sizeB;
            return result < sizeA && sizeB > 0 ? Long.MAX_VALUE : result;
        }

        @Override
        public void skipRows(Counter rowCount) {
            if (rowCount.get() == 0) {
                return;
            }

            if (!isSlaveSizeCalculated) {
                slaveCursor.calculateSize(this.circuitBreaker, tmpCounter);
                slaveSize = tmpCounter.get();
                isSlaveSizeCalculated = true;
                tmpCounter.clear();
            }

            if (slaveSize == 0) {
                return;
            }

            long masterToSkip = rowCount.get() / slaveSize;
            tmpCounter.set(masterToSkip);

            masterCursor.skipRows(tmpCounter);
            masterHasNext = masterCursor.hasNext();
            isMasterHasNextPending = false;

            long diff = (masterToSkip - tmpCounter.get()) * slaveSize;
            rowCount.dec(diff);

            if (!masterHasNext) {
                return;
            }

            if (!isSlaveReset) {
                slaveCursor.toTop();
                isSlaveReset = true;
            }
            slaveCursor.skipRows(rowCount);
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            slaveCursor.toTop();
            isMasterHasNextPending = true;

            isSlavePartialSizeCalculated = false;
            isSlaveReset = false;
            isSlaveSizeCalculated = false;
            isMasterSizeCalculated = false;
            tmpCounter.clear();
            masterSize = 0;
            slaveSize = 0;
            slavePartialSize = 0;
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            record.of(masterCursor.getRecord(), slaveCursor.getRecord());
            isMasterHasNextPending = true;
            this.circuitBreaker = circuitBreaker;

            isSlavePartialSizeCalculated = false;
            isSlaveReset = false;
            isSlaveSizeCalculated = false;
            isMasterSizeCalculated = false;
            tmpCounter.clear();
            masterSize = 0;
            slaveSize = 0;
            slavePartialSize = 0;
        }
    }
}
