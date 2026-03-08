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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.RecordIdSink;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Nested Loop with filter join.
 * Iterates on master factory in outer loop and on slave factory in inner loop
 * and returns all row pairs matching filter plus all unmatched rows from master and slave factory.
 */
public class NestedLoopFullJoinRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final NestedLoopFullRecordCursor cursor;
    private final Function filter;

    public NestedLoopFullJoinRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            @NotNull Function filter,
            @NotNull Record masterNullRecord,
            @NotNull Record slaveNullRecord
    ) {
        super(metadata, null, masterFactory, slaveFactory);
        this.filter = filter;
        Map matchIdsMap = null;
        try {
            matchIdsMap = MapFactory.createUnorderedMap(configuration, RecordIdSink.RECORD_ID_COLUMN_TYPE, ArrayColumnTypes.EMPTY);
            this.cursor = new NestedLoopFullRecordCursor(columnSplit, filter, matchIdsMap, masterNullRecord, slaveNullRecord);
        } catch (Throwable e) {
            Misc.free(matchIdsMap);
            close();
            throw e;
        }
    }

    @Override
    public boolean followedOrderByAdvice() {
        return slaveFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        RecordCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getCursor(executionContext);
            cursor.of(masterCursor, slaveCursor, executionContext);
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
        sink.type("Nested Loop Full Join");
        sink.attr("filter").val(filter);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Misc.freeIfCloseable(getMetadata());
        Misc.free(masterFactory);
        Misc.free(slaveFactory);
        Misc.free(filter);
        Misc.free(cursor);
    }

    private static class NestedLoopFullRecordCursor extends AbstractJoinCursor {
        private final Function filter;
        private final Map matchIdsMap;
        private final FullOuterJoinRecord record;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMasterHasNextPending;
        private boolean isMatch;
        private boolean isOpen;
        private boolean masterHasNext;
        private Record slaveRecord;

        public NestedLoopFullRecordCursor(int columnSplit, Function filter, Map matchIdsMap, Record masterNullRecord, Record slaveNullRecord) {
            super(columnSplit);
            this.record = new FullOuterJoinRecord(columnSplit, masterNullRecord, slaveNullRecord);
            this.filter = filter;
            this.isMatch = false;
            this.matchIdsMap = matchIdsMap;
            isOpen = true;
        }

        @Override
        public void close() {
            if (isOpen) {
                super.close();
                Misc.free(matchIdsMap);
                isOpen = false;
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            while (true) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                if (isMasterHasNextPending) {
                    masterHasNext = masterCursor.hasNext();
                    isMasterHasNextPending = false;
                }

                if (!masterHasNext) {
                    while (slaveCursor.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        MapKey keys = matchIdsMap.withKey();
                        keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                        if (keys.findValue() == null) {
                            record.hasMaster(false);
                            return true;
                        }
                    }
                    return false;
                }

                while (slaveCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    if (filter.getBool(record)) {
                        MapKey keys = matchIdsMap.withKey();
                        keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                        keys.createValue();
                        isMatch = true;
                        return true;
                    }
                }

                if (!isMatch) {
                    isMatch = true;
                    record.hasSlave(false);
                    return true;
                }

                isMatch = false;
                slaveCursor.toTop();
                record.hasSlave(true);
                isMasterHasNextPending = true;
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
            filter.toTop();
            isMatch = false;
            isMasterHasNextPending = true;
            record.hasMaster(true);
            matchIdsMap.clear();
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionContext executionContext) throws SqlException {
            this.masterCursor = masterCursor;
            this.slaveCursor = slaveCursor;
            filter.init(this, executionContext);
            this.slaveRecord = slaveCursor.getRecord();
            record.of(masterCursor.getRecord(), this.slaveRecord);
            isMasterHasNextPending = true;
            if (!isOpen) {
                isOpen = true;
                matchIdsMap.reopen();
            }
            circuitBreaker = executionContext.getCircuitBreaker();
        }
    }
}
