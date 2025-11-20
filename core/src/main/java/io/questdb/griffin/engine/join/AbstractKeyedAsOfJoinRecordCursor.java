/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.std.Rows;

/**
 * Abstract base class for keyed ASOF JOIN fast record cursors.
 * Contains the common hasNext() implementation and state management logic
 * shared between different keyed ASOF JOIN implementations.
 */
public abstract class AbstractKeyedAsOfJoinRecordCursor extends AbstractAsOfJoinFastRecordCursor {
    protected SqlExecutionCircuitBreaker circuitBreaker;
    protected boolean origHasSlave;
    protected int origSlaveFrameIndex = -1;
    protected long origSlaveRowId = -1;

    public AbstractKeyedAsOfJoinRecordCursor(
            int columnSplit,
            Record nullRecord,
            int masterTimestampIndex,
            int masterTimestampType,
            int slaveTimestampIndex,
            int slaveTimestampType,
            int lookahead
    ) {
        super(columnSplit, nullRecord, masterTimestampIndex, masterTimestampType, slaveTimestampIndex, slaveTimestampType, lookahead);
    }

    @Override
    public boolean hasNext() {
        // Common master cursor iteration logic
        if (isMasterHasNextPending) {
            masterHasNext = masterCursor.hasNext();
            isMasterHasNextPending = false;
        }
        if (!masterHasNext) {
            return false;
        }

        // Common slave cursor state restoration logic
        if (origSlaveRowId != -1) {
            slaveTimeFrameCursor.jumpTo(origSlaveFrameIndex);
            slaveTimeFrameCursor.open();
            slaveTimeFrameCursor.recordAt(slaveRecB, Rows.toRowID(origSlaveFrameIndex, origSlaveRowId));
        }
        record.hasSlave(origHasSlave);

        final long masterTimestamp = scaleTimestamp(masterRecord.getTimestamp(masterTimestampIndex), masterTimestampScale);
        if (masterTimestamp >= lookaheadTimestamp) {
            nextSlave(masterTimestamp);
        }
        // Set `isMasterHasNextPending` only now because `nextSlave()` may throw DataUnavailableException,
        // and in such a case we don't want to call `masterCursor.hasNext()` during the next call to `this.hasNext()`.
        // If we are here, it's clear that nextSlave() did not throw DataUnavailableException.
        isMasterHasNextPending = true;

        boolean hasSlave = record.hasSlave();
        origHasSlave = hasSlave;
        if (!hasSlave) {
            // The non-keyed algo did not find a matching record in the slave table.
            // This means the slave table does not have a single record with a timestamp that is less than or equal
            // to the master record's timestamp.
            // Thus, it cannot possibly have a record with a matching key, since matching timestamps is a prerequisite
            // before we even try to match keys -> we can safely skip the key matching part and report no match.
            return true;
        }

        // Common row ID backup logic
        long rowId = slaveRecB.getRowId();
        origSlaveFrameIndex = Rows.toPartitionIndex(rowId);
        origSlaveRowId = Rows.toLocalRowID(rowId);

        // Reset slave cursor to the current timeframe (nextSlave() call might have moved it)
        TimeFrame timeFrame = slaveTimeFrameCursor.getTimeFrame();
        slaveTimeFrameCursor.jumpTo(timeFrame.getFrameIndex());
        slaveTimeFrameCursor.open();

        // subclasses implement their specific key matching strategy
        performKeyMatching(masterTimestamp);

        return true;
    }

    public void of(RecordCursor masterCursor, TimeFrameRecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
        super.of(masterCursor, slaveCursor);
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void toTop() {
        super.toTop();
        origSlaveFrameIndex = -1;
        origSlaveRowId = -1;
        origHasSlave = false;
    }

    /**
     * Template method for performing key matching with the specific strategy
     * implemented by the concrete subclass.
     *
     * @param masterTimestamp the master record timestamp to match against
     */
    protected abstract void performKeyMatching(long masterTimestamp);
}
