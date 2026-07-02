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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjList;

class MergeUnionAllRecordCursor extends AbstractSetRecordCursor implements NoRandomAccessRecordCursor {
    private final boolean isAscending;
    private final AbstractUnionRecord record;
    private final int timestampIndex;
    private boolean hasPendingA;
    private boolean hasPendingB;
    private boolean isLastA;
    private boolean isStarted;
    private Record recordA;
    private Record recordB;

    public MergeUnionAllRecordCursor(
            ObjList<Function> castFunctionsA,
            ObjList<Function> castFunctionsB,
            int timestampIndex,
            boolean isAscending
    ) {
        if (castFunctionsA != null && castFunctionsB != null) {
            this.record = new UnionCastRecord(castFunctionsA, castFunctionsB);
        } else {
            assert castFunctionsA == null && castFunctionsB == null;
            this.record = new UnionRecord();
        }
        this.timestampIndex = timestampIndex;
        this.isAscending = isAscending;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        // Counts row by row rather than summing the branches' O(frames) fast size path (as concat
        // UnionAllRecordCursor does): the merge reads one row ahead from each branch, so delegating to
        // cursorA.calculateSize() + cursorB.calculateSize() would miscount the buffered rows. Only reached
        // when a branch's size() is unknown (e.g. a filtered branch). Cancellation still fires at the
        // branches' page-frame boundaries through cursorA/cursorB.hasNext().
        while (hasNext()) {
            counter.inc();
        }
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public boolean hasNext() {
        if (!isStarted) {
            hasPendingA = cursorA.hasNext();
            hasPendingB = cursorB.hasNext();
            isStarted = true;
        } else if (isLastA) {
            hasPendingA = cursorA.hasNext();
        } else {
            hasPendingB = cursorB.hasNext();
        }

        if (hasPendingA && hasPendingB) {
            final long tsA = recordA.getLong(timestampIndex);
            final long tsB = recordB.getLong(timestampIndex);
            isLastA = isAscending ? tsA <= tsB : tsA >= tsB;
        } else if (hasPendingA) {
            isLastA = true;
        } else if (hasPendingB) {
            isLastA = false;
        } else {
            return false;
        }
        record.setAb(isLastA);
        return true;
    }

    @Override
    public long preComputedStateSize() {
        return cursorA.preComputedStateSize() + cursorB.preComputedStateSize();
    }

    @Override
    public long size() {
        return sumBranchSizes();
    }

    @Override
    public void toTop() {
        isStarted = false;
        hasPendingA = false;
        hasPendingB = false;
        isLastA = true;
        record.setAb(true);
        cursorA.toTop();
        cursorB.toTop();
    }

    @Override
    void of(RecordCursor cursorA, RecordCursor cursorB, SqlExecutionContext executionContext) throws SqlException {
        super.of(cursorA, cursorB, executionContext);
        this.recordA = cursorA.getRecord();
        this.recordB = cursorB.getRecord();
        record.of(recordA, recordB);
        toTop();
    }
}
