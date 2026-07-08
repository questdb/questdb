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
    private long tsA;
    private long tsB;

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
        // Once iteration has started, one row is buffered per
        // branch and summing branch sizes would double-count them, so fall back to draining.
        if (!isStarted) {
            cursorA.calculateSize(circuitBreaker, counter);
            cursorB.calculateSize(circuitBreaker, counter);
            return;
        }
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
            if (hasPendingA && hasPendingB) {
                tsA = recordA.getLong(timestampIndex);
                tsB = recordB.getLong(timestampIndex);
            }
            isStarted = true;
        } else if (isLastA) {
            hasPendingA = cursorA.hasNext();
            if (hasPendingA && hasPendingB) {
                tsA = recordA.getLong(timestampIndex);
            }
        } else {
            hasPendingB = cursorB.hasNext();
            if (hasPendingA && hasPendingB) {
                tsB = recordB.getLong(timestampIndex);
            }
        }

        if (hasPendingA && hasPendingB) {
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
