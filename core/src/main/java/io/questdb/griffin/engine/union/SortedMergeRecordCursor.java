/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.RecordComparator;

public final class SortedMergeRecordCursor extends AbstractSetRecordCursor implements NoRandomAccessRecordCursor {
    private static final int INITIAL_STATE = 0;
    private static final int READ_FROM_A = 1;
    private static final int READ_FROM_B = 2;
    private static final int READ_FROM_B__A_EXHAUSTED = 3;
    private static final int READ_FROM_A__B_EXHAUSTED = 4;
    private static final int BOTH_EXHAUSTED = 5;

    private final ComparingUnionRecord unionRecord;
    private int state = INITIAL_STATE;


    public SortedMergeRecordCursor() {
        this.unionRecord = new ComparingUnionRecord();
    }

    @Override
    public Record getRecord() {
        return unionRecord;
    }

    @Override
    public boolean hasNext() {
        boolean aHasNext;
        boolean bHasNext;
        switch (state) {
            case INITIAL_STATE:
                aHasNext = cursorA.hasNext();
                bHasNext = cursorB.hasNext();
                if (!aHasNext && !bHasNext) {
                    state = BOTH_EXHAUSTED;
                    return false;
                }
                break;
            case READ_FROM_A:
                aHasNext = cursorA.hasNext();
                bHasNext = true;
                break;
            case READ_FROM_B:
                bHasNext = cursorB.hasNext();
                aHasNext = true;
                break;
            case READ_FROM_A__B_EXHAUSTED:
                aHasNext = cursorA.hasNext();
                if (!aHasNext) {
                    state = BOTH_EXHAUSTED;
                    return false;
                }
                bHasNext = false;
                break;
            case READ_FROM_B__A_EXHAUSTED:
                bHasNext = cursorB.hasNext();
                if (!bHasNext) {
                    state = BOTH_EXHAUSTED;
                    return false;
                }
                aHasNext = false;
                break;
            case BOTH_EXHAUSTED:
                return false;
            default:
                throw new AssertionError("cannot happen");
        }

        if (aHasNext && bHasNext) {
            // both cursors have remaining records, let's pick the next by a record comparator
            state = unionRecord.setByComparing() ? READ_FROM_A : READ_FROM_B;
        } else {
            assert aHasNext || bHasNext;
            unionRecord.setAb(aHasNext);
            state = aHasNext ? READ_FROM_A__B_EXHAUSTED : READ_FROM_B__A_EXHAUSTED;
        }
        return true;
    }

    @Override
    void of(RecordCursor cursorA, RecordCursor cursorB, SqlExecutionCircuitBreaker circuitBreaker) throws SqlException {
        throw new UnsupportedOperationException("use overloaded of() method with a comparator");
    }

    void of(RecordCursor cursorA, RecordCursor cursorB, RecordComparator comparator, SqlExecutionCircuitBreaker circuitBreaker) throws SqlException {
        super.of(cursorA, cursorB, circuitBreaker);
        unionRecord.of(cursorA.getRecord(), cursorB.getRecord(), comparator);
    }

    @Override
    public void toTop() {
        cursorA.toTop();
        cursorB.toTop();
        state = INITIAL_STATE;
    }

    @Override
    public long size() {
        final long sizeA = cursorA.size();
        final long sizeB = cursorB.size();
        // -1 indicates unknown size
        if (sizeA == -1 || sizeB == -1) {
            return -1;
        }
        return sizeA + sizeB;
    }
}
