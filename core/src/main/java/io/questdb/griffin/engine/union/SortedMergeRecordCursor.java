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
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.Misc;

public final class SortedMergeRecordCursor implements NoRandomAccessRecordCursor {
    private static final int INITIAL_STATE = 0;
    private static final int READ_FROM_A = 1;
    private static final int READ_FROM_B = 2;
    private static final int READ_FROM_B__A_EXHAUSTED = 3;
    private static final int READ_FROM_A__B_EXHAUSTED = 4;
    private static final int BOTH_EXHAUSTED = 5;

    private final SelectableRecord unionRecord;
    private int state = INITIAL_STATE;
    private RecordCursor cursorA;
    private RecordCursor cursorB;

    public SortedMergeRecordCursor() {
        this.unionRecord = new SelectableRecord();
    }

    @Override
    public Record getRecord() {
        return unionRecord;
    }

    @Override
    public boolean hasNext() {
        switch (state) {
            case INITIAL_STATE:
                return advanceFromInitState();
            case READ_FROM_A:
                if (cursorA.hasNext()) {
                    unionRecord.resetComparatorLeft();
                    state = unionRecord.selectByComparing() ? READ_FROM_A : READ_FROM_B;
                } else {
                    unionRecord.selectB();
                    state = READ_FROM_B__A_EXHAUSTED;
                }
                return true;
            case READ_FROM_B:
                if (cursorB.hasNext()) {
                    state = unionRecord.selectByComparing() ? READ_FROM_A : READ_FROM_B;
                } else {
                    unionRecord.selectA();
                    state = READ_FROM_A__B_EXHAUSTED;
                }
                return true;
            case READ_FROM_A__B_EXHAUSTED:
                if (cursorA.hasNext()) {
                    // state stays as it is
                    return true;
                } else {
                    state = BOTH_EXHAUSTED;
                    return false;
                }
            case READ_FROM_B__A_EXHAUSTED:
                if (cursorB.hasNext()) {
                    // state stays as it is
                    return true;
                } else {
                    state = BOTH_EXHAUSTED;
                    return false;
                }
            case BOTH_EXHAUSTED:
                return false;
            default:
                throw new AssertionError("cannot happen");
        }
    }

    private boolean advanceFromInitState() {
        if (cursorA.hasNext()) {
            if (cursorB.hasNext()) {
                unionRecord.resetComparatorLeft();
                state = unionRecord.selectByComparing() ? READ_FROM_A : READ_FROM_B;
            } else {
                unionRecord.selectA();
                state = READ_FROM_A__B_EXHAUSTED;
            }
            return true;
        } else if (cursorB.hasNext()) {
            unionRecord.selectB();
            state = READ_FROM_B__A_EXHAUSTED;
            return true;
        }
        state = BOTH_EXHAUSTED;
        return false;
    }

    void of(RecordCursor cursorA, RecordCursor cursorB, RecordComparator comparator) {
        this.cursorA = cursorA;
        this.cursorB = cursorB;
        unionRecord.of(cursorA.getRecord(), cursorB.getRecord(), comparator);
        toTop();
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

    @Override
    public void close() {
        this.cursorA = Misc.free(this.cursorA);
        this.cursorB = Misc.free(this.cursorB);
    }
}
