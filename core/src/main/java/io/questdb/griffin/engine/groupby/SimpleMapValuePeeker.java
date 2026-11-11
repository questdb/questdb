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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.sql.Record;

public class SimpleMapValuePeeker {
    private final SimpleMapValue currentRecord;
    private final SimpleMapValue nextRecord;
    private AbstractNoRecordSampleByCursor cursor;
    private boolean nextHasNext = false;
    private long nextLocalEpoch = -1;
    // the values that notKeyedLoop() left in the cursor fields of the same name without the prefix next
    private long nextNextSampleLocalEpoch;
    private long nextSampleLocalEpoch;
    private long nextTzOffset;

    SimpleMapValuePeeker(SimpleMapValue currentRecord, SimpleMapValue nextRecord) {
        this.currentRecord = currentRecord;
        this.nextRecord = nextRecord;
    }

    public void clear() {
        currentRecord.clear();
        nextRecord.clear();
        nextHasNext = false;
        nextLocalEpoch = -1;
    }

    // Aggregates the bucket after a gap before the cursor emits the gap rows. notKeyedLoop() reads the rows
    // up to the one that ends that bucket, and may cross a DST transition on the way, which changes tzOffset.
    // The gap rows come before that bucket, so before the transition: peek() restores localEpoch, which the
    // gap check reads, and tzOffset, which their timestamps read. The gap rows and that bucket used to take
    // the offset after the transition, which moved their timestamps by its delta, see GitHub issue #7753.
    Record peek() {
        final long localEpoch = cursor.localEpoch;
        final long tzOffset = cursor.tzOffset;
        nextHasNext = cursor.notKeyedLoop(nextRecord);
        nextLocalEpoch = cursor.localEpoch;
        nextTzOffset = cursor.tzOffset;
        nextSampleLocalEpoch = cursor.sampleLocalEpoch;
        nextNextSampleLocalEpoch = cursor.nextSampleLocalEpoch;
        cursor.localEpoch = localEpoch;
        cursor.tzOffset = tzOffset;
        return nextRecord;
    }

    // The cursor calls it when it emits the bucket after the gap. reset() restores the state that
    // notKeyedLoop() left, as the path without a gap keeps it: the bucket starts where the loop started it,
    // and the gap check continues from the bucket that the loop expects next. The gap chain can miss that
    // start after a DST transition: kludge() moves the chain by the offset delta, off the sampler grid for a
    // stride that the delta does not divide, and nextSamplePeriod() can shift the start off the grid. A
    // bucket labelled from the chain came up to a stride late.
    boolean reset() {
        cursor.localEpoch = nextLocalEpoch;
        cursor.tzOffset = nextTzOffset;
        cursor.sampleLocalEpoch = nextSampleLocalEpoch;
        cursor.nextSampleLocalEpoch = nextNextSampleLocalEpoch;
        currentRecord.copy(nextRecord);
        return nextHasNext;
    }

    void setCursor(AbstractNoRecordSampleByCursor cursor) {
        this.cursor = cursor;
    }
}
