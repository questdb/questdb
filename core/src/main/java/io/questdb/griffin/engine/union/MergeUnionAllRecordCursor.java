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
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

class MergeUnionAllRecordCursor extends AbstractSetRecordCursor implements NoRandomAccessRecordCursor {
    private final ObjList<Record> adaptedRecords;
    private final ObjList<UnionCastRecord> castRecords;
    private final IntList heap;
    private final boolean isAscending;
    private final MergeUnionAllRecord record = new MergeUnionAllRecord();
    private final ObjList<RecordCursor> sourceCursors;
    private final LongList timestamps;
    private final int timestampIndex;
    private int heapSize;
    private boolean isStarted;
    private int lastSource = -1;

    MergeUnionAllRecordCursor(
            ObjList<ObjList<Function>> castFunctions,
            int timestampIndex,
            boolean isAscending
    ) {
        final int sourceCount = castFunctions.size();
        this.adaptedRecords = new ObjList<>(sourceCount);
        this.castRecords = new ObjList<>(sourceCount);
        this.heap = new IntList(sourceCount);
        this.isAscending = isAscending;
        this.sourceCursors = new ObjList<>(sourceCount);
        this.timestamps = new LongList(sourceCount);
        this.timestampIndex = timestampIndex;
        adaptedRecords.setPos(sourceCount);
        castRecords.setPos(sourceCount);
        heap.setPos(sourceCount);
        sourceCursors.setPos(sourceCount);
        timestamps.setPos(sourceCount);
        for (int i = 0; i < sourceCount; i++) {
            final ObjList<Function> functions = castFunctions.getQuick(i);
            castRecords.setQuick(i, functions != null ? new UnionCastRecord(functions, functions) : null);
        }
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        if (!isStarted) {
            for (int i = 0, n = sourceCursors.size(); i < n; i++) {
                sourceCursors.getQuick(i).calculateSize(circuitBreaker, counter);
            }
            return;
        }
        NoRandomAccessRecordCursor.super.calculateSize(circuitBreaker, counter);
    }

    @Override
    public void close() {
        Misc.freeObjList(sourceCursors);
        circuitBreaker = null;
        clearState();
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public boolean hasNext() {
        if (!isStarted) {
            heapSize = 0;
            for (int i = 0, n = sourceCursors.size(); i < n; i++) {
                final RecordCursor cursor = sourceCursors.getQuick(i);
                if (cursor.hasNext()) {
                    timestamps.setQuick(i, cursor.getRecord().getLong(timestampIndex));
                    heap.setQuick(heapSize, i);
                    siftUp(heapSize++);
                }
            }
            isStarted = true;
        } else if (lastSource > -1) {
            final RecordCursor cursor = sourceCursors.getQuick(lastSource);
            if (cursor.hasNext()) {
                timestamps.setQuick(lastSource, cursor.getRecord().getLong(timestampIndex));
            } else {
                heapSize--;
                if (heapSize > 0) {
                    heap.setQuick(0, heap.getQuick(heapSize));
                }
            }
            if (heapSize > 0) {
                siftDown();
            }
        }

        if (heapSize == 0) {
            lastSource = -1;
            return false;
        }
        lastSource = heap.getQuick(0);
        record.of(adaptedRecords.getQuick(lastSource));
        return true;
    }

    @Override
    public long preComputedStateSize() {
        long size = 0;
        for (int i = 0, n = sourceCursors.size(); i < n; i++) {
            size += sourceCursors.getQuick(i).preComputedStateSize();
        }
        return size;
    }

    @Override
    public void setParquetDecodeHint(io.questdb.cairo.sql.ParquetDecodeHint hint) {
        for (int i = 0, n = sourceCursors.size(); i < n; i++) {
            sourceCursors.getQuick(i).setParquetDecodeHint(hint);
        }
    }

    @Override
    public long size() {
        long size = 0;
        for (int i = 0, n = sourceCursors.size(); i < n; i++) {
            final long sourceSize = sourceCursors.getQuick(i).size();
            if (sourceSize < 0) {
                return -1;
            }
            size += sourceSize;
        }
        return size;
    }

    @Override
    public void toTop() {
        for (int i = 0, n = sourceCursors.size(); i < n; i++) {
            sourceCursors.getQuick(i).toTop();
        }
        clearState();
    }

    RecordCursor getSourceCursor(int index) {
        return sourceCursors.getQuick(index);
    }

    void openSource(int index, RecordCursor cursor) {
        sourceCursors.setQuick(index, cursor);
        final UnionCastRecord castRecord = castRecords.getQuick(index);
        if (castRecord != null) {
            final Record sourceRecord = cursor.getRecord();
            castRecord.of(sourceRecord, sourceRecord);
            castRecord.setAb(true);
            adaptedRecords.setQuick(index, castRecord);
        } else {
            adaptedRecords.setQuick(index, cursor.getRecord());
        }
    }

    void of(SqlExecutionContext executionContext) throws SqlException {
        this.circuitBreaker = executionContext.getCircuitBreaker();
        clearState();
    }

    private void clearState() {
        heapSize = 0;
        isStarted = false;
        lastSource = -1;
    }

    private boolean isBefore(int leftSource, int rightSource) {
        final long leftTimestamp = timestamps.getQuick(leftSource);
        final long rightTimestamp = timestamps.getQuick(rightSource);
        if (leftTimestamp != rightTimestamp) {
            return isAscending ? leftTimestamp < rightTimestamp : leftTimestamp > rightTimestamp;
        }
        return leftSource < rightSource;
    }

    private void siftDown() {
        int parent = 0;
        final int source = heap.getQuick(0);
        while (true) {
            final int left = 2 * parent + 1;
            if (left >= heapSize) {
                break;
            }
            final int right = left + 1;
            int child = left;
            if (right < heapSize && isBefore(heap.getQuick(right), heap.getQuick(left))) {
                child = right;
            }
            if (!isBefore(heap.getQuick(child), source)) {
                break;
            }
            heap.setQuick(parent, heap.getQuick(child));
            parent = child;
        }
        heap.setQuick(parent, source);
    }

    private void siftUp(int child) {
        final int source = heap.getQuick(child);
        while (child > 0) {
            final int parent = (child - 1) >>> 1;
            final int parentSource = heap.getQuick(parent);
            if (!isBefore(source, parentSource)) {
                break;
            }
            heap.setQuick(child, parentSource);
            child = parent;
        }
        heap.setQuick(child, source);
    }
}
