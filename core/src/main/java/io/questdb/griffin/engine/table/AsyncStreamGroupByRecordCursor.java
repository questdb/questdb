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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Os;

class AsyncStreamGroupByRecordCursor implements NoRandomAccessRecordCursor {
    private final int dispatchLimit;
    private final boolean isIntKey;
    private final int keyIndex;
    private final DirectLongList memory;
    private final ObjList<Function> nonGroupByFunctions;
    private final VirtualRecord record;
    private final FlyweightPackedMapValue source;
    private final long stride;
    private final FlyweightPackedMapValue value;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private long cursor = -1;
    private boolean done;
    private int frameIndex = -1;
    private int frameLimit = -1;
    private PageFrameSequence<AsyncStreamGroupByAtom> frameSequence;
    private boolean prepared;
    private DirectLongList results;
    private long resultOffset;

    AsyncStreamGroupByRecordCursor(CairoConfiguration configuration, ColumnTypes stateTypes, ObjList<Function> functions) {
        dispatchLimit = Math.max(1, configuration.getSqlParallelFilterDispatchLimit());
        keyIndex = stateTypes.getColumnCount() - 1;
        isIntKey = stateTypes.getColumnType(keyIndex) == ColumnType.INT;
        value = new FlyweightPackedMapValue(stateTypes);
        source = new FlyweightPackedMapValue(stateTypes);
        stride = (value.getSizeInBytes() + 7) / 8;
        record = new VirtualRecord(functions);
        nonGroupByFunctions = GroupByUtils.extractNonGroupByFunctions(functions);
        record.of(value);
        memory = new DirectLongList(stride, MemoryTag.NATIVE_FAST_MAP, true);
    }

    @Override
    public void close() {
        try {
            if (frameSequence != null) {
                collect(true);
                if (prepared) {
                    frameSequence.await();
                }
                frameSequence.reset();
            }
        } finally {
            frameSequence = null;
            results = null;
            memory.close();
            memory.setMemoryTracker(null);
        }
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) record.getFunctions().getQuick(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (done) {
            return false;
        }
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        boolean found = false;
        while (nextPartial()) {
            source.of(results.getAddress() + resultOffset * Long.BYTES);
            if (!found) {
                value.copyFrom(source);
                found = true;
            } else if (isIntKey ? value.getInt(keyIndex) == source.getInt(keyIndex) : value.getLong(keyIndex) == source.getLong(keyIndex)) {
                frameSequence.getAtom().getUpdater(-1).merge(value, source);
            } else {
                return true;
            }
            resultOffset += stride;
        }
        done = true;
        return found;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) record.getFunctions().getQuick(columnIndex)).newSymbolTable();
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toTop() {
        collect(false);
        frameSequence.toTop();
        frameSequence.getAtom().toTop();
        frameIndex = -1;
        done = false;
    }

    void of(PageFrameSequence<AsyncStreamGroupByAtom> frameSequence, SqlExecutionContext executionContext) throws SqlException {
        this.frameSequence = frameSequence;
        circuitBreaker = executionContext.getCircuitBreaker();
        prepared = done = false;
        frameIndex = frameLimit = -1;
        cursor = -1;
        results = null;
        memory.setMemoryTracker(executionContext.getMemoryTracker());
        memory.ensureCapacity(stride);
        value.of(memory.getAddress());
        Function.init(nonGroupByFunctions, frameSequence.getSymbolTableSource(), executionContext, null);
    }

    private void collect(boolean force) {
        if (cursor >= 0) {
            frameSequence.collect(cursor, force);
            cursor = -1;
        }
        results = null;
    }

    private boolean nextPartial() {
        if (results != null && resultOffset < results.size()) {
            return true;
        }
        collect(false);
        if (!prepared) {
            frameSequence.prepareForDispatch();
            prepared = true;
            frameLimit = frameSequence.getFrameCount() - 1;
        }
        while (frameIndex < frameLimit) {
            circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
            cursor = frameSequence.next(dispatchLimit, false);
            if (cursor >= 0) {
                final PageFrameReduceTask task = frameSequence.getTask(cursor);
                if (task.hasError()) {
                    throw task.buildError();
                }
                if (!frameSequence.isActive()) {
                    throw frameSequence.buildInterruptionException();
                }
                frameIndex = task.getFrameIndex();
                results = task.getGroupByValues();
                resultOffset = 0;
                if (results.size() > 0) {
                    return true;
                }
                collect(false);
            } else if (cursor == -2) {
                break;
            } else {
                Os.pause();
            }
        }
        if (!frameSequence.isActive()) {
            throw frameSequence.buildInterruptionException();
        }
        return false;
    }
}
