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
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.map.MapRecordCursor;
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
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.StreamGroupByMap;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
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
    private final StreamGroupByMap segmentMap;
    private final FlyweightPackedMapValue source;
    private final ObjList<StreamGroupByMap> spanMaps = new ObjList<>();
    private final IntList spanSlots = new IntList();
    private final long stride;
    private final FlyweightPackedMapValue value;
    private long chainLastKey;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private long cursor = -1;
    private boolean done;
    private MapRecordCursor emitCursor;
    private StreamGroupByMap emitMap;
    private int frameIndex = -1;
    private int frameLimit = -1;
    private PageFrameSequence<AsyncStreamGroupByAtom> frameSequence;
    private boolean hasOpenKey;
    private boolean hasOpenSpans;
    private boolean isChainPending;
    private boolean isChainRowPending;
    private boolean isOpenCollected;
    private long openKey;
    private boolean prepared;
    private long resultOffset;
    private RecordChain resultRecords;
    private DirectLongList results;

    AsyncStreamGroupByRecordCursor(CairoConfiguration configuration, ColumnTypes stateTypes, ObjList<Function> functions, StreamGroupByMap segmentMap) {
        this.segmentMap = segmentMap;
        dispatchLimit = Math.max(1, configuration.getSqlParallelFilterDispatchLimit());
        keyIndex = stateTypes.getColumnCount() - 1;
        isIntKey = stateTypes.getColumnType(keyIndex) == ColumnType.INT;
        value = new FlyweightPackedMapValue(stateTypes);
        source = new FlyweightPackedMapValue(stateTypes);
        stride = (value.getSizeInBytes() + 7) / 8;
        record = new VirtualRecord(functions);
        nonGroupByFunctions = GroupByUtils.extractNonGroupByFunctions(functions);
        record.of(segmentMap != null ? segmentMap.getRecord() : value);
        memory = new DirectLongList(stride, MemoryTag.NATIVE_FAST_MAP, true);
    }

    @Override
    public void close() {
        try {
            if (frameSequence != null) {
                if (!done) {
                    frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_OK);
                }
                collect(true);
                if (prepared) {
                    frameSequence.await();
                }
                frameSequence.reset();
            }
        } finally {
            frameSequence = null;
            results = null;
            resultRecords = null;
            emitCursor = null;
            emitMap = null;
            spanMaps.clear();
            spanSlots.clear();
            memory.close();
            memory.setMemoryTracker(null);
            Function.cursorClosed(nonGroupByFunctions);
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
        if (segmentMap != null) {
            return hasNextSegment();
        }
        if (done) {
            return false;
        }
        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
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
        releaseSpanMaps();
        frameIndex = -1;
        done = false;
        resetSegments();
    }

    void of(PageFrameSequence<AsyncStreamGroupByAtom> frameSequence, SqlExecutionContext executionContext) throws SqlException {
        this.frameSequence = frameSequence;
        circuitBreaker = executionContext.getCircuitBreaker();
        prepared = done = false;
        frameIndex = frameLimit = -1;
        cursor = -1;
        results = null;
        resultRecords = null;
        emitCursor = null;
        emitMap = null;
        spanMaps.clear();
        spanSlots.clear();
        resetSegments();
        if (segmentMap == null) {
            memory.setMemoryTracker(executionContext.getMemoryTracker());
            memory.ensureCapacity(stride);
            value.of(memory.getAddress());
        }
        Function.init(nonGroupByFunctions, frameSequence.getSymbolTableSource(), executionContext, null);
    }

    private void closeOpenSegment() {
        hasOpenKey = false;
        emitMap = segmentMap;
        if (hasOpenSpans) {
            final AsyncStreamGroupByAtom atom = frameSequence.getAtom();
            final GroupByFunctionsUpdater updater = atom.getUpdater(-1);
            atom.takeSpanMaps(openKey, spanMaps, spanSlots);
            final int n = spanMaps.size();
            assert n > 0;
            int largest = 0;
            for (int i = 1; i < n; i++) {
                if (spanMaps.getQuick(i).size() > spanMaps.getQuick(largest).size()) {
                    largest = i;
                }
            }
            final StreamGroupByMap dest = spanMaps.getQuick(largest);
            spanMaps.setQuick(largest, spanMaps.getQuick(0));
            spanMaps.setQuick(0, dest);
            final int destSlot = spanSlots.getQuick(largest);
            spanSlots.setQuick(largest, spanSlots.getQuick(0));
            spanSlots.setQuick(0, destSlot);
            for (int i = n - 1; i > 0; i--) {
                final StreamGroupByMap map = spanMaps.getQuick(i);
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                dest.merge(map, updater);
                atom.releaseSpanMap(spanSlots.getQuick(i), map);
                spanMaps.setPos(i);
                spanSlots.setPos(i);
            }
            if (isOpenCollected) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                dest.merge(segmentMap, updater);
            }
            emitMap = dest;
        }
        hasOpenSpans = false;
        isOpenCollected = false;
        emitCursor = emitMap.getCursor();
        segmentMap.getRecord().of(emitCursor.getRecord());
    }

    private void collect(boolean force) {
        if (cursor >= 0) {
            frameSequence.collect(cursor, force);
            cursor = -1;
        }
        results = null;
        resultRecords = null;
    }

    private boolean hasNextSegment() {
        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
        while (true) {
            if (emitCursor != null) {
                if (emitCursor.hasNext()) {
                    emitMap.putSegmentKey(emitCursor.getRecord());
                    return true;
                }
                releaseSpanMaps();
            }
            if (isChainPending) {
                if (isChainRowPending || resultRecords.hasNext()) {
                    isChainRowPending = false;
                    final Record chainRecord = resultRecords.getRecord();
                    final long key = segmentMap.getSegmentKey(chainRecord);
                    if (key != chainLastKey) {
                        segmentMap.getRecord().of(chainRecord);
                        return true;
                    }
                    segmentMap.clear(key);
                    final GroupByFunctionsUpdater updater = frameSequence.getAtom().getUpdater(-1);
                    do {
                        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                        segmentMap.merge(chainRecord, updater);
                    } while (resultRecords.hasNext());
                    hasOpenKey = true;
                    openKey = key;
                    isOpenCollected = true;
                }
                isChainPending = false;
            }
            if (done) {
                return false;
            }
            if (!nextSegments()) {
                done = true;
                if (hasOpenKey) {
                    closeOpenSegment();
                }
            }
        }
    }

    private boolean nextPartial() {
        if (results != null && resultOffset < results.size()) {
            return true;
        }
        PageFrameReduceTask task;
        while ((task = nextTask()) != null) {
            results = task.getGroupByValues();
            resultOffset = 0;
            if (results.size() > 0) {
                return true;
            }
        }
        return false;
    }

    private boolean nextSegments() {
        PageFrameReduceTask task;
        do {
            task = nextTask();
            if (task == null) {
                return false;
            }
        } while (task.getGroupBySegmentCount() == 0);
        final long first = task.getGroupByFirstKey();
        if (hasOpenKey && openKey != first) {
            closeOpenSegment();
        }
        if (task.getGroupBySegmentCount() == 1) {
            if (!hasOpenKey) {
                hasOpenKey = true;
                openKey = first;
            }
            hasOpenSpans = true;
            return true;
        }
        resultRecords = task.getGroupByRecords();
        chainLastKey = task.getGroupByLastKey();
        isChainPending = true;
        if (hasOpenKey) {
            if (!isOpenCollected) {
                segmentMap.clear(first);
                isOpenCollected = true;
            }
            final GroupByFunctionsUpdater updater = frameSequence.getAtom().getUpdater(-1);
            while (resultRecords.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                final Record chainRecord = resultRecords.getRecord();
                if (segmentMap.getSegmentKey(chainRecord) != first) {
                    isChainRowPending = true;
                    break;
                }
                segmentMap.merge(chainRecord, updater);
            }
            closeOpenSegment();
        }
        return true;
    }

    private PageFrameReduceTask nextTask() {
        collect(false);
        if (!prepared) {
            frameSequence.prepareForDispatch();
            final AsyncFilterContext filterContext = frameSequence.getAtom().getFilterContext();
            if (filterContext != null) {
                filterContext.initMemoryPools(frameSequence.getPageFrameAddressCache(), frameSequence.getMemoryTracker());
            }
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
                frameIndex = task.getFrameIndex();
                if (!frameSequence.isActive()) {
                    if (frameSequence.getCancelReason() != SqlExecutionCircuitBreaker.STATE_OK) {
                        throw frameSequence.buildInterruptionException();
                    }
                    collect(false);
                    continue;
                }
                return task;
            } else if (cursor == -2) {
                break;
            } else {
                Os.pause();
            }
        }
        if (!frameSequence.isActive()) {
            throw frameSequence.buildInterruptionException();
        }
        return null;
    }

    private void releaseSpanMaps() {
        emitCursor = null;
        emitMap = null;
        final AsyncStreamGroupByAtom atom = frameSequence.getAtom();
        for (int i = 0, n = spanMaps.size(); i < n; i++) {
            atom.releaseSpanMap(spanSlots.getQuick(i), spanMaps.getQuick(i));
        }
        spanMaps.clear();
        spanSlots.clear();
    }

    private void resetSegments() {
        hasOpenKey = false;
        hasOpenSpans = false;
        isOpenCollected = false;
        isChainPending = false;
        isChainRowPending = false;
    }
}
