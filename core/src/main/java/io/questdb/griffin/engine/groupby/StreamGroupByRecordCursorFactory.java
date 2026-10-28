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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.TestOnly;

public class StreamGroupByRecordCursorFactory extends AbstractRecordCursorFactory {
    private final ObjList<GroupByFunction> groupByFunctions;
    private final Function keyFunction;
    private RecordCursorFactory base;
    private StreamGroupByRecordCursor cursor;
    private ObjList<Function> keyFunctions;
    private ObjList<Function> recordFunctions;

    public StreamGroupByRecordCursorFactory(
            BytecodeAssembler asm,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordMetadata metadata,
            ArrayColumnTypes keyTypes,
            ArrayColumnTypes valueTypes,
            ListColumnFilter columnFilter,
            int streamKeyIndex,
            Function keyFunction,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> keyFunctions,
            ObjList<Function> recordFunctions
    ) {
        super(metadata);
        this.base = base;
        this.keyFunction = keyFunction;
        this.groupByFunctions = groupByFunctions;
        this.keyFunctions = keyFunctions;
        this.recordFunctions = recordFunctions;
        try {
            this.cursor = new StreamGroupByRecordCursor(asm, configuration, keyTypes, valueTypes, columnFilter, streamKeyIndex);
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext);
            Function.init(recordFunctions, baseCursor, executionContext, null);
            Function.init(keyFunctions, baseCursor, executionContext, null);
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @TestOnly
    public StreamGroupByMap getSegmentMap() {
        return cursor.segmentMap;
    }

    // Stable iff every key function and aggregate and the base are stable, as for the hash group-by.
    @Override
    public boolean isNonDeterministic() {
        for (int i = 0, n = keyFunctions.size(); i < n; i++) {
            if (keyFunctions.getQuick(i).isNonDeterministic()) {
                return true;
            }
        }
        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
            if (groupByFunctions.getQuick(i).isNonDeterministic()) {
                return true;
            }
        }
        return base.isNonDeterministic();
    }

    @Override
    public boolean isStableWithinExecution() {
        for (int i = 0, n = keyFunctions.size(); i < n; i++) {
            if (!keyFunctions.getQuick(i).isStableWithinExecution()) {
                return false;
            }
        }
        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
            if (!groupByFunctions.getQuick(i).isStableWithinExecution()) {
                return false;
            }
        }
        return base.isStableWithinExecution();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Stream Group By");
        if (cursor.segmentMap != null) {
            sink.optAttr("segmentKey", keyFunction, true);
        }
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.optAttr("keyFunctions", keyFunctions, true);
        sink.optAttr("values", groupByFunctions, true);
        sink.child(base);
    }

    @Override
    protected void _close() {
        Throwable th = Misc.freeBestEffort(null, cursor);
        cursor = null;
        th = Misc.freeBestEffort(th, base);
        base = null;
        th = Misc.freeObjListBestEffort(th, keyFunctions);
        keyFunctions = null;
        th = Misc.freeObjListBestEffort(th, recordFunctions);
        recordFunctions = null;
        CairoException.rethrowCleanupFailure(th);
    }

    private class StreamGroupByRecordCursor implements NoRandomAccessRecordCursor {
        private final int keyIndex;
        private final DirectLongList memory;
        private final VirtualRecord record;
        private final StreamGroupByMap segmentMap;
        private final GroupByFunctionsUpdater updater;
        private final FlyweightPackedMapValue value;
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean done;
        private MapRecordCursor mapCursor;
        private boolean pending;
        private long pendingKey;
        private long rowId;

        private StreamGroupByRecordCursor(BytecodeAssembler asm, CairoConfiguration configuration, ArrayColumnTypes keyTypes,
                                          ArrayColumnTypes valueTypes, ListColumnFilter columnFilter, int streamKeyIndex) {
            keyIndex = valueTypes.getColumnCount();
            value = new FlyweightPackedMapValue(new ArrayColumnTypes().addAll(valueTypes).add(keyFunction.getType()));
            updater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            record = new VirtualRecord(recordFunctions);
            memory = new DirectLongList((value.getSizeInBytes() + 7) / 8, MemoryTag.NATIVE_FAST_MAP, true);
            segmentMap = keyTypes.getColumnCount() > 1
                    ? new StreamGroupByMap(configuration, asm, base.getMetadata(), columnFilter, keyTypes, valueTypes,
                    streamKeyIndex, keyFunctions, groupByFunctions, updater)
                    : null;
            record.of(segmentMap != null ? segmentMap.getRecord() : value);
        }

        @Override
        public void close() {
            Throwable th = Misc.freeBestEffort(null, baseCursor);
            baseCursor = null;
            mapCursor = null;
            th = Misc.freeBestEffort(th, segmentMap);
            th = Misc.freeBestEffort(th, memory);
            memory.setMemoryTracker(null);
            Function.cursorClosed(recordFunctions);
            Function.cursorClosed(keyFunctions);
            CairoException.rethrowCleanupFailure(th);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return (SymbolTable) recordFunctions.getQuick(columnIndex);
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
            final Record baseRecord = baseCursor.getRecord();
            if (!pending) {
                if (!baseCursor.hasNext()) {
                    done = true;
                    return false;
                }
                pendingKey = keyFunction.getLong(baseRecord);
            }
            final long key = pendingKey;
            pending = false;
            GroupByUtils.setStreamGroupByKey(value, keyIndex, keyFunction.getType(), key);
            updater.updateNew(value, baseRecord, rowId++);
            while (baseCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                pendingKey = keyFunction.getLong(baseRecord);
                if (pendingKey != key) {
                    pending = true;
                    return true;
                }
                updater.updateExisting(value, baseRecord, rowId++);
            }
            done = true;
            return true;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return ((SymbolFunction) recordFunctions.getQuick(columnIndex)).newSymbolTable();
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
            baseCursor.toTop();
            GroupByUtils.toTop(keyFunctions);
            GroupByUtils.toTop(groupByFunctions);
            mapCursor = null;
            done = pending = false;
            rowId = 0;
        }

        private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) {
            this.baseCursor = baseCursor;
            circuitBreaker = executionContext.getCircuitBreaker();
            if (segmentMap != null) {
                segmentMap.reopen(executionContext.getMemoryTracker());
            } else {
                memory.setMemoryTracker(executionContext.getMemoryTracker());
                memory.ensureCapacity((value.getSizeInBytes() + 7) / 8);
                value.of(memory.getAddress());
            }
            mapCursor = null;
            done = pending = false;
            rowId = 0;
        }

        private boolean hasNextSegment() {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            if (mapCursor != null && mapCursor.hasNext()) {
                return true;
            }
            if (done) {
                return false;
            }
            final Record baseRecord = baseCursor.getRecord();
            if (!pending) {
                if (!baseCursor.hasNext()) {
                    done = true;
                    return false;
                }
                pendingKey = keyFunction.getLong(baseRecord);
            }
            final long key = pendingKey;
            segmentMap.clear(key);
            pending = false;
            while (true) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                segmentMap.update(baseRecord, rowId++);
                if (!baseCursor.hasNext()) {
                    done = true;
                    break;
                }
                pendingKey = keyFunction.getLong(baseRecord);
                if (pendingKey != key) {
                    pending = true;
                    break;
                }
            }
            mapCursor = segmentMap.getCursor();
            return mapCursor.hasNext();
        }
    }
}
