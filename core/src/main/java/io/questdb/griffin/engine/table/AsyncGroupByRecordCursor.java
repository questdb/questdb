/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

class AsyncGroupByRecordCursor implements RecordCursor {

    private static final Log LOG = LogFactory.getLog(AsyncGroupByRecordCursor.class);
    private final Map dataMap; // used to accumulate all partial results
    private final ObjList<GroupByFunction> groupByFunctions;
    private final VirtualRecord recordA;
    private final VirtualRecord recordB;
    private final ObjList<Function> recordFunctions;
    private int frameLimit;
    private PageFrameSequence<AsyncGroupByAtom> frameSequence;
    private boolean isDataMapBuilt;
    private boolean isOpen;
    private RecordCursor mapCursor;

    public AsyncGroupByRecordCursor(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @NotNull ColumnTypes valueTypes,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions
    ) {
        this.groupByFunctions = groupByFunctions;
        this.recordFunctions = recordFunctions;
        this.recordA = new VirtualRecord(recordFunctions);
        this.recordB = new VirtualRecord(recordFunctions);
        this.dataMap = MapFactory.createMap(configuration, keyTypes, valueTypes);
        this.isOpen = true;
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            Misc.free(dataMap);
            Misc.clearObjList(groupByFunctions);
            mapCursor = Misc.free(mapCursor);

            if (frameSequence != null) {
                LOG.debug()
                        .$("closing [shard=").$(frameSequence.getShard())
                        .$(", frameCount=").$(frameLimit)
                        .I$();

                if (frameLimit > -1) {
                    frameSequence.await();
                }
                frameSequence.clear();
            }
        }
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) recordFunctions.getQuick(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (!isDataMapBuilt) {
            buildMap();
        }
        return mapCursor.hasNext();
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) recordFunctions.getQuick(columnIndex)).newSymbolTable();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        if (mapCursor != null) {
            mapCursor.recordAt(((VirtualRecord) record).getBaseRecord(), atRowId);
        }
    }

    @Override
    public long size() {
        if (!isDataMapBuilt) {
            return -1;
        }
        return mapCursor != null ? mapCursor.size() : -1;
    }

    @Override
    public void toTop() {
        if (mapCursor != null) {
            mapCursor.toTop();
            GroupByUtils.toTop(recordFunctions);
        }
    }

    private void buildMap() {
        if (frameLimit == -1) {
            frameSequence.prepareForDispatch();
            frameLimit = frameSequence.getFrameCount() - 1;
        }

        int frameIndex = -1;
        boolean allFramesActive = true;
        try {
            do {
                final long cursor = frameSequence.next();
                if (cursor > -1) {
                    PageFrameReduceTask task = frameSequence.getTask(cursor);
                    LOG.debug()
                            .$("collected [shard=").$(frameSequence.getShard())
                            .$(", frameIndex=").$(task.getFrameIndex())
                            .$(", frameCount=").$(frameSequence.getFrameCount())
                            .$(", active=").$(frameSequence.isActive())
                            .$(", cursor=").$(cursor)
                            .I$();
                    if (task.hasError()) {
                        throw CairoException.nonCritical().put(task.getErrorMsg());
                    }

                    allFramesActive &= frameSequence.isActive();
                    frameIndex = task.getFrameIndex();

                    final Map srcMap = task.getGroupByMap();
                    if (srcMap.size() > 0 && frameSequence.isActive()) {
                        // Merge the maps.
                        RecordCursor srcCursor = srcMap.getCursor();
                        MapRecord srcRecord = srcMap.getRecord();
                        while (srcCursor.hasNext()) {
                            MapKey destKey = dataMap.withKey();
                            srcRecord.copyKey(destKey);
                            MapValue destValue = destKey.createValue();
                            MapValue srcValue = srcRecord.getValue();
                            for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                                groupByFunctions.getQuick(i).merge(destValue, srcValue);
                            }
                        }
                    }

                    frameSequence.collect(cursor, false);
                } else if (cursor == -2) {
                    break; // No frames to filter.
                } else {
                    Os.pause();
                }
            } while (frameIndex < frameLimit);
        } catch (Throwable e) {
            LOG.error().$("group by error [ex=").$(e).I$();
            if (e instanceof CairoException) {
                CairoException ce = (CairoException) e;
                if (ce.isInterruption()) {
                    throwTimeoutException();
                } else {
                    throw ce;
                }
            }
            throw CairoException.nonCritical().put(e.getMessage());
        }

        if (!allFramesActive) {
            throwTimeoutException();
        }

        // If the map is still empty in the non-keyed case, initialize an empty entry.
        final AsyncGroupByAtom atom = frameSequence.getAtom();
        if (atom.isNonKeyed() && dataMap.size() == 0) {
            MapKey key = dataMap.withKey();
            atom.getMapSink().copy(null, key);
            MapValue value = key.createValue();
            atom.getFunctionUpdater().updateEmpty(value);
        }

        mapCursor = dataMap.getCursor();
        recordA.of(mapCursor.getRecord());
        recordB.of(mapCursor.getRecordB());
        isDataMapBuilt = true;
    }

    private void throwTimeoutException() {
        throw CairoException.nonCritical().put(AsyncFilteredRecordCursor.exceptionMessage).setInterruption(true);
    }

    void of(PageFrameSequence<AsyncGroupByAtom> frameSequence) {
        if (!isOpen) {
            isOpen = true;
            dataMap.reopen();
        }
        this.frameSequence = frameSequence;
        isDataMapBuilt = false;
        frameLimit = -1;
    }
}
