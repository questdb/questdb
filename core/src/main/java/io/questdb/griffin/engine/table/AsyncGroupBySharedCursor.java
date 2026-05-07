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

import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.ShardedMapCursor;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class AsyncGroupBySharedCursor implements RecordCursor {
    private final VirtualRecord record;
    private final VirtualRecord recordB;
    private final ObjList<Function> recordFunctions;
    private final ShardedMapCursor shardedMapCursor = new ShardedMapCursor();
    private MapRecordCursor mapCursor;
    private MapRecordCursor nonShardedMapCursor;
    private AsyncGroupByRecordCursor primaryCursor;

    public AsyncGroupBySharedCursor(ObjList<Function> recordFunctions) {
        this.recordFunctions = recordFunctions;
        this.record = new VirtualRecord(recordFunctions);
        this.recordB = new VirtualRecord(recordFunctions);
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        buildMapConditionally();
        mapCursor.calculateSize(circuitBreaker, counter);
    }

    @Override
    public void close() {
        mapCursor = Misc.free(mapCursor);
    }

    @Override
    public Record getRecord() {
        return record;
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
        buildMapConditionally();
        return mapCursor.hasNext();
    }

    @Override
    public void longTopK(DirectLongLongSortedList list, int columnIndex) {
        buildMapConditionally();
        primaryCursor.longTopK(list, recordFunctions.getQuick(columnIndex), mapCursor);
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
    public void recordAt(Record record, long atRowId) {
        if (mapCursor != null) {
            mapCursor.recordAt(((VirtualRecord) record).getBaseRecord(), atRowId);
        }
    }

    @Override
    public long size() {
        return mapCursor != null ? mapCursor.size() : -1;
    }

    @Override
    public void toTop() {
        if (mapCursor != null) {
            mapCursor.toTop();
            GroupByUtils.toTop(recordFunctions);
        }
    }

    private void buildMapConditionally() {
        if (mapCursor == null) {
            primaryCursor.buildMapConditionally();
            mapCursor = primaryCursor.initSharedMapCursor(shardedMapCursor, nonShardedMapCursor);
            if (mapCursor != shardedMapCursor) {
                nonShardedMapCursor = mapCursor;
            }
            record.of(mapCursor.getRecord());
            recordB.of(mapCursor.getRecordB());
        }
    }

    void of(AsyncGroupByRecordCursor primaryCursor) {
        this.primaryCursor = primaryCursor;
    }
}
