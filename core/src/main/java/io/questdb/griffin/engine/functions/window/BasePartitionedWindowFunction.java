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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import org.jetbrains.annotations.Nullable;

public abstract class BasePartitionedWindowFunction extends BaseWindowFunction implements Reopenable {
    // Non-final to allow streaming-LEAD variants to lazy-allocate the cached-fallback map on
    // first pass1 invocation. Subclasses that need eager allocation continue to assign in the
    // constructor; null at construction means "lazy", and every lifecycle method below is
    // null-safe.
    protected Map map;
    // Retained per-query tracker. Held even while map is null (streaming variants allocate their
    // map/ring lazily) so a later lazy allocation can bind the same tracker and stay on the
    // per-query counter. Bound by the owning cursor before reopen().
    protected MemoryTracker memoryTracker;
    protected final VirtualRecord partitionByRecord;
    protected final RecordSink partitionBySink;

    public BasePartitionedWindowFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
        super(arg);
        this.map = map;
        this.partitionByRecord = partitionByRecord;
        this.partitionBySink = partitionBySink;
        // Start the map closed (lazy), matching the openOnInit=false pattern used
        // elsewhere for tracker-aware state: the owning cursor binds a per-query
        // MemoryTracker via setMemoryTracker() and reopen() then allocates the backing
        // under it, with reset() freeing it at cursor close, symmetric on the
        // per-query counter. Direct callers (e.g. unit tests) must reopen() before use.
        if (map != null) {
            map.close();
        }
    }

    @Override
    public void close() {
        super.close();
        Misc.free(map);
        Misc.freeObjList(partitionByRecord.getFunctions());
    }

    public VirtualRecord getPartitionByRecord() {
        return partitionByRecord;
    }

    public RecordSink getPartitionBySink() {
        return partitionBySink;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        super.init(symbolTableSource, executionContext);
        Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
    }

    @Override
    public void reopen() {
        if (map != null) {
            map.reopen();
        }
    }

    @Override
    public void reset() {
        Misc.free(map);
    }

    @Override
    public void setMemoryTracker(@Nullable MemoryTracker tracker) {
        // Retain the tracker even when map is null: streaming variants allocate their map/ring
        // lazily (in streamingPass1/computeNext), after this call, and read memoryTracker to bind
        // it to the fresh allocation so it counts against the per-query limit.
        this.memoryTracker = tracker;
        if (map != null) {
            map.setMemoryTracker(tracker);
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(getName());
        if (arg != null) {
            sink.val('(').val(arg).val(')');
        } else {
            sink.val("(*)");
        }
        if (isIgnoreNulls()) {
            sink.val(" ignore nulls");
        }
        sink.val(" over (");
        sink.val("partition by ");
        sink.val(partitionByRecord.getFunctions());
        sink.val(')');
    }

    @Override
    public void toTop() {
        super.toTop();
        Misc.clear(map);
    }
}
