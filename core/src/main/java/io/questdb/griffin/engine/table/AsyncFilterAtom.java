/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.concurrent.atomic.LongAdder;

public class AsyncFilterAtom implements StatefulAtom, Closeable, Plannable {
    public static final LongAdder PRE_TOUCH_BLACK_HOLE = new LongAdder();
    private final IntList columnTypes;
    private final Function filter;
    private final boolean forceDisablePreTouch;
    private final ObjList<Function> perWorkerFilters;
    private final PerWorkerLocks perWorkerLocks;
    private boolean preTouchEnabled;

    public AsyncFilterAtom(
            @NotNull CairoConfiguration configuration,
            @NotNull Function filter,
            @Nullable ObjList<Function> perWorkerFilters,
            @NotNull IntList columnTypes,
            boolean forceDisablePreTouch
    ) {
        this.filter = filter;
        this.perWorkerFilters = perWorkerFilters;
        if (perWorkerFilters != null) {
            perWorkerLocks = new PerWorkerLocks(configuration, perWorkerFilters.size());
        } else {
            perWorkerLocks = null;
        }
        this.columnTypes = columnTypes;
        this.forceDisablePreTouch = forceDisablePreTouch;
    }

    @Override
    public void close() {
        Misc.freeObjList(perWorkerFilters);
    }

    public Function getFilter(int filterId) {
        if (filterId == -1 || perWorkerFilters == null) {
            return filter;
        }
        return perWorkerFilters.getQuick(filterId);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        filter.init(symbolTableSource, executionContext);
        if (perWorkerFilters != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                Function.init(perWorkerFilters, symbolTableSource, executionContext);
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }
        preTouchEnabled = executionContext.isColumnPreTouchEnabled();
    }

    @Override
    public void initCursor() {
        filter.initCursor();
        if (perWorkerFilters != null) {
            // Initialize all per-worker filters on the query owner thread to avoid
            // DataUnavailableException thrown on worker threads when filtering.
            Function.initCursor(perWorkerFilters);
        }
    }

    public int maybeAcquireFilter(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (perWorkerLocks == null) {
            return -1;
        }
        if (workerId == -1 && owner) {
            // Owner thread is free to use its own private filter anytime.
            return -1;
        }
        // All other threads, e.g. worker or work stealing threads, must always acquire a lock
        // to use shared resources.
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    /**
     * Pre-touches column values for the filtered rows, if the feature is configured.
     * <p>
     * The idea is to access the memory to page fault and, thus, warm up the pages
     * in parallel, on multiple threads, instead of relying on the "query owner" thread
     * to do it later serially.
     *
     * @param record record to use
     * @param rows   rows to pre-touch
     */
    public void preTouchColumns(PageFrameMemoryRecord record, DirectLongList rows) {
        if (!preTouchEnabled || forceDisablePreTouch) {
            return;
        }
        // We use a LongAdder as a black hole to make sure that the JVM JIT compiler keeps the load instructions in place.
        long sum = 0;
        for (long p = 0, n = rows.size(); p < n; p++) {
            long r = rows.get(p);
            record.setRowIndex(r);
            for (int i = 0; i < columnTypes.size(); i++) {
                int columnType = columnTypes.getQuick(i);
                switch (ColumnType.tagOf(columnType)) {
                    case ColumnType.BOOLEAN:
                        sum += record.getBool(i) ? 1 : 0;
                        break;
                    case ColumnType.BYTE:
                        sum += record.getByte(i);
                        break;
                    case ColumnType.SHORT:
                        sum += record.getShort(i);
                        break;
                    case ColumnType.CHAR:
                        sum += record.getChar(i);
                        break;
                    case ColumnType.INT:
                    case ColumnType.IPv4:
                    case ColumnType.SYMBOL: // We're interested in pre-touching pages, so we read the symbol key only.
                        sum += record.getInt(i);
                        break;
                    case ColumnType.LONG:
                    case ColumnType.DATE:
                    case ColumnType.TIMESTAMP:
                        sum += record.getLong(i);
                        break;
                    case ColumnType.FLOAT:
                        sum += (long) record.getFloat(i);
                        break;
                    case ColumnType.DOUBLE:
                        sum += (long) record.getDouble(i);
                        break;
                    case ColumnType.LONG256:
                        Long256 l256 = record.getLong256A(i);
                        // Touch only the first part of Long256.
                        sum += l256.getLong0();
                        break;
                    case ColumnType.GEOBYTE:
                        sum += record.getGeoByte(i);
                        break;
                    case ColumnType.GEOSHORT:
                        sum += record.getGeoShort(i);
                        break;
                    case ColumnType.GEOINT:
                        sum += record.getGeoInt(i);
                        break;
                    case ColumnType.GEOLONG:
                        sum += record.getGeoLong(i);
                        break;
                    case ColumnType.STRING:
                        // Touch only the first page of the string contents.
                        sum += record.getStrLen(i);
                        break;
                    case ColumnType.VARCHAR:
                        // Touch only the header of the varchar.
                        sum += record.getVarcharSize(i);
                        break;
                    case ColumnType.BINARY:
                        // Touch only the first page of the binary contents.
                        sum += record.getBinLen(i);
                        break;
                    case ColumnType.UUID:
                        // Touch only the first part of UUID.
                        sum += record.getLong128Lo(i);
                        break;
                }
            }
        }
        // Flush the accumulated sum to the black hole.
        PRE_TOUCH_BLACK_HOLE.add(sum);
    }

    public void releaseFilter(int filterId) {
        if (perWorkerLocks != null) {
            perWorkerLocks.releaseSlot(filterId);
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(filter);
    }
}
