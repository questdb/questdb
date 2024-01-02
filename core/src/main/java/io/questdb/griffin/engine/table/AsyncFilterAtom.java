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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.concurrent.atomic.LongAdder;

public class AsyncFilterAtom implements StatefulAtom, Closeable, Plannable {

    public static final LongAdder PRE_TOUCH_BLACK_HOLE = new LongAdder();

    private final Function filter;
    private final ObjList<Function> perWorkerFilters;
    private final PerWorkerLocks perWorkerLocks;
    private final IntList preTouchColumnTypes;
    private boolean preTouchEnabled;

    public AsyncFilterAtom(
            @NotNull CairoConfiguration configuration,
            @NotNull Function filter,
            @Nullable ObjList<Function> perWorkerFilters,
            @Nullable IntList preTouchColumnTypes
    ) {
        this.filter = filter;
        this.perWorkerFilters = perWorkerFilters;
        if (perWorkerFilters != null) {
            perWorkerLocks = new PerWorkerLocks(configuration, perWorkerFilters.size());
        } else {
            perWorkerLocks = null;
        }
        this.preTouchColumnTypes = preTouchColumnTypes;
    }

    public int acquireFilter(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (perWorkerLocks == null) {
            return -1;
        }
        if (workerId == -1 && owner) {
            // Owner thread is free to use the original filter anytime.
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    @Override
    public void close() {
        Misc.free(filter);
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
    public void preTouchColumns(PageAddressCacheRecord record, DirectLongList rows) {
        if (!preTouchEnabled || preTouchColumnTypes == null) {
            return;
        }
        // We use a LongAdder as a black hole to make sure that the JVM JIT compiler keeps the load instructions in place.
        long sum = 0;
        for (long p = 0; p < rows.size(); p++) {
            long r = rows.get(p);
            record.setRowIndex(r);
            for (int i = 0; i < preTouchColumnTypes.size(); i++) {
                int columnType = preTouchColumnTypes.getQuick(i);
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
                        sum += l256.getLong0();
                        sum += l256.getLong1();
                        sum += l256.getLong2();
                        sum += l256.getLong3();
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
                        CharSequence cs = record.getStr(i);
                        if (cs != null && cs.length() > 0) {
                            // Touch the first page of the string contents only.
                            sum += cs.charAt(0);
                        }
                        break;
                    case ColumnType.BINARY:
                        BinarySequence bs = record.getBin(i);
                        if (bs != null && bs.length() > 0) {
                            // Touch the first page of the binary contents only.
                            sum += bs.byteAt(0);
                        }
                        break;
                    case ColumnType.UUID:
                        sum += record.getLong128Hi(i);
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
