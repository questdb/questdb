/*******************************************************************************
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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * Per-slave configuration for multi-slave HORIZON JOIN.
 * Holds configuration and owned filters shared by ST and async execution paths.
 * Mutable resources (maps, sinks, records, helpers, cursors) are created
 * by the respective cursor/atom implementations.
 * <p>
 * Owns the slave {@link RecordCursorFactory}, its filter and optional worker clones.
 * Async factories may detach the factory; {@link #close()} frees the remaining owners.
 */
public class HorizonJoinSlaveState implements QuietCloseable {
    private final @Nullable ColumnTypes asOfJoinKeyTypes;
    private final boolean isKeyed;
    private final int masterColumnCount;
    private final int @Nullable [] masterSymbolKeyColumnIndices;
    private final long masterTsScale;
    private final int @Nullable [] slaveSymbolKeyColumnIndices;
    private final long slaveTsScale;
    private RecordCursorFactory factory;
    private @Nullable Function filter;
    private @Nullable ObjList<Function> perWorkerFilters;

    static Throwable cursorClosed(Throwable cleanupFailure, @Nullable Function filter) {
        if (filter != null) {
            try {
                filter.cursorClosed();
            } catch (Throwable th) {
                cleanupFailure = Misc.foldCleanupFailure(cleanupFailure, th);
            }
        }
        return cleanupFailure;
    }

    public HorizonJoinSlaveState(
            RecordCursorFactory factory,
            long masterTsScale,
            long slaveTsScale,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            int masterColumnCount,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices
    ) {
        this.factory = factory;
        this.masterTsScale = masterTsScale;
        this.slaveTsScale = slaveTsScale;
        this.asOfJoinKeyTypes = asOfJoinKeyTypes;
        this.masterColumnCount = masterColumnCount;
        this.masterSymbolKeyColumnIndices = masterSymbolKeyColumnIndices;
        this.slaveSymbolKeyColumnIndices = slaveSymbolKeyColumnIndices;
        this.isKeyed = asOfJoinKeyTypes != null;
    }

    @Override
    public void close() {
        final RecordCursorFactory factory = this.factory;
        this.factory = null;
        final Function filter = this.filter;
        this.filter = null;
        final ObjList<Function> perWorkerFilters = this.perWorkerFilters;
        this.perWorkerFilters = null;
        Throwable cleanupFailure = Misc.freeBestEffort(null, factory);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, filter);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerFilters);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    Throwable cursorClosed(Throwable cleanupFailure) {
        cleanupFailure = cursorClosed(cleanupFailure, filter);
        if (perWorkerFilters != null) {
            for (int i = 0, n = perWorkerFilters.size(); i < n; i++) {
                cleanupFailure = cursorClosed(cleanupFailure, perWorkerFilters.getQuick(i));
            }
        }
        return cleanupFailure;
    }

    void detachFactory() {
        this.factory = null;
    }

    public @Nullable ColumnTypes getAsOfJoinKeyTypes() {
        return asOfJoinKeyTypes;
    }

    public RecordCursorFactory getFactory() {
        return factory;
    }

    public @Nullable Function getFilter() {
        return filter;
    }

    public int getMasterColumnCount() {
        return masterColumnCount;
    }

    public int @Nullable [] getMasterSymbolKeyColumnIndices() {
        return masterSymbolKeyColumnIndices;
    }

    public long getMasterTsScale() {
        return masterTsScale;
    }

    public @Nullable ObjList<Function> getPerWorkerFilters() {
        return perWorkerFilters;
    }

    public int @Nullable [] getSlaveSymbolKeyColumnIndices() {
        return slaveSymbolKeyColumnIndices;
    }

    public long getSlaveTsScale() {
        return slaveTsScale;
    }

    public boolean isKeyed() {
        return isKeyed;
    }

    public void setFilter(@Nullable Function filter) {
        this.filter = filter;
    }

    public void setPerWorkerFilters(@Nullable ObjList<Function> perWorkerFilters) {
        this.perWorkerFilters = perWorkerFilters;
    }
}
