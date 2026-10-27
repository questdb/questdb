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
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkTemplate;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.groupby.GroupByUtils;
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
 * Owns the slave {@link RecordCursorFactory}, its filter, its key check and their optional worker
 * clones. Async factories may detach the factory; {@link #close()} frees the remaining owners.
 */
public class HorizonJoinSlaveState implements QuietCloseable {
    private final @Nullable ColumnTypes asOfJoinKeyTypes;
    private final boolean isKeyed;
    private final @Nullable RecordSinkTemplate masterAsOfJoinMapSinkTemplate;
    private final int masterColumnCount;
    private final int @Nullable [] masterSymbolKeyColumnIndices;
    private final long masterTsScale;
    private final @Nullable RecordSinkTemplate slaveAsOfJoinMapSinkTemplate;
    private final int @Nullable [] slaveSymbolKeyColumnIndices;
    private final long slaveTsScale;
    private RecordCursorFactory factory;
    private @Nullable Function filter;
    private @Nullable Function keyCheck;
    private @Nullable ObjList<Function> perWorkerFilters;
    private @Nullable ObjList<Function> perWorkerKeyChecks;

    public HorizonJoinSlaveState(
            RecordCursorFactory factory,
            long masterTsScale,
            long slaveTsScale,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            @Nullable RecordSinkTemplate masterAsOfJoinMapSinkTemplate,
            @Nullable RecordSinkTemplate slaveAsOfJoinMapSinkTemplate,
            int masterColumnCount,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices
    ) {
        this.factory = factory;
        this.masterTsScale = masterTsScale;
        this.slaveTsScale = slaveTsScale;
        this.asOfJoinKeyTypes = asOfJoinKeyTypes;
        this.masterAsOfJoinMapSinkTemplate = masterAsOfJoinMapSinkTemplate;
        this.slaveAsOfJoinMapSinkTemplate = slaveAsOfJoinMapSinkTemplate;
        this.masterColumnCount = masterColumnCount;
        this.masterSymbolKeyColumnIndices = masterSymbolKeyColumnIndices;
        this.slaveSymbolKeyColumnIndices = slaveSymbolKeyColumnIndices;
        this.isKeyed = asOfJoinKeyTypes != null;
        assert !isKeyed || (masterAsOfJoinMapSinkTemplate != null && slaveAsOfJoinMapSinkTemplate != null);
    }

    @Override
    public void close() {
        final RecordCursorFactory factory = this.factory;
        this.factory = null;
        final Function filter = this.filter;
        this.filter = null;
        final ObjList<Function> perWorkerFilters = this.perWorkerFilters;
        this.perWorkerFilters = null;
        final Function keyCheck = this.keyCheck;
        this.keyCheck = null;
        final ObjList<Function> perWorkerKeyChecks = this.perWorkerKeyChecks;
        this.perWorkerKeyChecks = null;
        Throwable cleanupFailure = Misc.freeBestEffort(null, factory);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, filter);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerFilters);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, keyCheck);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerKeyChecks);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    /**
     * Notifies the filter, the key check and their worker clones that the cursor has closed.
     */
    Throwable cursorClosed(Throwable cleanupFailure) {
        cleanupFailure = Function.cursorClosed(cleanupFailure, filter);
        cleanupFailure = Function.cursorClosed(cleanupFailure, perWorkerFilters);
        cleanupFailure = Function.cursorClosed(cleanupFailure, keyCheck);
        return Function.cursorClosed(cleanupFailure, perWorkerKeyChecks);
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

    /**
     * The predicate over the master row that a row of this slave can match it, or null when the join looks up every
     * key of this slave.
     */
    public @Nullable Function getKeyCheck() {
        return keyCheck;
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

    public @Nullable ObjList<Function> getPerWorkerKeyChecks() {
        return perWorkerKeyChecks;
    }

    public int @Nullable [] getSlaveSymbolKeyColumnIndices() {
        return slaveSymbolKeyColumnIndices;
    }

    public long getSlaveTsScale() {
        return slaveTsScale;
    }

    /**
     * Initializes the key check over the master's symbol tables, and its worker clones over clones of them. The caller
     * notifies them through {@link #cursorClosed}.
     */
    public void initKeyCheck(SymbolTableSource masterSymbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        if (keyCheck != null) {
            keyCheck.init(masterSymbolTableSource, executionContext);
        }
        if (perWorkerKeyChecks != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                Function.init(perWorkerKeyChecks, masterSymbolTableSource, executionContext, keyCheck);
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }
    }

    public boolean isKeyed() {
        return isKeyed;
    }

    /**
     * Creates a new master-side ASOF join key sink, or returns null for a slave without join keys.
     * Each caller that runs on its own thread needs its own instance.
     */
    public @Nullable RecordSink newMasterAsOfJoinMapSink() {
        return isKeyed ? masterAsOfJoinMapSinkTemplate.newInstance() : null;
    }

    /**
     * Creates a new slave-side ASOF join key sink, or returns null for a slave without join keys.
     * Each caller that runs on its own thread needs its own instance.
     */
    public @Nullable RecordSink newSlaveAsOfJoinMapSink() {
        return isKeyed ? slaveAsOfJoinMapSinkTemplate.newInstance() : null;
    }

    public void setFilter(@Nullable Function filter) {
        this.filter = filter;
    }

    public void setKeyCheck(@Nullable Function keyCheck) {
        this.keyCheck = keyCheck;
    }

    public void setPerWorkerFilters(@Nullable ObjList<Function> perWorkerFilters) {
        this.perWorkerFilters = perWorkerFilters;
    }

    public void setPerWorkerKeyChecks(@Nullable ObjList<Function> perWorkerKeyChecks) {
        this.perWorkerKeyChecks = perWorkerKeyChecks;
    }

    /**
     * Rewinds the key check and its worker clones.
     */
    public void toTopKeyCheck() {
        if (keyCheck != null) {
            keyCheck.toTop();
        }
        if (perWorkerKeyChecks != null) {
            GroupByUtils.toTop(perWorkerKeyChecks);
        }
    }
}
