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
    private final ClonedFunction filter = new ClonedFunction();
    private final boolean isKeyed;
    private final ClonedFunction keyCheck = new ClonedFunction();
    private final @Nullable RecordSinkTemplate masterAsOfJoinMapSinkTemplate;
    private final int masterColumnCount;
    private final int @Nullable [] masterSymbolKeyColumnIndices;
    private final long masterTsScale;
    private final @Nullable RecordSinkTemplate slaveAsOfJoinMapSinkTemplate;
    private final int @Nullable [] slaveSymbolKeyColumnIndices;
    private final long slaveTsScale;
    private RecordCursorFactory factory;

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
        Throwable cleanupFailure = Misc.freeBestEffort(null, factory);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, filter);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, keyCheck);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    /**
     * Notifies the filter, the key check and their worker clones that the cursor has closed.
     */
    Throwable cursorClosed(Throwable cleanupFailure) {
        cleanupFailure = filter.cursorClosed(cleanupFailure);
        return keyCheck.cursorClosed(cleanupFailure);
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
        return filter.getFunction();
    }

    /**
     * The filter the worker in {@code slotId} evaluates: its clone, or the filter itself for the owner thread, slot
     * -1, and when the filter has no clones.
     */
    public @Nullable Function getFilter(int slotId) {
        return filter.get(slotId);
    }

    /**
     * The predicate over the master row that a row of this slave can match it, or null when the join looks up every
     * key of this slave.
     */
    public @Nullable Function getKeyCheck() {
        return keyCheck.getFunction();
    }

    /**
     * The key check the worker in {@code slotId} evaluates: its clone, or the key check itself for the owner thread,
     * slot -1, and when the key check has no clones.
     */
    public @Nullable Function getKeyCheck(int slotId) {
        return keyCheck.get(slotId);
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

    public int @Nullable [] getSlaveSymbolKeyColumnIndices() {
        return slaveSymbolKeyColumnIndices;
    }

    public long getSlaveTsScale() {
        return slaveTsScale;
    }

    /**
     * Initializes the filter over the slave's symbol tables and the key check over the master's, and their worker
     * clones over clones of them. The caller notifies them through {@link #cursorClosed}.
     */
    public void init(
            SymbolTableSource masterSymbolTableSource,
            SymbolTableSource slaveSymbolTableSource,
            SqlExecutionContext executionContext
    ) throws SqlException {
        filter.init(slaveSymbolTableSource, executionContext);
        keyCheck.init(masterSymbolTableSource, executionContext);
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
        this.filter.setFunction(filter);
    }

    public void setKeyCheck(@Nullable Function keyCheck) {
        this.keyCheck.setFunction(keyCheck);
    }

    public void setPerWorkerFilters(@Nullable ObjList<Function> perWorkerFilters) {
        filter.setPerWorkerFunctions(perWorkerFilters);
    }

    public void setPerWorkerKeyChecks(@Nullable ObjList<Function> perWorkerKeyChecks) {
        keyCheck.setPerWorkerFunctions(perWorkerKeyChecks);
    }

    /**
     * Rewinds the filter, the key check and their worker clones.
     */
    public void toTop() {
        filter.toTop();
        keyCheck.toTop();
    }
}
