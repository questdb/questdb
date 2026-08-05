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

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.BitSet;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * Per-slave configuration for multi-slave HORIZON JOIN.
 * Holds stateless config shared by both ST and async execution paths, including everything
 * needed to build this slave's ASOF join-key {@link RecordSink}s (the class generated for it,
 * plus the metadata/filter/bitset arguments {@link io.questdb.cairo.RecordSinkFactory} needs
 * to build a correct instance -- bytecode-backed or a {@code LoopingRecordSink} fallback).
 * Sink construction stays lazy: each cursor/atom builds only the instances its own execution
 * path actually uses, the first time it needs them, instead of every path building all of them
 * upfront in the query planner.
 * <p>
 * Mutable resources (maps, sinks, records, helpers, cursors) are created
 * by the respective cursor/atom implementations.
 * <p>
 * Owns the slave {@link RecordCursorFactory} and frees it on {@link #close()}.
 */
public class HorizonJoinSlaveState implements QuietCloseable {
    private final @Nullable ColumnTypes asOfJoinKeyTypes;
    private final boolean isKeyed;
    private final @Nullable Class<RecordSink> masterAsOfJoinMapSinkClass;
    private final int masterColumnCount;
    private final @Nullable ListColumnFilter masterColumnFilter;
    private final int @Nullable [] masterSymbolKeyColumnIndices;
    private final long masterTsScale;
    private final @Nullable Class<RecordSink> slaveAsOfJoinMapSinkClass;
    private final @Nullable ListColumnFilter slaveColumnFilter;
    private final @Nullable RecordMetadata slaveMetadata;
    private final int @Nullable [] slaveSymbolKeyColumnIndices;
    private final long slaveTsScale;
    private final @Nullable BitSet writeStringAsVarcharA;
    private final @Nullable BitSet writeStringAsVarcharB;
    private final @Nullable BitSet writeSymbolAsString;
    private final @Nullable BitSet writeTimestampAsNanosA;
    private final @Nullable BitSet writeTimestampAsNanosB;
    private RecordCursorFactory factory;

    public HorizonJoinSlaveState(
            RecordCursorFactory factory,
            long masterTsScale,
            long slaveTsScale,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            int masterColumnCount,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices,
            @Nullable RecordMetadata slaveMetadata,
            @Nullable Class<RecordSink> masterAsOfJoinMapSinkClass,
            @Nullable Class<RecordSink> slaveAsOfJoinMapSinkClass,
            @Nullable ListColumnFilter masterColumnFilter,
            @Nullable ListColumnFilter slaveColumnFilter,
            @Nullable BitSet writeSymbolAsString,
            @Nullable BitSet writeStringAsVarcharA,
            @Nullable BitSet writeStringAsVarcharB,
            @Nullable BitSet writeTimestampAsNanosA,
            @Nullable BitSet writeTimestampAsNanosB
    ) {
        this.factory = factory;
        this.masterTsScale = masterTsScale;
        this.slaveTsScale = slaveTsScale;
        this.asOfJoinKeyTypes = asOfJoinKeyTypes;
        this.masterColumnCount = masterColumnCount;
        this.masterSymbolKeyColumnIndices = masterSymbolKeyColumnIndices;
        this.slaveSymbolKeyColumnIndices = slaveSymbolKeyColumnIndices;
        this.slaveMetadata = slaveMetadata;
        this.masterAsOfJoinMapSinkClass = masterAsOfJoinMapSinkClass;
        this.slaveAsOfJoinMapSinkClass = slaveAsOfJoinMapSinkClass;
        this.masterColumnFilter = masterColumnFilter;
        this.slaveColumnFilter = slaveColumnFilter;
        this.writeSymbolAsString = writeSymbolAsString;
        this.writeStringAsVarcharA = writeStringAsVarcharA;
        this.writeStringAsVarcharB = writeStringAsVarcharB;
        this.writeTimestampAsNanosA = writeTimestampAsNanosA;
        this.writeTimestampAsNanosB = writeTimestampAsNanosB;
        this.isKeyed = asOfJoinKeyTypes != null;
    }

    @Override
    public void close() {
        factory = Misc.free(factory);
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

    public @Nullable Class<RecordSink> getMasterAsOfJoinMapSinkClass() {
        return masterAsOfJoinMapSinkClass;
    }

    public int getMasterColumnCount() {
        return masterColumnCount;
    }

    public @Nullable ListColumnFilter getMasterColumnFilter() {
        return masterColumnFilter;
    }

    public int @Nullable [] getMasterSymbolKeyColumnIndices() {
        return masterSymbolKeyColumnIndices;
    }

    public long getMasterTsScale() {
        return masterTsScale;
    }

    public @Nullable Class<RecordSink> getSlaveAsOfJoinMapSinkClass() {
        return slaveAsOfJoinMapSinkClass;
    }

    public @Nullable ListColumnFilter getSlaveColumnFilter() {
        return slaveColumnFilter;
    }

    public @Nullable RecordMetadata getSlaveMetadata() {
        return slaveMetadata;
    }

    public int @Nullable [] getSlaveSymbolKeyColumnIndices() {
        return slaveSymbolKeyColumnIndices;
    }

    public long getSlaveTsScale() {
        return slaveTsScale;
    }

    public @Nullable BitSet getWriteStringAsVarcharA() {
        return writeStringAsVarcharA;
    }

    public @Nullable BitSet getWriteStringAsVarcharB() {
        return writeStringAsVarcharB;
    }

    public @Nullable BitSet getWriteSymbolAsString() {
        return writeSymbolAsString;
    }

    public @Nullable BitSet getWriteTimestampAsNanosA() {
        return writeTimestampAsNanosA;
    }

    public @Nullable BitSet getWriteTimestampAsNanosB() {
        return writeTimestampAsNanosB;
    }

    public boolean isKeyed() {
        return isKeyed;
    }
}
