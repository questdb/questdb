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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * Per-slave configuration for multi-slave HORIZON JOIN.
 * Holds stateless config shared by both ST and async execution paths.
 * Mutable resources (maps, sinks, records, helpers, cursors) are created
 * by the respective cursor/atom implementations.
 * <p>
 * Owns the slave {@link RecordCursorFactory} and frees it on {@link #close()}.
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
        factory = Misc.free(factory);
    }

    public @Nullable ColumnTypes getAsOfJoinKeyTypes() {
        return asOfJoinKeyTypes;
    }

    public RecordCursorFactory getFactory() {
        return factory;
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

    public boolean isKeyed() {
        return isKeyed;
    }
}
