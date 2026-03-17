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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * Encapsulates per-slave configuration and runtime state for HORIZON JOIN.
 * Used by multi-slave ST factories to group all slave-related fields.
 */
public class HorizonJoinSlaveState implements QuietCloseable {
    private final @Nullable Map asOfJoinMap;
    // not freed on close() call; must be closed separately
    private final RecordCursorFactory factory;
    private final boolean isKeyed;
    private final @Nullable RecordSink masterAsOfJoinMapSink;
    private final int masterColumnCount;
    private final int @Nullable [] masterSymbolKeyColumnIndices;
    private final long masterTsScale;
    private final @Nullable RecordSink slaveAsOfJoinMapSink;
    private final int @Nullable [] slaveSymbolKeyColumnIndices;
    private final long slaveTsScale;
    private final HorizonJoinTimeFrameHelper timeFrameHelper;
    private @Nullable SymbolTranslatingRecord symbolTranslatingRecord;
    private TimeFrameCursor timeFrameCursor;

    public HorizonJoinSlaveState(
            CairoConfiguration configuration,
            RecordCursorFactory factory,
            long masterTsScale,
            long slaveTsScale,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            @Nullable RecordSink masterAsOfJoinMapSink,
            @Nullable RecordSink slaveAsOfJoinMapSink,
            int masterColumnCount,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices
    ) {
        this.factory = factory;
        this.masterTsScale = masterTsScale;
        this.slaveTsScale = slaveTsScale;
        this.masterAsOfJoinMapSink = masterAsOfJoinMapSink;
        this.slaveAsOfJoinMapSink = slaveAsOfJoinMapSink;
        this.masterColumnCount = masterColumnCount;
        this.masterSymbolKeyColumnIndices = masterSymbolKeyColumnIndices;
        this.slaveSymbolKeyColumnIndices = slaveSymbolKeyColumnIndices;
        this.isKeyed = asOfJoinKeyTypes != null;
        this.timeFrameHelper = new HorizonJoinTimeFrameHelper(configuration.getSqlAsOfJoinLookAhead(), slaveTsScale);

        if (asOfJoinKeyTypes != null) {
            SingleColumnType asOfValueTypes = new SingleColumnType(ColumnType.LONG);
            this.asOfJoinMap = MapFactory.createUnorderedMap(configuration, asOfJoinKeyTypes, asOfValueTypes);
        } else {
            this.asOfJoinMap = null;
        }

        if (masterSymbolKeyColumnIndices != null) {
            this.symbolTranslatingRecord = new SymbolTranslatingRecord(masterColumnCount, masterSymbolKeyColumnIndices, slaveSymbolKeyColumnIndices);
        } else {
            this.symbolTranslatingRecord = null;
        }
    }

    @Override
    public void close() {
        timeFrameCursor = Misc.free(timeFrameCursor);
        Misc.free(asOfJoinMap);
        Misc.clear(symbolTranslatingRecord);
    }

    public @Nullable Map getAsOfJoinMap() {
        return asOfJoinMap;
    }

    public RecordCursorFactory getFactory() {
        return factory;
    }

    public @Nullable RecordSink getMasterAsOfJoinMapSink() {
        return masterAsOfJoinMapSink;
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

    public Record getRecord() {
        return timeFrameHelper.getRecord();
    }

    public @Nullable RecordSink getSlaveAsOfJoinMapSink() {
        return slaveAsOfJoinMapSink;
    }

    public int @Nullable [] getSlaveSymbolKeyColumnIndices() {
        return slaveSymbolKeyColumnIndices;
    }

    public long getSlaveTsScale() {
        return slaveTsScale;
    }

    public @Nullable SymbolTranslatingRecord getSymbolTranslatingRecord() {
        return symbolTranslatingRecord;
    }

    public HorizonJoinTimeFrameHelper getTimeFrameHelper() {
        return timeFrameHelper;
    }

    public boolean isKeyed() {
        return isKeyed;
    }

    public void reopen() {
        if (asOfJoinMap != null) {
            asOfJoinMap.reopen();
        }
    }

    public void setTimeFrameCursor(TimeFrameCursor cursor) {
        this.timeFrameCursor = cursor;
    }
}
