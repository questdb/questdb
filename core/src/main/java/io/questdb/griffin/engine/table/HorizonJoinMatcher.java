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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor.scaleTimestamp;

/**
 * Finds the ASOF match of a horizon timestamp in every slave of a row-preserving HORIZON JOIN.
 * <p>
 * An instance holds the lookup state of one thread: per slave, a time frame helper, the keyed
 * ASOF cache, the key sinks and the symbol translator. The parallel projection atom keeps one
 * instance per worker slot plus one for the owner thread, and the serial projection cursor keeps
 * one. {@link #match} writes one slave row id per slave, or {@code Long.MIN_VALUE} when the slave
 * has no row at or before the horizon timestamp for the master row's key.
 * <p>
 * The caller owns the slave time frame cursors. It binds them with {@link #of} before matching and
 * calls {@link #toTop()} before each batch of horizon timestamps: the helpers keep sequential scan
 * state that is only valid for a monotonic walk.
 */
public class HorizonJoinMatcher implements QuietCloseable, Mutable {
    private final ObjList<Map> asOfJoinMaps;
    private final ObjList<HorizonJoinTimeFrameHelper> helpers;
    private final boolean isKeyed;
    private final ObjList<RecordSink> masterSinks;
    private final long[] masterTsScales;
    private final int slaveCount;
    private final ObjList<RecordSink> slaveSinks;
    private final ObjList<SymbolTranslatingRecord> translatingRecords;

    public HorizonJoinMatcher(
            @NotNull CairoConfiguration configuration,
            @NotNull ObjList<HorizonJoinSlaveState> slaveStates,
            @Nullable Class<RecordSink> @NotNull [] masterAsOfJoinMapSinkClasses,
            @Nullable Class<RecordSink> @NotNull [] slaveAsOfJoinMapSinkClasses
    ) {
        this.slaveCount = slaveStates.size();
        this.asOfJoinMaps = new ObjList<>(slaveCount);
        this.helpers = new ObjList<>(slaveCount);
        this.masterSinks = new ObjList<>(slaveCount);
        this.masterTsScales = new long[slaveCount];
        this.slaveSinks = new ObjList<>(slaveCount);
        this.translatingRecords = new ObjList<>(slaveCount);
        boolean isKeyed = false;
        try {
            final SingleColumnType asOfValueTypes = new SingleColumnType(ColumnType.LONG);
            for (int s = 0; s < slaveCount; s++) {
                final HorizonJoinSlaveState state = slaveStates.getQuick(s);
                masterTsScales[s] = state.getMasterTsScale();
                helpers.add(new HorizonJoinTimeFrameHelper(
                        configuration.getSqlAsOfJoinLookAhead(),
                        state.getSlaveTsScale(),
                        configuration.getSqlHorizonJoinBwdScanAbsoluteThreshold(),
                        configuration.getSqlHorizonJoinBwdScanMinGap(),
                        configuration.getSqlHorizonJoinBwdScanSwitchFactor()
                ));
                if (state.isKeyed()) {
                    assert masterAsOfJoinMapSinkClasses[s] != null && slaveAsOfJoinMapSinkClasses[s] != null;
                    isKeyed = true;
                    // RecordSink instances have mutable state, so every thread gets its own copy.
                    masterSinks.add(RecordSinkFactory.getInstance(masterAsOfJoinMapSinkClasses[s], null, null, null, null, null, null, null));
                    slaveSinks.add(RecordSinkFactory.getInstance(slaveAsOfJoinMapSinkClasses[s], null, null, null, null, null, null, null));
                    // openOnInit=false: of() allocates the backing under the per-query tracker.
                    asOfJoinMaps.add(MapFactory.createUnorderedMap(configuration, state.getAsOfJoinKeyTypes(), asOfValueTypes, false, false));
                } else {
                    masterSinks.add(null);
                    slaveSinks.add(null);
                    asOfJoinMaps.add(null);
                }
                translatingRecords.add(state.getMasterSymbolKeyColumnIndices() != null
                        ? new SymbolTranslatingRecord(state.getMasterColumnCount(), state.getMasterSymbolKeyColumnIndices(), state.getSlaveSymbolKeyColumnIndices())
                        : null);
            }
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
        this.isKeyed = isKeyed;
    }

    /**
     * Releases the key caches and the symbol tables of the current query. {@link #of} reopens them.
     */
    @Override
    public void clear() {
        Throwable failure = Misc.freeObjListAndKeepObjectsBestEffort(null, asOfJoinMaps);
        failure = Misc.freeObjListAndKeepObjectsBestEffort(failure, translatingRecords);
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void close() {
        Throwable failure = Misc.freeObjListBestEffort(null, asOfJoinMaps);
        failure = Misc.freeObjListBestEffort(failure, translatingRecords);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Returns the time frame helper of one slave. Its record reads a matched slave row after
     * {@link HorizonJoinTimeFrameHelper#recordAt(long)}; positioning it between two {@link #match}
     * calls is safe, the lookups re-position the record themselves.
     */
    public HorizonJoinTimeFrameHelper getHelper(int slaveIndex) {
        return helpers.getQuick(slaveIndex);
    }

    /**
     * Reports whether any slave is joined on a key. Only then does {@link #match} read the master
     * record, so a caller may skip positioning it otherwise.
     */
    public boolean isKeyed() {
        return isKeyed;
    }

    /**
     * Writes the ASOF match of {@code horizonTimestamp} in every slave to {@code slaveCount}
     * consecutive longs at {@code outAddress}. The master record must be positioned on the master
     * row the horizon timestamp belongs to whenever {@link #isKeyed()} is true.
     */
    public void match(long horizonTimestamp, Record masterRecord, long outAddress) {
        for (int s = 0; s < slaveCount; s++) {
            final HorizonJoinTimeFrameHelper helper = helpers.getQuick(s);
            long matchRowId = helper.findAsOfRow(scaleTimestamp(horizonTimestamp, masterTsScales[s]));
            final Map asOfJoinMap = asOfJoinMaps.getQuick(s);
            if (asOfJoinMap != null) {
                Record keyRecord = masterRecord;
                final SymbolTranslatingRecord translatingRecord = translatingRecords.getQuick(s);
                if (translatingRecord != null) {
                    translatingRecord.of(masterRecord);
                    keyRecord = translatingRecord;
                }
                matchRowId = helper.findKeyedAsOfMatch(
                        matchRowId,
                        keyRecord,
                        masterSinks.getQuick(s),
                        slaveSinks.getQuick(s),
                        asOfJoinMap,
                        translatingRecord
                );
            }
            Unsafe.putLong(outAddress + ((long) s << 3), matchRowId);
        }
    }

    /**
     * Binds one slave's time frame cursor for the current query and opens its key cache.
     *
     * @param slaveIndex              slave position in the join
     * @param slaveCursor             time frame cursor owned by the caller
     * @param masterSymbolTableSource symbol tables of the master rows passed to {@link #match}
     * @param slaveSymbolTableSource  symbol tables of the slave
     * @param memoryTracker           per-query tracker charged for the key cache, or null
     */
    public void of(
            int slaveIndex,
            TimeFrameCursor slaveCursor,
            SymbolTableSource masterSymbolTableSource,
            SymbolTableSource slaveSymbolTableSource,
            @Nullable MemoryTracker memoryTracker
    ) {
        helpers.getQuick(slaveIndex).of(slaveCursor);
        final SymbolTranslatingRecord translatingRecord = translatingRecords.getQuick(slaveIndex);
        if (translatingRecord != null) {
            translatingRecord.initSources(masterSymbolTableSource, slaveSymbolTableSource);
        }
        final Map asOfJoinMap = asOfJoinMaps.getQuick(slaveIndex);
        if (asOfJoinMap != null) {
            asOfJoinMap.setMemoryTracker(memoryTracker);
            asOfJoinMap.reopen();
            asOfJoinMap.clear();
        }
    }

    /**
     * Resets the sequential scan state before a new walk over horizon timestamps.
     */
    public void toTop() {
        for (int s = 0; s < slaveCount; s++) {
            helpers.getQuick(s).toTop();
            final Map asOfJoinMap = asOfJoinMaps.getQuick(s);
            if (asOfJoinMap != null) {
                asOfJoinMap.clear();
            }
        }
    }
}
