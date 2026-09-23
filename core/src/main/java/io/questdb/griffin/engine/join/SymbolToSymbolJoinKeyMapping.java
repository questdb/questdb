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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Maps master symbol keys to slave symbol keys and caches the translations.
 * <p>
 * The cache lives in native memory, so that a large cache built by one execution
 * does not linger on the heap while the owning factory sits in the query cache.
 * The cache stays closed, and takes no memory, until {@link #reopen()} opens it.
 * The owning cursor calls {@link #reopen()} before it adopts the master and slave
 * cursors, {@link #of} after, and {@link #close()} when it closes.
 * <p>
 * The cache holds at most {@link CairoConfiguration#getSqlAsOfJoinShortCircuitCacheCapacity()}
 * entries. Once the cache is full, master symbol keys missing from it get translated
 * via their string values on every lookup.
 */
public final class SymbolToSymbolJoinKeyMapping implements SymbolJoinKeyMapping {
    private static final int CACHE_INITIAL_CAPACITY = 16;
    private static final double CACHE_LOAD_FACTOR = 0.5;
    // Master symbol keys are non-negative, and getSlaveKey() handles VALUE_IS_NULL
    // before the cache lookup, so VALUE_IS_NULL never appears as a real key.
    private static final int NO_ENTRY_KEY = SymbolTable.VALUE_IS_NULL;
    // The cache holds only non-negative slave keys, so -1 is free.
    private static final int NO_ENTRY_VALUE = -1;
    private final CairoConfiguration config;
    // Closed until reopen() opens it; close() releases its native memory.
    private final DirectIntIntHashMap masterKeyToSlaveKey = new DirectIntIntHashMap(
            CACHE_INITIAL_CAPACITY,
            CACHE_LOAD_FACTOR,
            NO_ENTRY_KEY,
            NO_ENTRY_VALUE,
            MemoryTag.NATIVE_JOIN_MAP,
            false
    );
    private final int masterSymbolIndex;
    private final int slaveSymbolIndex;
    private int maxCacheSize = 0;
    private StaticSymbolTable slaveSymbolTable;

    public SymbolToSymbolJoinKeyMapping(CairoConfiguration config, int masterSymbolIndex, int slaveSymbolIndex) {
        this.config = config;
        this.masterSymbolIndex = masterSymbolIndex;
        this.slaveSymbolIndex = slaveSymbolIndex;
    }

    /**
     * Releases the native cache. The instance stays reusable: the next
     * {@link #reopen()} call opens the cache again.
     */
    @Override
    public void close() {
        masterKeyToSlaveKey.close();
        slaveSymbolTable = null;
    }

    @TestOnly
    public int getCacheSize() {
        return masterKeyToSlaveKey.size();
    }

    @Override
    public int getSlaveKey(Record masterRecord) {
        assert slaveSymbolTable != null : "slaveSymbolTable must be set before calling getSlaveKey";
        assert masterKeyToSlaveKey.isOpen() : "cache must be open before calling getSlaveKey";

        final int masterKey = masterRecord.getInt(masterSymbolIndex);
        if (masterKey == SymbolTable.VALUE_IS_NULL) {
            // containsNullValue() reads a flag, so a cache lookup would not make it any cheaper
            return slaveSymbolTable.containsNullValue() ? SymbolTable.VALUE_IS_NULL : StaticSymbolTable.VALUE_NOT_FOUND;
        }

        final long index = masterKeyToSlaveKey.keyIndex(masterKey);
        if (index < 0) {
            return masterKeyToSlaveKey.valueAt(index);
        }

        final CharSequence strSym = masterRecord.getSymA(masterSymbolIndex);
        final int slaveKey = slaveSymbolTable.keyOf(strSym);
        // We could consider adding a cache also for keys known to be not found.
        // Not implemented for now.
        if (slaveKey != StaticSymbolTable.VALUE_NOT_FOUND && masterKeyToSlaveKey.size() < maxCacheSize) {
            masterKeyToSlaveKey.putAt(index, masterKey, slaveKey);
        }
        return slaveKey;
    }

    @Override
    public boolean isShortCircuit(Record masterRecord) {
        return getSlaveKey(masterRecord) == StaticSymbolTable.VALUE_NOT_FOUND;
    }

    @Override
    public void of(TimeFrameCursor slaveCursor) {
        this.slaveSymbolTable = slaveCursor.getSymbolTable(slaveSymbolIndex);
        resetCache();
    }

    @Override
    public void of(RecordCursor slaveCursor) {
        this.slaveSymbolTable = SymbolJoinKeyMapping.toStaticSymbolTable(slaveCursor.getSymbolTable(slaveSymbolIndex));
        resetCache();
    }

    @Override
    public void reopen() {
        masterKeyToSlaveKey.reopen();
    }

    @Override
    public void setMemoryTracker(@Nullable MemoryTracker tracker) {
        masterKeyToSlaveKey.setMemoryTracker(tracker);
    }

    private void resetCache() {
        maxCacheSize = config.getSqlAsOfJoinShortCircuitCacheCapacity();
        // restoreInitialCapacity() opens a closed cache, and shrinks and clears an open one,
        // so each execution starts small, even if the owning cursor skipped close()
        masterKeyToSlaveKey.restoreInitialCapacity();
    }
}
