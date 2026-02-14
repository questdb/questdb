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

import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;

import java.util.Arrays;

/**
 * A record wrapper that translates master symbol IDs to slave symbol IDs
 * for efficient integer-based key comparison in HORIZON JOIN.
 * <p>
 * Instead of resolving symbol IDs to strings for comparison, this record
 * intercepts {@link #getInt(int)} calls for symbol join key columns and
 * returns the corresponding slave symbol ID via a lazily-populated cache.
 * <p>
 * Symbol tables are obtained lazily from the configured sources on first
 * cache miss, following the same pattern as {@code PageFrameMemoryRecord}'s
 * symbol table cache.
 * <p>
 * Each instance is thread-unsafe and must be used by a single worker.
 */
public class SymbolTranslatingRecord extends DelegatingRecord implements QuietCloseable, Mutable {
    private static final int NO_ENTRY = -1; // IntIntHashMap.noEntryValue
    private final IntIntHashMap[] caches;
    // Maps column index to cache/symbol table array index; -1 for non-symbol columns.
    // Sized to the total number of master columns, so no bounds check is needed.
    private final int[] columnToKeyIndex;
    // Master column indices for symbol key columns, used for symbol table source lookups.
    private final int[] masterColumnIndices;
    private final SymbolTable[] masterSymbolTableCache;
    // Slave column indices for symbol key columns, used for symbol table source lookups.
    private final int[] slaveColumnIndices;
    private final StaticSymbolTable[] slaveSymbolTableCache;
    private SymbolTableSource masterSource;
    private SymbolTableSource slaveSource;

    /**
     * @param masterColumnCount            total number of columns in the master record metadata
     * @param masterSymbolKeyColumnIndices master column indices for symbol key columns used in join
     * @param slaveSymbolKeyColumnIndices  slave column indices for symbol key columns used in join
     */
    public SymbolTranslatingRecord(int masterColumnCount, int[] masterSymbolKeyColumnIndices, int[] slaveSymbolKeyColumnIndices) {
        final int joinColumnCount = masterSymbolKeyColumnIndices.length;
        this.caches = new IntIntHashMap[joinColumnCount];
        this.masterSymbolTableCache = new SymbolTable[joinColumnCount];
        this.slaveSymbolTableCache = new StaticSymbolTable[joinColumnCount];
        this.masterColumnIndices = masterSymbolKeyColumnIndices;
        this.slaveColumnIndices = slaveSymbolKeyColumnIndices;
        for (int i = 0; i < joinColumnCount; i++) {
            caches[i] = new IntIntHashMap();
        }

        columnToKeyIndex = new int[masterColumnCount];
        Arrays.fill(columnToKeyIndex, -1);
        for (int i = 0; i < joinColumnCount; i++) {
            columnToKeyIndex[masterSymbolKeyColumnIndices[i]] = i;
        }
    }

    @Override
    public void clear() {
        Misc.clear(caches);
    }

    @Override
    public void close() {
        Misc.freeIfCloseable(masterSymbolTableCache);
        Misc.freeIfCloseable(slaveSymbolTableCache);
        masterSource = null;
        slaveSource = null;
    }

    @Override
    public int getInt(int col) {
        int idx = columnToKeyIndex[col];
        if (idx >= 0) {
            return translate(idx, base.getInt(col));
        }
        return base.getInt(col);
    }

    /**
     * Set the symbol table sources for lazy symbol table resolution.
     * Must be called before any {@link #getInt(int)} call on symbol key columns.
     */
    public void initSources(SymbolTableSource masterSource, SymbolTableSource slaveSource) {
        Misc.freeIfCloseable(masterSymbolTableCache);
        Misc.freeIfCloseable(slaveSymbolTableCache);
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;
    }

    private SymbolTable getMasterSymbolTable(int idx) {
        SymbolTable st = masterSymbolTableCache[idx];
        if (st == null) {
            st = masterSource.newSymbolTable(masterColumnIndices[idx]);
            masterSymbolTableCache[idx] = st;
        }
        return st;
    }

    private StaticSymbolTable getSlaveSymbolTable(int idx) {
        StaticSymbolTable st = slaveSymbolTableCache[idx];
        if (st == null) {
            st = (StaticSymbolTable) slaveSource.newSymbolTable(slaveColumnIndices[idx]);
            slaveSymbolTableCache[idx] = st;
        }
        return st;
    }

    private int translate(int idx, int masterSymKey) {
        if (masterSymKey == SymbolTable.VALUE_IS_NULL) {
            return SymbolTable.VALUE_IS_NULL;
        }
        int slaveKey = caches[idx].get(masterSymKey);
        if (slaveKey != NO_ENTRY) {
            return slaveKey;
        }
        // Cache miss: resolve via string using lazily-obtained symbol tables
        final CharSequence symValue = getMasterSymbolTable(idx).valueOf(masterSymKey);
        slaveKey = getSlaveSymbolTable(idx).keyOf(symValue);
        caches[idx].put(masterSymKey, slaveKey);
        return slaveKey;
    }
}
