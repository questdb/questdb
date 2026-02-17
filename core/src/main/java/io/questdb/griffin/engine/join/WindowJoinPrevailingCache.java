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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.DirectIntLongHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;

/**
 * Cache for lazy prevailing row lookups in window joins with INCLUDE PREVAILING semantics.
 * Used in fast window join factories, in situations when we have a join on a symbol.
 * <p>
 * Stores mappings from master symbol keys to prevailing slave row IDs. When a key is not
 * cached, performs a backward scan from the last known position to find the prevailing
 * (most recent) matching row in the slave table. During the scan, opportunistically caches
 * other matching keys encountered.
 */
public class WindowJoinPrevailingCache implements QuietCloseable, Mutable, Reopenable {
    // used when the symbol key is not present in the cache
    private static final long NO_ENTRY_VALUE = Rows.toRowID(-1, 0);
    // holds <master_key, slave_rowid> pairs
    private final DirectIntLongHashMap cache;
    private int frameIndex = -1;
    private long rowIndex = Long.MIN_VALUE;

    WindowJoinPrevailingCache() {
        this.cache = new DirectIntLongHashMap(
                AsyncWindowJoinFastAtom.SLAVE_MAP_INITIAL_CAPACITY,
                AsyncWindowJoinFastAtom.SLAVE_MAP_LOAD_FACTOR,
                0,
                NO_ENTRY_VALUE,
                MemoryTag.NATIVE_UNORDERED_MAP
        );
    }

    @Override
    public void clear() {
        cache.clear();
        frameIndex = -1;
        rowIndex = Long.MIN_VALUE;
    }

    @Override
    public void close() {
        cache.close();
    }

    public long findPrevailingSlaveRowId(
            WindowJoinTimeFrameHelper slaveTimeFrameHelper,
            Record slaveRecord,
            int slaveSymbolIndex,
            DirectIntIntHashMap slaveSymbolLookupMap,
            int masterKey
    ) {
        if (frameIndex == -1) {
            return Long.MIN_VALUE;
        }

        // fast path: check the cache
        final int masterCacheKey = AsyncWindowJoinFastAtom.toSymbolMapKey(masterKey);
        final long cachedRowId = cache.get(masterCacheKey);
        if (cachedRowId != NO_ENTRY_VALUE) {
            return cachedRowId;
        }

        // slow path: we need to start/continue the backward scan
        if (rowIndex < 0) {
            // oops, previously we've scanned the slave table until the very start
            // or the row index was never initialized (Long.MIN_VALUE)
            return Long.MIN_VALUE;
        }

        final int savedFrameIndex = slaveTimeFrameHelper.getBookmarkedFrameIndex();
        final long savedRowId = slaveTimeFrameHelper.getBookmarkedRowIndex();

        try {
            long scanStart = rowIndex;
            slaveTimeFrameHelper.restoreBookmark(frameIndex, rowIndex);
            do {
                frameIndex = slaveTimeFrameHelper.getTimeFrameIndex();
                // actual row index doesn't matter here due to the later recordAtRowIndex() call
                slaveTimeFrameHelper.recordAt(Rows.toRowID(frameIndex, 0));

                long rowLo = slaveTimeFrameHelper.getTimeFrameRowLo();
                long rowHi = slaveTimeFrameHelper.getTimeFrameRowHi();
                scanStart = Math.min(scanStart, rowHi - 1);

                for (long r = scanStart; r >= rowLo; r--) {
                    slaveTimeFrameHelper.recordAtRowIndex(r);

                    final int slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                    final int matchingMasterKey = slaveSymbolLookupMap.get(AsyncWindowJoinFastAtom.toSymbolMapKey(slaveKey));
                    if (matchingMasterKey == masterKey) {
                        // Hurray! We've found the key.
                        final long rowId = Rows.toRowID(slaveTimeFrameHelper.getTimeFrameIndex(), r);
                        cache.put(masterCacheKey, rowId);
                        rowIndex = r - 1;
                        return rowId;
                    } else if (matchingMasterKey != StaticSymbolTable.VALUE_NOT_FOUND) {
                        // It's another matching key. Cache it if it's not already there.
                        cache.putIfAbsent(
                                AsyncWindowJoinFastAtom.toSymbolMapKey(matchingMasterKey),
                                Rows.toRowID(frameIndex, r)
                        );
                    }
                }
                rowIndex = rowLo - 1;
                scanStart = Long.MAX_VALUE;
            } while (slaveTimeFrameHelper.previousFrame());
        } finally {
            slaveTimeFrameHelper.restoreBookmark(savedFrameIndex, savedRowId);
        }
        return Long.MIN_VALUE;
    }

    public DirectIntLongHashMap getCache() {
        return cache;
    }

    public void of(int frameIndex, long rowIndex) {
        cache.clear();
        this.frameIndex = frameIndex;
        this.rowIndex = rowIndex;
    }

    @Override
    public void reopen() {
        cache.reopen();
        frameIndex = -1;
        rowIndex = Long.MIN_VALUE;
    }
}
