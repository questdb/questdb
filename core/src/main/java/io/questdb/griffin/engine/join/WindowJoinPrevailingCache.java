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

import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.DirectIntLongHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    // the backward scan consults the circuit breaker once per this many rows (a power of two)
    private static final int CIRCUIT_BREAKER_CHECK_ROWS = 1024;
    private static final long NO_ENTRY_VALUE = Rows.toRowID(-1, 0);
    // holds <master_key, slave_rowid> pairs
    private final DirectIntLongHashMap cache;
    private SqlExecutionCircuitBreaker circuitBreaker = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
    private int frameIndex = -1;
    // with summaries: the block whose summary the next lookup starts from, once the scan is done
    private int nextSummaryBlock = -1;
    private long rowIndex = Long.MIN_VALUE;
    // the block the scan stops at the start of; -1 when the scan runs to the table start
    private int scanStopBlock = -1;
    private @Nullable WindowJoinPrevailingSummaries summaries;
    // optional dense slave key -> master key map, see setDenseLookup()
    private long denseLookupAddress;
    private int denseLookupCount;

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
        scanStopBlock = -1;
        nextSummaryBlock = -1;
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
        if (rowIndex == Long.MIN_VALUE) {
            // oops, previously we've scanned the slave table until the very start
            // or the row index was never initialized (Long.MIN_VALUE)
            if (nextSummaryBlock >= 0) {
                // the scan stopped at a block boundary; the blocks below it are summarised
                return findBelowScanStop(slaveTimeFrameHelper, slaveRecord, slaveSymbolIndex, slaveSymbolLookupMap, masterKey, masterCacheKey);
            }
            return Long.MIN_VALUE;
        }

        final int savedFrameIndex = slaveTimeFrameHelper.getBookmarkedFrameIndex();
        final long savedRowId = slaveTimeFrameHelper.getBookmarkedRowIndex();

        int rowsScanned = 0;
        try {
            long scanStart = rowIndex;
            slaveTimeFrameHelper.restoreBookmark(frameIndex, rowIndex);
            do {
                frameIndex = slaveTimeFrameHelper.getTimeFrameIndex();
                // actual row index doesn't matter here due to the later recordAtRowIndex() call
                slaveTimeFrameHelper.recordAt(frameIndex, 0);

                long rowLo = slaveTimeFrameHelper.getTimeFrameRowLo();
                long rowHi = slaveTimeFrameHelper.getTimeFrameRowHi();
                scanStart = Math.min(scanStart, rowHi - 1);

                if (scanStopBlock >= 0 && summaries.getBlockOf(frameIndex) < scanStopBlock) {
                    // walked out of the scan's own block: the rest is summarised
                    rowIndex = Long.MIN_VALUE;
                    nextSummaryBlock = scanStopBlock - 1;
                    break;
                }
                // the key column's memory, when the frame has it as plain values: read it directly
                final long keyAddress = slaveRecord instanceof PageFrameMemoryRecord frameRecord ? frameRecord.getPageAddress(slaveSymbolIndex) : 0;
                for (long r = scanStart; r >= rowLo; r--) {
                    if ((rowsScanned++ & (CIRCUIT_BREAKER_CHECK_ROWS - 1)) == 0) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                    }
                    final int slaveKey;
                    if (keyAddress != 0) {
                        slaveKey = Unsafe.getInt(keyAddress + (r << 2));
                    } else {
                        slaveTimeFrameHelper.recordAtRowIndex(r);
                        slaveKey = slaveRecord.getInt(slaveSymbolIndex);
                    }
                    final int matchingMasterKey = lookupMasterKey(slaveSymbolLookupMap, slaveKey);
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
            if (nextSummaryBlock < 0) {
                rowIndex = Long.MIN_VALUE; // we've scanned until the very beginning, no more rows to check
            }
        } finally {
            slaveTimeFrameHelper.restoreBookmark(savedFrameIndex, savedRowId);
        }
        if (nextSummaryBlock >= 0) {
            return findBelowScanStop(slaveTimeFrameHelper, slaveRecord, slaveSymbolIndex, slaveSymbolLookupMap, masterKey, masterCacheKey);
        }
        return Long.MIN_VALUE;
    }

    /**
     * Gives the backward scan a dense array for the slave key to master key lookup, in place of the
     * hash map: {@code count} ints at {@code address}, the master key of slave key k at index k + 1,
     * of NULL at index 0, {@link StaticSymbolTable#VALUE_NOT_FOUND} for a key that cannot join. It
     * must say what the hash map says. 0 switches it off.
     */
    public void setDenseLookup(long address, int count) {
        this.denseLookupAddress = address;
        this.denseLookupCount = count;
    }

    private int lookupMasterKey(DirectIntIntHashMap slaveSymbolLookupMap, int slaveKey) {
        if (denseLookupAddress != 0) {
            final int index = Math.max(slaveKey + 1, 0);
            return index < denseLookupCount ? Unsafe.getInt(denseLookupAddress + ((long) index << 2)) : StaticSymbolTable.VALUE_NOT_FOUND;
        }
        return slaveSymbolLookupMap.get(AsyncWindowJoinFastAtom.toSymbolMapKey(slaveKey));
    }

    public DirectIntLongHashMap getCache() {
        return cache;
    }

    public void of(int frameIndex, long rowIndex) {
        of(frameIndex, rowIndex, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
    }

    /**
     * Starts the lookups of a page frame whose backward scan begins at the given slave row.
     *
     * @param circuitBreaker the circuit breaker of the worker reducing the page frame; the scans and
     *                       the waits for a summary another worker is building check it
     */
    public void of(int frameIndex, long rowIndex, @NotNull SqlExecutionCircuitBreaker circuitBreaker) {
        cache.clear();
        this.circuitBreaker = circuitBreaker;
        this.frameIndex = frameIndex;
        this.rowIndex = rowIndex;
        this.nextSummaryBlock = -1;
        // The scan covers its own block from the start row down; the blocks below come from the
        // summaries. Without summaries, or in the first block, it runs to the table start as before.
        this.scanStopBlock = frameIndex >= 0 && summaries != null && summaries.isEnabled()
                ? summaries.getBlockOf(frameIndex)
                : -1;
        if (scanStopBlock == 0) {
            scanStopBlock = -1;
        }
    }

    @Override
    public void reopen() {
        cache.reopen();
        frameIndex = -1;
        rowIndex = Long.MIN_VALUE;
        scanStopBlock = -1;
        nextSummaryBlock = -1;
    }

    /**
     * Binds the per-query tracker the backing map charges. The cache holds one entry per master
     * symbol key it has resolved, so it grows with the join's symbol cardinality and belongs under
     * the per-query limit. Callers bind right before {@link #reopen()}.
     */
    public void setMemoryTracker(@Nullable MemoryTracker memoryTracker) {
        cache.setMemoryTracker(memoryTracker);
    }

    /**
     * Shares the per-block summaries of the slave with this cache; {@code null} keeps the plain
     * backward scan. Takes effect from the next {@link #of(int, long)}.
     */
    public void setSummaries(@Nullable WindowJoinPrevailingSummaries summaries) {
        this.summaries = summaries;
    }

    // The key's last row below the block boundary the scan stopped at: from the summaries or, when
    // they are off for the query (the memory tracker refused them), from the plain scan, resumed at
    // the top of the block below the boundary and left to run to the table start from then on.
    private long findBelowScanStop(
            WindowJoinTimeFrameHelper slaveTimeFrameHelper,
            Record slaveRecord,
            int slaveSymbolIndex,
            DirectIntIntHashMap slaveSymbolLookupMap,
            int masterKey,
            int masterCacheKey
    ) {
        final long rowId = findInSummaries(slaveTimeFrameHelper, slaveRecord, slaveSymbolIndex, masterKey, masterCacheKey);
        if (rowId != WindowJoinPrevailingSummaries.UNAVAILABLE) {
            return rowId;
        }
        assert summaries != null;
        final int savedFrameIndex = slaveTimeFrameHelper.getBookmarkedFrameIndex();
        final long savedRowId = slaveTimeFrameHelper.getBookmarkedRowIndex();
        try {
            int resumeFrameIndex = summaries.getFirstFrameOf(scanStopBlock) - 1;
            while (resumeFrameIndex >= 0 && slaveTimeFrameHelper.openFrame(resumeFrameIndex) <= 0) {
                resumeFrameIndex--;
            }
            scanStopBlock = -1;
            nextSummaryBlock = -1;
            if (resumeFrameIndex < 0) {
                // no rows below the boundary
                rowIndex = Long.MIN_VALUE;
                return Long.MIN_VALUE;
            }
            frameIndex = resumeFrameIndex;
            rowIndex = slaveTimeFrameHelper.getTimeFrameRowHi() - 1;
        } finally {
            slaveTimeFrameHelper.restoreBookmark(savedFrameIndex, savedRowId);
        }
        return findPrevailingSlaveRowId(slaveTimeFrameHelper, slaveRecord, slaveSymbolIndex, slaveSymbolLookupMap, masterKey);
    }

    // Walks the summarised blocks below the scan, newest first, for the key's last row;
    // WindowJoinPrevailingSummaries.UNAVAILABLE when the summaries are off for the query.
    private long findInSummaries(
            WindowJoinTimeFrameHelper slaveTimeFrameHelper,
            Record slaveRecord,
            int slaveSymbolIndex,
            int masterKey,
            int masterCacheKey
    ) {
        final int savedFrameIndex = slaveTimeFrameHelper.getBookmarkedFrameIndex();
        final long savedRowId = slaveTimeFrameHelper.getBookmarkedRowIndex();
        try {
            for (int block = nextSummaryBlock; block >= 0; block--) {
                final long rowId = summaries.lastRowIdInBlock(block, masterKey, slaveTimeFrameHelper, slaveRecord, slaveSymbolIndex, circuitBreaker);
                if (rowId == WindowJoinPrevailingSummaries.UNAVAILABLE) {
                    return WindowJoinPrevailingSummaries.UNAVAILABLE;
                }
                if (rowId != Long.MIN_VALUE) {
                    cache.put(masterCacheKey, rowId);
                    return rowId;
                }
            }
        } finally {
            slaveTimeFrameHelper.restoreBookmark(savedFrameIndex, savedRowId);
        }
        // nowhere before the span: remember it, so the next row of this key does not walk again
        cache.put(masterCacheKey, Long.MIN_VALUE);
        return Long.MIN_VALUE;
    }
}
