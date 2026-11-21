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

import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.DirectIntIntPagedMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Translates probe-side SYMBOL join keys into the build table's symbol key domain, so that the
 * frozen build stores the build's own keys and the probe loop compares plain INT keys. The
 * translation is lazy by default: a probe key resolves the first time a worker meets it, and a
 * probe key the frame never reads costs nothing. A probe that reads many distinct keys against a
 * small build dictionary would then look up far more texts than the build holds, so in that case
 * the cache opens filled from the build side instead; see
 * {@link #of(StaticSymbolTable, StaticSymbolTable, boolean, long, boolean, int, MemoryTracker, SqlExecutionCircuitBreaker)}.
 * <p>
 * The cache is a dense, tracked native array indexed by probe symbol key and sized by the probe
 * dictionary. Every worker shares one cache, because an entry is a function of the two
 * dictionaries alone: two workers that fill the same entry write the same value, so the race is
 * benign and no synchronization is needed. A cache filled from the build holds no unresolved
 * entry, so the workers only read it. The owner opens the cache through {@link #of} before it
 * publishes the first probe task, so the publication that hands a worker its task also makes the
 * cache's contents visible to it. Any failure in {@link #of} releases the cache; {@link #close}
 * releases it and leaves the instance reusable.
 * <p>
 * Opening the shared cache costs work and memory that grow with the probe dictionary, whatever
 * the probe reads, so it opens only for a probe that reads enough rows per probe key: for the fused
 * GROUP BY, at least one row per 256 probe keys, and for the parallel hash join, whose serial plan
 * opens no such cache, at least one row per 16 probe keys when the cache fills from the build, and
 * one per probe key when it opens unresolved; see
 * {@link #of(StaticSymbolTable, StaticSymbolTable, boolean, long, boolean, int, MemoryTracker, SqlExecutionCircuitBreaker)}.
 * For a probe of fewer rows, the translator allocates nothing, and each {@link View} translates
 * through a lazy cache of its own instead, as the ordinary join's SymbolTranslatingRecord does:
 * it opens on the view's first miss, keeps each probe key it resolves, and stops allocating once
 * it holds {@code cairo.sql.join.symbol.translation.cache.capacity} entries. A view's cache is
 * the view's slot's alone, which one thread uses at a time, and charges the translator's tracker.
 * A caller that opened the translation for the first rows of a probe, which may read on, widens it
 * to the rows it then reads, see {@link #widen(long, SqlExecutionCircuitBreaker)}, which opens the
 * shared cache once those rows reach it.
 * <p>
 * The dictionaries are not shared. Each worker translates through its own {@link View}, which
 * holds that worker's pair of symbol tables and reads the shared cache, or its own.
 */
public final class SymbolKeyTranslator implements QuietCloseable {
    // Clearing checks the breaker once per MiB, as the build's buffers do.
    private static final long FILL_CHUNK_SIZE = 1024 * 1024;
    // Filling from the build checks the breaker once per 1,024 build keys.
    private static final int FILL_FROM_BUILD_CHECK_MASK = 1024 - 1;
    // Caps the serial work that filling from the build adds when a selective probe reads few
    // keys. A lookup in an eligible probe dictionary of a million texts took 0.1 to 0.5 us, so
    // under the default settings, 32,768 build keys against such a dictionary added 5 to 15 ms
    // to a selective query, 12 ms at the median.
    private static final int FILL_FROM_BUILD_MAX_KEY_COUNT = 32_768;
    // Fills from the build only when the probe dictionary holds at most two texts per unit of
    // its declared capacity, as auto-scaling keeps every column after a commit: by default it
    // raises the capacity of a column that holds more than 0.8 texts per unit to more than half
    // the column's text count. A table's symbol map hashes its texts into capacity / 2 buckets,
    // so a lookup then scans four texts or fewer on average. With auto-scaling disabled, a column
    // can grow far past its capacity, and a lookup scans every text of a long bucket: a million
    // texts at capacity 256 took about 120 us a lookup.
    private static final int FILL_FROM_BUILD_MAX_PROBE_LOAD = 2;
    // Fills from the build only when that takes at most an eighth of the lookups that resolving
    // every probe key would.
    private static final int FILL_FROM_BUILD_MIN_PROBE_RATIO = 8;
    // The parallel hash join's shared cache, once filled from the build, opens only for a probe that
    // reads at least one row per this many probe keys. Its serial plan opens no cache, and the
    // views' caches cost about 25 ns per distinct probe key on each of four workers, so a filled
    // cache, which costs about 1.6 ns per probe key when its pages are fresh, as glibc maps them for
    // a large array, and 0.15 ns when warm, as jemalloc reuses them, pays off from about one row per
    // 14 probe keys on fresh pages and per 128 on warm ones. Measured with
    // ParallelHashJoinBenchmark's SYMBOL_10M_PREFIX_D1K over 10M probe keys; at this bound the join
    // takes at most about 2.1x the serial plan's time under either allocator.
    private static final int JOIN_FILLED_CACHE_MAX_KEYS_PER_PROBE_ROW = 16;
    // The parallel hash join's shared cache, opened unresolved, makes the views' lookups itself, so it
    // spares the views only their repeats of a key across slots, and costs no less than their caches
    // until the probe reads about one row per two probe keys, under either allocator; it opens for at
    // least one row per probe key. Measured with ParallelHashJoinBenchmark's SYMBOL_10M_PREFIX_D50K.
    private static final int JOIN_UNRESOLVED_CACHE_MAX_KEYS_PER_PROBE_ROW = 1;
    // The fused GROUP BY's shared cache opens only for a probe that reads at least one row per this
    // many probe keys. Before the bound, it opened for every probe, so below it the views only spare
    // the cache's clear, and at it the views' lookups would cost about what the clear does on warm
    // pages.
    private static final int SHARED_CACHE_MAX_KEYS_PER_PROBE_ROW = 256;
    // Translated keys are nonnegative, VALUE_IS_NULL or VALUE_NOT_FOUND, never -1.
    private static final int UNRESOLVED = -1;
    // The dictionaries of a translation that left the views to their own caches, which widen() opens
    // the shared cache over; null otherwise.
    @Nullable
    private StaticSymbolTable buildKeyTable;
    private long cacheAddress;
    private int cacheKeyCount;
    private long cacheSize;
    private boolean isFilledFromBuild;
    // How the translation that left the views to their own caches was opened, which widen() opens
    // the shared cache by; false otherwise.
    private boolean isParallelHashJoin;
    private boolean isProbeDictionaryEligible;
    // True when the probe reads too few rows for the shared cache, so each view caches on its own.
    private boolean isSlotCached;
    @Nullable
    private MemoryTracker memoryTracker;
    @Nullable
    private StaticSymbolTable probeKeyTable;
    // The entries past which a view's own cache stops allocating.
    private int slotCacheCapacity;

    /**
     * Returns true when the cache may fill from the build through this probe dictionary, which
     * is the probe input's shared symbol table for the key column, not a view of it. Only a
     * table's own symbol map qualifies, and only while it holds at most two texts per unit of
     * its declared capacity:
     * <ul>
     *     <li>The fill writes a build key at the probe key that keyOf() returns for its text, so
     *     the dictionary must hold each text under one key. A symbol map does: its writer appends
     *     a text only after looking it up. A live view served from memory overlays its table's
     *     map with the texts it interned since, and that overlay can hold a text under two keys,
     *     so a probe row may carry the key the fill never writes.</li>
     *     <li>The fill makes up to 32,768 lookups on the owner before the first probe task, so a
     *     lookup must cost what the lazy path's lookups cost. A symbol map's keyOf() scans one
     *     hash bucket, and the bucket grows with the texts per unit of capacity. Auto-scaling
     *     keeps a column within the limit, so it binds only where auto-scaling is disabled or
     *     has not yet run.</li>
     * </ul>
     * Any other dictionary, null included, translates lazily, as before the fill existed.
     */
    public static boolean isEligibleProbeDictionary(@Nullable SymbolTable probeDictionary) {
        return probeDictionary instanceof SymbolMapReaderImpl symbolMap
                && symbolMap.getSymbolCount() <= (long) symbolMap.getSymbolCapacity() * FILL_FROM_BUILD_MAX_PROBE_LOAD;
    }

    @Override
    public void close() {
        if (cacheAddress != 0) {
            cacheAddress = Unsafe.free(cacheAddress, cacheSize, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
        }
        cacheSize = 0;
        cacheKeyCount = 0;
        isFilledFromBuild = false;
        isSlotCached = false;
        slotCacheCapacity = 0;
        memoryTracker = null;
        probeKeyTable = null;
        buildKeyTable = null;
        isProbeDictionaryEligible = false;
        isParallelHashJoin = false;
    }

    public long getSizeInBytes() {
        return cacheSize;
    }

    /**
     * True when the open cache was filled from the build dictionary; false when it opened
     * unresolved, and once it is closed.
     */
    @TestOnly
    public boolean isFilledFromBuild() {
        return isFilledFromBuild;
    }

    /**
     * True when the open translator left the probe to the views' own caches and opened no shared
     * cache; false once it is closed.
     */
    @TestOnly
    public boolean isSlotCached() {
        return isSlotCached;
    }

    /**
     * Allocates an unresolved cache for the probe dictionary's current key count. A probe key
     * beyond that count, which a concurrent writer may have added, resolves uncached.
     */
    public void of(int probeKeyCount, @Nullable MemoryTracker memoryTracker, SqlExecutionCircuitBreaker circuitBreaker) {
        close();
        this.memoryTracker = memoryTracker;
        try {
            if (probeKeyCount > 0) {
                final long size = (long) probeKeyCount * Integer.BYTES;
                cacheAddress = Unsafe.malloc(size, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
                cacheSize = size;
                cacheKeyCount = probeKeyCount;
                for (long offset = 0; offset < size; offset += FILL_CHUNK_SIZE) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                    Vect.memset(cacheAddress + offset, Math.min(size - offset, FILL_CHUNK_SIZE), 0xFF);
                }
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Opens the translation for a probe of {@code probeRowCount} rows over the probe dictionary's
     * current key count, which the owner's probe table reports.
     * <p>
     * The shared cache opens filled from the build when the caller found the probe dictionary
     * eligible, see {@link #isEligibleProbeDictionary(SymbolTable)}, the build dictionary holds at
     * most 32,768 keys and the probe dictionary at least eight times as many: every entry reads
     * VALUE_NOT_FOUND, then each build key looks its text up in the probe dictionary and writes
     * itself at the probe key it finds. An eligible probe dictionary holds each text once, and
     * keyOf() and valueOf() match texts exactly, so each entry then holds the build key that
     * resolving its probe key lazily would have returned. Otherwise it opens unresolved, as
     * {@link #of(int, MemoryTracker, SqlExecutionCircuitBreaker)} opens it.
     * <p>
     * It opens only for a probe of at least one row per so many probe keys: for the parallel hash
     * join 16 when it would fill from the build and 1 when it would open unresolved, for the fused
     * GROUP BY 256. A probe of fewer rows leaves the translation to the views' own caches, each of
     * which stops allocating at {@code slotCacheCapacity} entries: the translator then allocates
     * nothing, looks no text up and checks no breaker.
     *
     * @param probeRowCount      the most probe rows whose keys the execution may translate first, or
     *                           {@code Long.MAX_VALUE} when the caller cannot tell
     * @param isParallelHashJoin true for the parallel hash join, whose serial plan opens no cache,
     *                           false for the fused GROUP BY, which opened the shared cache for any
     *                           probe before the views' caches existed
     * @param slotCacheCapacity  the entries past which a view's own cache stops allocating
     */
    public void of(
            StaticSymbolTable probeKeyTable,
            StaticSymbolTable buildKeyTable,
            boolean isProbeDictionaryEligible,
            long probeRowCount,
            boolean isParallelHashJoin,
            int slotCacheCapacity,
            @Nullable MemoryTracker memoryTracker,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final int probeKeyCount = probeKeyTable.getSymbolCount();
        final int buildKeyCount = buildKeyTable.getSymbolCount();
        final boolean isFilled = isProbeDictionaryEligible && isFillableFromBuild(probeKeyCount, buildKeyCount);
        final int maxKeysPerProbeRow = !isParallelHashJoin
                ? SHARED_CACHE_MAX_KEYS_PER_PROBE_ROW
                : isFilled ? JOIN_FILLED_CACHE_MAX_KEYS_PER_PROBE_ROW : JOIN_UNRESOLVED_CACHE_MAX_KEYS_PER_PROBE_ROW;
        if (!isSharedCacheInProbeReach(probeKeyCount, probeRowCount, maxKeysPerProbeRow)) {
            close();
            this.memoryTracker = memoryTracker;
            this.slotCacheCapacity = slotCacheCapacity;
            this.probeKeyTable = probeKeyTable;
            this.buildKeyTable = buildKeyTable;
            this.isProbeDictionaryEligible = isProbeDictionaryEligible;
            this.isParallelHashJoin = isParallelHashJoin;
            isSlotCached = true;
            return;
        }
        if (!isFilled) {
            of(probeKeyCount, memoryTracker, circuitBreaker);
            return;
        }
        close();
        this.memoryTracker = memoryTracker;
        try {
            if (probeKeyCount > 0) {
                final long size = (long) probeKeyCount * Integer.BYTES;
                cacheAddress = Unsafe.malloc(size, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
                cacheSize = size;
                cacheKeyCount = probeKeyCount;
                final long chunkKeyCount = FILL_CHUNK_SIZE / Integer.BYTES;
                for (long key = 0; key < probeKeyCount; key += chunkKeyCount) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                    Vect.setMemoryInt(cacheAddress + key * Integer.BYTES, SymbolTable.VALUE_NOT_FOUND,
                            Math.min(probeKeyCount - key, chunkKeyCount));
                }
                for (int buildKey = 0; buildKey < buildKeyCount; buildKey++) {
                    if ((buildKey & FILL_FROM_BUILD_CHECK_MASK) == 0) {
                        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                    }
                    final CharSequence value = buildKeyTable.valueOf(buildKey);
                    // A key without a text is no probe text's build key, so it resolves nothing.
                    if (value == null) {
                        continue;
                    }
                    // A miss, and any key outside the cache, leaves VALUE_NOT_FOUND in place.
                    final int probeKey = probeKeyTable.keyOf(value);
                    if (probeKey > -1 && probeKey < probeKeyCount) {
                        final long address = cacheAddress + (long) probeKey * Integer.BYTES;
                        // An entry fills once, as a dictionary holds a text once. Should a build
                        // dictionary hold one twice, the entry takes the key that keyOf() picks, as
                        // resolving the probe key lazily does.
                        Unsafe.putInt(address, Unsafe.getInt(address) == SymbolTable.VALUE_NOT_FOUND
                                ? buildKey
                                : buildKeyTable.keyOf(value));
                    }
                }
            }
            isFilledFromBuild = true;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Reopens a translation that left the views to their own caches for a probe of more rows,
     * {@code probeRowCount}, over the same dictionaries, by the rule of
     * {@link #of(StaticSymbolTable, StaticSymbolTable, boolean, long, boolean, int, MemoryTracker, SqlExecutionCircuitBreaker)}:
     * the shared cache opens, filled from the build or unresolved, once the rows reach it, and the
     * translation stays as it is otherwise. Returns true when the shared cache opened, after which
     * the caller points every view at it, see {@link View#rebind(SymbolKeyTranslator)}, and false
     * when the translation already shares a cache, or still leaves the views to their own. The
     * caller widens only while no view translates. On failure the translator closes, as {@code of()}
     * does, and the views keep translating through their own caches.
     */
    public boolean widen(long probeRowCount, SqlExecutionCircuitBreaker circuitBreaker) {
        if (!isSlotCached) {
            return false;
        }
        assert probeKeyTable != null && buildKeyTable != null;
        // of() closes the translator first, which clears what it reopens from.
        of(
                probeKeyTable,
                buildKeyTable,
                isProbeDictionaryEligible,
                probeRowCount,
                isParallelHashJoin,
                slotCacheCapacity,
                memoryTracker,
                circuitBreaker
        );
        return !isSlotCached;
    }

    private static boolean isFillableFromBuild(int probeKeyCount, int buildKeyCount) {
        return buildKeyCount <= FILL_FROM_BUILD_MAX_KEY_COUNT
                && (long) buildKeyCount * FILL_FROM_BUILD_MIN_PROBE_RATIO <= probeKeyCount;
    }

    // True when the probe reads at least ceil(probeKeyCount / maxKeysPerProbeRow) rows, which a
    // dictionary without keys always does. The key count is an int, so the long sum cannot overflow.
    private static boolean isSharedCacheInProbeReach(int probeKeyCount, long probeRowCount, int maxKeysPerProbeRow) {
        return probeRowCount >= ((long) probeKeyCount + maxKeysPerProbeRow - 1) / maxKeysPerProbeRow;
    }

    /**
     * One worker's translation path: its own two symbol tables over the shared cache, or over a
     * cache of its own when the translator opened none. A view is valid while the translator stays
     * open and both tables stay open, which is one execution; the object itself survives
     * {@link #close()} and rebinds for the next execution.
     */
    public static final class View implements QuietCloseable {
        // The view's own cache starts as the ordinary join's SymbolTranslatingRecord's do: a page
        // table of 32 slots and a hash map of 16 entries, 256 bytes each.
        private static final int SLOT_CACHE_INITIAL_HASH_CAPACITY = 16;
        private static final int SLOT_CACHE_INITIAL_PAGE_TABLE_CAPACITY = 32;
        private static final double SLOT_CACHE_LOAD_FACTOR = 0.5;
        // Closed, and holding no memory, until the first miss of a view without a shared cache. A
        // probe key is never VALUE_IS_NULL there, which translate() answers first.
        private final DirectIntIntPagedMap slotCache = new DirectIntIntPagedMap(
                SLOT_CACHE_INITIAL_PAGE_TABLE_CAPACITY,
                SLOT_CACHE_INITIAL_HASH_CAPACITY,
                SLOT_CACHE_LOAD_FACTOR,
                SymbolTable.VALUE_IS_NULL,
                UNRESOLVED,
                MemoryTag.NATIVE_JOIN_MAP
        );
        private StaticSymbolTable buildKeyTable;
        private long cacheAddress;
        private int cacheKeyCount;
        private boolean isSlotCached;
        private SymbolTable probeKeyTable;
        private int slotCacheCapacity;

        /**
         * Both tables are borrowed views of this execution's sources; the caller keeps them alive.
         * The view's own cache starts empty, under the translator's tracker: a key that a previous
         * binding cached may stand for another text in these tables.
         */
        public void of(SymbolKeyTranslator translator, SymbolTable probeKeyTable, StaticSymbolTable buildKeyTable) {
            this.cacheAddress = translator.cacheAddress;
            this.cacheKeyCount = translator.cacheKeyCount;
            this.isSlotCached = translator.isSlotCached;
            this.slotCacheCapacity = translator.slotCacheCapacity;
            this.probeKeyTable = probeKeyTable;
            this.buildKeyTable = buildKeyTable;
            slotCache.close();
            slotCache.setMemoryTracker(translator.memoryTracker);
        }

        /**
         * Points the view at the shared cache that the translator's
         * {@link SymbolKeyTranslator#widen(long, SqlExecutionCircuitBreaker)} opened, over the view's
         * own tables, and releases the view's own cache, which the shared one supersedes.
         */
        public void rebind(SymbolKeyTranslator translator) {
            assert !translator.isSlotCached;
            of(translator, probeKeyTable, buildKeyTable);
        }

        @Override
        public void close() {
            slotCache.close();
            cacheAddress = 0;
            cacheKeyCount = 0;
            isSlotCached = false;
            probeKeyTable = null;
            buildKeyTable = null;
        }

        /**
         * Returns the build symbol key for a probe symbol key: VALUE_IS_NULL for null, and
         * VALUE_NOT_FOUND when the build dictionary lacks the text, so no build row can match.
         */
        public int translate(int probeKey) {
            if (probeKey == SymbolTable.VALUE_IS_NULL) {
                return SymbolTable.VALUE_IS_NULL;
            }
            if (probeKey < 0 || probeKey >= cacheKeyCount) {
                return translateOutsideCache(probeKey);
            }
            final long address = cacheAddress + (long) probeKey * Integer.BYTES;
            int buildKey = Unsafe.getInt(address);
            if (buildKey == UNRESOLVED) {
                buildKey = buildKeyTable.keyOf(probeKeyTable.valueOf(probeKey));
                Unsafe.putInt(address, buildKey);
            }
            return buildKey;
        }

        // A key outside the shared cache: any key of a view without one, which goes through the view's
        // own cache as SymbolTranslatingRecord's keys do, and, past the cap, stays only where its page
        // exists; or a key past the dictionary that the shared cache covers, which a concurrent writer
        // added, or a negative one, which resolves without caching.
        private int translateOutsideCache(int probeKey) {
            if (isSlotCached && probeKey > -1) {
                final int cachedBuildKey = slotCache.get(probeKey);
                if (cachedBuildKey != UNRESOLVED) {
                    return cachedBuildKey;
                }
                final int buildKey = buildKeyTable.keyOf(probeKeyTable.valueOf(probeKey));
                if (slotCache.size() < slotCacheCapacity) {
                    slotCache.reopen();
                    slotCache.put(probeKey, buildKey);
                } else {
                    slotCache.putIfPageExists(probeKey, buildKey);
                }
                return buildKey;
            }
            return buildKeyTable.keyOf(probeKeyTable.valueOf(probeKey));
        }
    }
}
