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
 * {@link #of(StaticSymbolTable, StaticSymbolTable, boolean, MemoryTracker, SqlExecutionCircuitBreaker)}.
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
 * The dictionaries are not shared. Each worker translates through its own {@link View}, which
 * holds that worker's pair of symbol tables and reads the shared cache.
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
    // Translated keys are nonnegative, VALUE_IS_NULL or VALUE_NOT_FOUND, never -1.
    private static final int UNRESOLVED = -1;
    private long cacheAddress;
    private int cacheKeyCount;
    private long cacheSize;
    private boolean isFilledFromBuild;
    @Nullable
    private MemoryTracker memoryTracker;

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
        memoryTracker = null;
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
     * Allocates the cache for the probe dictionary's current key count, which the owner's probe
     * table reports. When the caller found the probe dictionary eligible, see
     * {@link #isEligibleProbeDictionary(SymbolTable)}, the build dictionary holds at most 32,768
     * keys and the probe dictionary at least eight times as many, the cache opens filled from the
     * build: every entry reads VALUE_NOT_FOUND, then each build key looks its text up in the probe
     * dictionary and writes itself at the probe key it finds. An eligible probe dictionary holds
     * each text once, and keyOf() and valueOf() match texts exactly, so each entry then holds the
     * build key that resolving its probe key lazily would have returned. Otherwise the cache opens
     * unresolved, as {@link #of(int, MemoryTracker, SqlExecutionCircuitBreaker)} opens it.
     */
    public void of(
            StaticSymbolTable probeKeyTable,
            StaticSymbolTable buildKeyTable,
            boolean isProbeDictionaryEligible,
            @Nullable MemoryTracker memoryTracker,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final int probeKeyCount = probeKeyTable.getSymbolCount();
        final int buildKeyCount = buildKeyTable.getSymbolCount();
        if (!isProbeDictionaryEligible || !isFillableFromBuild(probeKeyCount, buildKeyCount)) {
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

    private static boolean isFillableFromBuild(int probeKeyCount, int buildKeyCount) {
        return buildKeyCount <= FILL_FROM_BUILD_MAX_KEY_COUNT
                && (long) buildKeyCount * FILL_FROM_BUILD_MIN_PROBE_RATIO <= probeKeyCount;
    }

    /**
     * One worker's translation path: its own two symbol tables over the shared cache. A view is
     * valid while the cache stays open and both tables stay open, which is one execution; the
     * object itself survives {@link #close()} and rebinds for the next execution.
     */
    public static final class View implements QuietCloseable {
        private StaticSymbolTable buildKeyTable;
        private long cacheAddress;
        private int cacheKeyCount;
        private SymbolTable probeKeyTable;

        /**
         * Both tables are borrowed views of this execution's sources; the caller keeps them alive.
         */
        public void of(SymbolKeyTranslator translator, SymbolTable probeKeyTable, StaticSymbolTable buildKeyTable) {
            this.cacheAddress = translator.cacheAddress;
            this.cacheKeyCount = translator.cacheKeyCount;
            this.probeKeyTable = probeKeyTable;
            this.buildKeyTable = buildKeyTable;
        }

        @Override
        public void close() {
            cacheAddress = 0;
            cacheKeyCount = 0;
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
                // The probe dictionary grew past the cache. Resolve the key like
                // SymbolTranslatingRecord does for the ordinary join, without caching.
                return buildKeyTable.keyOf(probeKeyTable.valueOf(probeKey));
            }
            final long address = cacheAddress + (long) probeKey * Integer.BYTES;
            int buildKey = Unsafe.getInt(address);
            if (buildKey == UNRESOLVED) {
                buildKey = buildKeyTable.keyOf(probeKeyTable.valueOf(probeKey));
                Unsafe.putInt(address, buildKey);
            }
            return buildKey;
        }
    }
}
