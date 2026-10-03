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

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;

/**
 * Translates probe-side SYMBOL join keys into the build table's symbol key domain, so that the
 * frozen build stores the build's own keys and the probe loop compares plain INT keys. The
 * translation is lazy: a probe key resolves the first time a worker meets it, and a probe key
 * the frame never reads costs nothing.
 * <p>
 * The cache is a dense, tracked native array indexed by probe symbol key and sized by the probe
 * dictionary. Every worker shares one cache, because an entry is a function of the two
 * dictionaries alone: two workers that fill the same entry write the same value, so the race is
 * benign and no synchronization is needed. The owner allocates the cache through {@link #of} and
 * any failure there releases it; {@link #close} releases it and leaves the instance reusable.
 * <p>
 * The dictionaries are not shared. Each worker translates through its own {@link View}, which
 * holds that worker's pair of symbol tables and reads the shared cache.
 */
public final class SymbolKeyTranslator implements QuietCloseable {
    // Clearing checks the breaker once per MiB, as the build's buffers do.
    private static final long FILL_CHUNK_SIZE = 1024 * 1024;
    // Translated keys are nonnegative, VALUE_IS_NULL or VALUE_NOT_FOUND, never -1.
    private static final int UNRESOLVED = -1;
    private long cacheAddress;
    private int cacheKeyCount;
    private long cacheSize;
    @Nullable
    private MemoryTracker memoryTracker;

    @Override
    public void close() {
        if (cacheAddress != 0) {
            cacheAddress = Unsafe.free(cacheAddress, cacheSize, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
        }
        cacheSize = 0;
        cacheKeyCount = 0;
        memoryTracker = null;
    }

    public long getSizeInBytes() {
        return cacheSize;
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
     * One worker's translation path: its own two symbol tables over the shared cache. A view is
     * valid while the cache stays open and both tables stay open, which is one execution; the
     * object itself survives {@link #close()} and rebinds for the next execution.
     */
    public static final class View implements QuietCloseable {
        private StaticSymbolTable buildKeyTable;
        private long cacheAddress;
        private int cacheKeyCount;
        private SymbolTable probeKeyTable;

        /** Both tables are borrowed views of this execution's sources; the caller keeps them alive. */
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
