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
 * Translates build-side SYMBOL join keys into the probe table's symbol key domain,
 * so that the frozen build and the probe loop compare plain INT keys. Each distinct
 * build key resolves its text through the probe dictionary once per execution. The
 * owner thread uses one instance at a time; both symbol tables are borrowed views.
 * <p>
 * The cache is a dense, tracked native array indexed by build symbol key and sized by
 * the build dictionary. {@link #of} allocates it and any failure there releases it.
 * {@link #close} releases it and drops the borrowed tables; the instance is reusable.
 */
public final class SymbolKeyTranslator implements QuietCloseable {
    // Clearing checks the breaker once per MiB, as the build's buffers do.
    private static final long FILL_CHUNK_SIZE = 1024 * 1024;
    // Translated keys are nonnegative, VALUE_IS_NULL or VALUE_NOT_FOUND, never -1.
    private static final int UNRESOLVED = -1;
    private StaticSymbolTable buildKeyTable;
    private long cacheAddress;
    private int cacheKeyCount;
    private long cacheSize;
    @Nullable
    private MemoryTracker memoryTracker;
    private StaticSymbolTable probeKeyTable;

    @Override
    public void close() {
        if (cacheAddress != 0) {
            cacheAddress = Unsafe.free(cacheAddress, cacheSize, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
        }
        cacheSize = 0;
        cacheKeyCount = 0;
        memoryTracker = null;
        probeKeyTable = null;
        buildKeyTable = null;
    }

    public long getSizeInBytes() {
        return cacheSize;
    }

    /** Binds both dictionaries and allocates an unresolved cache for the build dictionary. */
    public void of(
            StaticSymbolTable probeKeyTable,
            StaticSymbolTable buildKeyTable,
            @Nullable MemoryTracker memoryTracker,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        close();
        this.probeKeyTable = probeKeyTable;
        this.buildKeyTable = buildKeyTable;
        this.memoryTracker = memoryTracker;
        try {
            final int keyCount = buildKeyTable.getSymbolCount();
            if (keyCount > 0) {
                final long size = (long) keyCount * Integer.BYTES;
                cacheAddress = Unsafe.malloc(size, MemoryTag.NATIVE_JOIN_MAP, memoryTracker);
                cacheSize = size;
                cacheKeyCount = keyCount;
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
     * Returns the probe symbol key for a build symbol key: VALUE_IS_NULL for null, and
     * VALUE_NOT_FOUND when the probe dictionary lacks the text, so no probe row can match.
     */
    public int translate(int buildKey) {
        if (buildKey == SymbolTable.VALUE_IS_NULL) {
            return SymbolTable.VALUE_IS_NULL;
        }
        if (buildKey < 0 || buildKey >= cacheKeyCount) {
            // The build cursor's dictionary does not hold this key. Resolve it like
            // SymbolTranslatingRecord does for the ordinary join, without caching.
            return probeKeyTable.keyOf(buildKeyTable.valueOf(buildKey));
        }
        final long address = cacheAddress + (long) buildKey * Integer.BYTES;
        int probeKey = Unsafe.getInt(address);
        if (probeKey == UNRESOLVED) {
            probeKey = probeKeyTable.keyOf(buildKeyTable.valueOf(buildKey));
            Unsafe.putInt(address, probeKey);
        }
        return probeKey;
    }
}
