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

package io.questdb.cairo;

import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.DirectIntCharSequenceHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.NativeMemoryBudget;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * Shares symbol caches between readers of the same dictionary version until the last reader releases them.
 */
public class SymbolMapCachePool implements QuietCloseable {
    private final NativeMemoryBudget budget;
    private final BiFunction<CharSequence, Entry, Entry> getOrCreate;
    private final ConcurrentHashMap<Entry> pool = new ConcurrentHashMap<>();
    private volatile boolean isClosed;

    public SymbolMapCachePool(NativeMemoryBudget budget) {
        this.budget = budget;
        getOrCreate = (key, value) -> value != null && value.incrementRefCount() ? value : new Entry(Chars.toString(key));
    }

    public @Nullable Entry acquire(Utf8Sequence tableDir, CharSequence columnName, long symbolTableNameTxn, long truncateVersion) {
        if (isClosed) {
            return null;
        }
        // Length prefixes keep names containing separators unambiguous.
        StringSink key = new StringSink();
        key.put(tableDir.size()).put(':').put(tableDir);
        key.put(columnName.length()).put(':').put(columnName).put(':').put(symbolTableNameTxn).put(':').put(truncateVersion);
        Entry entry = pool.compute(key, getOrCreate);
        // Close may have cleared the registry before this compute published its entry.
        if (isClosed) {
            pool.remove(key, entry);
            entry.close();
            return null;
        }
        return entry;
    }

    public void clear() {
        // Referenced entries remain owned by their readers, even after engine shutdown.
        pool.clear();
    }

    @Override
    public void close() {
        isClosed = true;
        clear();
    }

    @TestOnly
    public NativeMemoryBudget getBudget() {
        return budget;
    }

    @TestOnly
    public int getEntryCount() {
        return pool.size();
    }

    public class Entry implements QuietCloseable {
        private final String key;
        private final SymbolMapLowSlice lowSlice = new SymbolMapLowSlice();
        private final DirectIntCharSequenceHashMap map = new DirectIntCharSequenceHashMap(4, 0.7, MemoryTag.NATIVE_SYMBOL_CACHE, budget);
        private final AtomicInteger refCount = new AtomicInteger(1);
        private boolean hasLoggedLowOom;

        private Entry(String key) {
            this.key = key;
        }

        @Override
        public void close() {
            int count = refCount.decrementAndGet();
            assert count >= 0;
            if (count == 0 && refCount.compareAndSet(0, -1)) {
                map.close();
                lowSlice.close();
                pool.remove(key, this);
            }
        }

        public DirectIntCharSequenceHashMap getMap() {
            return map;
        }

        void ensureLow(SymbolMapReaderImpl reader, MemoryR offsetMem, MemoryR charMem, int want) {
            synchronized (lowSlice) {
                boolean isReady = lowSlice.ensure(offsetMem, charMem, want);
                // Bind under the lock so the count cannot include an unpublished append.
                lowSlice.bind(reader, want);
                if (!isReady && !hasLoggedLowOom) {
                    hasLoggedLowOom = true;
                    reader.logLowSliceOom();
                }
            }
        }

        NativeMemoryBudget getBudget() {
            return budget;
        }

        @TestOnly
        long getLowAllocatedBytes() {
            synchronized (lowSlice) {
                return lowSlice.allocatedBytes();
            }
        }

        private boolean incrementRefCount() {
            int count;
            do {
                count = refCount.get();
                if (count < 0) {
                    return false;
                }
            } while (!refCount.compareAndSet(count, count + 1));
            return true;
        }
    }
}
