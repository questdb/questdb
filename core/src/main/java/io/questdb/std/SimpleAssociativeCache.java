/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.std;

import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import io.questdb.metrics.NullCounter;
import io.questdb.metrics.NullLongGauge;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Thread-unsafe cache implementation.
 */
public class SimpleAssociativeCache<V> implements AssociativeCache<V> {
    private static final int MIN_BLOCKS = 1;
    private static final int MIN_ROWS = 1;
    private static final int NOT_FOUND = -1;
    private final int blockShift;
    private final int blocks;
    private final LongGauge cachedGauge;
    private final Counter hitCounter;
    private final String[] keys;
    private final Counter missCounter;
    private final int rowMask;
    private final int rows;
    private final V[] values;

    public SimpleAssociativeCache(int blocks, int rows) {
        this(blocks, rows, NullLongGauge.INSTANCE, NullCounter.INSTANCE, NullCounter.INSTANCE);
    }

    public SimpleAssociativeCache(int blocks, int rows, LongGauge cachedGauge) {
        this(blocks, rows, cachedGauge, NullCounter.INSTANCE, NullCounter.INSTANCE);
    }

    @SuppressWarnings("unchecked")
    public SimpleAssociativeCache(int blocks, int rows, LongGauge cachedGauge, Counter hitCounter, Counter missCounter) {
        this.blocks = Math.max(MIN_BLOCKS, Numbers.ceilPow2(blocks));
        this.rows = Math.max(MIN_ROWS, Numbers.ceilPow2(rows));

        int capacity = this.rows * this.blocks;
        if (capacity < 0) {
            throw new OutOfMemoryError();
        }
        this.keys = new String[capacity];
        this.values = (V[]) new Object[capacity];
        this.rowMask = this.rows - 1;
        this.blockShift = Numbers.msb(this.blocks);
        this.cachedGauge = cachedGauge;
        this.hitCounter = hitCounter;
        this.missCounter = missCounter;
    }

    @Override
    public int capacity() {
        return rows * blocks;
    }

    @Override
    public void clear() {
        long freed = 0;
        for (int i = 0, n = keys.length; i < n; i++) {
            if (keys[i] != null) {
                keys[i] = null;
                if (values[i] != null) {
                    values[i] = Misc.freeIfCloseable(values[i]);
                    freed++;
                }
            }
        }
        cachedGauge.add(-freed);
    }

    @Override
    public void close() {
        clear();
    }

    public int keyIndex(CharSequence key) {
        int lo = lo(key);
        for (int i = lo, hi = lo + blocks; i < hi; i++) {
            CharSequence k = keys[i];
            if (k == null) {
                return NOT_FOUND;
            }

            if (Chars.equals(k, key)) {
                return i;
            }
        }
        return NOT_FOUND;
    }

    public V peek(@NotNull CharSequence key) {
        return peek(keyIndex(key));
    }

    public V peek(int keyIndex) {
        if (keyIndex != NOT_FOUND) {
            return values[keyIndex];
        }
        return null;
    }

    @Override
    public V poll(@NotNull CharSequence key) {
        return poll(keyIndex(key));
    }

    public @Nullable V poll(int keyIndex) {
        if (keyIndex == NOT_FOUND) {
            missCounter.inc();
            return null;
        }
        V value = values[keyIndex];
        values[keyIndex] = null;
        if (value != null) {
            // The value is present, so we're decrementing the gauge.
            cachedGauge.dec();
            hitCounter.inc();
        } else {
            missCounter.inc();
        }
        // We do not null the key reference to avoid creating another immutable key.
        return value;
    }

    @Override
    public void put(@NotNull CharSequence key, @Nullable V value) {
        final int lo = lo(key);
        V outgoingValue;

        if (Chars.equalsNc(key, keys[lo])) {
            // Present entry case.
            if (values[lo] == value) {
                return;
            }
            outgoingValue = values[lo];
        } else {
            // New entry case.
            outgoingValue = values[lo + blocks - 1];

            System.arraycopy(keys, lo, keys, lo + 1, blocks - 1);
            System.arraycopy(values, lo, values, lo + 1, blocks - 1);
            keys[lo] = Chars.toString(key);
        }
        values[lo] = value;

        if (outgoingValue == null) {
            // We're inserting.
            cachedGauge.inc();
        } else {
            // We're replacing the value with another one, no need to change the gauge.
            Misc.freeIfCloseable(outgoingValue);
        }
    }

    private int lo(CharSequence key) {
        return (Hash.spread(Chars.hashCode(key)) & rowMask) << blockShift;
    }
}
