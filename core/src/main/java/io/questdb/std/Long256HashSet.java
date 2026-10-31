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


package io.questdb.std;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * Open addressing linear probing hash set of LONG256 values, laid out like {@link LongLongHashSet}: each slot
 * owns four adjacent longs of a single long array. The LONG256 NULL marks an empty slot, so the set records
 * a NULL member in a flag of its own.
 * <p>
 * This class is not thread safe.
 */
public final class Long256HashSet implements Mutable, Sinkable {
    private static final int MIN_INITIAL_CAPACITY = 16;
    private final double loadFactor;
    private int capacity;
    private boolean hasNull;
    private int mask;
    private int size;
    private long[] values;

    public Long256HashSet(int initialCapacity, double loadFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < load factor < 1");
        }
        this.loadFactor = loadFactor;
        this.capacity = Math.max(initialCapacity, MIN_INITIAL_CAPACITY);
        final int slots = Numbers.ceilPow2((int) (capacity / loadFactor));
        this.values = new long[4 * slots];
        this.mask = slots - 1;
        Arrays.fill(values, Numbers.LONG_NULL);
    }

    public void add(Long256 value) {
        if (Long256Impl.isNull(value)) {
            hasNull = true;
            return;
        }
        final long l0 = value.getLong0();
        final long l1 = value.getLong1();
        final long l2 = value.getLong2();
        final long l3 = value.getLong3();
        final int slot = keyIndex(l0, l1, l2, l3);
        if (slot >= 0) {
            set(values, slot, l0, l1, l2, l3);
            if (++size == capacity) {
                rehash();
            }
        }
    }

    @Override
    public void clear() {
        Arrays.fill(values, Numbers.LONG_NULL);
        size = 0;
        hasNull = false;
    }

    public boolean contains(Long256 value) {
        if (Long256Impl.isNull(value)) {
            return hasNull;
        }
        return keyIndex(value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3()) < 0;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii('[');
        boolean pastFirst = false;
        for (int i = 0, n = values.length; i < n; i += 4) {
            if (!Long256Impl.isNull(values[i], values[i + 1], values[i + 2], values[i + 3])) {
                if (pastFirst) {
                    sink.putAscii(',');
                }
                Numbers.appendLong256(values[i], values[i + 1], values[i + 2], values[i + 3], sink);
                pastFirst = true;
            }
        }
        sink.putAscii(']');
    }

    private static void set(long[] values, int slot, long l0, long l1, long l2, long l3) {
        final int i = slot * 4;
        values[i] = l0;
        values[i + 1] = l1;
        values[i + 2] = l2;
        values[i + 3] = l3;
    }

    private int keyIndex(long l0, long l1, long l2, long l3) {
        int slot = (int) Hash.hashLong256_64(l0, l1, l2, l3) & mask;
        do {
            final int i = slot * 4;
            if (values[i] == l0 && values[i + 1] == l1 && values[i + 2] == l2 && values[i + 3] == l3) {
                return -slot - 1;
            }
            if (Long256Impl.isNull(values[i], values[i + 1], values[i + 2], values[i + 3])) {
                return slot;
            }
            slot = (slot + 1) & mask;
        } while (true);
    }

    private void rehash() {
        final int newCapacity = capacity * 2;
        final int slots = Numbers.ceilPow2((int) (newCapacity / loadFactor));
        if (slots < 0 || slots * 4 < 0) {
            throw new IllegalStateException("cannot rehash, required capacity is too large. [current-capacity=" + capacity + ", load-factor=" + loadFactor + "]");
        }
        final long[] oldValues = values;
        values = new long[4 * slots];
        Arrays.fill(values, Numbers.LONG_NULL);
        mask = slots - 1;
        for (int i = 0, n = oldValues.length; i < n; i += 4) {
            if (!Long256Impl.isNull(oldValues[i], oldValues[i + 1], oldValues[i + 2], oldValues[i + 3])) {
                set(values, keyIndex(oldValues[i], oldValues[i + 1], oldValues[i + 2], oldValues[i + 3]), oldValues[i], oldValues[i + 1], oldValues[i + 2], oldValues[i + 3]);
            }
        }
        capacity = newCapacity;
    }
}
