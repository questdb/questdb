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
 * Thread-safe cache implementation.
 * <p>
 * The cache uses striped locking, i.e. each row is synchronized with its own lock.
 */
public class ConcurrentAssociativeCache<V> implements AssociativeCache<V> {
    private static final int MIN_BLOCKS = 1;
    private static final int MIN_ROWS = 1;
    private final int blocks;
    private final LongGauge cachedGauge;
    private final Counter hitCounter;
    // Each row has its own array of keys.
    // Arrays are also used for locking.
    private final ObjList<CharSequence[]> keys;
    private final Counter missCounter;
    private final int rowMask;
    private final int rows;
    // Each row has its own array of values.
    private final ObjList<V[]> values;

    public ConcurrentAssociativeCache(int blocks, int rows) {
        this(blocks, rows, NullLongGauge.INSTANCE, NullCounter.INSTANCE, NullCounter.INSTANCE);
    }

    @SuppressWarnings("unchecked")
    public ConcurrentAssociativeCache(int blocks, int rows, LongGauge cachedGauge, Counter hitCounter, Counter missCounter) {
        this.blocks = Math.max(MIN_BLOCKS, Numbers.ceilPow2(blocks));
        this.rows = Math.max(MIN_ROWS, Numbers.ceilPow2(rows));

        int capacity = this.rows * this.blocks;
        if (capacity < 0) {
            throw new OutOfMemoryError();
        }
        this.keys = new ObjList<>(this.rows);
        this.values = new ObjList<>(this.rows);
        for (int i = 0; i < this.rows; i++) {
            keys.add(new CharSequence[this.blocks]);
            values.add((V[]) new Object[this.blocks]);
        }
        this.rowMask = this.rows - 1;
        this.cachedGauge = cachedGauge;
        this.hitCounter = hitCounter;
        this.missCounter = missCounter;
    }

    @Override
    public int capacity() {
        return rows * blocks;
    }

    @Override
    public void close() {
        long freed = 0;
        for (int i = 0; i < rows; i++) {
            final CharSequence[] rowKeys = keys.getQuick(i);
            final V[] rowValues = values.getQuick(i);
            synchronized (rowKeys) {
                for (int j = 0; j < blocks; j++) {
                    if (rowKeys[j] != null) {
                        rowKeys[j] = null;
                        if (rowValues[j] != null) {
                            rowValues[j] = Misc.freeIfCloseable(rowValues[j]);
                            freed++;
                        }
                    }
                }
            }
        }
        cachedGauge.add(-freed);
    }

    @Override
    public V poll(@NotNull CharSequence key) {
        final int row = row(key);
        final CharSequence[] rowKeys = keys.getQuick(row);
        final V[] rowValues = values.getQuick(row);
        V value = null;

        synchronized (rowKeys) {
            for (int i = 0; i < blocks; i++) {
                if (rowKeys[i] == null) {
                    break;
                }
                if (Chars.equals(key, rowKeys[i])) {
                    value = rowValues[i];
                    rowValues[i] = null;
                    break;
                }
            }
        }

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
        final int row = row(key);
        final CharSequence[] rowKeys = keys.getQuick(row);
        final V[] rowValues = values.getQuick(row);
        CharSequence outgoingKey = null;
        V outgoingValue;

        synchronized (rowKeys) {
            if (Chars.equalsNc(key, rowKeys[0])) {
                // Present entry case.
                outgoingValue = rowValues[0];
            } else {
                // New entry case.
                outgoingKey = rowKeys[blocks - 1];
                outgoingValue = rowValues[blocks - 1];
                rowValues[blocks - 1] = null;

                System.arraycopy(rowKeys, 0, rowKeys, 1, blocks - 1);
                System.arraycopy(rowValues, 0, rowValues, 1, blocks - 1);
                rowKeys[0] = value != null ? Chars.toString(key) : null;
            }
            rowValues[0] = value;
        }

        if (outgoingKey != null) {
            if (outgoingValue == null) {
                // The value for the outgoing key was previously cleared by poll(), so we're inserting.
                cachedGauge.inc();
            } else {
                // We're replacing the value with another one, no need to change the gauge.
                Misc.freeIfCloseable(outgoingValue);
            }
        } else {
            // The block has empty entries, so we're inserting.
            cachedGauge.inc();
        }
    }

    private int row(CharSequence key) {
        return Hash.spread(Chars.hashCode(key)) & rowMask;
    }
}
