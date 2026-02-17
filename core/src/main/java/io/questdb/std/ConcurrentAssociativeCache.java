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

package io.questdb.std;

import io.questdb.metrics.Counter;
import io.questdb.metrics.LongGauge;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Thread-safe cache implementation.
 * <p>
 * The cache is organized in rows (think, hash table buckets), each holding a fixed number
 * of blocks (items). A row is represented with an array of string keys and an array of
 * cached object values. The arrays are filled left-to-right.
 * <p>
 * When an object is taken from the cache, its key is kept around. This way, when the object
 * is later returned, there is a chance that we're able to reuse the key and avoid String
 * allocation. Objects are always inserted to the first block which means that eviction
 * in each row is FIFO.
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
    private final ObjList<String[]> keys;
    private final Counter missCounter;
    private final int rowMask;
    private final int rows;
    // Each row has its own array of values.
    private final ObjList<V[]> values;

    @SuppressWarnings("unchecked")
    public ConcurrentAssociativeCache(ConcurrentCacheConfiguration configuration) {
        this.blocks = Math.max(MIN_BLOCKS, Numbers.ceilPow2(configuration.getBlocks()));
        this.rows = Math.max(MIN_ROWS, Numbers.ceilPow2(configuration.getRows()));

        int capacity = this.rows * this.blocks;
        if (capacity < 0) {
            throw new OutOfMemoryError();
        }
        this.keys = new ObjList<>(this.rows);
        this.values = new ObjList<>(this.rows);
        for (int i = 0; i < this.rows; i++) {
            keys.add(new String[this.blocks]);
            values.add((V[]) new Object[this.blocks]);
        }
        this.rowMask = this.rows - 1;
        this.cachedGauge = configuration.getCachedGauge();
        this.hitCounter = configuration.getHiCounter();
        this.missCounter = configuration.getMissCounter();
    }

    @Override
    public int capacity() {
        return rows * blocks;
    }

    @Override
    public void clear() {
        long freed = 0;
        for (int i = 0; i < rows; i++) {
            final String[] rowKeys = keys.getQuick(i);
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
    public void close() {
        clear();
    }

    @Override
    public V poll(@NotNull CharSequence key) {
        final int row = row(key);
        final String[] rowKeys = keys.getQuick(row);
        final V[] rowValues = values.getQuick(row);
        V value = null;

        synchronized (rowKeys) {
            for (int i = 0; i < blocks; i++) {
                if (rowKeys[i] == null) {
                    break;
                }
                if (rowValues[i] != null && Chars.equals(key, rowKeys[i])) {
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
        final String[] rowKeys = keys.getQuick(row);
        final V[] rowValues = values.getQuick(row);
        V outgoingValue;

        synchronized (rowKeys) {
            // Find a block to place the object.
            int idx = blocks - 1;
            for (int i = 0; i < blocks; i++) {
                if (rowKeys[i] == null) {
                    // Empty block found.
                    idx = i;
                    break;
                }
                if (rowValues[i] == null) {
                    // The value was previously cleared by poll().
                    idx = i;
                    if (Chars.equals(key, rowKeys[i])) {
                        // That's our key, so reuse it to avoid String allocation.
                        key = rowKeys[i];
                        break;
                    }
                }
            }

            // Evict object at the found block (or the very last block).
            outgoingValue = rowValues[idx];
            // Shift the arrays to be able to insert to the first block.
            System.arraycopy(rowKeys, 0, rowKeys, 1, idx);
            System.arraycopy(rowValues, 0, rowValues, 1, idx);
            // We can now insert the new object.
            rowKeys[0] = Chars.toString(key);
            rowValues[0] = value;
        }

        if (outgoingValue == null) {
            // We're inserting.
            cachedGauge.inc();
        } else {
            // We're replacing the value with another one, no need to change the gauge.
            Misc.freeIfCloseable(outgoingValue);
        }
    }

    private int row(CharSequence key) {
        return Hash.spread(Chars.hashCode(key)) & rowMask;
    }
}
