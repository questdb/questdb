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

package io.questdb.cairo.lv;

import io.questdb.std.Hash;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * Reusable content index for checkpoint binary keys. The two primitive qualifiers
 * let a caller keep one flat table for several function ordinals and state versions
 * without joining them to the key or allocating a wrapper for each probe.
 * <p>
 * Keys are held by reference and must remain immutable while indexed. Values are
 * non-negative integers; {@code -1} is reserved for a missing lookup.
 */
public final class LiveViewCheckpointBinaryKeyIndex implements Mutable {
    private static final double LOAD_FACTOR = 0.4;
    private static final int MIN_INITIAL_CAPACITY = 16;
    private int capacity = MIN_INITIAL_CAPACITY;
    private int free = capacity;
    private byte[][] keys;
    private int mask;
    private int[] namespaces;
    private int[] values;
    private int[] versions;

    public LiveViewCheckpointBinaryKeyIndex() {
        allocateSlots();
    }

    @Override
    public void clear() {
        Arrays.fill(keys, null);
        free = capacity;
    }

    public int get(int namespace, int version, @NotNull byte[] key) {
        final int index = keyIndex(namespace, version, key);
        return index < 0 ? values[-index - 1] : -1;
    }

    public void put(int namespace, int version, @NotNull byte[] key, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("negative live view checkpoint binary index value");
        }
        final int index = keyIndex(namespace, version, key);
        if (index < 0) {
            values[-index - 1] = value;
            return;
        }
        keys[index] = key;
        namespaces[index] = namespace;
        versions[index] = version;
        values[index] = value;
        if (--free < 1) {
            rehash();
        }
    }

    public int size() {
        return capacity - free;
    }

    private void allocateSlots() {
        final int slotCount = Numbers.ceilPow2((int) (capacity / LOAD_FACTOR));
        keys = new byte[slotCount][];
        namespaces = new int[slotCount];
        versions = new int[slotCount];
        values = new int[slotCount];
        mask = slotCount - 1;
    }

    /**
     * Returns the empty slot for a missing key, or {@code -slot - 1} for a match.
     */
    private int keyIndex(int namespace, int version, byte[] key) {
        final int hash = 31 * (31 * Arrays.hashCode(key) + namespace) + version;
        int index = Hash.spread(hash) & mask;
        do {
            final byte[] slot = keys[index];
            if (slot == null) {
                return index;
            }
            if (namespaces[index] == namespace
                    && versions[index] == version
                    && Arrays.equals(slot, key)) {
                return -index - 1;
            }
            index = (index + 1) & mask;
        } while (true);
    }

    private void rehash() {
        final byte[][] oldKeys = keys;
        final int[] oldNamespaces = namespaces;
        final int[] oldVersions = versions;
        final int[] oldValues = values;
        capacity *= 2;
        free = capacity;
        allocateSlots();
        for (int i = 0, n = oldKeys.length; i < n; i++) {
            final byte[] key = oldKeys[i];
            if (key != null) {
                final int index = keyIndex(oldNamespaces[i], oldVersions[i], key);
                keys[index] = key;
                namespaces[index] = oldNamespaces[i];
                versions[index] = oldVersions[i];
                values[index] = oldValues[i];
                free--;
            }
        }
    }
}
