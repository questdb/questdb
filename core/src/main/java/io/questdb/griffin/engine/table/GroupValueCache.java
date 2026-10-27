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

package io.questdb.griffin.engine.table;

import io.questdb.std.Hash;

import java.util.Arrays;

/**
 * A small open-addressing map from a group's cache key (a build key, optionally combined with one probe symbol
 * key) to the address of its map value. A value address is never 0, so 0 marks an empty slot and a miss.
 */
final class GroupValueCache {
    private long[] addresses = new long[64];
    private long[] keys = new long[64];
    private int mask = 63;
    private int size;

    void clear() {
        if (size > 0) {
            Arrays.fill(addresses, 0L);
            size = 0;
        }
    }

    long get(long key) {
        int i = Hash.hashLong32(key) & mask;
        long address;
        while ((address = addresses[i]) != 0) {
            if (keys[i] == key) {
                return address;
            }
            i = (i + 1) & mask;
        }
        return 0;
    }

    void put(long key, long address) {
        if ((size + 1) * 2 > addresses.length) {
            grow();
        }
        int i = Hash.hashLong32(key) & mask;
        while (addresses[i] != 0) {
            if (keys[i] == key) {
                addresses[i] = address;
                return;
            }
            i = (i + 1) & mask;
        }
        keys[i] = key;
        addresses[i] = address;
        size++;
    }

    private void grow() {
        final long[] oldKeys = keys;
        final long[] oldAddresses = addresses;
        keys = new long[oldKeys.length * 2];
        addresses = new long[oldKeys.length * 2];
        mask = keys.length - 1;
        size = 0;
        for (int j = 0; j < oldKeys.length; j++) {
            if (oldAddresses[j] != 0) {
                put(oldKeys[j], oldAddresses[j]);
            }
        }
    }
}
