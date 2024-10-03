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

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class AllocationsTracker {
    private static final ConcurrentNavigableMap<Long, Long> ALLOCATIONS;
    private static final boolean TRACK_ALLOCATIONS;

    public static void assertAllocatedMemory(long address, long size) {
        if (!TRACK_ALLOCATIONS) {
            return;
        }
        if (size == 0) {
            return;
        }
        if (address == 0) {
            throw new AssertionError("address is zero [size=" + size + "]");
        }
        Map.Entry<Long, Long> allocEntry = ALLOCATIONS.floorEntry(address);
        assert allocEntry != null;
        long lo = allocEntry.getKey();
        long hi = allocEntry.getValue();

        if (address < lo) {
            throw new AssertionError("address is below allocated memory [address=" + address + ", size=" + size + ", lo=" + lo + ", hi=" + hi + "]");
        }
        if (address + size > hi) {
            throw new AssertionError("address is above allocated memory [address=" + address + ", size=" + size + ", lo=" + lo + ", hi=" + hi + "]");
        }
    }

    public static void onFree(long address) {
        if (!TRACK_ALLOCATIONS) {
            return;
        }
        Long remove = ALLOCATIONS.remove(address);
        assert remove != null;
    }

    public static void onMalloc(long address, long size) {
        if (!TRACK_ALLOCATIONS) {
            return;
        }
        if (size == 0) {
            return;
        }
        Long put = ALLOCATIONS.put(address, address + size);
        assert put == null;
    }

    static {
        TRACK_ALLOCATIONS = Boolean.getBoolean("questdb.track.allocations") || "true".equals(System.getenv().get("QUESTDB_TRACK_ALLOCATIONS"));
        if (TRACK_ALLOCATIONS) {
            System.out.println("Allocations tracking enabled, this will have severe performance impact!");
            ALLOCATIONS = new ConcurrentSkipListMap<>();
        } else {
            ALLOCATIONS = null;
        }
    }

}
