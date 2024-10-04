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

import io.questdb.log.Log;
import io.questdb.log.LogRecord;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class AllocationsTracker {
    private static final ConcurrentNavigableMap<Long, Object> ALLOCATIONS;
    private static final boolean TRACK_ALLOCATIONS;
    public static boolean COLLECT_STACKTRACES = false;

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
        Map.Entry<Long, ?> allocEntry = ALLOCATIONS.floorEntry(address);
        assert allocEntry != null;
        long lo = allocEntry.getKey();
        long hi = allocEntry.getValue() instanceof Allocation ? ((Allocation) allocEntry.getValue()).hi : (long) allocEntry.getValue();

        if (address < lo) {
            throw new AssertionError("address is below allocated memory [address=" + address + ", size=" + size + ", lo=" + lo + ", hi=" + hi + "]");
        }
        if (address + size > hi) {
            throw new AssertionError("address is above allocated memory [address=" + address + ", size=" + size + ", lo=" + lo + ", hi=" + hi + "]");
        }
    }

    public static void dumpAllocations(Log log, CharSequence prefix) {
        if (!TRACK_ALLOCATIONS) {
            return;
        }
        long total = 0;
        int allocCount = 0;
        for (Map.Entry<Long, ?> entry : ALLOCATIONS.entrySet()) {
            Object v = entry.getValue();
            long hi = v instanceof Allocation ? ((Allocation) v).hi : (long) v;
            total += (hi - entry.getKey());

            // we want allocation count to be consistent with what we counted
            // so we increment it here instead of using ALLOCATIONS.size()
            allocCount++;
        }
        if (log != null) {
            LogRecord info = log.info();
            if (prefix != null) {
                info.$(prefix).$(" ");
            }
            info.$("total native allocations [bytes=").$(total).$(", count=").$(allocCount).$(']').$();
        } else {
            StringBuilder sb = new StringBuilder();
            if (prefix != null) {
                sb.append(prefix).append(' ');
            }
            sb.append("total native allocations [bytes=").append(total).append(", count=").append(allocCount).append(']');
            System.out.println(sb);
        }
    }

    public static void dumpNewAllocationsStacktraces() {
        if (!TRACK_ALLOCATIONS) {
            return;
        }
        for (Map.Entry<Long, ?> entry : ALLOCATIONS.entrySet()) {
            Object v = entry.getValue();
            if (v instanceof Allocation) {
                Allocation a = (Allocation) v;
                if (!a.dumped) {
                    a.dumped = true;
                    a.stackTrace.printStackTrace();
                }
            }
        }
    }

    public static void onFree(long address) {
        if (!TRACK_ALLOCATIONS) {
            return;
        }
        Object remove = ALLOCATIONS.remove(address);
        if (remove == null) {
            throw new AssertionError("address to free() not found [address=" + address + "]");
        }
    }

    public static void onMalloc(long address, long size) {
        if (!TRACK_ALLOCATIONS) {
            return;
        }
        if (size == 0) {
            return;
        }
        Object v;
        if (COLLECT_STACKTRACES) {
            v = ALLOCATIONS.put(address, new Allocation(address + size, new Exception("size: " + size + ", thread: " + Thread.currentThread().getName() + ", threadId: " + Thread.currentThread().getId())));
        } else {
            v = ALLOCATIONS.put(address, address + size);
        }
        if (v != null) {
            long hi = v instanceof Allocation ? ((Allocation) v).hi : (long) v;
            long oldSize = hi - address;
            String msg = "address already allocated [address=" + address + ", size=" + size + ", oldSize=" + oldSize + "]";
            if (v instanceof Allocation) {
                throw new AssertionError(msg, ((Allocation) v).stackTrace);
            } else {
                throw new AssertionError(msg);
            }
        }
    }

    private static class Allocation {
        final long hi;
        final Exception stackTrace;
        boolean dumped = false;

        private Allocation(long hi, Exception stackTrace) {
            this.hi = hi;
            this.stackTrace = stackTrace;
        }
    }

    static {
        String prop = System.getProperty("questdb.track.allocations");
        if (prop == null) {
            prop = System.getenv("QUESTDB_TRACK_ALLOCATIONS");
        }
        if (prop == null) {
            TRACK_ALLOCATIONS = false;
            ALLOCATIONS = null;
        } else {
            if ("true".equals(prop)) {
                TRACK_ALLOCATIONS = true;
                ALLOCATIONS = new ConcurrentSkipListMap<>();
            } else if ("full".equals(prop)) {
                TRACK_ALLOCATIONS = true;
                ALLOCATIONS = new ConcurrentSkipListMap<>();
                COLLECT_STACKTRACES = true;
            } else {
                TRACK_ALLOCATIONS = false;
                ALLOCATIONS = null;
            }
        }
    }

}
