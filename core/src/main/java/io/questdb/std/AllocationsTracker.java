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

// @formatter:off
import io.questdb.log.Log;
import io.questdb.log.LogRecord;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

// This utility method tracks memory allocations and deallocations.
// It is used to detect memory leaks in native memory and to ensure that accessed memory is within the allocated range.

// By default, the bodies of public entry methods are intentionally commented out to prevent accidental use in production.
// These methods are uncommented at compile time using the Maven profile 'track-allocation'.
// Example #1, run all test in Core: $ mvn clean install -P track-allocation -pl core
// Example #2, run a specific test in Core: mvn test -P track-allocation -pl core -Dtest=io.questdb.test.griffin.engine.window.WindowFunctionUnitTest
// If you run Maven with this profile locally then Idea may freak out about duplicated files. You can fit it by running `mvn clean`


// How to use it locally from IDE?
// Because the preprocessor does not work well with IntelliJ IDEA, it is recommended to manually remove the comments
// that are normally removed by the preprocessor. Be careful not to accidentally commit these changes!
// You will also need to set an environment variable to enable tracking:
// export QUESTDB_TRACK_ALLOCATIONS=full - to track allocations and deallocations with stack traces
// export QUESTDB_TRACK_ALLOCATIONS=true - to track allocations and deallocations without stack traces
// Intellij IDEA can be configured to run the application with these environment variables set.

public final class AllocationsTracker {
    private static final ConcurrentNavigableMap<Long, Object> ALLOCATIONS;
    private static final boolean TRACK_ALLOCATIONS;
    private static boolean COLLECT_STACKTRACES = false;

    /**
     * Asserts that memory at given address is allocated and has at least a given size.
     *
     * @param address memory address
     * @param size   size of the memory block
     */
    public static void assertAllocatedMemory(long address, long size) {
        if (!TRACK_ALLOCATIONS) {
            return;
        }
        assertAllocatedMemory0(address, size);
    }

    /**
     * Prints total allocated memory and number of allocations to log. Log message can be optionally prefixed.
     *
     * @param log   log to print to, can be null to print to stdout
     * @param prefix optional prefix
     */
    public static void dumpAllocations(Log log, CharSequence prefix) {
        if (!TRACK_ALLOCATIONS) {
            return;
        }
        dumpAllocations0(log, prefix);
    }

    /**
     * Prints stack traces of all new allocations since last call to this method that have not been freed yet.
     *
     * @param log log to print to, can be null to print to stdout
     */
    public static void dumpNewAllocationsStacktraces(Log log) {
        if (!TRACK_ALLOCATIONS) {
            return;
        }
        dumpNewAllocationsStacktraces0(log);
    }

    /**
     * Notifies tracker that memory at given address is freed.
     *
     * @param address memory address
     * @throws AssertionError if address is not found in the tracker - this likely indicates a double free
     */
    public static void onFree(long address) {
        if (!TRACK_ALLOCATIONS) {
            return;
        }
        onFree0(address);
    }

    /**
     * Notifies tracker that memory at given address is allocated.
     *
     * @param address memory address
     * @param size   size of the memory block
     * @throws AssertionError if address is already allocated - this indicates a bug in tracking logic
     */
    public static void onMalloc(long address, long size) {
        if (!TRACK_ALLOCATIONS) {
            return;
        }
        onMalloc0(address, size);
    }

    private static void assertAllocatedMemory0(long address, long size) {
        if (size == 0) {
            return;
        }
        if (address == 0) {
            throw new AssertionError("address is zero [size=" + size + "]");
        }
        Map.Entry<Long, ?> allocEntry = ALLOCATIONS.floorEntry(address);
        assert allocEntry != null;
        long lo = allocEntry.getKey();
        long hi = allocEntry.getValue() instanceof Allocation ? ((Allocation) allocEntry.getValue()).hi : (Long) allocEntry.getValue();

        if (address < lo) {
            throw new AssertionError("address is below allocated memory [address=" + address + ", size=" + size + ", lo=" + lo + ", hi=" + hi + "]");
        }
        if (address + size > hi) {
            throw new AssertionError("address is above allocated memory [address=" + address + ", size=" + size + ", lo=" + lo + ", hi=" + hi + "]");
        }
    }

    private static void dumpAllocations0(Log log, CharSequence prefix) {
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

    private static void dumpNewAllocationsStacktraces0(Log log) {
        for (Map.Entry<Long, ?> entry : ALLOCATIONS.entrySet()) {
            Object v = entry.getValue();
            if (v instanceof Allocation) {
                Allocation a = (Allocation) v;
                if (!a.dumped) {
                    a.dumped = true;
                    if (log != null) {
                        log.error().$("unfreed memory [address=").$(entry.getKey()).$(", size=").$(a.hi - entry.getKey()).$(']').$(a.stackTrace).$();
                    } else {
                        System.out.println("unfreed memory [address=" + entry.getKey() + ", size=" + (a.hi - entry.getKey()) + "]");
                        a.stackTrace.printStackTrace();
                    }
                }
            }
        }
    }

    private static void onFree0(long address) {
        Object remove = ALLOCATIONS.remove(address);
        if (remove == null) {
            throw new AssertionError("address to free() not found [address=" + address + "]");
        }
    }

    private static void onMalloc0(long address, long size) {
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
            TRACK_ALLOCATIONS = true;
            ALLOCATIONS = new ConcurrentSkipListMap<>();
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
