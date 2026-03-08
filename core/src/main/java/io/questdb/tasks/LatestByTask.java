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

package io.questdb.tasks;

import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.griffin.engine.functions.geohash.GeoHashNative;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;

public class LatestByTask implements QuietCloseable, Mutable {
    // We're using page frame memory only and do single scan, hence cache size of 1.
    private final PageFrameMemoryPool frameMemoryPool = new PageFrameMemoryPool(1);
    private long argsAddress;
    private ExecutionCircuitBreaker circuitBreaker;
    private CountDownLatchSPI doneLatch;
    private int frameIndex;
    private int hashColumnIndex;
    private int hashColumnType;
    private long keyBaseAddress;
    private long keysMemorySize;
    private long prefixesAddress;
    private long prefixesCount;
    private long rowHi;
    private long rowLo;
    private long unIndexedNullCount;
    private long valueBaseAddress;
    private int valueBlockCapacity;
    private long valuesMemorySize;

    @Override
    public void clear() {
        frameMemoryPool.clear();
    }

    @Override
    public void close() {
        Misc.free(frameMemoryPool);
    }

    public void of(
            PageFrameAddressCache addressCache,
            long keyBaseAddress,
            long keysMemorySize,
            long valueBaseAddress,
            long valuesMemorySize,
            long argsAddress,
            long unIndexedNullCount,
            long rowHi,
            long rowLo,
            int frameIndex,
            int valueBlockCapacity,
            int hashColumnIndex,
            int hashColumnType,
            long prefixesAddress,
            long prefixesCount,
            CountDownLatchSPI doneLatch,
            ExecutionCircuitBreaker circuitBreaker
    ) {
        this.frameMemoryPool.of(addressCache);
        this.keyBaseAddress = keyBaseAddress;
        this.keysMemorySize = keysMemorySize;
        this.valueBaseAddress = valueBaseAddress;
        this.valuesMemorySize = valuesMemorySize;
        this.argsAddress = argsAddress;
        this.unIndexedNullCount = unIndexedNullCount;
        this.rowHi = rowHi;
        this.rowLo = rowLo;
        this.frameIndex = frameIndex;
        this.valueBlockCapacity = valueBlockCapacity;
        this.hashColumnIndex = hashColumnIndex;
        this.hashColumnType = hashColumnType;
        this.prefixesAddress = prefixesAddress;
        this.prefixesCount = prefixesCount;
        this.doneLatch = doneLatch;
        this.circuitBreaker = circuitBreaker;
    }

    public boolean run() {
        try {
            if (!circuitBreaker.checkIfTripped()) {
                GeoHashNative.latestByAndFilterPrefix(
                        frameMemoryPool,
                        keyBaseAddress,
                        keysMemorySize,
                        valueBaseAddress,
                        valuesMemorySize,
                        argsAddress,
                        unIndexedNullCount,
                        rowHi,
                        rowLo,
                        frameIndex,
                        valueBlockCapacity,
                        hashColumnIndex,
                        hashColumnType,
                        prefixesAddress,
                        prefixesCount
                );
            }
            doneLatch.countDown();
            return true;
        } finally {
            frameMemoryPool.close();
        }
    }
}
