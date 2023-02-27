/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.engine.functions.geohash.GeoHashNative;
import io.questdb.mp.CountDownLatchSPI;

public class LatestByTask {
    private long argsAddress;
    private ExecutionCircuitBreaker circuitBreaker;
    private CountDownLatchSPI doneLatch;
    private int hashLength;
    private long hashesAddress;
    private long keyBaseAddress;
    private long keysMemorySize;
    private int partitionIndex;
    private long prefixesAddress;
    private long prefixesCount;
    private long rowHi;
    private long rowLo;
    private long unIndexedNullCount;
    private long valueBaseAddress;
    private int valueBlockCapacity;
    private long valuesMemorySize;

    public void of(
            long keyBaseAddress,
            long keysMemorySize,
            long valueBaseAddress,
            long valuesMemorySize,
            long argsAddress,
            long unIndexedNullCount,
            long rowHi,
            long rowLo,
            int partitionIndex,
            int valueBlockCapacity,
            long hashesAddress,
            int hashLength,
            long prefixesAddress,
            long prefixesCount,
            CountDownLatchSPI doneLatch,
            ExecutionCircuitBreaker circuitBreaker
    ) {
        this.keyBaseAddress = keyBaseAddress;
        this.keysMemorySize = keysMemorySize;
        this.valueBaseAddress = valueBaseAddress;
        this.valuesMemorySize = valuesMemorySize;
        this.argsAddress = argsAddress;
        this.unIndexedNullCount = unIndexedNullCount;
        this.rowHi = rowHi;
        this.rowLo = rowLo;
        this.partitionIndex = partitionIndex;
        this.valueBlockCapacity = valueBlockCapacity;
        this.hashesAddress = hashesAddress;
        this.hashLength = hashLength;
        this.prefixesAddress = prefixesAddress;
        this.prefixesCount = prefixesCount;
        this.doneLatch = doneLatch;
        this.circuitBreaker = circuitBreaker;
    }

    public boolean run() {
        if (!circuitBreaker.checkIfTripped()) {
            GeoHashNative.latestByAndFilterPrefix(
                    keyBaseAddress,
                    keysMemorySize,
                    valueBaseAddress,
                    valuesMemorySize,
                    argsAddress,
                    unIndexedNullCount,
                    rowHi,
                    rowLo,
                    partitionIndex,
                    valueBlockCapacity,
                    hashesAddress,
                    hashLength,
                    prefixesAddress,
                    prefixesCount
            );
        }

        doneLatch.countDown();
        return true;
    }
}
