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

package io.questdb.griffin.engine.table;

/**
 * Execution counters for the benchmark runner. Read only after result computation;
 * workers keep counters in acquired slots and the owner combines them after draining.
 * Phase times are wall-clock nanoseconds. Peak native memory is sampled by the runner
 * across acquisition, all phases and final sorting, including temporary allocations.
 */
public final class HashJoinGroupByMetrics {
    long buildRows;
    long buildKeys;
    long buildBytes;
    long scannedRows;
    long matchedPairs;
    long nullExtendedRows;
    long survivingRows;
    long mergeCardinality;
    long buildNanos;
    long initNanos;
    long probeNanos;
    long mergeNanos;

    public long getBuildRows() {
        return buildRows;
    }

    public long getBuildKeys() {
        return buildKeys;
    }

    public long getBuildBytes() {
        return buildBytes;
    }

    public long getScannedRows() {
        return scannedRows;
    }

    public long getMatchedPairs() {
        return matchedPairs;
    }

    public long getNullExtendedRows() {
        return nullExtendedRows;
    }

    public long getSurvivingRows() {
        return survivingRows;
    }

    public long getMergeCardinality() {
        return mergeCardinality;
    }

    public long getBuildNanos() {
        return buildNanos;
    }

    public long getInitNanos() {
        return initNanos;
    }

    public long getProbeNanos() {
        return probeNanos;
    }

    public long getMergeNanos() {
        return mergeNanos;
    }

    void clear() {
        buildRows = 0;
        buildKeys = 0;
        buildBytes = 0;
        scannedRows = 0;
        matchedPairs = 0;
        nullExtendedRows = 0;
        survivingRows = 0;
        mergeCardinality = 0;
        buildNanos = 0;
        initNanos = 0;
        probeNanos = 0;
        mergeNanos = 0;
    }
}
