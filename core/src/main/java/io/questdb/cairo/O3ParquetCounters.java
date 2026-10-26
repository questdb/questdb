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

package io.questdb.cairo;

import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-wide counts of the paths {@link O3PartitionJob} takes on a parquet partition,
 * so fuzz tests can assert they still reach the paths they target. Counters bump once per
 * partition task (per row group for DROP) and include attempts whose commit later failed.
 */
public final class O3ParquetCounters {
    private static final AtomicLong dedupIdenticalCount = new AtomicLong();
    private static final AtomicLong dropInPlaceCount = new AtomicLong();
    private static final AtomicLong dropRewriteCount = new AtomicLong();
    private static final AtomicLong inPlaceCount = new AtomicLong();
    private static final AtomicLong replaceNoopCount = new AtomicLong();
    private static final AtomicLong replaceRemovalCount = new AtomicLong();
    private static final AtomicLong rewriteCount = new AtomicLong();

    private O3ParquetCounters() {
    }

    /**
     * Dedup commits identical to the partition, which leave it unchanged.
     */
    @TestOnly
    public static long getDedupIdenticalCount() {
        return dedupIdenticalCount.get();
    }

    /**
     * Row groups a replace commit dropped in place (update mode).
     */
    @TestOnly
    public static long getDropInPlaceCount() {
        return dropInPlaceCount.get();
    }

    /**
     * Row groups a replace commit dropped by not copying them into a rewrite.
     */
    @TestOnly
    public static long getDropRewriteCount() {
        return dropRewriteCount.get();
    }

    /**
     * Partition tasks that updated the parquet file in place (same name txn).
     */
    @TestOnly
    public static long getInPlaceCount() {
        return inPlaceCount.get();
    }

    /**
     * Replace commits whose range missed every row of the partition and brought none.
     */
    @TestOnly
    public static long getReplaceNoopCount() {
        return replaceNoopCount.get();
    }

    /**
     * Replace commits that removed every row of the partition, removing the partition.
     */
    @TestOnly
    public static long getReplaceRemovalCount() {
        return replaceRemovalCount.get();
    }

    /**
     * Partition tasks that rewrote the parquet file into a new txn-named directory.
     */
    @TestOnly
    public static long getRewriteCount() {
        return rewriteCount.get();
    }

    static void onDedupIdentical() {
        dedupIdenticalCount.incrementAndGet();
    }

    static void onDrop(boolean isInPlace) {
        (isInPlace ? dropInPlaceCount : dropRewriteCount).incrementAndGet();
    }

    static void onPartitionWritten(boolean isRewrite) {
        (isRewrite ? rewriteCount : inPlaceCount).incrementAndGet();
    }

    static void onReplaceNoop() {
        replaceNoopCount.incrementAndGet();
    }

    static void onReplaceRemoval() {
        replaceRemovalCount.incrementAndGet();
    }
}
