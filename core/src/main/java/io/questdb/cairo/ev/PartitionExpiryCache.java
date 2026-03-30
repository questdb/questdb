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

package io.questdb.cairo.ev;

import io.questdb.std.LongList;
import io.questdb.std.Mutable;

/**
 * Caches per-partition min/max expiry column values to avoid re-scanning
 * unchanged partitions on every cleanup run. Invalidated when partition
 * nameTxn changes.
 *
 * Layout: parallel lists indexed by partition index.
 * - minExpiry[i], maxExpiry[i]: cached min/max of the expiry column
 * - nameTxn[i]: the partition nameTxn at the time of caching (for invalidation)
 */
public class PartitionExpiryCache implements Mutable {
    private final LongList maxExpiry = new LongList();
    private final LongList minExpiry = new LongList();
    private final LongList nameTxns = new LongList();

    @Override
    public void clear() {
        minExpiry.clear();
        maxExpiry.clear();
        nameTxns.clear();
    }

    public void ensureCapacity(int partitionCount) {
        while (minExpiry.size() < partitionCount) {
            minExpiry.add(Long.MIN_VALUE);
            maxExpiry.add(Long.MIN_VALUE);
            nameTxns.add(Long.MIN_VALUE);
        }
    }

    public long getMaxExpiry(int partitionIndex) {
        if (partitionIndex < maxExpiry.size()) {
            return maxExpiry.getQuick(partitionIndex);
        }
        return Long.MIN_VALUE;
    }

    public long getMinExpiry(int partitionIndex) {
        if (partitionIndex < minExpiry.size()) {
            return minExpiry.getQuick(partitionIndex);
        }
        return Long.MIN_VALUE;
    }

    public boolean isValid(int partitionIndex, long currentNameTxn) {
        if (partitionIndex >= nameTxns.size()) {
            return false;
        }
        return nameTxns.getQuick(partitionIndex) == currentNameTxn
                && minExpiry.getQuick(partitionIndex) != Long.MIN_VALUE;
    }

    public void set(int partitionIndex, long min, long max, long nameTxn) {
        ensureCapacity(partitionIndex + 1);
        minExpiry.setQuick(partitionIndex, min);
        maxExpiry.setQuick(partitionIndex, max);
        nameTxns.setQuick(partitionIndex, nameTxn);
    }
}
