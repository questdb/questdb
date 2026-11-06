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

package io.questdb.griffin.engine.orderby;

/**
 * Sizes the entries of an encoded sort, and the materialized sort's chain, for an execution whose
 * row count the base does not know, from the previous execution's count: a repeated large sort
 * then does not grow its entries (and copy them) a doubling at a time. The entries start at no
 * more than {@link #START}, and once an execution reaches its capacity they grow {@link #GROWTH}
 * times, up to the previous execution's count. So an execution never holds more than GROWTH times
 * the entries it has met, whatever the previous one held: a 70k-row run after a 50M-row one grows
 * to 512k entries, not to 50M.
 */
final class EncodedSortCapacityHint {
    // the factor the entries grow by each time an execution reaches their capacity
    static final long GROWTH = 8;
    // the entries an execution sized from the previous one starts with
    static final long START = 64 * 1024;

    private EncodedSortCapacityHint() {
    }

    /**
     * The chain bytes to reserve for {@code entries} entries, in proportion to the previous
     * execution's {@code lastChainBytes} for its {@code lastCount}.
     */
    static long chainBytes(long entries, long lastCount, long lastChainBytes) {
        return entries >= lastCount ? lastChainBytes : (long) ((double) lastChainBytes * entries / lastCount);
    }

    /**
     * The entries to grow to once an execution has {@code reached} entries.
     */
    static long next(long reached, long lastCount, long maxEntries) {
        return Math.min(Math.min(lastCount, maxEntries), reached > Long.MAX_VALUE / GROWTH ? Long.MAX_VALUE : reached * GROWTH);
    }

    /**
     * The entries an execution starts with.
     */
    static long start(long lastCount, long maxEntries) {
        return Math.min(Math.min(lastCount, maxEntries), START);
    }
}
