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

import io.questdb.std.LongList;
import io.questdb.std.Mutable;

/**
 * An ordered log of partition removals a {@link TableWriter} made durable, one event per
 * physical partition, in the order the writer committed them.
 * <p>
 * Each event carries the seqTxn of the transaction whose apply removed the partition,
 * the exact timestamp interval the partition covered, the number of rows it held and
 * whether TTL enforcement or an explicit {@code DROP PARTITION} removed it. The interval
 * is {@code [lo, hiExclusive)} in the table's timestamp units: {@code lo} is the physical
 * partition's own timestamp and {@code hiExclusive} is the smaller of its logical
 * partition's ceiling and the next attached partition's timestamp, so a split partition
 * reports the sub-range it actually held rather than the whole logical partition.
 * <p>
 * Events are never coalesced: dropping January 1 and January 3 records two disjoint
 * intervals, which is what lets a consumer keep whatever sits between them. A consumer
 * that needs a single figure reads {@link #getTotalRemovedRows()}.
 * <p>
 * The log records only removals that reached the table's {@code _txn}. The writer stages
 * a removal while it mutates its in-memory partition list and publishes it into this
 * log only after the commit that carries it returns, so a removal that rolls back, or a
 * commit that fails, never appears here. It is a plain reusable buffer: heap-backed,
 * grown once to the largest batch it has seen and cleared by its owner between batches,
 * so recording an event allocates nothing on the steady state.
 */
public class PartitionRemovalEvents implements Mutable {
    public static final byte SOURCE_DROP_PARTITION = 0;
    public static final byte SOURCE_TTL = 1;
    private static final int HI_OFFSET = 2;
    private static final int LONGS_PER_EVENT = 5;
    private static final int LO_OFFSET = 1;
    private static final int ROWS_OFFSET = 3;
    private static final int SEQ_TXN_OFFSET = 0;
    private static final int SOURCE_OFFSET = 4;
    private final LongList events = new LongList();
    private long totalRemovedRows;

    public void add(long seqTxn, long lo, long hiExclusive, long removedRows, byte source) {
        assert lo < hiExclusive : "removal interval is empty";
        assert removedRows >= 0 : "negative removed row count";
        events.add(seqTxn);
        events.add(lo);
        events.add(hiExclusive);
        events.add(removedRows);
        events.add(source);
        totalRemovedRows += removedRows;
    }

    /**
     * Appends every event of {@code that} after this log's own, preserving order.
     */
    public void addAll(PartitionRemovalEvents that) {
        events.addAll(that.events);
        totalRemovedRows += that.totalRemovedRows;
    }

    @Override
    public void clear() {
        events.clear();
        totalRemovedRows = 0;
    }

    public long getHiExclusive(int index) {
        return events.getQuick(index * LONGS_PER_EVENT + HI_OFFSET);
    }

    public long getLo(int index) {
        return events.getQuick(index * LONGS_PER_EVENT + LO_OFFSET);
    }

    public long getRemovedRows(int index) {
        return events.getQuick(index * LONGS_PER_EVENT + ROWS_OFFSET);
    }

    public long getSeqTxn(int index) {
        return events.getQuick(index * LONGS_PER_EVENT + SEQ_TXN_OFFSET);
    }

    public byte getSource(int index) {
        return (byte) events.getQuick(index * LONGS_PER_EVENT + SOURCE_OFFSET);
    }

    /**
     * The sum of {@link #getRemovedRows(int)} over every event in the log.
     */
    public long getTotalRemovedRows() {
        return totalRemovedRows;
    }

    public boolean isEmpty() {
        return events.size() == 0;
    }

    public boolean isTtl(int index) {
        return getSource(index) == SOURCE_TTL;
    }

    public int size() {
        return events.size() / LONGS_PER_EVENT;
    }
}
