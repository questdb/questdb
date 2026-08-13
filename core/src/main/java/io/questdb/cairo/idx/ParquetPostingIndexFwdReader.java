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

package io.questdb.cairo.idx;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.IndexMetaFileReader;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.DirectIntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

/**
 * {@link IndexReader#DIR_FORWARD} reader over a parquet-form covering index.
 * Serves a key's postings in ascending {@code row_id} order.
 */
public class ParquetPostingIndexFwdReader extends AbstractParquetPostingIndexReader {
    private final FwdCursor cursor = new FwdCursor();

    /**
     * Frees the pooled cursor's decode buffers alongside the reader's mappings.
     * Detached cursors are the worker's to close -- they are handed out one per
     * call and never returned here.
     */
    @Override
    public void close() {
        cursor.freeResources();
        super.close();
    }

    /**
     * A cursor a single worker owns outright, for the parallel covered decode.
     * <p>
     * Never drawn from or returned to the reader's pooled cursor, so N workers
     * may iterate N of these over ONE reader: each owns its decode buffers, its
     * projection and its cover slot-to-chunk map, sharing only the _im and
     * parquet mappings, which do not move while the reader is frozen. Sharing
     * any of the three would interleave two groups in one allocation or let one
     * cursor's projection overwrite another's.
     */
    @Override
    public RowCursor getDetachedCursor(int key, long minValue, long maxValue, int[] requiredCoverColumns) {
        final FwdCursor detached = new FwdCursor();
        detached.detached = true;
        detached.of(key, minValue, maxValue, requiredCoverColumns);
        return detached;
    }

    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue) {
        cursor.of(key, minValue, maxValue, null);
        return cursor;
    }

    /**
     * Serves the postings AND the requested covered values from one decode.
     * <p>
     * {@code requiredCoverColumns} are cover SLOTS; the projection maps each to
     * its descriptor index, which is the parquet column index. Only the
     * requested slots are decoded, so an unused covered column costs nothing --
     * the improvement over the native {@code .pc} layout, where each covered
     * column is a separate file read.
     */
    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue, int[] requiredCoverColumns) {
        cursor.of(key, minValue, maxValue, requiredCoverColumns);
        return cursor;
    }

    /**
     * Walks the row-group run the {@code _im} directory resolved for the key,
     * decoding one group at a time and yielding the postings that both carry
     * the key and fall inside {@code [minValue, maxValue]}.
     * <p>
     * Two filters, and both are needed. A row group is PACKED -- it holds a run
     * of whole keys -- so the key the directory pointed at is one of several in
     * the group, and rows belonging to its neighbours have to be skipped. The
     * row-id bound is the caller's page-frame window, which does not align with
     * row-group boundaries.
     * <p>
     * The postings within a key are written in ascending {@code row_id} order
     * and the groups are visited in ascending order, so the emitted sequence
     * ascends without a sort. That is asserted by the test rather than assumed
     * here.
     * <p>
     * Row groups whose row-id extent misses the window are skipped without a
     * decode -- pruning level 2, exact because row id is monotone in the
     * designated timestamp within a partition.
     */
    private class FwdCursor extends AbstractCoveringCursor {
        private long groupRows;
        private boolean hasNext;
        private int key;
        private long keyIdPtr;
        private long maxValue;
        private long minValue;
        private long next;
        private int rg;
        private int rgHi;
        private long rowHi;
        private long rowIdPtr;
        private long rowLo;
        private CountingCursor keyProbe;
        private boolean detached;
        private int[] requiredCoverColumns;
        private long rowInGroup;

        /**
         * Releases the decoded row group. The buffers hold a whole group's
         * {@code key_id} and {@code row_id} chunks -- hundreds of KiB for a
         * default-sized group -- and a cursor that keeps them past close is
         * retained RSS charged to the query, which the test harness caps at
         * 64 KiB. {@code reopen()} before the next decode re-allocates, so the
         * reader stays reusable.
         */
        @Override
        public void close() {
            if (keyProbe != null) {
                keyProbe.close();
                keyProbe = null;
            }
            if (detached) {
                freeResources();
            } else {
                rowGroupBuffers.close();
            }
            keyIdPtr = 0;
            rowIdPtr = 0;
            hasNext = false;
        }

        private CountingCursor probe() {
            if (keyProbe == null) {
                keyProbe = new CountingCursor();
            }
            return keyProbe;
        }

        @Override
        public boolean hasNext() {
            if (hasNext) {
                return true;
            }
            while (rg <= rgHi) {
                if (keyIdPtr == 0 && !decodeCurrentGroup()) {
                    return false;
                }
                while (rowInGroup < groupRows) {
                    final long i = rowInGroup++;
                    if (Unsafe.getUnsafe().getInt(keyIdPtr + (i << 2)) != key) {
                        continue;
                    }
                    final long rowId = Unsafe.getUnsafe().getLong(rowIdPtr + (i << 3));
                    if (rowId < minValue || rowId > maxValue) {
                        continue;
                    }
                    setEmittedRow(i);
                    next = rowId;
                    hasNext = true;
                    return true;
                }
                // Group exhausted; force a decode of the next one.
                rg++;
                keyIdPtr = 0;
            }
            return false;
        }

        @Override
        public long next() {
            hasNext = false;
            return next;
        }

        private boolean decodeCurrentGroup() {
            // Walk forward over groups that cannot contribute, rather than
            // recursing: a key's run can be long, and a narrow window can
            // exclude most of it.
            while (rg <= rgHi) {
                groupRows = imReader.getRowGroupNumRows(rg);
                if (groupRows <= 0) {
                    // An empty group cannot hold a posting. Skipping it here
                    // rather than calling the decoder keeps a zero-row decode --
                    // which the native side treats as an error -- off the path.
                    rg++;
                    continue;
                }
                if (isRowGroupPruned(rg, minValue, maxValue)) {
                    // Pruning level 2: the group's row-id extent misses the
                    // caller's window entirely, so nothing in it could be
                    // emitted. Skipped without a decode, and deliberately
                    // without counting one.
                    rg++;
                    continue;
                }
                // Pruning level 3: bound the value decode to the key's own
                // rows. In a packed group most rows belong to other keys, and
                // decoding them costs row_id plus every covered column.
                final long keyRange = keyRowRangeInGroup(probe(), rg, key, groupRows);
                if (keyRange == IndexMetaFileReader.KEY_ABSENT) {
                    // The directory said this group COULD hold the key; the
                    // probe says it does not. An ordinary miss, not an error.
                    rg++;
                    continue;
                }
                rowLo = Numbers.decodeLowInt(keyRange);
                rowHi = Numbers.decodeHighInt(keyRange);
                final DirectIntList columns = coveringProjection(requiredCoverColumns);
                rowGroupBuffers.reopen();
                decoder().decodeRowGroup(rowGroupBuffers, columns, rg, (int) rowLo, (int) rowHi);
                onRowGroupDecoded(rowHi - rowLo);
                groupRows = rowHi - rowLo;
                // Chunk ordinals follow the projection's order, not the parquet
                // file's: key_id was added first, row_id second.
                keyIdPtr = rowGroupBuffers.getChunkDataPtr(0);
                rowIdPtr = rowGroupBuffers.getChunkDataPtr(1);
                rowInGroup = 0;
                return true;
            }
            return false;
        }

        void of(int key, long minValue, long maxValue, int[] requiredCoverColumns) {
            this.requiredCoverColumns = requiredCoverColumns;
            this.key = key;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.hasNext = false;
            this.next = -1;
            this.keyIdPtr = 0;
            this.rowIdPtr = 0;
            this.rowInGroup = 0;
            this.groupRows = 0;
            setEmittedRow(-1);

            final long range = rowGroupRangeForKey(key);
            if (range == IndexMetaFileReader.KEY_ABSENT) {
                // Exhausted before it starts: rg > rgHi, so hasNext() returns
                // false without decoding anything. An absent key is an ordinary
                // answer, not an error -- a query for a symbol this partition
                // never saw must return no rows.
                rg = 1;
                rgHi = 0;
                return;
            }
            rg = Numbers.decodeLowInt(range);
            rgHi = Numbers.decodeHighInt(range);
        }
    }
}
