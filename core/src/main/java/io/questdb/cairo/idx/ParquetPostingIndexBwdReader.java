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
 * {@link IndexReader#DIR_BACKWARD} reader over a parquet-form covering index.
 * Serves a key's postings in descending {@code row_id} order.
 */
public class ParquetPostingIndexBwdReader extends AbstractParquetPostingIndexReader {
    private final BwdCursor cursor = new BwdCursor();

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
        final BwdCursor detached = new BwdCursor();
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
     * @see ParquetPostingIndexFwdReader#getCursor(int, long, long, int[])
     */
    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue, int[] requiredCoverColumns) {
        cursor.of(key, minValue, maxValue, requiredCoverColumns);
        return cursor;
    }

    /**
     * The forward cursor's run, walked in reverse.
     * <p>
     * Reversing is sound because the file is key-major and {@code row_id}
     * ascends within a key, so a key's row groups are themselves ordered by
     * {@code row_id}: walking groups from {@code rgHi} down, and each decoded
     * group from its last row back, yields strictly descending ids without a
     * sort. The test asserts that against the forward cursor's output reversed
     * rather than trusting the argument.
     * <p>
     * The two in-group filters and the zone-map skip are identical to the
     * forward cursor's -- only the traversal order differs.
     */
    private class BwdCursor extends AbstractCoveringCursor {
        private long groupRows;
        private boolean hasNext;
        private int key;
        private long keyIdPtr;
        private long maxValue;
        private long minValue;
        private long next;
        private int rg;
        private int rgLo;
        private long rowIdPtr;
        private boolean detached;
        private int[] requiredCoverColumns;
        private long rowInGroup;

        /**
         * @see ParquetPostingIndexFwdReader
         */
        @Override
        public void close() {
            if (detached) {
                freeResources();
            } else {
                rowGroupBuffers.close();
            }
            keyIdPtr = 0;
            rowIdPtr = 0;
            hasNext = false;
        }

        @Override
        public boolean hasNext() {
            if (hasNext) {
                return true;
            }
            while (rg >= rgLo) {
                if (keyIdPtr == 0 && !decodeCurrentGroup()) {
                    return false;
                }
                while (rowInGroup > 0) {
                    final long i = --rowInGroup;
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
                // Group exhausted; force a decode of the previous one.
                rg--;
                keyIdPtr = 0;
            }
            return false;
        }

        @Override
        public long next() {
            hasNext = false;
            return next;
        }

        /**
         * The first row a backward cursor yields IS the last in row order, so
         * this is one step rather than a scan -- the reason the covering
         * LATEST ON path asks a backward reader for it.
         */
        @Override
        public long seekToLast() {
            if (hasNext()) {
                return next();
            }
            return -1;
        }

        private boolean decodeCurrentGroup() {
            while (rg >= rgLo) {
                groupRows = imReader.getRowGroupNumRows(rg);
                if (groupRows <= 0) {
                    rg--;
                    continue;
                }
                if (isRowGroupPruned(rg, minValue, maxValue)) {
                    rg--;
                    continue;
                }
                final DirectIntList columns = coveringProjection(requiredCoverColumns);
                rowGroupBuffers.reopen();
                decoder().decodeRowGroup(rowGroupBuffers, columns, rg, 0, (int) groupRows);
                onRowGroupDecoded();
                keyIdPtr = rowGroupBuffers.getChunkDataPtr(0);
                rowIdPtr = rowGroupBuffers.getChunkDataPtr(1);
                // Walked from the end: rowInGroup is a countdown, not an index.
                rowInGroup = groupRows;
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
                // Exhausted before it starts: rg < rgLo.
                rg = 0;
                rgLo = 1;
                return;
            }
            rgLo = Numbers.decodeLowInt(range);
            rg = Numbers.decodeHighInt(range);
        }
    }
}
