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
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

/**
 * {@link IndexReader#DIR_FORWARD} reader over a parquet-form covering index.
 * Serves a key's postings in ascending {@code row_id} order.
 */
public class ParquetPostingIndexFwdReader extends AbstractParquetPostingIndexReader {
    private final FwdCursor cursor = new FwdCursor();

    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue) {
        cursor.of(key, minValue, maxValue);
        return cursor;
    }

    /**
     * Refuses when the caller wants covered values, because this reader cannot
     * supply them yet -- Task 7 projects the covered columns into a
     * {@link io.questdb.cairo.idx.CoveringRowCursor}.
     * <p>
     * The default implementation on {@link IndexReader} drops the argument and
     * delegates to the three-arg cursor. Inheriting that would hand the covering
     * factory a plain row cursor carrying no covered values, and the query would
     * return NO ROWS with no error -- the silent empty result this whole
     * dispatch exists to prevent, arriving one task later through a different
     * door. Refusing keeps an unfinished 2C impossible to ship quietly.
     */
    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue, int[] requiredCoverColumns) {
        if (requiredCoverColumns != null && requiredCoverColumns.length > 0) {
            throw CairoException.critical(0)
                    .put("parquet-form covering index cannot project covered columns yet [column=")
                    .put(columnName).put(", indexTxn=").put(indexTxn).put(']');
        }
        return getCursor(key, minValue, maxValue);
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
     * Row groups are NOT skipped by their row-id zone maps yet: that is pruning
     * level 2, and doing it here would make its negative control vacuous.
     */
    private class FwdCursor implements RowCursor {
        private long groupRows;
        private boolean hasNext;
        private int key;
        private long keyIdPtr;
        private long maxValue;
        private long minValue;
        private long next;
        private int rg;
        private int rgHi;
        private long rowIdPtr;
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
            rowGroupBuffers.close();
            keyIdPtr = 0;
            rowIdPtr = 0;
            hasNext = false;
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
            groupRows = imReader.getRowGroupNumRows(rg);
            if (groupRows <= 0) {
                // An empty group cannot hold a posting. Skipping it here rather
                // than calling the decoder keeps a zero-row decode -- which the
                // native side treats as an error -- off the path.
                rg++;
                return rg <= rgHi && decodeCurrentGroup();
            }
            final DirectIntList columns = decodeProjection();
            // No-op once allocated; the buffers are destroyed by close() and a
            // pooled reader is rebound without being reconstructed.
            rowGroupBuffers.reopen();
            decoder.decodeRowGroup(rowGroupBuffers, columns, rg, 0, (int) groupRows);
            // Chunk ordinals follow the projection's order, not the parquet
            // file's: key_id was added first, row_id second.
            keyIdPtr = rowGroupBuffers.getChunkDataPtr(0);
            rowIdPtr = rowGroupBuffers.getChunkDataPtr(1);
            rowInGroup = 0;
            return true;
        }

        private void of(int key, long minValue, long maxValue) {
            this.key = key;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.hasNext = false;
            this.next = -1;
            this.keyIdPtr = 0;
            this.rowIdPtr = 0;
            this.rowInGroup = 0;
            this.groupRows = 0;

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
