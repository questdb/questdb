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
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import java.util.Arrays;

/**
 * {@link IndexReader#DIR_FORWARD} reader over a parquet-form covering index.
 * Serves a key's postings in ascending {@code row_id} order.
 */
public class ParquetPostingIndexFwdReader extends AbstractParquetPostingIndexReader {
    /**
     * Keys a row group must hold before a scan over it is served by decoding
     * the group whole rather than each key's run in turn. Below it the two read
     * the same number of rows and the whole-group buffer is pure overhead.
     */
    private static final int WHOLE_GROUP_KEY_THRESHOLD = 8;
    /**
     * Every pooled cursor this reader has handed out, free or checked out, so
     * that close() releases them all without depending on a caller having
     * returned them.
     */
    private final ObjList<FwdCursor> cursors = new ObjList<>();
    private final ObjList<FwdCursor> freeCursors = new ObjList<>();

    /**
     * Frees every pooled cursor's decode buffers alongside the reader's
     * mappings. Detached cursors are the worker's to close -- they are handed
     * out one per call and never returned here.
     */
    @Override
    public void close() {
        for (int i = 0, n = cursors.size(); i < n; i++) {
            cursors.getQuick(i).freeResources();
        }
        cursors.clear();
        freeCursors.clear();
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
        return getCursor(key, minValue, maxValue, null);
    }

    /**
     * Draws a cursor from the free list, or builds one.
     * <p>
     * A reader serves MORE THAN ONE cursor at a time, so a single instance
     * re-{@code of()}-ed per call is not enough: {@code CoveringIndexRecordCursorFactory}
     * asks for the next key's cursor BEFORE freeing the one it is holding
     * ({@code tryOpenKey}, {@code findLatestRow}), and an interval scan hands
     * the same partition -- so the same reader -- to that loop more than once.
     * With one instance the second call resets the first mid-iteration and the
     * subsequent free closes the cursor just handed out. Both native readers
     * pool for the same reason.
     */
    private FwdCursor nextCursor() {
        final FwdCursor c;
        if (freeCursors.size() > 0) {
            c = freeCursors.popLast();
            c.pooled = false;
        } else {
            c = new FwdCursor();
            cursors.add(c);
        }
        return c;
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
        final FwdCursor c = nextCursor();
        try {
            c.of(key, minValue, maxValue, requiredCoverColumns);
        } catch (Throwable th) {
            // Popped from the pool but not yet owned by the caller, so nothing
            // else would ever return it.
            c.pooled = true;
            freeCursors.add(c);
            throw th;
        }
        return c;
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
        private boolean decodedGroup;
        /** True while {@link #rowIdPtr} addresses the mapping, not a decode buffer. */
        private boolean directRowIds;
        /** True when the range was cut to the window, so no row needs testing. */
        private boolean windowNarrowed;
        /** Row group {@link #rowGroupBuffers} currently holds, or -1. */
        private int cachedRowGroup = -1;
        /** Cover slots the cached decode projected, so a different ask re-decodes. */
        private int[] cachedCovers;
        /** The group the PREVIOUS lookup touched, which is how a scan is spotted. */
        private int lastTouchedRowGroup = -1;
        private long maxValue;
        private long minValue;
        private long next;
        private long nullCount;
        private long nullPos;
        private int rg;
        private int rgHi;
        private long rowHi;
        private long rowIdPtr;
        private long rowLo;
        private boolean detached;
        private boolean pooled;
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
        protected void freeResources() {
            cachedRowGroup = -1;
            cachedCovers = null;
            lastTouchedRowGroup = -1;
            super.freeResources();
        }

        @Override
        public void close() {
            keyProbe = Misc.free(keyProbe);
            if (detached) {
                freeResources();
            } else {
                // The decoded group is KEPT for a pooled cursor. Keys arrive in
                // ascending order and a row group is key-major, so the next
                // lookup usually wants the group this one just decoded --
                // scanning 1M keys otherwise pays 1M decodes where a few hundred
                // would do. Freed when the reader closes, which frees every
                // pooled cursor; a detached cursor still releases immediately,
                // since nothing comes back for it.
                if (!pooled) {
                    // Guarded so a double close cannot put one cursor on the
                    // free list twice and have two callers handed the same
                    // instance.
                    pooled = true;
                    freeCursors.add(this);
                }
            }
            decodedGroup = false;
            directRowIds = false;
            windowNarrowed = false;
            rowIdPtr = 0;
            hasNext = false;
        }

        /**
         * First index in {@code [lo, hi)} whose row id is at least
         * {@code value}, or {@code hi}. The run ascends, so this is a binary
         * search rather than a scan.
         */
        private static long seekFirstAtLeast(long rowIdPtr, long lo, long hi, long value) {
            while (lo < hi) {
                final long mid = (lo + hi) >>> 1;
                if (Unsafe.getUnsafe().getLong(rowIdPtr + (mid << 3)) < value) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            return lo;
        }

        /** First index in {@code [lo, hi)} whose row id exceeds {@code value}, or {@code hi}. */
        private static long seekFirstAbove(long rowIdPtr, long lo, long hi, long value) {
            while (lo < hi) {
                final long mid = (lo + hi) >>> 1;
                if (Unsafe.getUnsafe().getLong(rowIdPtr + (mid << 3)) <= value) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            return lo;
        }

        @Override
        public boolean hasNext() {
            if (hasNext) {
                return true;
            }
            if (nullPos < nullCount) {
                // The implicit-null prefix, which comes FIRST in row order and
                // is not in the index at all: rows below columnTop carry no
                // value, so key 0 owns them implicitly. Emitted here rather
                // than by a wrapper cursor because this reader's own
                // countMatchesClamped and selectKthMatch already count them --
                // a cursor that skipped them would make count(*) disagree with
                // the rows a scan produces, and would trip the covered
                // re-decode row count check outright.
                next = nullPos++;
                // No decoded group backs a prefix row, so no covered value can
                // be served for it. -1 makes isCoveredAvailable false and the
                // accessors throw, rather than handing back whatever row the
                // last decode left addressed.
                setEmittedRow(-1);
                hasNext = true;
                return true;
            }
            while (rg <= rgHi) {
                if (!decodedGroup && !decodeCurrentGroup()) {
                    return false;
                }
                while (rowInGroup < groupRows) {
                    final long i = rowInGroup++;
                    final long rowId = Unsafe.getUnsafe().getLong(rowIdPtr + (i << 3));
                    // seekFirstAtLeast/seekFirstAbove cut the range to exactly
                    // the rows inside the window, so when they ran every row
                    // left is emitted and the bounds test below is a tautology.
                    // This is the hottest line in a scan -- the loop was 23% of
                    // a low-cardinality scan profile -- so it is worth skipping.
                    if (!windowNarrowed && (rowId < minValue || rowId > maxValue)) {
                        continue;
                    }
                    setEmittedRow(i);
                    next = rowId;
                    hasNext = true;
                    return true;
                }
                // Group exhausted; force a decode of the next one.
                rg++;
                decodedGroup = false;
            }
            return false;
        }

        /**
         * Row ids are returned RELATIVE to {@code minValue}, which is the
         * contract {@code IndexReader.getCursor} states and what the native
         * readers do. Returning the absolute id agrees with the native reader
         * only when {@code minValue} is 0 -- true of a single-partition query
         * and false of every page frame that starts mid-partition, so it is
         * invisible to any test that does not compare the two readers over a
         * window with a non-zero lower bound.
         */
        @Override
        public long next() {
            hasNext = false;
            return next - minValue;
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
                final long keyRange = imReader.getKeyRowRangeInGroup(rg, key);
                if (keyRange == IndexMetaFileReader.KEY_ABSENT) {
                    // The directory said this group COULD hold the key; the
                    // probe says it does not. An ordinary miss, not an error.
                    rg++;
                    continue;
                }
                rowLo = Numbers.decodeLowInt(keyRange);
                rowHi = Numbers.decodeHighInt(keyRange);
                // Three ways to serve the key's run, and which one is cheapest
                // depends on whether this is a scan or a point read.
                //
                // A bounded decode reads only the key's rows, which is right for
                // a point read: the whole cost is the one page header it parses,
                // and decoding 4 rows costs no more than decoding 8192.
                //
                // But that same fixed cost is ruinous for a SCAN, which asks for
                // every key in turn: a group holding 2048 keys is decoded 2048
                // times. Decoding it ONCE and serving every key from the buffer
                // turns that into one decode -- and the _im directory gives each
                // key's offsets within the group, so no key_id is needed to find
                // them.
                //
                // A scan is spotted by the previous lookup having landed in this
                // same group. Random point reads over many groups almost never
                // do, so they keep the bounded decode.
                // Fastest path of all: when the caller wants no covered value
                // and row_id's chunk is a single uncompressed PLAIN page, its
                // values are just int64s in the mapping, so the key's run is
                // read straight from there. No JNI crossing, no thrift page
                // header, no buffer -- which is the entire cost of a point read
                // once pruning has narrowed it to a handful of rows.
                if (requiredCoverColumns == null || requiredCoverColumns.length == 0) {
                    final long dataOffset = rowIdDataOffset(rg);
                    if (dataOffset >= 0) {
                        decodedGroup = true;
                        directRowIds = true;
                        rowIdPtr = pidxAddr + dataOffset;
                        // The run is ascending, so the caller's window is a
                        // sub-range of it and can be found rather than filtered
                        // for. Walking the whole run and testing each row is
                        // what made a windowed read cost the same as an
                        // unwindowed one, where the native chain seeks into its
                        // stride index and reads only what the window asks for.
                        rowInGroup = seekFirstAtLeast(rowIdPtr, rowLo, rowHi, minValue);
                        groupRows = seekFirstAbove(rowIdPtr, rowInGroup, rowHi, maxValue);
                        windowNarrowed = true;
                        lastTouchedRowGroup = rg;
                        if (rowInGroup >= groupRows) {
                            // The window excludes this group's run entirely.
                            rg++;
                            decodedGroup = false;
                            continue;
                        }
                        return true;
                    }
                }
                final boolean coversMatch = cachedCovers == requiredCoverColumns
                        || Arrays.equals(cachedCovers, requiredCoverColumns);
                if (cachedRowGroup == rg && coversMatch) {
                    // Already in the buffer, whole. No decode at all.
                    decodedGroup = true;
                    directRowIds = false;
                    windowNarrowed = false;
                    rowIdPtr = rowGroupBuffers.getChunkDataPtr(0);
                    rowInGroup = rowLo;
                    groupRows = rowHi;
                    lastTouchedRowGroup = rg;
                    return true;
                }
                final DirectIntList columns = coveringProjection(requiredCoverColumns, false);
                rowGroupBuffers.reopen();
                // Amortising needs keys to amortise OVER. A group holding a
                // handful of wide keys decodes just as many rows either way, so
                // the whole-group read buys nothing and costs one large buffer:
                // on a 16-key, 400k-row fixture it measured 2x SLOWER than the
                // bounded decode it replaced.
                if (rg == lastTouchedRowGroup && imReader.getRowGroupKeyCount(rg) >= WHOLE_GROUP_KEY_THRESHOLD) {
                    // Second consecutive key from a group with many keys: a
                    // scan. Decode it whole and keep it, so the rest come free.
                    decoder().decodeRowGroup(rowGroupBuffers, columns, rg, 0, (int) groupRows);
                    onRowGroupDecoded(groupRows);
                    cachedRowGroup = rg;
                    cachedCovers = requiredCoverColumns;
                    rowIdPtr = rowGroupBuffers.getChunkDataPtr(0);
                    rowInGroup = rowLo;
                    groupRows = rowHi;
                } else {
                    decoder().decodeRowGroup(rowGroupBuffers, columns, rg, (int) rowLo, (int) rowHi);
                    onRowGroupDecoded(rowHi - rowLo);
                    // The buffer holds the key's run alone, so indices restart.
                    cachedRowGroup = -1;
                    cachedCovers = null;
                    rowIdPtr = rowGroupBuffers.getChunkDataPtr(0);
                    rowInGroup = 0;
                    groupRows = rowHi - rowLo;
                }
                decodedGroup = true;
                directRowIds = false;
                windowNarrowed = false;
                lastTouchedRowGroup = rg;
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
            this.decodedGroup = false;
            this.rowIdPtr = 0;
            this.rowInGroup = 0;
            this.groupRows = 0;
            setEmittedRow(-1);
            // Bounded by the UNCLAMPED caller max and by columnTop only: the
            // prefix is independent of the index, so getEntryMaxValue does not
            // clamp it. Matches PostingIndexFwdReader.getCursor's NullCursor,
            // including its Long.MAX_VALUE guard -- maxValue + 1 wraps negative
            // there and would leave nullCount at Long.MIN_VALUE.
            if (key == 0 && columnTop > 0 && minValue < columnTop) {
                final long hi = maxValue == Long.MAX_VALUE ? Long.MAX_VALUE : maxValue + 1;
                this.nullCount = Math.min(columnTop, hi);
                this.nullPos = minValue;
            } else {
                this.nullCount = 0;
                this.nullPos = 0;
            }

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
