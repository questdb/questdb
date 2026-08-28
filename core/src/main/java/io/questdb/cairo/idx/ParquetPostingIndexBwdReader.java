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
 * {@link IndexReader#DIR_BACKWARD} reader over a parquet-form covering index.
 * Serves a key's postings in descending {@code row_id} order.
 */
public class ParquetPostingIndexBwdReader extends AbstractParquetPostingIndexReader {
    /**
     * Amortising a whole-group decode needs keys to amortise OVER. Same bound
     * and same reasoning as the forward reader: a group holding a handful of
     * wide keys decodes just as many rows either way, so the whole-group read
     * buys nothing and costs one large buffer.
     */
    private static final int WHOLE_GROUP_KEY_THRESHOLD = 8;
    /**
     * @see ParquetPostingIndexFwdReader
     */
    private final ObjList<BwdCursor> cursors = new ObjList<>();
    private final ObjList<BwdCursor> freeCursors = new ObjList<>();

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
        final BwdCursor detached = new BwdCursor();
        detached.detached = true;
        detached.of(key, minValue, maxValue, requiredCoverColumns);
        return detached;
    }

    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue) {
        return getCursor(key, minValue, maxValue, null);
    }

    /**
     * @see ParquetPostingIndexFwdReader#getCursor(int, long, long, int[])
     */
    private BwdCursor nextCursor() {
        final BwdCursor c;
        if (freeCursors.size() > 0) {
            c = freeCursors.popLast();
            c.pooled = false;
        } else {
            c = new BwdCursor();
            cursors.add(c);
        }
        return c;
    }

    /**
     * @see ParquetPostingIndexFwdReader#getCursor(int, long, long, int[])
     */
    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue, int[] requiredCoverColumns) {
        final BwdCursor c = nextCursor();
        try {
            c.of(key, minValue, maxValue, requiredCoverColumns);
        } catch (Throwable th) {
            c.pooled = true;
            freeCursors.add(c);
            throw th;
        }
        return c;
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
        private boolean decodedGroup;
        private long maxValue;
        private long minValue;
        private long next;
        private long nullCount;
        private long nullPos;
        private int rg;
        private int rgLo;
        private long rowHi;
        private long rowIdPtr;
        private long rowLo;
        private boolean detached;
        private boolean pooled;
        private int[] requiredCoverColumns;
        private long rowInGroup;
        // Lower bound of the countdown. Zero when a decode produced a buffer
        // holding only the key's rows; rowLo when reading the group's row_id
        // column in place, where the key's run sits at an offset.
        private long rowFloor;
        private int cachedRowGroup = -1;
        private int[] cachedCovers;
        private int lastTouchedRowGroup = -1;
        // Set when the window was narrowed by seeking, so hasNext can skip the
        // per-row bounds test.
        private boolean windowNarrowed;

        /**
         * Releases the decoded row group. Mirrors the forward cursor: the cache
         * handle MUST be cleared with the buffers it points at, or a later
         * cache hit would read freed memory.
         */
        @Override
        protected void freeResources() {
            cachedRowGroup = -1;
            cachedCovers = null;
            lastTouchedRowGroup = -1;
            super.freeResources();
        }

        /**
         * @see ParquetPostingIndexFwdReader
         */
        @Override
        public void close() {
            keyProbe = Misc.free(keyProbe);
            if (detached) {
                freeResources();
            } else {
                // The decoded group is KEPT for a pooled cursor, matching the
                // forward cursor. Closing the buffers here is what forced a
                // fresh decode on every key even when the next key lived in the
                // group just decoded -- a scan paid one decode PER KEY.
                if (!pooled) {
                    pooled = true;
                    freeCursors.add(this);
                }
            }
            decodedGroup = false;
            rowIdPtr = 0;
            rowFloor = 0;
            windowNarrowed = false;
            hasNext = false;
        }

        @Override
        public boolean hasNext() {
            if (hasNext) {
                return true;
            }
            while (rg >= rgLo) {
                if (!decodedGroup && !decodeCurrentGroup()) {
                    // Out of groups, NOT out of answers: the implicit-null
                    // prefix below still has to be served. Returning here is
                    // what would drop it.
                    break;
                }
                while (rowInGroup > rowFloor) {
                    final long i = --rowInGroup;
                    final long rowId = Unsafe.getUnsafe().getLong(rowIdPtr + (i << 3));
                    if (!windowNarrowed && (rowId < minValue || rowId > maxValue)) {
                        continue;
                    }
                    setEmittedRow(i);
                    next = rowId;
                    hasNext = true;
                    return true;
                }
                // Group exhausted; force a decode of the previous one.
                rg--;
                decodedGroup = false;
            }
            if (nullPos > minValue) {
                // The implicit-null prefix, emitted LAST here rather than first:
                // it is the lowest run of row ids, and this cursor descends.
                // Same rows as the forward cursor's, same bound, opposite end.
                next = --nullPos;
                // No decoded group backs a prefix row.
                setEmittedRow(-1);
                hasNext = true;
                return true;
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
                // Pruning level 3: bound the value decode to the key's own
                // rows. In a packed group most rows belong to other keys, and
                // decoding them costs row_id plus every covered column.
                final long keyRange = imReader.getKeyRowRangeInGroup(rg, key);
                if (keyRange == IndexMetaFileReader.KEY_ABSENT) {
                    // The directory said this group COULD hold the key; the
                    // probe says it does not. An ordinary miss, not an error.
                    rg--;
                    continue;
                }
                rowLo = Numbers.decodeLowInt(keyRange);
                rowHi = Numbers.decodeHighInt(keyRange);
                if (packedPayload) {
                    // Same shape as the forward reader's packed path, and
                    // unconditional for the same reason: a packed file has no
                    // row_id column to fall back to, so a blob that cannot be
                    // addressed is a file this reader cannot serve rather than
                    // one it should serve slowly.
                    if (packedDataAddr(rg) == 0) {
                        throw CairoException.critical(0)
                                .put("covering index packed payload is not addressable [rowGroup=").put(rg)
                                .put(", column=").put(columnName).put(']');
                    }
                    clearCoverOrdinals();
                    cachedRowGroup = -1;
                    cachedCovers = null;
                    rowIdPtr = unpackRowIds(rg, (int) rowLo, (int) (rowHi - rowLo));
                    // Ordinals are relative to the widened run, so both bounds
                    // restart at 0. The run still ascends, so this descending
                    // cursor walks it from the top exactly as before.
                    rowFloor = seekFirstAtLeast(rowIdPtr, 0, rowHi - rowLo, minValue);
                    rowInGroup = seekFirstAbove(rowIdPtr, rowFloor, rowHi - rowLo, maxValue);
                    if (rowInGroup <= rowFloor) {
                        rg--;
                        continue;
                    }
                    windowNarrowed = true;
                    decodedGroup = true;
                    lastTouchedRowGroup = rg;
                    return true;
                }
                // Fastest path, and the forward reader has had it all along:
                // when the caller wants no covered value and row_id's chunk is a
                // single uncompressed PLAIN page, the values are just int64s in
                // the mapping. Reading them in place skips the JNI crossing, the
                // thrift page header and the buffer -- which is the whole cost of
                // a backward point read once pruning has narrowed it. Without
                // this the cursor decoded a row group PER KEY.
                if (requiredCoverColumns == null || requiredCoverColumns.length == 0) {
                    final long dataOffset = rowIdDataOffset(rg);
                    if (dataOffset >= 0) {
                        // No projection is built on this path, so last bind's
                        // ordinals would otherwise survive into this one. The
                        // CACHE has to go with them: a later covers-bearing
                        // lookup on the same group takes the cache-hit path,
                        // which skips coveringProjection and would then read
                        // ordinals this cleared. They are one invariant, not two.
                        clearCoverOrdinals();
                        cachedRowGroup = -1;
                        cachedCovers = null;
                        rowIdPtr = pidxAddr + dataOffset;
                        // The run ascends even though this cursor descends, so
                        // the window is a sub-range that can be found rather
                        // than filtered for.
                        rowFloor = seekFirstAtLeast(rowIdPtr, rowLo, rowHi, minValue);
                        rowInGroup = seekFirstAbove(rowIdPtr, rowFloor, rowHi, maxValue);
                        if (rowInGroup <= rowFloor) {
                            // The window excludes this group's run entirely.
                            rg--;
                            continue;
                        }
                        windowNarrowed = true;
                        decodedGroup = true;
                        lastTouchedRowGroup = rg;
                        return true;
                    }
                }
                final boolean coversMatch = cachedCovers == requiredCoverColumns
                        || Arrays.equals(cachedCovers, requiredCoverColumns);
                if (cachedRowGroup == rg && coversMatch) {
                    // Already in the buffer, whole. No decode at all.
                    decodedGroup = true;
                    windowNarrowed = false;
                    rowIdPtr = rowGroupBuffers.getChunkDataPtr(0);
                    // Whole-group indices, so the key's run sits at an offset.
                    rowFloor = rowLo;
                    rowInGroup = rowHi;
                    lastTouchedRowGroup = rg;
                    return true;
                }
                final DirectIntList columns = coveringProjection(requiredCoverColumns, false);
                rowGroupBuffers.reopen();
                // Invalidate BEFORE the decode, not after it. A throw from
                // decodeRowGroup can leave the buffers partially overwritten
                // while cachedRowGroup still names the PREVIOUS group, and a
                // later lookup for that group would then be served a corrupt
                // buffer as a cache hit -- a silent wrong answer that outlives
                // the failed query on a pooled cursor.
                cachedRowGroup = -1;
                cachedCovers = null;
                if (rg == lastTouchedRowGroup && imReader.getRowGroupKeyCount(rg) >= WHOLE_GROUP_KEY_THRESHOLD) {
                    // Second consecutive key from a group with many keys: a
                    // scan. Decode it whole and keep it, so the rest come free.
                    decoder().decodeRowGroup(rowGroupBuffers, columns, rg, 0, (int) groupRows);
                    onRowGroupDecoded(groupRows);
                    cachedRowGroup = rg;
                    cachedCovers = requiredCoverColumns;
                    rowIdPtr = rowGroupBuffers.getChunkDataPtr(0);
                    rowFloor = rowLo;
                    rowInGroup = rowHi;
                    decodedGroup = true;
                    windowNarrowed = false;
                    lastTouchedRowGroup = rg;
                    return true;
                }
                decoder().decodeRowGroup(rowGroupBuffers, columns, rg, (int) rowLo, (int) rowHi);
                onRowGroupDecoded(rowHi - rowLo);
                // The buffer holds the key's run alone, so indices restart.
                // cachedRowGroup was already invalidated above.
                lastTouchedRowGroup = rg;
                groupRows = rowHi - rowLo;
                // key_id is not in the projection: the decoded window is the
                // key's own run, so there is nothing to filter against.
                decodedGroup = true;
                windowNarrowed = false;
                rowIdPtr = rowGroupBuffers.getChunkDataPtr(0);
                // Walked from the end: rowInGroup is a countdown, not an index.
                // The buffer holds only the key's rows, so the floor is zero.
                rowFloor = 0;
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
            this.decodedGroup = false;
            this.rowIdPtr = 0;
            this.rowInGroup = 0;
            this.rowFloor = 0;
            this.windowNarrowed = false;
            this.groupRows = 0;
            setEmittedRow(-1);
            // @see ParquetPostingIndexFwdReader.FwdCursor#of -- same bound, but
            // nullPos starts at the TOP of the prefix and counts down, so the
            // prefix leaves the cursor after the postings and descending order
            // is preserved without a sort.
            if (key == 0 && columnTop > 0 && minValue < columnTop) {
                final long hi = maxValue == Long.MAX_VALUE ? Long.MAX_VALUE : maxValue + 1;
                this.nullCount = Math.min(columnTop, hi);
                this.nullPos = this.nullCount;
            } else {
                this.nullCount = 0;
                this.nullPos = 0;
            }

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
