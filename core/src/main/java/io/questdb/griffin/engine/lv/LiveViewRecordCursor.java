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

package io.questdb.griffin.engine.lv;

import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;

/**
 * The cursor returned by {@link LiveViewRecordCursorFactory}. Pins the LV's
 * in-memory tier slot at open and releases it on close. Keeping the reader
 * visible to the refresh worker's slow-path {@code tryAcquireWrite} ensures
 * the writer trails rather than progressing past a slow reader.
 * <p>
 * Routing (V1 interim, max-disk-ts): the cursor drains the disk side first,
 * tracking the maximum disk timestamp, then iterates the pinned in-mem buffer
 * rows whose timestamp is strictly greater than that maximum. This is NOT
 * seam_ts routing (which would make in-mem authoritative for
 * {@code ts >= seam_ts}); the buffer records a seamTs but the read path does
 * not consult it. Inline apply commits the disk tier and stages the in-mem
 * rows in the same refresh cycle, so disk always covers the in-mem tier and the
 * {@code ts > maxDiskTs} predicate is never satisfied in steady state - the
 * in-mem tier is read-inert (populated but not read). The routing engages only
 * in narrow races (a cursor opening between disk apply and in-mem publish).
 * O3 replay rewrites the disk tier and resets the in-mem tier (the refresh
 * worker empties it - see {@code LiveViewRefreshJob.resetInMemoryTier}), so a
 * post-O3 cursor reads the rewritten rows from disk. Full seam_ts routing, the
 * union {@code size()}, in-mem rowIds, and the atomic O3 in-mem rebuild are
 * deferred to the benchmark-gated Phase 3a completion; until then the in-mem
 * tier gives no read benefit (its freshness payoff needs the Phase 4 hand-off
 * ring that lets it outrun disk).
 * <p>
 * The in-mem tier stores the full output row, so the cursor routes through it
 * only when the read projects every output column in declared order (see
 * {@link #isFullSchemaProjection}). Pruned or reordered projections - e.g.
 * {@code SELECT max(rn)}, where column pruning drops the timestamp and leaves
 * {@code timestampColumnIndex < 0} - serve from disk alone, which is correct
 * given the steady-state in-mem-as-subset-of-disk property.
 * <p>
 * In-mem rows have no rowId — {@link #recordAt(Record, long)}
 * targets only disk rows. ASOF JOIN as RHS and other random-access readers
 * cannot land on an in-mem row; this is consistent with the steady-state
 * in-mem-as-subset-of-disk property.
 * <p>
 * Single-shot lifecycle: the factory allocates a fresh instance per
 * {@link LiveViewRecordCursorFactory#getCursor(io.questdb.griffin.SqlExecutionContext)}.
 * {@link #of} is invoked exactly once during construction.
 */
public class LiveViewRecordCursor implements RecordCursor {

    private final MergedRecord recordA = new MergedRecord();
    private final MergedRecord recordB = new MergedRecord();
    private RecordCursor diskCursor;
    private boolean diskExhausted;
    private boolean inMemEligible;
    private long inMemRow;
    private long maxDiskTs;
    private LiveViewInMemoryBuffer pinnedSlot;
    private int slotIdx;
    private LiveViewInMemoryTier tier;
    private int timestampColumnIndex;

    public LiveViewRecordCursor() {
        this.slotIdx = -1;
    }

    @Override
    public void close() {
        releaseSlot();
        pinnedSlot = null;
        diskCursor = Misc.free(diskCursor);
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return diskCursor.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (!diskExhausted) {
            if (diskCursor.hasNext()) {
                if (inMemEligible) {
                    // Track the max disk timestamp only when the in-mem tier may
                    // contribute rows. The probe reads the projected timestamp
                    // column, which is present (index >= 0) only for full-schema
                    // reads; pruned/reordered projections serve from disk alone.
                    long ts = diskCursor.getRecord().getTimestamp(timestampColumnIndex);
                    if (maxDiskTs == Numbers.LONG_NULL || ts > maxDiskTs) {
                        maxDiskTs = ts;
                    }
                }
                recordA.toDiskMode();
                return true;
            }
            diskExhausted = true;
        }
        if (inMemEligible) {
            long rn = pinnedSlot.rowCount();
            while (inMemRow + 1 < rn) {
                inMemRow++;
                long ts = pinnedSlot.getLong(inMemRow, timestampColumnIndex);
                if (maxDiskTs == Numbers.LONG_NULL || ts > maxDiskTs) {
                    recordA.toInMemMode(inMemRow);
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return diskCursor.newSymbolTable(columnIndex);
    }

    public void of(RecordCursor diskCursor, RecordMetadata baseMetadata, LiveViewInstance instance, int timestampColumnIndex) {
        releaseSlot();
        this.diskCursor = diskCursor;
        this.timestampColumnIndex = timestampColumnIndex;
        this.maxDiskTs = Numbers.LONG_NULL;
        this.diskExhausted = false;
        this.inMemRow = -1;
        this.pinnedSlot = null;
        this.inMemEligible = false;
        if (instance != null) {
            LiveViewInMemoryTier candidate = instance.getInMemoryTier();
            if (candidate != null) {
                int pin = candidate.acquireRead();
                if (pin >= 0) {
                    // acquireRead succeeded: keep the tier reference so close()
                    // can call releaseRead with the matching index. A return of
                    // -1 means the tier was concurrently closed (LV dropped);
                    // in that case we hold neither the global pin lease nor a
                    // per-slot rc and must not touch the tier again.
                    this.tier = candidate;
                    this.slotIdx = pin;
                    this.pinnedSlot = candidate.getSlot(pin);
                    this.inMemEligible = isFullSchemaProjection(baseMetadata, pinnedSlot, timestampColumnIndex);
                }
            }
        }
        recordA.bindDisk(diskCursor.getRecord(), this, pinnedSlot);
        recordB.bindDisk(diskCursor.getRecordB(), this, pinnedSlot);
    }

    @Override
    public long preComputedStateSize() {
        return diskCursor == null ? 0 : diskCursor.preComputedStateSize();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        // In-mem rows are not addressable via rowId — only disk
        // rows have rowIds. Random-access readers (ASOF JOIN as RHS, etc.)
        // land on the disk side; the in-mem tier is a subset of disk in
        // steady state so no data is hidden by this choice.
        MergedRecord mr = (MergedRecord) record;
        mr.toDiskMode();
        diskCursor.recordAt(mr.diskRecord(), atRowId);
    }

    @Override
    public long size() {
        // In steady state in-mem is a subset of disk, so disk.size()
        // is the cursor's actual size. Returning -1 (unknown) would defeat
        // LIMIT pushdown for the disk slice.
        return diskCursor.size();
    }

    @Override
    public void toTop() {
        if (diskCursor != null) {
            diskCursor.toTop();
        }
        maxDiskTs = Numbers.LONG_NULL;
        diskExhausted = false;
        inMemRow = -1;
    }

    /**
     * The in-mem tier stores the live view's full output row. A read whose
     * projection prunes or reorders columns would index the buffer by the wrong
     * column, and a read that prunes the timestamp leaves
     * {@code timestampColumnIndex < 0}, which would address the buffer and the
     * disk record out of bounds. Such reads serve from disk only, which is
     * correct because the in-mem tier is a subset of disk in steady state. Only
     * an identity projection (every output column, in declared order) may route
     * through the tier; the type-by-type match below establishes that.
     */
    private static boolean isFullSchemaProjection(
            RecordMetadata baseMetadata,
            LiveViewInMemoryBuffer buffer,
            int timestampColumnIndex
    ) {
        if (timestampColumnIndex < 0 || buffer == null) {
            return false;
        }
        final int columnCount = buffer.columnCount();
        if (baseMetadata.getColumnCount() != columnCount) {
            return false;
        }
        for (int i = 0; i < columnCount; i++) {
            if (baseMetadata.getColumnType(i) != buffer.columnType(i)) {
                return false;
            }
        }
        return true;
    }

    private void releaseSlot() {
        if (tier != null && slotIdx >= 0) {
            // Safe even after the LV's DROP marked the tier closed: the deferred-
            // close protocol on LiveViewInMemoryTier keeps native memory alive
            // until the last pin drains (DROP LIVE VIEW "modulo cursor pins").
            tier.releaseRead(slotIdx);
        }
        tier = null;
        slotIdx = -1;
    }

    /**
     * Mode-switching record proxy. In disk mode every accessor delegates to
     * the bound {@link Record} from the disk cursor via {@link DelegatingRecord}.
     * In in-mem mode the supported fixed-width accessors read directly from
     * the pinned buffer; SYMBOL ids resolve via the cursor's
     * {@link RecordCursor#getSymbolTable(int)} (the in-mem tier stores raw
     * {@code lv_id} ints and the LV's own symbol table maps them to strings).
     * <p>
     * Var-length accessors (STRING / VARCHAR / BINARY / ARRAY) inherit the
     * disk-only delegation. Those columns prevent the in-mem tier from being
     * allocated in the first place
     * (see {@link LiveViewInMemoryBuffer#areColumnTypesSupported}), so
     * {@code inMemMode == true} is unreachable for LVs whose schema contains
     * them.
     */
    private static class MergedRecord extends DelegatingRecord {
        private LiveViewInMemoryBuffer buffer;
        private long bufferRow;
        private RecordCursor cursor;
        private boolean inMemMode;

        @Override
        public boolean getBool(int col) {
            return inMemMode ? buffer.getBool(bufferRow, col) : super.getBool(col);
        }

        @Override
        public byte getByte(int col) {
            return inMemMode ? buffer.getByte(bufferRow, col) : super.getByte(col);
        }

        @Override
        public char getChar(int col) {
            return inMemMode ? (char) buffer.getShort(bufferRow, col) : super.getChar(col);
        }

        @Override
        public long getDate(int col) {
            return inMemMode ? buffer.getLong(bufferRow, col) : super.getDate(col);
        }

        @Override
        public double getDouble(int col) {
            return inMemMode ? buffer.getDouble(bufferRow, col) : super.getDouble(col);
        }

        @Override
        public float getFloat(int col) {
            return inMemMode ? buffer.getFloat(bufferRow, col) : super.getFloat(col);
        }

        @Override
        public byte getGeoByte(int col) {
            return inMemMode ? buffer.getByte(bufferRow, col) : super.getGeoByte(col);
        }

        @Override
        public int getGeoInt(int col) {
            return inMemMode ? buffer.getInt(bufferRow, col) : super.getGeoInt(col);
        }

        @Override
        public long getGeoLong(int col) {
            return inMemMode ? buffer.getLong(bufferRow, col) : super.getGeoLong(col);
        }

        @Override
        public short getGeoShort(int col) {
            return inMemMode ? buffer.getShort(bufferRow, col) : super.getGeoShort(col);
        }

        @Override
        public int getIPv4(int col) {
            return inMemMode ? buffer.getInt(bufferRow, col) : super.getIPv4(col);
        }

        @Override
        public int getInt(int col) {
            return inMemMode ? buffer.getInt(bufferRow, col) : super.getInt(col);
        }

        @Override
        public long getLong(int col) {
            return inMemMode ? buffer.getLong(bufferRow, col) : super.getLong(col);
        }

        @Override
        public long getRowId() {
            // In-mem rows have no rowId — they are not addressable
            // via recordAt. Throw on access to fail loudly if a caller tries
            // to round-trip through random access.
            if (inMemMode) {
                throw new UnsupportedOperationException(
                        "live view in-mem row has no rowId"
                );
            }
            return base.getRowId();
        }

        @Override
        public short getShort(int col) {
            return inMemMode ? buffer.getShort(bufferRow, col) : super.getShort(col);
        }

        @Override
        public CharSequence getSymA(int col) {
            if (!inMemMode) {
                return super.getSymA(col);
            }
            return cursor.getSymbolTable(col).valueOf(buffer.getInt(bufferRow, col));
        }

        @Override
        public CharSequence getSymB(int col) {
            if (!inMemMode) {
                return super.getSymB(col);
            }
            return cursor.getSymbolTable(col).valueOf(buffer.getInt(bufferRow, col));
        }

        @Override
        public long getTimestamp(int col) {
            return inMemMode ? buffer.getLong(bufferRow, col) : super.getTimestamp(col);
        }

        void bindDisk(Record diskRecord, RecordCursor cursor, LiveViewInMemoryBuffer buffer) {
            this.base = diskRecord;
            this.cursor = cursor;
            this.buffer = buffer;
            this.bufferRow = -1;
            this.inMemMode = false;
        }

        Record diskRecord() {
            return base;
        }

        void toDiskMode() {
            this.inMemMode = false;
        }

        void toInMemMode(long row) {
            this.bufferRow = row;
            this.inMemMode = true;
        }
    }
}
