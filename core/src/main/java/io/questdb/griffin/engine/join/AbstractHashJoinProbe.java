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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

/**
 * The half of a probe that every hash join build shares: walking the matches of one lookup
 * and positioning the payload reader at each. A subclass adds the lookup itself, keyed by an
 * INT ({@link IntHashJoinBuild}) or by the probe record ({@link MapHashJoinBuild}), and writes
 * {@link #next} from what it finds.
 * <p>
 * A probe is slot-local and lives across executions: {@link #close()} releases what the
 * execution charged and {@code reopen()} rebinds the probe to the next frozen snapshot. The
 * row heap's generation, which every rebind takes, is what tells a stale probe from a live
 * one - including when a reusable build hands out the same snapshot object twice.
 * <p>
 * A build with payload columns stores row ids, and the probe reads the columns through its own
 * {@link HashJoinPayloadSource.Reader}, which it positions on every match. A build without
 * payload columns has no reader and no record.
 */
abstract class AbstractHashJoinProbe implements FrozenHashJoinBuild.Probe {
    // A generation no snapshot carries: the heap's first freeze is 1.
    private static final long EXPIRED = -1;
    protected final HashJoinRowHeap rowHeap;
    /** The row link of the next duplicate: a byte offset plus eight, zero at a chain end. */
    protected long next;
    // Null for a build without payload columns, whose rows store no id.
    @Nullable
    protected HashJoinPayloadSource.Reader payload;
    protected long heapAddress;
    private long handleBase;
    // The source that handed out the reader, so that a snapshot of another source replaces it.
    @Nullable
    private HashJoinPayloadSource payloadSource;
    private long probeGeneration;
    private long rowsCount;

    AbstractHashJoinProbe(HashJoinRowHeap rowHeap) {
        this.rowHeap = rowHeap;
    }

    @Override
    public void close() {
        if (payload != null) {
            payload.close();
        }
        next = 0;
        // No snapshot generation matches this, so a read before the next reopen() trips an
        // assertion instead of reaching memory the build may since have released.
        probeGeneration = EXPIRED;
    }

    /** The payload reader, positioned at the last match; null for a build without payload columns. */
    @Override
    public final Record getRecord() {
        return payload;
    }

    @Override
    public final SymbolTable getSymbolTable(int columnIndex) {
        assert isCurrent() && payload != null;
        return payload.getSymbolTable(columnIndex);
    }

    @Override
    public final boolean hasNext() {
        return next != 0;
    }

    @Override
    public final SymbolTable newSymbolTable(int columnIndex) {
        assert isCurrent() && payload != null;
        return payload.newSymbolTable(columnIndex);
    }

    @Override
    public final long next() {
        assert isCurrent();
        if (next == 0) {
            throw new IllegalStateException("hash join probe is exhausted");
        }
        final long offset = next - 8;
        final long row = heapAddress + offset;
        if (payload != null) {
            payload.position(HashJoinRowHeap.getRowId(row));
        }
        next = Unsafe.getLong(row);
        return handleBase + offset;
    }

    @Override
    public final void recordAt(long handle) {
        assert isCurrent();
        final long offset = handle - handleBase;
        assert offset >= 0 && offset % rowHeap.getRowSize() == 0 && offset / rowHeap.getRowSize() < rowsCount;
        positionAt(offset);
    }

    /** True while this probe is bound to the build's current frozen snapshot. */
    protected final boolean isCurrent() {
        return probeGeneration == rowHeap.getGeneration();
    }

    /**
     * Rebinds the shared half to a frozen snapshot, after consumer drain. The subclass rebinds
     * its own lookup around this call. The payload reader takes symbol tables from the build's
     * input, so this runs on the owner; only a build with payload columns needs a source.
     */
    protected final void ofSnapshot(@Nullable HashJoinPayloadSource payloadSource, long handleBase, long rowsAddress, long rowsCount, long generation) {
        if (rowHeap.hasRowId() && payloadSource != this.payloadSource) {
            // Readers keep state of the source that made them; a reusable build keeps one source
            // across executions, so this runs once per probe there.
            payload = Misc.free(payload);
            payload = payloadSource.newReader();
            this.payloadSource = payloadSource;
        }
        this.handleBase = handleBase;
        this.heapAddress = rowsAddress;
        this.rowsCount = rowsCount;
        this.probeGeneration = generation;
        next = 0;
        if (payload != null) {
            payload.reopen();
        }
    }

    /** Positions the payload reader at the row at this byte offset of the heap. */
    protected final void positionAt(long offset) {
        if (payload != null) {
            payload.position(HashJoinRowHeap.getRowId(heapAddress + offset));
        }
    }
}
