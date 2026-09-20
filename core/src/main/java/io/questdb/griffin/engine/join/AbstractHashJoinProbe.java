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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

/**
 * The half of a probe that every hash join build shares: walking the matches of one lookup
 * and reading their payload. A subclass adds the lookup itself, keyed by an INT
 * ({@link IntHashJoinBuild}) or by the probe record ({@link MapHashJoinBuild}), and writes
 * {@link #next} from what it finds.
 * <p>
 * A probe is slot-local and lives across executions: {@link #close()} releases what the
 * execution charged and {@code reopen()} rebinds the probe to the next frozen snapshot. The
 * row heap's generation, which every rebind takes, is what tells a stale probe from a live
 * one - including when a reusable build hands out the same snapshot object twice.
 */
abstract class AbstractHashJoinProbe implements FrozenHashJoinBuild.Probe {
    // A generation no snapshot carries: the heap's first freeze is 1.
    private static final long EXPIRED = -1;
    protected final HashJoinRowHeap.PayloadRecord record;
    protected final HashJoinRowHeap rowHeap;
    /** The row link of the next duplicate: a byte offset plus eight, zero at a chain end. */
    protected long next;
    protected long payloadRowsAddress;
    private long handleBase;
    private long probeGeneration;
    private long rowsCount;
    private SymbolTableSource symbols;

    AbstractHashJoinProbe(HashJoinRowHeap rowHeap) {
        this.rowHeap = rowHeap;
        this.record = rowHeap.newRecord();
    }

    @Override
    public void close() {
        record.clear();
        next = 0;
        // No snapshot generation matches this, so a read before the next reopen() trips an
        // assertion instead of reaching memory the build may since have released.
        probeGeneration = EXPIRED;
    }

    @Override
    public final Record getRecord() {
        return record;
    }

    @Override
    public final SymbolTable getSymbolTable(int columnIndex) {
        assert isCurrent();
        return record.getSymbolTable(columnIndex);
    }

    @Override
    public final boolean hasNext() {
        return next != 0;
    }

    @Override
    public final SymbolTable newSymbolTable(int columnIndex) {
        assert isCurrent() && rowHeap.isSymbol(columnIndex);
        return symbols.newSymbolTable(rowHeap.getSourceColumn(columnIndex));
    }

    @Override
    public final long next() {
        assert isCurrent();
        if (next == 0) {
            throw new IllegalStateException("hash join probe is exhausted");
        }
        final long handle = handleBase + next - 8;
        record.address = payloadRowsAddress + next - 8;
        next = Unsafe.getLong(record.address);
        return handle;
    }

    @Override
    public final void recordAt(long handle) {
        assert isCurrent();
        final long offset = handle - handleBase;
        assert offset >= 0 && offset % rowHeap.getRowSize() == 0 && offset / rowHeap.getRowSize() < rowsCount;
        record.address = payloadRowsAddress + offset;
    }

    /** True while this probe is bound to the build's current frozen snapshot. */
    protected final boolean isCurrent() {
        return probeGeneration == rowHeap.getGeneration();
    }

    /**
     * Rebinds the shared half to a frozen snapshot, after consumer drain. The subclass rebinds
     * its own lookup around this call. Symbol tables come from the build's source, so this runs
     * on the owner; only a build with SYMBOL payloads needs a source.
     */
    protected final void ofSnapshot(@Nullable SymbolTableSource symbols, long handleBase, long rowsAddress, long rowsCount, long generation) {
        this.symbols = symbols;
        this.handleBase = handleBase;
        this.payloadRowsAddress = rowsAddress;
        this.rowsCount = rowsCount;
        this.probeGeneration = generation;
        next = 0;
        record.of(symbols, generation);
    }
}
