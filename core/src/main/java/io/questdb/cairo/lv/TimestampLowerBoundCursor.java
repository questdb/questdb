/*******************************************************************************
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

package io.questdb.cairo.lv;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;

/**
 * Skips rows from the base cursor whose timestamp is strictly less than
 * {@code lowTs}. Used by the live-view O3 replay path to bound a base
 * {@code TableReader} scan to {@code [lowTs, +inf)} without compiling a
 * filter Function (the LV's pre-compiled SELECT does not carry a ts range
 * predicate, and adding one would require bind-variable infrastructure
 * the LV path does not own).
 * <p>
 * Rows are expected to arrive in ts-ascending order (the
 * {@code FullFwdPartitionFrameCursor} -&gt; {@code PageFrameRecordCursorImpl}
 * stack reads partition-by-partition in chronological order, and within a
 * partition each post-apply read is ts-ordered). The cursor is therefore a
 * one-pass skip-prefix: once the boundary is crossed, every subsequent
 * {@link #hasNext()} delegates directly to the base.
 * <p>
 * The wrapper exposes the same {@link Record} / {@link Record} B / symbol
 * table sources as its base, so downstream cursors observe an identical
 * row stream sans the prefix.
 */
final class TimestampLowerBoundCursor implements RecordCursor {
    private RecordCursor base;
    private long lowTs;
    private boolean skipped;
    private int timestampIndex = -1;

    @Override
    public void close() {
        base = null;
        timestampIndex = -1;
        lowTs = 0L;
        skipped = false;
    }

    @Override
    public Record getRecord() {
        return base.getRecord();
    }

    @Override
    public Record getRecordB() {
        return base.getRecordB();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return base.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (skipped) {
            return base.hasNext();
        }
        // First-call skip-prefix: advance until we cross the boundary.
        // Rows are ts-ascending, so this terminates at the first qualifying
        // row or at end-of-stream.
        while (base.hasNext()) {
            if (base.getRecord().getTimestamp(timestampIndex) >= lowTs) {
                skipped = true;
                return true;
            }
        }
        skipped = true;
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return base.newSymbolTable(columnIndex);
    }

    public void of(RecordCursor base, int timestampIndex, long lowTs) {
        this.base = base;
        this.timestampIndex = timestampIndex;
        this.lowTs = lowTs;
        this.skipped = false;
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        base.recordAt(record, atRowId);
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toTop() {
        base.toTop();
        skipped = false;
    }
}
