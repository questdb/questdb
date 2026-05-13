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

import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Misc;

/**
 * The cursor returned by {@link LiveViewRecordCursorFactory}. Pins the LV's
 * in-memory tier slot at open and releases it on close — RFC 123 §"Stall
 * behavior" relies on this to keep readers visible to the refresh worker's
 * slow-path {@code tryAcquireWrite} so the writer trails rather than
 * progressing past a slow reader.
 * <p>
 * Phase 1b reads still come entirely from the disk cursor. Every in-mem row
 * is also durable on disk because the inline apply (Phase 1b Commit 1) commits
 * synchronously with the LV WAL write, so disk-only reads give the complete
 * picture. Seam-routing reads layer onto this scaffolding once a later phase
 * makes the in-mem tier strictly fresher than disk (e.g. Phase 4 hand-off ring
 * with deferred apply).
 * <p>
 * Single-shot lifecycle: the factory allocates a fresh instance per
 * {@link LiveViewRecordCursorFactory#getCursor(io.questdb.griffin.SqlExecutionContext)}.
 * {@link #of} is invoked exactly once during construction.
 */
public class LiveViewRecordCursor implements RecordCursor {

    private RecordCursor diskCursor;
    private int slotIdx;
    private LiveViewInMemoryTier tier;

    public LiveViewRecordCursor() {
        this.slotIdx = -1;
    }

    @Override
    public void close() {
        releaseSlot();
        diskCursor = Misc.free(diskCursor);
    }

    @Override
    public Record getRecord() {
        return diskCursor.getRecord();
    }

    @Override
    public Record getRecordB() {
        return diskCursor.getRecordB();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return diskCursor.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        return diskCursor.hasNext();
    }

    public void of(RecordCursor diskCursor, LiveViewInstance instance) {
        releaseSlot();
        this.diskCursor = diskCursor;
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
                }
            }
        }
    }

    @Override
    public long preComputedStateSize() {
        return diskCursor == null ? 0 : diskCursor.preComputedStateSize();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        diskCursor.recordAt(record, atRowId);
    }

    @Override
    public long size() {
        return diskCursor.size();
    }

    @Override
    public void toTop() {
        if (diskCursor != null) {
            diskCursor.toTop();
        }
    }

    private void releaseSlot() {
        if (tier != null && slotIdx >= 0) {
            // Safe even after the LV's DROP marked the tier closed: the deferred-
            // close protocol on LiveViewInMemoryTier keeps native memory alive
            // until the last pin drains (RFC 123 §"DROP LIVE VIEW" step 4
            // "modulo cursor pins").
            tier.releaseRead(slotIdx);
        }
        tier = null;
        slotIdx = -1;
    }
}
