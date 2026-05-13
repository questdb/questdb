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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

/**
 * Live-view read path (RFC 123 Phase 1b Commit 4). Wraps the standard
 * {@code PageFrameRecordCursorFactory} that {@code SqlCodeGenerator} builds
 * for the LV's WAL-backed table, and pins the LV's in-memory tier slot for
 * the cursor's lifetime so the refresh worker's slow-path
 * {@code tryAcquireWrite} sees the reader (RFC 123 §"Stall behavior").
 * <p>
 * Phase 1b reads still come entirely from the disk cursor: the inline apply
 * (Phase 1b Commit 1) commits the LV WAL block synchronously with the
 * refresh worker's compute step, so every row visible in the in-mem tier is
 * also durable on disk and the disk cursor returns the complete picture.
 * The tier pin is therefore scaffolding for §"Stall behavior" and for the
 * future seam-routing cursor (Phase 4 hand-off ring with deferred apply),
 * not a read source today.
 * <p>
 * Each {@link #getCursor(SqlExecutionContext)} call allocates a fresh
 * {@link LiveViewRecordCursor}: the cursor pins a tier slot until
 * {@code close()}, so reusing a single cursor across consecutive
 * {@code getCursor} calls would release the previous reader's pin if both
 * cursors are still live (e.g. a plan-explain probe over the same factory).
 * Allocation here is once per query, not on the row hot path, so the cost
 * is negligible.
 */
public class LiveViewRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final CairoEngine engine;
    private final TableToken liveViewToken;

    public LiveViewRecordCursorFactory(CairoEngine engine, TableToken liveViewToken, RecordCursorFactory base) {
        super(base.getMetadata());
        this.engine = engine;
        this.liveViewToken = liveViewToken;
        this.base = base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor diskCursor = base.getCursor(executionContext);
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(liveViewToken.getTableName());
        LiveViewRecordCursor cursor = new LiveViewRecordCursor();
        try {
            cursor.of(diskCursor, instance);
        } catch (Throwable t) {
            Misc.free(cursor);
            throw t;
        }
        return cursor;
    }

    @Override
    public TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        // RFC 123 Phase 1b: every in-mem row is also durable on disk after the
        // inline apply, so ASOF JOIN-as-RHS reading via the base factory's
        // time-frame cursor sees the full dataset. Future phases (when in-mem
        // outpaces disk via the hand-off ring) will need a dedicated
        // time-frame cursor that bridges both tiers.
        return base.getTimeFrameCursor(executionContext);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean supportsTimeFrameCursor() {
        return base.supportsTimeFrameCursor();
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return base.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("LiveView");
        sink.optAttr("view", liveViewToken.getTableName());
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        Misc.free(base);
    }
}
