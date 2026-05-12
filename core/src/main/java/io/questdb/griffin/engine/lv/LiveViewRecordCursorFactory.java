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
 * {@code PageFrameRecordCursorFactory} that {@code SqlCodeGenerator} builds for
 * the LV's WAL-backed table, and adds the in-memory tier seam-routing on top.
 * <p>
 * At {@link #getCursor} time:
 * <ul>
 *     <li>The wrapped factory's cursor is opened (the disk side).</li>
 *     <li>The LV's in-memory tier is looked up via {@link CairoEngine#getLiveViewRegistry}.
 *     If the tier exists and is non-empty, a slot is pinned via
 *     {@code acquireRead}; the cursor's {@code seamTs} is set from the slot's
 *     {@code seamTs}. Otherwise the cursor runs disk-only (seam = MAX,
 *     no in-mem reads).</li>
 *     <li>Sequential reads ({@code hasNext} / {@code getRecord}) walk disk
 *     rows whose ts is strictly below the seam, then exhaust the pinned slot
 *     from the seam forward. ASOF JOIN as RHS uses
 *     {@link #getTimeFrameCursor} which routes to the base factory's
 *     disk-only time-frame cursor — correct in Phase 1b because every in-mem
 *     row is also durable on disk.</li>
 * </ul>
 */
public class LiveViewRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final CairoEngine engine;
    private final TableToken liveViewToken;
    private final LiveViewRecordCursor cursor;

    public LiveViewRecordCursorFactory(CairoEngine engine, TableToken liveViewToken, RecordCursorFactory base) {
        super(base.getMetadata());
        this.engine = engine;
        this.liveViewToken = liveViewToken;
        this.base = base;
        this.cursor = new LiveViewRecordCursor();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor diskCursor = base.getCursor(executionContext);
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(liveViewToken.getTableName());
        cursor.of(diskCursor, instance);
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
        Misc.free(cursor);
    }
}
