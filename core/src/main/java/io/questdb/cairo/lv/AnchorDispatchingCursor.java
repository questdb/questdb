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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

/**
 * Wraps a base {@link RecordCursor} with the per-row anchor dispatch contract:
 * before each row leaves {@link #hasNext()}, the row is fed to
 * {@link LiveViewWindow#processRow(Record)} so that any partition whose anchor
 * value just changed gets its window functions reset.
 * <p>
 * The wrapping is intentionally narrow: this cursor must sit between the LV's
 * source / filter cursor and the {@code WindowRecordCursorFactory.getIncrementalCursor},
 * so the window function's pass1 sees state already reset for the current
 * partition+anchor.
 * <p>
 * Per-row {@code latestSeenTs} stamping lives on the refresh-job row loop
 * (after the window cursor produces a row) rather than here, so that LVs
 * without an anchored named window also drive the O3 detection watermark.
 */
final class AnchorDispatchingCursor implements RecordCursor {
    private RecordCursor base;
    private LiveViewWindow window;

    @Override
    public void close() {
        base = null;
        window = null;
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
        if (!base.hasNext()) {
            return false;
        }
        window.processRow(base.getRecord());
        return true;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return base.newSymbolTable(columnIndex);
    }

    public void of(
            RecordCursor base,
            LiveViewWindow window,
            SqlExecutionContext executionContext
    ) throws SqlException {
        this.base = base;
        this.window = window;
        // The anchor expression is initialised once per refresh cycle so that bind
        // variables and any per-cursor cached state pick up the current source.
        window.init(base, executionContext);
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
        window.toTop();
    }
}
