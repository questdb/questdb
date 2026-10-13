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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import org.jetbrains.annotations.NotNull;

/**
 * The page frames of a covering-index scan, for the Async Filter of its parallel rows plan (see
 * {@link CoveringIndexRecordCursorFactory#setParallelRows}). It borrows the scan, which owns it
 * through that plan, and offers page frames only.
 */
public class CoveringFrameFactory extends AbstractRecordCursorFactory {
    private final CoveringIndexRecordCursorFactory covering;

    public CoveringFrameFactory(@NotNull CoveringIndexRecordCursorFactory covering) {
        super(covering.getMetadata());
        this.covering = covering;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        return covering.getPageFrameCursor(executionContext, order);
    }

    @Override
    public int getScanDirection() {
        return covering.getScanDirection();
    }

    @Override
    public TableToken getTableToken() {
        return covering.getTableToken();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("CoveringFrames");
    }

    @Override
    protected void _close() {
        // the scan is borrowed
    }
}
