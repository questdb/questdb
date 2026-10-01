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

package io.questdb.cairo.sql;

import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

public interface RowCursorFactory extends Plannable, QuietCloseable {

    static void init(
            ObjList<? extends RowCursorFactory> factories,
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        for (int i = 0, n = factories.size(); i < n; i++) {
            factories.getQuick(i).init(pageFrameCursor, sqlExecutionContext);
        }
    }

    static void prepareCursor(ObjList<? extends RowCursorFactory> factories, PageFrameCursor pageFrameCursor) {
        for (int i = 0, n = factories.size(); i < n; i++) {
            factories.getQuick(i).prepareCursor(pageFrameCursor);
        }
    }

    @Override
    default void close() {
    }

    RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory);

    default void init(PageFrameCursor pageFrameCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
        // no-op
    }

    boolean isEntity();

    /**
     * Indicates whether the returned RowCursor yields frame rows in ascending
     * row-index order. The parquet decode clamp in
     * {@code PageFrameRecordCursorImpl.skipRows} treats {@code isEntity() &&
     * isForwardScan()} as permission to decode only the leading rows of a frame,
     * so a factory whose cursor visits rows in any other order MUST override
     * this to return false: with the unsafe default it would read undecoded
     * memory under a clamped LIMIT scan.
     */
    default boolean isForwardScan() {
        return true;
    }

    /**
     * Returns true only when every value this row cursor evaluates to select frame rows is itself
     * stable within a single query execution (same {@code SqlExecutionContext}). Composed into
     * {@code PageFrameRecordCursorFactory#isStableWithinExecution()} which gates scalar sub-query
     * timestamp interval pruning in {@code WhereClauseParser}.
     * <p>
     * Fail-safe like {@link RecordCursorFactory#isStableWithinExecution()}: the default reports
     * {@code false} so an unrecognised row-cursor shape never enables pruning. A plain entity scan
     * embeds no selecting value and overrides to {@code true} (frame-set stability is proven
     * separately by the partition-frame factory); index/function-driven cursors override to compose
     * the property from their key and (residual) filter functions. Reporting {@code true} for a
     * genuinely unstable cursor could silently drop rows, so unknown shapes must stay {@code false}.
     *
     * @return true if every cursor open within one execution selects the same frame rows
     */
    default boolean isStableWithinExecution() {
        return false;
    }

    /**
     * Indicates if the factory uses index
     *
     * @return true if the returned RowCursor is using an index, false otherwise
     */
    default boolean isUsingIndex() {
        return false;
    }

    default void prepareCursor(PageFrameCursor pageFrameCursor) {
        // no-op
    }
}
