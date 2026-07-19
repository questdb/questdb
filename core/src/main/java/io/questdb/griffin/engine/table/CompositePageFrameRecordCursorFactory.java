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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

/**
 * Base-table scan factory for a composite (time + non-time dimension) table. Extends
 * {@link PageFrameRecordCursorFactory} but replaces the record cursor with a
 * {@link CompositeMergePartitionRecordCursor}, so {@link #getCursor(SqlExecutionContext)} yields a genuinely
 * global-designated-timestamp-ordered stream (the plain factory's per-cell-concatenated stream is
 * misordered for a composite table -- see the merge cursor's class doc).
 * <p>
 * Because the merged stream IS ordered, {@link #getScanDirection()} is truthful, which is exactly what the
 * order-consuming plan-time decisions rely on (ORDER BY sort-skip, SAMPLE BY eligibility, join
 * both-ascending validation). The factory advertises NO direct page-frame / time-frame access
 * ({@link #supportsPageFrameCursor()} / {@link #supportsTimeFrameCursor()} return false, and the frame /
 * time-frame accessors return null): the merge is row-granular (a page frame is one cell's contiguous
 * memory and cannot interleave two cells), so every consumer -- vectorized aggregates, async filter, fast
 * joins -- degrades to the row-based {@link #getCursor(SqlExecutionContext)} path, which is correct. This
 * mirrors the base's {@code framingSupported == false} configuration.
 * <p>
 * {@code convertToSampleByIndexPageFrameCursorFactory()} is deliberately NOT overridden: the inherited null
 * keeps SAMPLE BY FIRST/LAST off the page-frame path (overriding it non-null would make it call
 * {@code getPageFrameCursor()} unconditionally and NPE on the null returned here).
 */
public class CompositePageFrameRecordCursorFactory extends PageFrameRecordCursorFactory {
    private final boolean forward;
    private final CompositeMergePartitionRecordCursor mergeCursor;

    public CompositePageFrameRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            RecordMetadata metadata,
            PartitionFrameCursorFactory partitionFrameCursorFactory,
            RowCursorFactory rowCursorFactory,
            boolean followsOrderByAdvice,
            @Nullable Function filter,
            boolean framingSupported,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts,
            boolean supportsRandomAccess,
            boolean singleRowFactory
    ) {
        super(
                configuration,
                metadata,
                partitionFrameCursorFactory,
                rowCursorFactory,
                followsOrderByAdvice,
                filter,
                framingSupported,
                columnIndexes,
                columnSizeShifts,
                supportsRandomAccess,
                singleRowFactory
        );
        // ORDER_ASC/ORDER_ANY -> forward (min-heap merge), ORDER_DESC -> backward (max-heap merge). Mirrors
        // AbstractPageFrameRecordCursorFactory.initPageFrameCursor's Fwd/Bwd choice.
        this.forward = partitionFrameCursorFactory.getOrder() != ORDER_DESC;
        this.mergeCursor = new CompositeMergePartitionRecordCursor(
                configuration,
                metadata,
                metadata.getTimestampIndex(),
                forward
        );
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) {
        // Row-granular merge cannot feed a page-frame consumer; force everything through getCursor().
        return null;
    }

    @Override
    public int getScanDirection() {
        // Truthful: the merged stream really is ordered in this direction.
        return forward ? SCAN_DIRECTION_FORWARD : SCAN_DIRECTION_BACKWARD;
    }

    @Override
    public TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) {
        // Disable the fast-join slave seam; fast joins fall back to light joins over getCursor() (correct,
        // since getScanDirection() is truthful and validateBothTimestampOrders passes).
        return null;
    }

    @Override
    public ConcurrentTimeFrameCursor newTimeFrameCursor() {
        return null;
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return false;
    }

    @Override
    public boolean supportsTimeFrameCursor() {
        return false;
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(mergeCursor);
    }

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor frameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        mergeCursor.of(frameCursor, executionContext);
        return mergeCursor;
    }
}
