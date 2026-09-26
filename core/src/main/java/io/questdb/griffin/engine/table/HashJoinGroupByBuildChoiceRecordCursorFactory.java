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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * Picks the build input of an INNER fused hash join aggregation per execution. The planner
 * compiles two orientations of one query when either input scans a timestamp interval: the
 * primary one builds the input with fewer table rows, as every fused plan does, and the
 * alternate one builds the other input. An interval can leave either input the smaller one,
 * and which one it leaves can change with every execution, through bind variables or
 * {@code now()}. {@link #getCursor(SqlExecutionContext)} counts the rows each input's intervals
 * select and runs the orientation that builds fewer of them.
 * <p>
 * The counts are upper bounds: they ignore row filters, which only the scan evaluates. A tie
 * or an input whose count is unknown keeps the primary orientation. Takes ownership of both
 * factories on entry, including construction failure.
 */
public final class HashJoinGroupByBuildChoiceRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursor.Counter counter = new RecordCursor.Counter();
    private AsyncHashJoinGroupByRecordCursorFactory alternate;
    // The cursor of the last execution, whichever orientation returned it.
    private RecordCursor cursor;
    private boolean isAlternateChosen;
    private AsyncHashJoinGroupByRecordCursorFactory primary;

    public HashJoinGroupByBuildChoiceRecordCursorFactory(
            AsyncHashJoinGroupByRecordCursorFactory primary,
            AsyncHashJoinGroupByRecordCursorFactory alternate
    ) {
        super(primary.getMetadata());
        this.primary = primary;
        this.alternate = alternate;
        if (!isSameOutput(primary.getMetadata(), alternate.getMetadata())) {
            final IllegalArgumentException failure = new IllegalArgumentException("fused hash join orientations disagree on output");
            Misc.free(this, failure);
            throw failure;
        }
    }

    /**
     * True when the two orientations return the same columns, which they do by construction:
     * both compile the same GROUP BY projection. The planner checks it before it pairs them.
     */
    public static boolean isSameOutput(RecordMetadata a, RecordMetadata b) {
        if (a.getColumnCount() != b.getColumnCount()) {
            return false;
        }
        for (int i = 0, n = a.getColumnCount(); i < n; i++) {
            if (a.getColumnType(i) != b.getColumnType(i) || !Chars.equals(a.getColumnName(i), b.getColumnName(i))) {
                return false;
            }
        }
        return true;
    }

    @TestOnly
    public AsyncHashJoinGroupByRecordCursorFactory getAlternate() {
        return alternate;
    }

    /** The primary orientation, so that a walk down the base factories still finds a fused factory. */
    @Override
    public RecordCursorFactory getBaseFactory() {
        return primary;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        // A caller that asks again without closing keeps one open cursor, as with one factory:
        // the orientation this execution picks may not be the one that returned the last cursor.
        cursor = Misc.free(cursor);
        // Each orientation probes the input the other one builds.
        final long primaryBuildRows = countRows(alternate.getBaseFactory(), executionContext);
        final long alternateBuildRows = countRows(primary.getBaseFactory(), executionContext);
        isAlternateChosen = primaryBuildRows > -1 && alternateBuildRows > -1 && alternateBuildRows < primaryBuildRows;
        cursor = (isAlternateChosen ? alternate : primary).getCursor(executionContext);
        return cursor;
    }

    @TestOnly
    public AsyncHashJoinGroupByRecordCursorFactory getPrimary() {
        return primary;
    }

    @Override
    public int getScanDirection() {
        return SCAN_DIRECTION_OTHER;
    }

    /** Which orientation the last {@link #getCursor(SqlExecutionContext)} ran. */
    @TestOnly
    public boolean isAlternateChosen() {
        return isAlternateChosen;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return primary.recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Hash Join Group By Build Choice");
        sink.attr("builds").val("input with fewer rows in its intervals");
        sink.child("Primary", primary);
        sink.child("Alternate", alternate);
    }

    @Override
    public boolean usesCompiledFilter() {
        return primary.usesCompiledFilter() || alternate.usesCompiledFilter();
    }

    /**
     * Rows the probe input's frames hold, before any row filter: exact for a full scan, and for
     * an interval scan the interval cursor's binary search over the partitions it overlaps. -1
     * when the frame cursor cannot tell.
     */
    private long countRows(RecordCursorFactory probeFactory, SqlExecutionContext executionContext) throws SqlException {
        try (PageFrameCursor frames = probeFactory.getPageFrameCursor(executionContext, ORDER_ASC)) {
            if (frames == null) {
                return -1;
            }
            final long size = frames.size();
            if (size > -1 || !frames.supportsSizeCalculation()) {
                return size;
            }
            counter.clear();
            frames.calculateSize(counter);
            return counter.get();
        }
    }

    @Override
    protected void _close() {
        Throwable failure = Misc.freeBestEffort(null, cursor);
        cursor = null;
        failure = Misc.freeBestEffort(failure, primary);
        primary = null;
        failure = Misc.freeBestEffort(failure, alternate);
        alternate = null;
        CairoException.rethrowCleanupFailure(failure);
    }
}
