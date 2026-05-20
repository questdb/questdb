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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ProjectableRecordCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.PushdownFilterExtractor;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

/**
 * Factory for single-threaded read_parquet() SQL function.
 */
public class ReadParquetRecordCursorFactory extends ProjectableRecordCursorFactory {
    private ReadParquetRecordCursor cursor;
    private Path path;
    private @Nullable ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions;
    // When true, the cursor walks the file in DESC ts order. The planner
    // flips this flag (via {@link #setReverseScan}) when it detects
    // ORDER BY ts DESC on a file whose sorting_columns metadata claims
    // the designated timestamp sorted; the existing reverse-elision check
    // in SqlCodeGenerator.generateOrderBy then returns this factory
    // unchanged because getScanDirection() == SCAN_DIRECTION_BACKWARD
    // matches the requested DESC.
    private boolean reverseScan;

    public ReadParquetRecordCursorFactory(@Transient Path path, RecordMetadata metadata) {
        super(metadata);
        this.path = new Path().of(path);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final CairoConfiguration configuration = executionContext.getCairoEngine().getConfiguration();
        if (cursor == null) {
            cursor = new ReadParquetRecordCursor(
                    configuration.getFilesFacade(),
                    executionContext.getCairoEngine().getParquetFileCache(),
                    getMetadata(),
                    pushdownFilterConditions
            );
        }
        // Sync the cursor's iteration direction with the factory's flag
        // BEFORE of() - the cursor's toTop (called from of()) seeds its row
        // group index based on reverse, so a stale value would leave the
        // first hasNext misaligned.
        cursor.setReverse(reverseScan);
        try {
            cursor.of(path.$(), executionContext);
            // Sort-claim hardening: when reverseScan is set and the operator
            // has opted in via cairo.sql.parquet.verify.sort.claim.enabled,
            // verify the file's sorting_columns claim against the per-row-
            // group min/max statistics. A dishonest claim would silently
            // corrupt the DESC result of the elided sort; failing here
            // surfaces the bad file with a clear message. The check is a
            // single O(#row-groups) stats walk with no decode.
            if (reverseScan && configuration.isSqlParquetVerifySortClaimEnabled()) {
                final ParquetFileDecoder decoder = cursor.getDecoder();
                final int tsIdx = decoder.metadata().getTimestampIndex();
                if (tsIdx >= 0 && !decoder.verifyAscendingSortAcrossRowGroups(tsIdx)) {
                    throw CairoException.nonCritical()
                            .put("parquet file's sorting_columns claim is dishonest: ts not ASC across row groups [path=")
                            .put(path)
                            .put(']');
                }
            }
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return reverseScan ? SCAN_DIRECTION_BACKWARD : SCAN_DIRECTION_FORWARD;
    }

    /**
     * Returns the resolved local parquet file path this factory wraps. The
     * footer-only MIN/MAX aggregate shortcut in {@code SqlCodeGenerator}
     * uses this to construct a {@link ParquetFooterAggregateRecordCursorFactory}
     * against the same file when the planner detects a min/max-on-sorted-ts
     * aggregate shape. Callers MUST treat the returned reference as borrowed
     * for the call duration only - this factory still owns the underlying
     * {@link Path}, which is freed in {@code _close()}.
     */
    public @org.jetbrains.annotations.NotNull Path getPath() {
        assert path != null : "getPath() called after _close()";
        return path;
    }

    @Override
    public boolean mayHaveParquetPartitions(SqlExecutionContext executionContext) {
        return true;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void setPushdownFilterCondition(ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions) {
        this.pushdownFilterConditions = pushdownFilterConditions;
    }

    /**
     * Flips this factory between forward (ASC) and reverse (DESC) iteration.
     * The planner calls this when {@code ORDER BY ts DESC} is detected on
     * a file whose {@code sorting_columns} metadata claims the designated
     * timestamp sorted. The existing reverse-elision check in
     * {@code SqlCodeGenerator.generateOrderBy} then returns this factory
     * unchanged because {@link #getScanDirection} reports
     * {@code SCAN_DIRECTION_BACKWARD} and matches the requested direction.
     * <p>
     * Safe to call before any {@code getCursor()} - the flag is synced into
     * the cursor on every getCursor. NOT safe to flip mid-iteration (the
     * cursor's toTop seeds its row group index based on the flag and a
     * stale value would misalign the next hasNext); call this once at
     * factory construction time and don't change it afterwards.
     */
    public void setReverseScan(boolean reverseScan) {
        this.reverseScan = reverseScan;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type(reverseScan ? "parquet file sequential scan (reverse)" : "parquet file sequential scan");
        sink.attr("columns").val(getMetadata());
    }

    @Override
    protected void _close() {
        cursor = Misc.free(cursor);
        path = Misc.free(path);
        Misc.freeObjListAndClear(pushdownFilterConditions);
    }
}
