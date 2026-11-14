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

package io.questdb.cutlass.parquet;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PriorityMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.std.ObjList;

/**
 * Determines how a query's result set is fed into the Parquet writer.
 */
public enum ParquetExportMode {
    /**
     * All columns materialized row-by-row from a RecordCursor (e.g. CROSS JOIN).
     */
    CURSOR_BASED,
    /**
     * All columns are direct page frame references — zero-copy fast path.
     */
    DIRECT_PAGE_FRAME,
    /**
     * Mix of zero-copy pass-through columns and computed columns materialized
     * from a VirtualRecordCursorFactory whose base supports page frames.
     */
    PAGE_FRAME_BACKED,
    /**
     * Direct table export via TableReader ({@code COPY table_name TO ...}).
     */
    TABLE_READER,
    /**
     * Fallback: query is materialized into a temp table first, then exported
     * partition-by-partition. Used for re-partitioning, for a BINARY column that
     * the cursor path cannot write, and for a plan that reads a lazily
     * type-converted column but cannot stream its timestamp.
     */
    TEMP_TABLE;

    /**
     * Determines the export mode for a given RecordCursorFactory.
     * Shared by both HTTP and SQL export paths.
     *
     * @param factory          the query's record cursor factory
     * @param isDescending     true when the query produces rows in descending order.
     *                         The hybrid path (PAGE_FRAME_BACKED) uses zero-copy pointers whose
     *                         data is always in storage (ascending) order, so descending queries
     *                         fall back to CURSOR_BASED to produce fully reversed row order.
     * @param executionContext the SQL execution context, used to detect lazily type-converted
     *                         parquet columns.
     */
    public static ParquetExportMode determineExportMode(RecordCursorFactory factory, boolean isDescending, SqlExecutionContext executionContext) {
        // A lazily type-converted parquet column is decoded in its pre-conversion source layout;
        // the conversion to the current type happens only in the row-wise Record accessors. The
        // zero-copy export paths (DIRECT_PAGE_FRAME / PAGE_FRAME_BACKED) ship the raw page buffer
        // straight to the encoder configured for the target type, which misreads it. CURSOR_BASED
        // reads every value through the converting accessors. It writes the plan's timestamp as
        // the file's designated timestamp, never NULL, in the order the plan reports, which only a
        // table's designated timestamp read through a scan, filter, projection, LIMIT or a sort on
        // it is known to keep. Other plans, and plans with a BINARY column, which the cursor path
        // cannot write, materialize through a temp table.
        if (factory.hasParquetConvertedColumns(executionContext)) {
            return !hasBinaryColumn(factory.getMetadata()) && hasStreamableTimestamp(factory) ? CURSOR_BASED : TEMP_TABLE;
        }
        RecordCursorFactory unwrapped = unwrapFactory(factory);
        // A metadata-only page-frame producer (the covering-index single-key scan) emits
        // placeholder covered-column addresses that only the async covered-decode framework
        // fills; a DIRECT_PAGE_FRAME reader would ship all-null covered columns. Fall through
        // to the row-wise cursor path, which drives the same covered decode the query path
        // uses. Multi-key covering frames are materialized eagerly and stay on the fast path.
        if (factory.supportsPageFrameCursor() && factory.producesMaterializedPageFrames()) {
            return DIRECT_PAGE_FRAME;
        }
        if (unwrapped instanceof VirtualRecordCursorFactory vf && vf.getBaseFactory().supportsPageFrameCursor()) {
            if (hasComputedBinaryColumn(vf)) {
                return TEMP_TABLE;
            }
            // PAGE_FRAME_BACKED reads the base's raw page-frame addresses for the pass-through
            // columns, so a metadata-only base (single-key covering scan under a projection, e.g.
            // SELECT sym, price * qty FROM t WHERE sym = 'x') would ship all-null covered columns
            // just like the DIRECT path. Only stay on the hybrid fast path when the base
            // materializes its frames; otherwise export row-wise via CURSOR_BASED.
            if (!vf.getBaseFactory().producesMaterializedPageFrames()) {
                return CURSOR_BASED;
            }
            return isDescending ? CURSOR_BASED : PAGE_FRAME_BACKED;
        }
        return hasBinaryColumn(factory.getMetadata()) ? TEMP_TABLE : CURSOR_BASED;
    }

    /**
     * Determines the export mode of a plan that an export compiled again after its plan went stale. Both COPY and
     * HTTP export read the page frames of a recompiled plan ascending, so a plan that scans backward streams its
     * cursor instead, or goes through a temp table when it has a BINARY column, which the cursor path cannot write.
     *
     * @param factory          the recompiled query's record cursor factory
     * @param executionContext the SQL execution context, used to detect lazily type-converted
     *                         parquet columns.
     */
    public static ParquetExportMode determineRecompiledExportMode(RecordCursorFactory factory, SqlExecutionContext executionContext) {
        final ParquetExportMode mode = determineExportMode(factory, false, executionContext);
        if ((mode == DIRECT_PAGE_FRAME || mode == PAGE_FRAME_BACKED)
                && factory.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_BACKWARD) {
            return hasBinaryColumn(factory.getMetadata()) ? TEMP_TABLE : CURSOR_BASED;
        }
        return mode;
    }

    /**
     * Checks whether a VirtualRecordCursorFactory has any computed BINARY columns.
     * Computed BINARY columns cannot be materialized into buffers for Parquet export.
     */
    public static boolean hasComputedBinaryColumn(VirtualRecordCursorFactory vf) {
        PriorityMetadata pm = vf.getPriorityMetadata();
        ObjList<Function> functions = vf.getFunctions();
        RecordMetadata meta = vf.getMetadata();
        for (int i = 0, n = meta.getColumnCount(); i < n; i++) {
            if (ColumnType.tagOf(meta.getColumnType(i)) != ColumnType.BINARY) {
                continue;
            }
            Function func = functions.getQuick(i);
            if (func instanceof ColumnFunction cf) {
                if (pm.getBaseColumnIndex(cf.getColumnIndex()) >= 0) {
                    continue; // pass-through BINARY is fine
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Returns true when a CURSOR_BASED export can write the plan's timestamp as the file's designated timestamp: the
     * plan has none, or its timestamp is a table's designated timestamp in the forward or backward order the plan
     * reports, see {@link RecordCursorFactory#hasTableScanTimestamp()}. The plan reports its direction only once it
     * answered true, so the question commits no join to its sides.
     */
    public static boolean hasStreamableTimestamp(RecordCursorFactory factory) {
        if (factory.getMetadata().getTimestampIndex() < 0) {
            return true;
        }
        if (!factory.hasTableScanTimestamp()) {
            return false;
        }
        final int direction = factory.getScanDirection();
        return direction == RecordCursorFactory.SCAN_DIRECTION_FORWARD || direction == RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
    }

    /**
     * Unwraps a QueryProgress wrapper to access the underlying factory.
     */
    public static RecordCursorFactory unwrapFactory(RecordCursorFactory factory) {
        return factory instanceof QueryProgress qp ? qp.getBaseFactory() : factory;
    }

    private static boolean hasBinaryColumn(RecordMetadata metadata) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (ColumnType.tagOf(metadata.getColumnType(i)) == ColumnType.BINARY) {
                return true;
            }
        }
        return false;
    }
}
