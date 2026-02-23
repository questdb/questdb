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

package io.questdb.cutlass.parquet;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PriorityMetadata;
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
     * partition-by-partition. Used for computed BINARY columns or re-partitioning.
     */
    TEMP_TABLE;

    /**
     * Determines the export mode for a given RecordCursorFactory.
     * Shared by both HTTP and SQL export paths.
     *
     * @param factory      the query's record cursor factory
     * @param isDescending true when the query produces rows in descending order.
     *                     The hybrid path (PAGE_FRAME_BACKED) uses zero-copy pointers whose
     *                     data is always in storage (ascending) order, so descending queries
     *                     fall back to CURSOR_BASED to produce fully reversed row order.
     */
    public static ParquetExportMode determineExportMode(RecordCursorFactory factory, boolean isDescending) {
        RecordCursorFactory unwrapped = unwrapFactory(factory);
        if (factory.supportsPageFrameCursor()) {
            return DIRECT_PAGE_FRAME;
        }
        if (unwrapped instanceof VirtualRecordCursorFactory vf && vf.getBaseFactory().supportsPageFrameCursor()) {
            if (hasComputedBinaryColumn(vf)) {
                return TEMP_TABLE;
            }
            return isDescending ? CURSOR_BASED : PAGE_FRAME_BACKED;
        }
        RecordMetadata meta = factory.getMetadata();
        for (int i = 0, n = meta.getColumnCount(); i < n; i++) {
            if (ColumnType.tagOf(meta.getColumnType(i)) == ColumnType.BINARY) {
                return TEMP_TABLE;
            }
        }
        return CURSOR_BASED;
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
     * Unwraps a QueryProgress wrapper to access the underlying factory.
     */
    public static RecordCursorFactory unwrapFactory(RecordCursorFactory factory) {
        return factory instanceof QueryProgress qp ? qp.getBaseFactory() : factory;
    }
}
