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

/**
 * Determines how a query's result set is fed into the Parquet writer.
 *
 * @see HybridColumnMaterializer#determineExportMode(io.questdb.cairo.sql.RecordCursorFactory)
 */
public enum ParquetExportMode {
    // All columns materialized row-by-row from a RecordCursor (e.g. CROSS JOIN).
    CURSOR_BASED,
    // All columns are direct page frame references — zero-copy fast path.
    DIRECT_PAGE_FRAME,
    // Mix of zero-copy pass-through columns and computed columns materialized
    // from a VirtualRecordCursorFactory whose base supports page frames.
    PAGE_FRAME_BACKED,
    // Direct table export via TableReader (COPY table_name TO ...).
    TABLE_READER,
    // Fallback: query is materialized into a temp table first, then exported
    // partition-by-partition. Used for computed BINARY columns or re-partitioning.
    TEMP_TABLE
}
