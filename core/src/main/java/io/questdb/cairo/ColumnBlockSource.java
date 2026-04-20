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

package io.questdb.cairo;

import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;

/**
 * Read-only view over a block of columnar memory, exposing the four pointer
 * accessors that {@link SortedRowSink} implementations need to decode a row
 * at a given local row index. Decouples sinks from any specific upstream
 * decoder so that both the parquet-run pipeline (backed by
 * {@link RowGroupBuffers}) and the intermediate-table pipeline (backed by
 * an in-memory table's partition column memory) can feed the same sinks
 * without any sink-side change.
 * <p>
 * The memory-layout contract mirrors what {@link RowGroupBuffers} exposes:
 * <ul>
 *   <li><b>Fixed-width columns</b>: {@code getChunkDataPtr(c)} is the base
 *       pointer of a contiguous array of {@code elemSize * rowCount} bytes.
 *       A row at index {@code r} lives at
 *       {@code getChunkDataPtr(c) + r * elemSize}.
 *       {@code getChunkAuxPtr(c)} is unused (0).</li>
 *   <li><b>VARCHAR</b>: aux holds QuestDB's native aux entries (16 B each)
 *       and data holds the UTF-8 bytes. Decode via
 *       {@code VarcharTypeDriver.getSplitValue(auxPtr, auxLim, dataPtr,
 *       dataLim, row, ...)}. Both aux and data sizes are mandatory so the
 *       decoder can range-check reads.</li>
 *   <li><b>BINARY</b>: aux holds 8-byte offsets per row, data holds
 *       {@code (i64 len, bytes)} records.</li>
 *   <li><b>STRING (UTF-16)</b>: aux holds 8-byte offsets per row, data
 *       holds {@code (i32 char_len, utf16 chars)} records.</li>
 * </ul>
 * Implementations do not own the underlying memory and must outlive any
 * sink read that happens through them, but do not need to be thread-safe:
 * phaseB drives sinks from a single thread.
 */
public interface ColumnBlockSource {

    /**
     * Returns the base pointer of the aux block for column {@code columnIndex},
     * or 0 for fixed-width columns that do not use an aux block.
     */
    long getChunkAuxPtr(int columnIndex);

    /**
     * Returns the size in bytes of the aux block for column
     * {@code columnIndex}. For fixed-width columns without an aux block,
     * callers should treat a returned 0 as "no aux block".
     */
    long getChunkAuxSize(int columnIndex);

    /**
     * Returns the base pointer of the data block for column
     * {@code columnIndex}. Never 0 for a column that has any rows.
     */
    long getChunkDataPtr(int columnIndex);

    /**
     * Returns the size in bytes of the data block for column
     * {@code columnIndex}.
     */
    long getChunkDataSize(int columnIndex);
}
