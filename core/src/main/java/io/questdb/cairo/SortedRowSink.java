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

import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.QuietCloseable;

/**
 * Consumer of a globally sorted row stream produced by
 * {@link ExternalSortedParquetImporter#phaseB}.
 * <p>
 * Lifecycle:
 * <ol>
 *   <li>{@link #onStart} — invoked exactly once with the parquet schema,
 *       the index of the timestamp column, and the total row count that will
 *       be delivered (sum of all input runs).</li>
 *   <li>{@link #acceptRow} — invoked once per row, in ascending timestamp
 *       order. The sink reads values from the supplied {@code RowGroupBuffers}
 *       at the given local row index; the buffers and their backing native
 *       memory are owned by the merger and must not be retained past the
 *       call.</li>
 *   <li>{@link #onFinish} — invoked exactly once after the last row, before
 *       the merger releases its own resources.</li>
 * </ol>
 * The sink is not required to be thread-safe; phaseB drives it from a single
 * thread.
 */
public interface SortedRowSink extends QuietCloseable {

    /**
     * Accept a single row from the sorted stream. {@code src} points at the
     * decoded column buffers of the run that currently holds this row; read
     * fields out via the usual chunk data/aux pointer APIs. Do not retain
     * {@code src} past the return of this method — the merger may rebind it
     * to a different run on the next call.
     *
     * @param src the source run's decoded row group buffers
     * @param row local row index within {@code src}
     * @param ts  timestamp of this row (already extracted by the merger)
     */
    void acceptRow(RowGroupBuffers src, int row, long ts);

    /**
     * Called after the final {@link #acceptRow} call and before the merger
     * tears down. Sinks that buffer output should flush here.
     */
    void onFinish();

    /**
     * Called once before any {@link #acceptRow}. Sinks that need to pre-size
     * buffers should do so from {@code totalRows}; sinks that need schema
     * information should read it from {@code meta}.
     *
     * @param meta           metadata of the first run (all runs share a schema)
     * @param tsColumnIndex  index of the timestamp column in {@code meta}
     * @param totalRows      exact count of rows that will be delivered to
     *                       {@link #acceptRow}
     */
    void onStart(PartitionDecoder.Metadata meta, int tsColumnIndex, long totalRows);
}
