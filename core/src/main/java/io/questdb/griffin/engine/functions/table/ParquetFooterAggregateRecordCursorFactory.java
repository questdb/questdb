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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;

/**
 * Footer-only aggregate factory for {@code SELECT min(ts), max(ts) FROM
 * read_parquet(<file>)} where {@code ts} is the parquet file's designated
 * timestamp and the file declares it sorted via {@code sorting_columns}.
 * <p>
 * Bypasses row decode entirely. The cursor walks per-row-group min/max
 * statistics from the parquet footer (the existing
 * {@code rowGroupMinTimestamp} / {@code rowGroupMaxTimestamp} natives) and
 * emits a single row with the global min/max. For a 500k-row file with 50
 * row groups this is ~50 µs vs the ~2.6 ms paid by the serial cursor
 * walking every page, and ~0.6 ms paid by the parallel vector aggregate
 * path that still walks one Unsafe.getLong per frame.
 * <p>
 * Planner integration lands in a follow-up commit; this commit is the
 * factory+cursor scaffolding only. The detection branch sits in
 * {@code SqlCodeGenerator.generateSelectGroupBy} before the
 * {@code AsyncGroupByNotKeyedRecordCursorFactory} branch fires and gates on:
 * <ul>
 *   <li>zero GROUP BY keys</li>
 *   <li>every aggregate is {@code min} or {@code max} on the same parquet
 *       column index</li>
 *   <li>that column has a sort claim ({@code getColumnOrderBy != 0}) and
 *       is the designated timestamp</li>
 *   <li>no WHERE filter survives pushdown</li>
 *   <li>base factory is {@link ReadParquetRecordCursorFactory} or
 *       {@code HivePartitionedReadParquetRecordCursorFactory}</li>
 * </ul>
 * <p>
 * Trust model: the file's {@code sorting_columns} claim is taken at face
 * value, exactly like the read-side ORDER BY elision. If a caller wrote an
 * inaccurate claim via the writer knob, min/max will be wrong; that is the
 * same hazard the elision already inherits.
 */
public class ParquetFooterAggregateRecordCursorFactory extends AbstractRecordCursorFactory {
    private final ParquetFooterAggregateRecordCursor cursor;
    private Path path;

    /**
     * @param outputMetadata metadata of the SINGLE output row - column types,
     *                       names and ordering must match the aggregates the
     *                       planner detected (e.g. {@code min_ts TIMESTAMP,
     *                       max_ts TIMESTAMP})
     * @param aggregateKinds per-output-column aggregate flag, parallel to
     *                       {@code outputMetadata}: {@code true} for max,
     *                       {@code false} for min. Sized to match
     *                       {@code outputMetadata.getColumnCount()}.
     */
    public ParquetFooterAggregateRecordCursorFactory(
            @Transient Path path,
            RecordMetadata outputMetadata,
            boolean[] aggregateKinds
    ) {
        super(outputMetadata);
        this.path = new Path().of(path);
        this.cursor = new ParquetFooterAggregateRecordCursor(aggregateKinds);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(path.$(), executionContext);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        // The cursor implements NoRandomAccessRecordCursor - it has no
        // recordAt(at, rowId) implementation. The cursor is single-row so
        // random access has no practical meaning, but the factory's
        // contract must match the cursor's actual capability or
        // assertQueryNoLeakCheck downstream fails.
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("parquet footer aggregate");
        sink.attr("file").val(path);
    }

    @Override
    protected void _close() {
        path = Misc.free(path);
        Misc.free(cursor);
    }
}
