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
import org.jetbrains.annotations.NotNull;

/**
 * Hive-partitioned variant of {@link ParquetFooterAggregateRecordCursorFactory}.
 * Walks every matched file in the wrapped hive factory's planning-time snapshot,
 * reads per-row-group min/max statistics from each file's footer via
 * {@code rowGroupMin/MaxTimestamp}, and accumulates the global aggregate. Emits
 * exactly one row.
 * <p>
 * Owns the wrapped {@link HivePartitionedReadParquetRecordCursorFactory} -
 * {@code _close()} cascades into the hive factory's own _close so the matched-
 * files list, file cache, and prefetch executor are torn down together. This
 * matches the count() shortcut pattern in {@code SqlCodeGenerator} (it
 * wraps the subquery factory and assumes ownership).
 * <p>
 * Planner gate: the detection branch in
 * {@code SqlCodeGenerator.generateSelectGroupBy} only routes here when the
 * wrapped factory's metadata has {@code getColumnOrderBy(tsIdx) != 0} (the
 * file declares ts ASC sorted) and no WHERE clause survives pushdown. A
 * partition-only WHERE would in principle still compose, but the iteration
 * needs to first apply partition prune before walking footers; deferred until
 * a clearer use case shows up.
 */
public class HivePartitionedFooterAggregateRecordCursorFactory extends AbstractRecordCursorFactory {
    private final HivePartitionedReadParquetRecordCursorFactory base;
    private final HivePartitionedFooterAggregateRecordCursor cursor;

    /**
     * Legacy 3-arg constructor: defaults the aggregate column to each file's
     * designated timestamp. Retained for callers that pre-date the generic
     * non-ts MIN/MAX path.
     */
    public HivePartitionedFooterAggregateRecordCursorFactory(
            @NotNull HivePartitionedReadParquetRecordCursorFactory base,
            RecordMetadata outputMetadata,
            boolean[] aggregateKinds
    ) {
        this(base, outputMetadata, aggregateKinds, null);
    }

    /**
     * @param base wrapped hive factory whose matchedFiles + openCachedFile cache
     *             this factory consumes. Ownership transfers - this factory's
     *             _close cascades to base._close.
     * @param outputMetadata metadata of the SINGLE output row - column types,
     *                       names and ordering must match the aggregates the
     *                       planner detected (e.g. {@code min_ts TIMESTAMP,
     *                       max_ts TIMESTAMP})
     * @param aggregateKinds per-output-column aggregate flag, parallel to
     *                       {@code outputMetadata}: {@code true} for max,
     *                       {@code false} for min.
     * @param aggregateColumnName the parquet column whose min/max the cursor
     *                            reads from each file's footer. When
     *                            {@code null} the cursor defaults to the
     *                            file's designated timestamp; otherwise it
     *                            resolves the column by name per-file and
     *                            uses the generic
     *                            {@code rowGroupMinValueLong/MaxValueLong}
     *                            native that reads typed stats.
     */
    public HivePartitionedFooterAggregateRecordCursorFactory(
            @NotNull HivePartitionedReadParquetRecordCursorFactory base,
            RecordMetadata outputMetadata,
            boolean[] aggregateKinds,
            String aggregateColumnName
    ) {
        super(outputMetadata);
        this.base = base;
        this.cursor = new HivePartitionedFooterAggregateRecordCursor(aggregateKinds, aggregateColumnName);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(base, executionContext);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        // The cursor implements NoRandomAccessRecordCursor; match.
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("parquet hive footer aggregate");
        sink.attr("files").val(base.getMatchedFiles().size());
    }

    @Override
    protected void _close() {
        Misc.free(cursor);
        Misc.free(base);
    }
}
