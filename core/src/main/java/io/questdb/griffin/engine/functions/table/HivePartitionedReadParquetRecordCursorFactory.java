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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.MutableMetadataRecordCursorFactory;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.PageFrameRecordCursorImpl;
import io.questdb.griffin.engine.table.PageFrameRowCursorFactory;
import io.questdb.griffin.engine.table.PushdownFilterExtractor;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * Reads many parquet files matched by a glob pattern as a single result set.
 * Owns the underlying glob cursor factory and the single-file parquet cursor.
 * <p>
 * The factory's metadata is the parquet schema (first {@code parquetColumnCount}
 * columns) concatenated with hive partition columns derived from {@code key=value}
 * segments in the directory path. Partition column types are inferred from the
 * values encountered across all matched files.
 * <p>
 * Two execution paths coexist:
 * <ul>
 *   <li>Parallel page frame path when no partition column is VARCHAR. Emits one
 *       frame per parquet row group across all files; partition values come from
 *       per-file native buffers via the virtual page overlay.</li>
 *   <li>Sequential record cursor path when any partition column is VARCHAR.
 *       Walks files single-threaded and materialises partition values on the
 *       wrapping {@link io.questdb.cairo.sql.Record}.</li>
 * </ul>
 */
public class HivePartitionedReadParquetRecordCursorFactory extends MutableMetadataRecordCursorFactory {
    private final boolean canPageFrame;
    private final CairoConfiguration configuration;
    private final RecordCursorFactory globCursorFactory;
    private final CharSequence globPattern;
    private final CharSequence nonGlobRoot;
    private final int parquetColumnCount;
    private final GenericRecordMetadata parquetMetadata;
    private final ObjList<String> partitionColumnNames;
    private final IntList partitionColumnTypes;
    private PageFrameRecordCursorImpl pageFrameRecordCursor;
    private HivePartitionedReadParquetPageFrameCursor pageFrameCursor;
    private @Nullable ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions;

    public HivePartitionedReadParquetRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory globCursorFactory,
            @NotNull CharSequence globPattern,
            @NotNull CharSequence nonGlobRoot,
            @NotNull GenericRecordMetadata wrappingMetadata,
            @NotNull GenericRecordMetadata parquetMetadata,
            @NotNull ObjList<String> partitionColumnNames,
            @NotNull IntList partitionColumnTypes
    ) {
        super(wrappingMetadata);
        this.configuration = configuration;
        this.globCursorFactory = globCursorFactory;
        this.globPattern = globPattern.toString();
        this.nonGlobRoot = nonGlobRoot.toString();
        this.parquetColumnCount = parquetMetadata.getColumnCount();
        this.parquetMetadata = parquetMetadata;
        this.partitionColumnNames = partitionColumnNames;
        this.partitionColumnTypes = partitionColumnTypes;
        this.canPageFrame = computeCanPageFrame(partitionColumnTypes);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        if (canPageFrame) {
            return getPageFrameBackedCursor(executionContext);
        }
        return getLegacyCursor(executionContext);
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        if (!canPageFrame) {
            return null;
        }
        if (pageFrameCursor == null) {
            pageFrameCursor = new HivePartitionedReadParquetPageFrameCursor(
                    configuration.getFilesFacade(),
                    globCursorFactory.getCursor(executionContext),
                    parquetMetadata,
                    parquetColumnCount,
                    partitionColumnNames,
                    partitionColumnTypes,
                    nonGlobRoot,
                    pushdownFilterConditions
            );
        }
        pageFrameCursor.of(executionContext);
        return pageFrameCursor;
    }

    @Override
    public boolean mayHaveParquetPartitions(SqlExecutionContext executionContext) {
        return true;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        // The page-frame-backed cursor (used when no VARCHAR partition columns are present)
        // wraps a PageFrameRecordCursorImpl which supports random access. The legacy path
        // walks files sequentially and does not.
        return canPageFrame;
    }

    @Override
    public void setPushdownFilterCondition(ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions) {
        this.pushdownFilterConditions = pushdownFilterConditions;
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return canPageFrame;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Parquet glob scan").attr("glob").val(globPattern);
    }

    @Override
    protected void _close() {
        Misc.free(globCursorFactory);
        Misc.free(pageFrameRecordCursor);
        Misc.free(pageFrameCursor);
        Misc.freeObjListAndClear(pushdownFilterConditions);
    }

    private static boolean computeCanPageFrame(IntList partitionColumnTypes) {
        // VARCHAR partition values need an aux+data layout that the page-frame
        // path doesn't yet supply. Until that lands, fall back to the sequential
        // record cursor when any partition column is VARCHAR.
        for (int i = 0, n = partitionColumnTypes.size(); i < n; i++) {
            if (ColumnType.tagOf(partitionColumnTypes.getQuick(i)) == ColumnType.VARCHAR) {
                return false;
            }
        }
        return true;
    }

    private RecordCursor getLegacyCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor globCursor = globCursorFactory.getCursor(executionContext);
        ReadParquetRecordCursor parquetCursor = null;
        try {
            parquetCursor = new ReadParquetRecordCursor(
                    configuration.getFilesFacade(),
                    parquetMetadata,
                    pushdownFilterConditions
            );
            HivePartitionedReadParquetRecordCursor cursor = new HivePartitionedReadParquetRecordCursor(
                    globCursor,
                    parquetCursor,
                    nonGlobRoot,
                    parquetColumnCount,
                    partitionColumnNames,
                    partitionColumnTypes
            );
            cursor.of(executionContext);
            return cursor;
        } catch (Throwable th) {
            Misc.free(parquetCursor);
            globCursor.close();
            throw th;
        }
    }

    private RecordCursor getPageFrameBackedCursor(SqlExecutionContext executionContext) throws SqlException {
        if (pageFrameCursor == null) {
            pageFrameCursor = new HivePartitionedReadParquetPageFrameCursor(
                    configuration.getFilesFacade(),
                    globCursorFactory.getCursor(executionContext),
                    parquetMetadata,
                    parquetColumnCount,
                    partitionColumnNames,
                    partitionColumnTypes,
                    nonGlobRoot,
                    pushdownFilterConditions
            );
        }
        if (pageFrameRecordCursor == null) {
            pageFrameRecordCursor = new PageFrameRecordCursorImpl(
                    configuration,
                    getMetadata(),
                    new PageFrameRowCursorFactory(ORDER_ASC),
                    true,
                    null
            );
        }
        pageFrameCursor.of(executionContext);
        try {
            pageFrameRecordCursor.of(pageFrameCursor, executionContext);
            return pageFrameRecordCursor;
        } catch (Throwable th) {
            pageFrameRecordCursor.close();
            throw th;
        }
    }
}
