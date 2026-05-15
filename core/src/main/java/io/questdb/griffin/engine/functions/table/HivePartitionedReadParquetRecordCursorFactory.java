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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ProjectableRecordCursorFactory;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.PageFrameRecordCursorImpl;
import io.questdb.griffin.engine.table.PageFrameRowCursorFactory;
import io.questdb.griffin.engine.table.PushdownFilterExtractor;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8StringList;
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
 *   <li>Page-frame path (default). Emits one frame per parquet row group across
 *       all files; partition values come from per-file native buffers via the
 *       virtual page overlay. Supports every inferred partition column type, and
 *       the surrounding async filter pipeline executes frames in parallel when
 *       worker threads are available.</li>
 *   <li>Sequential record cursor path, used when
 *       {@code cairo.sql.parquet.hive.parallel.enabled=false}. Walks files
 *       single-threaded and materialises partition values on the wrapping
 *       {@link io.questdb.cairo.sql.Record}. Provided as a safety fallback.</li>
 * </ul>
 */
public class HivePartitionedReadParquetRecordCursorFactory extends ProjectableRecordCursorFactory {
    private final boolean canPageFrame;
    private final CairoConfiguration configuration;
    private final CharSequence globPattern;
    // The matched file list is enumerated once at planning time and held here for
    // the factory's lifetime. Both cursor backends iterate this list directly - no
    // per-getCursor / per-size() / per-iteration directory walks. Owned by the
    // factory; freed in _close. Critical prep for remote-backed reads where
    // re-enumeration is expensive.
    private final DirectUtf8StringList matchedFiles;
    private final int nonGlobRootByteLen;
    private final int parquetColumnCount;
    private final GenericRecordMetadata parquetMetadata;
    private final ObjList<String> partitionColumnNames;
    private final IntList partitionColumnTypes;
    private PageFrameRecordCursorImpl pageFrameRecordCursor;
    private HivePartitionedReadParquetPageFrameCursor pageFrameCursor;
    // Per pruned-column metadata, built by setQueryProjectedMetadata. For each
    // entry, exactly one of projectedToParquetWriterIdx / projectedToPartitionIdx
    // is >= 0 (the other is -1). Both null means no projection - cursor uses the
    // full schema with parquet columns first then partition virtual columns.
    private int[] projectedToParquetWriterIdx;
    private int[] projectedToPartitionIdx;
    private @Nullable ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions;

    public HivePartitionedReadParquetRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull DirectUtf8StringList matchedFiles,
            @NotNull CharSequence globPattern,
            int nonGlobRootByteLen,
            @NotNull GenericRecordMetadata wrappingMetadata,
            @NotNull GenericRecordMetadata parquetMetadata,
            @NotNull ObjList<String> partitionColumnNames,
            @NotNull IntList partitionColumnTypes
    ) {
        super(wrappingMetadata);
        this.configuration = configuration;
        this.matchedFiles = matchedFiles;
        this.globPattern = globPattern.toString();
        this.nonGlobRootByteLen = nonGlobRootByteLen;
        this.parquetColumnCount = parquetMetadata.getColumnCount();
        this.parquetMetadata = parquetMetadata;
        this.partitionColumnNames = partitionColumnNames;
        this.partitionColumnTypes = partitionColumnTypes;
        this.canPageFrame = configuration.isSqlParquetHiveParallelEnabled();
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
                    matchedFiles,
                    parquetMetadata,
                    parquetColumnCount,
                    partitionColumnNames,
                    partitionColumnTypes,
                    nonGlobRootByteLen,
                    configuration.getSqlParquetHiveMaxOpenFiles(),
                    projectedToParquetWriterIdx,
                    projectedToPartitionIdx,
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
        // The page-frame-backed cursor wraps a PageFrameRecordCursorImpl which supports
        // random access. The sequential fallback walks files in order and does not.
        return canPageFrame;
    }

    @Override
    public void setPushdownFilterCondition(ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions) {
        this.pushdownFilterConditions = pushdownFilterConditions;
    }

    @Override
    public void setQueryProjectedMetadata(RecordMetadata metadata) {
        super.setQueryProjectedMetadata(metadata);
        // Compute the projection -> (parquet writer index, partition column index)
        // mapping for the cursor. Each pruned column is either a real parquet column
        // (look it up by name in parquetMetadata, store its position as the writer
        // index that PageFrameMemoryPool will use to find it in the file's column id
        // map) or a hive partition virtual column (store the original partition
        // index so the virtual page overlay can address the right per-file buffer).
        // Sentinels: -1 in the array we don't apply to a given column.
        final int n = metadata.getColumnCount();
        final int[] parquetWriterIdx = new int[n];
        final int[] partitionIdx = new int[n];
        for (int i = 0; i < n; i++) {
            CharSequence name = metadata.getColumnName(i);
            int pIdx = indexOfPartitionColumn(name);
            if (pIdx >= 0) {
                parquetWriterIdx[i] = -1;
                partitionIdx[i] = pIdx;
            } else {
                int pqIdx = parquetMetadata.getColumnIndexQuiet(name);
                // SqlCodeGenerator only projects columns that exist in the factory's
                // declared metadata, so a projected column we don't recognise here
                // is a contract violation worth surfacing rather than silently
                // dropping.
                assert pqIdx >= 0 : "projected column not in parquet or partition schema: " + name;
                parquetWriterIdx[i] = pqIdx;
                partitionIdx[i] = -1;
            }
        }
        this.projectedToParquetWriterIdx = parquetWriterIdx;
        this.projectedToPartitionIdx = partitionIdx;
    }

    private int indexOfPartitionColumn(CharSequence name) {
        for (int i = 0, n = partitionColumnNames.size(); i < n; i++) {
            if (Chars.equalsIgnoreCase(partitionColumnNames.getQuick(i), name)) {
                return i;
            }
        }
        return -1;
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
        Misc.free(matchedFiles);
        Misc.free(pageFrameRecordCursor);
        Misc.free(pageFrameCursor);
        Misc.freeObjListAndClear(pushdownFilterConditions);
    }

    private RecordCursor getLegacyCursor(SqlExecutionContext executionContext) throws SqlException {
        ReadParquetRecordCursor parquetCursor = null;
        try {
            parquetCursor = new ReadParquetRecordCursor(
                    configuration.getFilesFacade(),
                    parquetMetadata,
                    pushdownFilterConditions
            );
            HivePartitionedReadParquetRecordCursor cursor = new HivePartitionedReadParquetRecordCursor(
                    matchedFiles,
                    parquetCursor,
                    nonGlobRootByteLen,
                    parquetColumnCount,
                    partitionColumnNames,
                    partitionColumnTypes
            );
            cursor.of(executionContext);
            return cursor;
        } catch (Throwable th) {
            Misc.free(parquetCursor);
            throw th;
        }
    }

    private RecordCursor getPageFrameBackedCursor(SqlExecutionContext executionContext) throws SqlException {
        if (pageFrameCursor == null) {
            pageFrameCursor = new HivePartitionedReadParquetPageFrameCursor(
                    configuration.getFilesFacade(),
                    matchedFiles,
                    parquetMetadata,
                    parquetColumnCount,
                    partitionColumnNames,
                    partitionColumnTypes,
                    nonGlobRootByteLen,
                    configuration.getSqlParquetHiveMaxOpenFiles(),
                    projectedToParquetWriterIdx,
                    projectedToPartitionIdx,
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
