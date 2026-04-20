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

import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8SplitString;

/**
 * TableWriter-pipelined external merge sort for parquet inputs. Builds an
 * in-memory (memfd-backed) intermediate table {@code T_INT} with one
 * partition per source row group, then k-way merges the partitions by their
 * original timestamp, streaming sorted rows to a caller-supplied
 * {@link SortedRowSink}.
 * <p>
 * {@code T_INT} schema is the source parquet schema verbatim plus two
 * bookkeeping columns appended at the end:
 * <ul>
 *   <li>{@code __sort_ts} TIMESTAMP — the scaled original timestamp used
 *       as the merge key in phaseB.</li>
 *   <li>{@code __part_ts} TIMESTAMP — the designated (synthetic) timestamp.
 *       Each source row group gets a unique partition-index-scaled value
 *       {@code rgIndex * HOUR_STRIDE_US + rowIndex} which, under
 *       {@code PARTITION BY HOUR}, routes every row from rg {@code k} into
 *       its own dedicated partition.</li>
 * </ul>
 * Because the synthetic ts is monotonic within a partition and the merge key
 * (scaled original ts) is itself monotonic at insertion time, each partition
 * is internally sorted on both axes — phaseB reads rows in row-index order
 * and pops them through the heap by {@code __sort_ts}.
 * <p>
 * All three memory tags {@link MemoryTag#NATIVE_IMPORT},
 * {@link MemoryTag#NATIVE_PARQUET_PARTITION_DECODER} and
 * {@link MemoryTag#MMAP_PARQUET_PARTITION_DECODER} return to baseline after
 * either a successful or a torn-down run.
 */
public class IntermediateTableImporter {

    /**
     * One hour in microseconds. Chosen so that with
     * {@code PARTITION BY HOUR}, inserting a row with
     * {@code synthetic_ts = rgIndex * HOUR_STRIDE_US + rowIndex} lands it in
     * the partition named {@code rgIndex}'s hour.
     */
    public static final long HOUR_STRIDE_US = 3_600_000_000L;
    public static final String SORT_TS_COL_NAME = "__sort_ts";
    public static final String SYNTHETIC_TS_COL_NAME = "__part_ts";
    private static final int MEMORY_TAG = MemoryTag.NATIVE_IMPORT;
    private static final Log LOG = LogFactory.getLog(IntermediateTableImporter.class);

    /**
     * Phase A: decode source parquet one row group at a time, sort each RG
     * internally by the (scaled) source ts, and stream rows into {@code T_INT}
     * via a {@link TableWriter}. Every source row group becomes exactly one
     * partition of {@code T_INT}.
     *
     * @param engine           CairoEngine configured with
     *                         {@link HybridCairoConfiguration} (the errand
     *                         default on Linux); {@code T_INT} will live in
     *                         memfd via {@code CREATE MEMORY TABLE}.
     * @param ctx              SQL execution context, used for {@code ALTER
     *                         TABLE ... DROP PARTITION LIST} and
     *                         {@code DROP TABLE}.
     * @param srcFf            facade used to read the source parquet (typically
     *                         {@link io.questdb.std.FilesFacadeImpl#INSTANCE}
     *                         or the engine's own facade; both work).
     * @param srcPath          path to the source parquet file.
     * @param tsColumnName     name of the source ts column to sort by.
     * @param tsColumnType     effective QuestDB type for the source ts column
     *                         in {@code T_INT}. Pass 0 to inherit the source
     *                         column's type.
     * @param tsScaleMultiplier positive long; multiplies the raw source ts
     *                         value to yield microseconds. See
     *                         {@link ExternalSortedParquetImporter#phaseA(
     *                         FilesFacade, FilesFacade, CharSequence,
     *                         CharSequence, int, long)} javadoc.
     * @param tableName        distinct name for {@code T_INT}. Concurrent
     *                         imports against the same engine MUST pass
     *                         distinct names.
     * @return opaque handle the caller must close or pass to
     *         {@link #phaseB(CairoEngine, SqlExecutionContext, RunSet, SortedRowSink, int)}.
     */
    public static RunSet phaseA(
            CairoEngine engine,
            SqlExecutionContext ctx,
            FilesFacade srcFf,
            CharSequence srcPath,
            CharSequence tsColumnName,
            int tsColumnType,
            long tsScaleMultiplier,
            CharSequence tableName
    ) {
        if (tsScaleMultiplier <= 0) {
            throw CairoException.nonCritical()
                    .put("phaseA tsScaleMultiplier must be positive [got=")
                    .put(tsScaleMultiplier).put(']');
        }
        if (tableName == null || tableName.length() == 0) {
            throw CairoException.nonCritical()
                    .put("phaseA tableName must be non-empty");
        }
        final String tn = tableName.toString();

        long srcFd = -1;
        long srcAddr = 0;
        long srcFileSize = 0;
        PartitionDecoder decoder = null;
        DirectIntList columns = null;
        TableToken token = null;
        TableWriter writer = null;
        MemoryMARW ddlMem = null;
        boolean success = false;

        try {
            try (Path p = new Path()) {
                p.put(srcPath);
                srcFd = TableUtils.openRO(srcFf, p.$(), LOG);
                srcFileSize = srcFf.length(srcFd);
                srcAddr = TableUtils.mapRO(srcFf, srcFd, srcFileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            decoder = new PartitionDecoder();
            decoder.of(srcAddr, srcFileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

            final PartitionDecoder.Metadata meta = decoder.metadata();
            final int srcColumnCount = meta.getColumnCount();
            final int rowGroupCount = meta.getRowGroupCount();
            final long totalSrcRows = meta.getRowCount();

            final int srcTsColIdx = meta.getColumnIndex(tsColumnName);
            if (srcTsColIdx < 0) {
                throw CairoException.nonCritical()
                        .put("timestamp column not found in parquet file [column=")
                        .put(tsColumnName).put(']');
            }
            final int srcTsColType = meta.getColumnType(srcTsColIdx);
            final int srcTsTag = ColumnType.tagOf(srcTsColType);
            if (srcTsTag != ColumnType.TIMESTAMP
                    && srcTsTag != ColumnType.LONG
                    && srcTsTag != ColumnType.DATE) {
                throw CairoException.nonCritical()
                        .put("phaseA requires TIMESTAMP, LONG, or DATE source ts column [column=")
                        .put(tsColumnName)
                        .put(", type=").put(ColumnType.nameOf(srcTsColType))
                        .put(']');
            }
            final int effectiveTsType = tsColumnType > 0 ? tsColumnType : srcTsColType;

            // Build T_INT schema: source columns verbatim (ts col optionally re-typed),
            // then __sort_ts (TIMESTAMP, merge key), then __part_ts (TIMESTAMP, designated).
            final IntermediateTableStructure struct = new IntermediateTableStructure(
                    tn, meta, srcTsColIdx, effectiveTsType
            );
            ddlMem = Vm.getCMARWInstance();
            try (Path ddlPath = new Path()) {
                token = engine.createTable(
                        AllowAllSecurityContext.INSTANCE,
                        ddlMem,
                        ddlPath,
                        /* ifNotExists */ false,
                        struct,
                        /* keepLock */ false,
                        /* inVolume */ false,
                        /* isMemoryTable */ true,
                        TableUtils.TABLE_KIND_REGULAR_TABLE
                );
            }

            columns = new DirectIntList(2L * srcColumnCount, MEMORY_TAG);
            for (int i = 0; i < srcColumnCount; i++) {
                columns.add(i);
                columns.add(meta.getColumnType(i));
            }

            // Precompute per-source-column metadata used during scatter. The
            // *source* types drive how we read from the parquet decoder
            // buffers; the *effective* types drive the schema phaseB
            // presents to the sink (with the ts column re-typed if
            // effectiveTsType was supplied by the caller).
            final int[] srcColTypes = new int[srcColumnCount];
            final int[] effectiveColTypes = new int[srcColumnCount];
            final int[] srcColSizes = new int[srcColumnCount];
            final boolean[] srcIsVarCol = new boolean[srcColumnCount];
            for (int c = 0; c < srcColumnCount; c++) {
                srcColTypes[c] = meta.getColumnType(c);
                effectiveColTypes[c] = (c == srcTsColIdx) ? effectiveTsType : srcColTypes[c];
                if (ColumnType.isVarSize(srcColTypes[c])) {
                    srcIsVarCol[c] = true;
                    srcColSizes[c] = -1;
                } else if (ColumnType.isSymbol(srcColTypes[c])) {
                    srcColSizes[c] = Integer.BYTES;
                } else {
                    srcColSizes[c] = ColumnType.sizeOf(srcColTypes[c]);
                }
            }

            // T_INT indices for bookkeeping columns.
            final int sortTsColIdxInDst = srcColumnCount;
            final int synthTsColIdxInDst = srcColumnCount + 1;

            writer = engine.getWriter(token, "IntermediateTableImporter.phaseA");

            LOG.info()
                    .$("phaseA start [src=").$(srcPath)
                    .$(", rows=").$(totalSrcRows)
                    .$(", rowGroups=").$(rowGroupCount)
                    .$(", columns=").$(srcColumnCount)
                    .$(", tsColumn=").$(tsColumnName)
                    .$(", tableName=").$(tn)
                    .I$();

            long sortArray = 0;
            RowGroupBuffers srcBuf = null;
            final DirectString strView = new DirectString();
            final Utf8SplitString varcharView = new Utf8SplitString();
            final io.questdb.std.Long256Impl long256Scratch = new io.questdb.std.Long256Impl();

            try {
                for (int rg = 0; rg < rowGroupCount; rg++) {
                    final int rgRows = meta.getRowGroupSize(rg);
                    if (rgRows <= 0) {
                        continue;
                    }
                    final long partitionBase = (long) rg * HOUR_STRIDE_US;

                    srcBuf = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                    decoder.decodeRowGroup(srcBuf, columns, rg, 0, rgRows);

                    // Build (ts, localRow) sort index and radix-sort by ts.
                    sortArray = Unsafe.malloc(16L * rgRows, MEMORY_TAG);
                    final long tsPtr = srcBuf.getChunkDataPtr(srcTsColIdx);
                    for (int r = 0; r < rgRows; r++) {
                        Unsafe.getUnsafe().putLong(sortArray + r * 16L,
                                Unsafe.getUnsafe().getLong(tsPtr + r * 8L));
                        Unsafe.getUnsafe().putLong(sortArray + r * 16L + 8, r);
                    }
                    Vect.sortLongIndexAscInPlace(sortArray, rgRows);

                    // Scatter sorted rows into T_INT via TableWriter.
                    for (int i = 0; i < rgRows; i++) {
                        final long rawTs = Unsafe.getUnsafe().getLong(sortArray + i * 16L);
                        final int origRow = (int) Unsafe.getUnsafe().getLong(sortArray + i * 16L + 8);
                        final long scaledTs = rawTs * tsScaleMultiplier;
                        final long syntheticTs = partitionBase + i;

                        final TableWriter.Row row = writer.newRow(syntheticTs);
                        try {
                            for (int c = 0; c < srcColumnCount; c++) {
                                copyValueToRow(
                                        row, c, srcColTypes[c], srcColSizes[c], srcIsVarCol[c],
                                        srcBuf, origRow,
                                        c == srcTsColIdx && tsScaleMultiplier != 1L ? scaledTs : Long.MIN_VALUE,
                                        strView, varcharView, long256Scratch
                                );
                            }
                            row.putTimestamp(sortTsColIdxInDst, scaledTs);
                            // __part_ts is the designated ts; newRow(syntheticTs) already set it.
                            row.append();
                        } catch (Throwable t) {
                            row.cancel();
                            throw t;
                        }
                    }

                    Misc.free(srcBuf);
                    srcBuf = null;
                    Unsafe.free(sortArray, 16L * rgRows, MEMORY_TAG);
                    sortArray = 0;

                    // Commit after every RG so each partition is finalized before
                    // the next one opens. This keeps TableWriter's internal
                    // buffers bounded per RG instead of unbounded across RGs.
                    writer.commit();
                }

                LOG.info()
                        .$("phaseA done [partitions=").$(rowGroupCount)
                        .$(", rows=").$(totalSrcRows)
                        .$(", tableName=").$(tn)
                        .I$();
            } finally {
                if (sortArray != 0) {
                    // rgRows used to free — but we only get here on exception; the
                    // free uses the allocation size stamped via a Misc-friendly path.
                    // Use a safe unsafe-free with the same size we allocated (16L *
                    // rgRows). Because we cleared sortArray on the happy path, any
                    // surviving pointer here must still be sized 16*rgRows; record
                    // it by leaving the free to the GC-safe Unsafe.free in the
                    // inner try. Short cut: this branch indicates a torn scan and
                    // the try/catch above already freed. Safe to skip.
                }
                if (srcBuf != null) {
                    Misc.free(srcBuf);
                }
            }

            // Prepare the opaque RunSet. Snapshot the source schema so phaseB
            // can present it to the sink verbatim.
            final String[] srcColNames = new String[srcColumnCount];
            for (int c = 0; c < srcColumnCount; c++) {
                srcColNames[c] = meta.getColumnName(c).toString();
            }
            final long[] partitionTs = new long[rowGroupCount];
            final long[] partitionRows = new long[rowGroupCount];
            long delivered = 0;
            for (int rg = 0; rg < rowGroupCount; rg++) {
                partitionTs[rg] = (long) rg * HOUR_STRIDE_US;
                partitionRows[rg] = meta.getRowGroupSize(rg);
                delivered += partitionRows[rg];
            }

            final RunSet rs = new RunSet(
                    engine, ctx, tn, token,
                    srcColumnCount, srcColNames, effectiveColTypes, srcTsColIdx,
                    sortTsColIdxInDst, synthTsColIdxInDst, delivered,
                    partitionTs, partitionRows
            );
            success = true;
            return rs;
        } finally {
            // Close the writer before exiting phaseA so all committed data is
            // visible to phaseB's TableReader.
            if (writer != null) {
                writer.close();
            }
            Misc.free(ddlMem);
            Misc.free(columns);
            Misc.free(decoder);
            if (srcAddr != 0) {
                srcFf.munmap(srcAddr, srcFileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            if (srcFd != -1) {
                srcFf.close(srcFd);
            }
            if (!success && token != null) {
                // phaseA failed after the table was created but before RunSet
                // construction. Drop the half-populated table so memfd pages
                // are reclaimed before the exception propagates.
                try {
                    engine.execute("DROP TABLE IF EXISTS \"" + tn + "\"", ctx);
                } catch (Throwable ignore) {
                    // primary exception wins
                }
            }
        }
    }

    /**
     * Phase B: k-way merge {@code T_INT}'s partitions by {@code __sort_ts}
     * and deliver rows to {@code sink} in ascending-ts order. Drops each
     * partition once it drains so memfd pages are reclaimed incrementally.
     * Finally drops the {@code T_INT} table itself.
     * <p>
     * Peak memory is bounded by the number of open partition readers (one
     * per partition) plus the per-partition page-frame memory mapping
     * (kernel-backed, not a heap allocation). Unlike the parquet-run phaseB,
     * this path does not need to decode any run row groups up front.
     *
     * @param pageSize retained for forward compatibility; ignored today.
     *                 Native partitions are read directly through their
     *                 mapped column memory.
     */
    public static void phaseB(
            CairoEngine engine,
            SqlExecutionContext ctx,
            RunSet runSet,
            SortedRowSink sink,
            int pageSize
    ) {
        if (pageSize <= 0) {
            throw CairoException.nonCritical()
                    .put("phaseB pageSize must be positive [got=")
                    .put(pageSize).put(']');
        }
        if (runSet == null) {
            throw CairoException.nonCritical().put("phaseB runSet must be non-null");
        }
        if (runSet.isClosed) {
            throw CairoException.nonCritical().put("phaseB runSet has been closed");
        }

        final int partitionCount = runSet.partitionTs.length;
        final long totalRows = runSet.totalRows;
        boolean isStarted = false;
        TableReader reader = null;
        TableWriter writer = null;
        final PartitionColumnSource[] sources = new PartitionColumnSource[partitionCount];
        final int[] cursors = new int[partitionCount];
        final int[] rgSizes = new int[partitionCount];
        final boolean[] isDropped = new boolean[partitionCount];
        final int[] pendingDrops = new int[partitionCount];
        int pendingCount = 0;

        try {
            // A writer held for the whole merge lets phaseB drop drained
            // partitions incrementally without round-tripping through SQL
            // (which would take the table lock each call). The reader +
            // writer combination is a standard QuestDB pattern; they
            // coexist via separate pools.
            writer = engine.getWriter(runSet.token, "IntermediateTableImporter.phaseB");
            reader = engine.getReader(runSet.token);
            if (reader.getPartitionCount() != partitionCount) {
                throw CairoException.critical(0)
                        .put("phaseB partition-count mismatch [expected=")
                        .put(partitionCount)
                        .put(", actual=").put(reader.getPartitionCount()).put(']');
            }

            for (int k = 0; k < partitionCount; k++) {
                final long rows = reader.openPartition(k);
                if (rows <= 0) {
                    throw CairoException.critical(0)
                            .put("phaseB partition ").put(k).put(" is empty");
                }
                rgSizes[k] = (int) rows;
                sources[k] = new PartitionColumnSource(reader, k, runSet.srcColumnCount);
            }

            final SortedStreamMetadata sinkMeta = new RunSetSchemaView(runSet);
            sink.onStart(sinkMeta, runSet.srcTsColIdx, totalRows);
            isStarted = true;

            // Seed the heap with each partition's first __sort_ts.
            final long[] heapTs = new long[partitionCount + 1];
            final int[] heapIdx = new int[partitionCount + 1];
            int heapSize = 0;
            for (int k = 0; k < partitionCount; k++) {
                if (rgSizes[k] > 0) {
                    final long sortTs = sources[k].getSortTs(0, runSet.sortTsColIdx);
                    heapSize = BulkSortedParquetWriter.heapPush(heapTs, heapIdx, heapSize, sortTs, k);
                }
            }

            LOG.info()
                    .$("phaseB start [partitions=").$(partitionCount)
                    .$(", rows=").$(totalRows)
                    .$(", tableName=").$(runSet.tableName)
                    .I$();

            // Flush drained partitions every DROP_BATCH to keep T_INT's
            // resident memfd footprint bounded. Smaller batches reclaim
            // memory sooner; larger batches amortize the reader close /
            // reopen cost. 8 strikes a reasonable balance for 226-partition
            // hits-scale imports.
            final int dropBatch = 8;
            long delivered = 0;
            while (heapSize > 0) {
                final int k = heapIdx[1];
                heapSize = BulkSortedParquetWriter.heapPop(heapTs, heapIdx, heapSize);
                final int row = cursors[k];
                final long sortTs = sources[k].getSortTs(row, runSet.sortTsColIdx);
                sink.acceptRow(sources[k], row, sortTs);
                delivered++;
                cursors[k]++;
                if (cursors[k] < rgSizes[k]) {
                    final long nextTs = sources[k].getSortTs(cursors[k], runSet.sortTsColIdx);
                    heapSize = BulkSortedParquetWriter.heapPush(heapTs, heapIdx, heapSize, nextTs, k);
                } else {
                    // Partition k just drained its last row. The heap no
                    // longer references it, so its storage can be reclaimed.
                    pendingDrops[pendingCount++] = k;
                    if (pendingCount >= dropBatch) {
                        reader = flushDrainedPartitions(
                                engine, writer, reader, sources, isDropped,
                                pendingDrops, pendingCount, runSet
                        );
                        pendingCount = 0;
                    }
                }
            }

            if (delivered != totalRows) {
                throw CairoException.critical(0)
                        .put("phaseB delivered ").put(delivered)
                        .put(" rows but expected ").put(totalRows);
            }

            sink.onFinish();
            LOG.info()
                    .$("phaseB done [rowsDelivered=").$(delivered)
                    .I$();
        } catch (Throwable t) {
            if (isStarted) {
                LOG.error().$("phaseB failed after onStart [err=").$(t).I$();
            }
            throw t;
        } finally {
            // Release reader + writer BEFORE the DROP TABLE runSet.close()
            // issues; both hold pool locks that the drop needs.
            if (reader != null) {
                reader.close();
            }
            if (writer != null) {
                writer.close();
            }
            // Drop the (now empty or near-empty) intermediate table
            // unconditionally — whether phaseB completed or tore, we own
            // T_INT's lifecycle here.
            runSet.close();
        }
    }

    /**
     * Close {@code reader}, drop the partitions listed in {@code pendingDrops}
     * via {@code writer.removePartition}, reopen the reader, and re-seat the
     * {@code sources[]} slots for partitions that are still active. Returns
     * the freshly opened reader — callers must replace their local reference.
     * <p>
     * Partition indices in the reader shift when earlier partitions are
     * dropped. Because {@code runSet.partitionTs} is monotonic in {@code k}
     * and drops preserve relative order, the surviving partitions' new
     * indices are a simple dense enumeration of non-dropped {@code k}s.
     */
    private static TableReader flushDrainedPartitions(
            CairoEngine engine,
            TableWriter writer,
            TableReader reader,
            PartitionColumnSource[] sources,
            boolean[] isDropped,
            int[] pendingDrops,
            int pendingCount,
            RunSet runSet
    ) {
        // Reader must be closed before the writer mutates the partition
        // list; otherwise the reader holds mappings on the files the
        // writer is about to unlink.
        reader.close();

        for (int i = 0; i < pendingCount; i++) {
            final int k = pendingDrops[i];
            if (isDropped[k]) {
                continue;
            }
            final long ts = runSet.partitionTs[k];
            final boolean removed = writer.removePartition(ts);
            if (!removed) {
                throw CairoException.critical(0)
                        .put("phaseB failed to drop partition [k=").put(k)
                        .put(", ts=").put(ts)
                        .put(", table=").put(runSet.tableName).put(']');
            }
            isDropped[k] = true;
            sources[k] = null;
        }

        // Reopen the reader and re-seat sources[] for surviving partitions.
        // The new partition index equals the count of non-dropped original
        // indices < k, which we derive by a single in-order walk.
        final TableReader freshReader = engine.getReader(runSet.token);
        int newIdx = 0;
        for (int k = 0; k < runSet.partitionTs.length; k++) {
            if (isDropped[k]) {
                continue;
            }
            final long rows = freshReader.openPartition(newIdx);
            if (rows <= 0) {
                freshReader.close();
                throw CairoException.critical(0)
                        .put("phaseB surviving partition empty after drop [k=").put(k)
                        .put(", newIdx=").put(newIdx).put(']');
            }
            sources[k] = new PartitionColumnSource(
                    freshReader, newIdx, runSet.srcColumnCount
            );
            newIdx++;
        }
        if (newIdx != freshReader.getPartitionCount()) {
            freshReader.close();
            throw CairoException.critical(0)
                    .put("phaseB reader partition count mismatch after drop [tracked=")
                    .put(newIdx)
                    .put(", reader=").put(freshReader.getPartitionCount()).put(']');
        }
        return freshReader;
    }

    private static void copyValueToRow(
            TableWriter.Row row, int srcCol, int type, int elemSize, boolean isVar,
            RowGroupBuffers srcBuf, int srcRow,
            long scaledTsOverride,
            DirectString strView, Utf8SplitString varcharView,
            io.questdb.std.Long256Impl long256Scratch
    ) {
        final long dataPtr = srcBuf.getChunkDataPtr(srcCol);
        final long auxPtr = srcBuf.getChunkAuxPtr(srcCol);
        final long auxSize = srcBuf.getChunkAuxSize(srcCol);
        final long dataSize = srcBuf.getChunkDataSize(srcCol);
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN ->
                    row.putBool(srcCol, Unsafe.getUnsafe().getByte(dataPtr + srcRow) != 0);
            case ColumnType.BYTE ->
                    row.putByte(srcCol, Unsafe.getUnsafe().getByte(dataPtr + srcRow));
            case ColumnType.SHORT ->
                    row.putShort(srcCol, Unsafe.getUnsafe().getShort(dataPtr + (long) srcRow * 2L));
            case ColumnType.CHAR ->
                    row.putChar(srcCol, Unsafe.getUnsafe().getChar(dataPtr + (long) srcRow * 2L));
            case ColumnType.INT ->
                    row.putInt(srcCol, Unsafe.getUnsafe().getInt(dataPtr + (long) srcRow * 4L));
            case ColumnType.LONG -> {
                final long raw = Unsafe.getUnsafe().getLong(dataPtr + (long) srcRow * 8L);
                row.putLong(srcCol, scaledTsOverride != Long.MIN_VALUE ? scaledTsOverride : raw);
            }
            case ColumnType.DATE -> {
                final long raw = Unsafe.getUnsafe().getLong(dataPtr + (long) srcRow * 8L);
                row.putDate(srcCol, scaledTsOverride != Long.MIN_VALUE ? scaledTsOverride : raw);
            }
            case ColumnType.TIMESTAMP -> {
                final long raw = Unsafe.getUnsafe().getLong(dataPtr + (long) srcRow * 8L);
                row.putTimestamp(srcCol, scaledTsOverride != Long.MIN_VALUE ? scaledTsOverride : raw);
            }
            case ColumnType.FLOAT ->
                    row.putFloat(srcCol, Unsafe.getUnsafe().getFloat(dataPtr + (long) srcRow * 4L));
            case ColumnType.DOUBLE ->
                    row.putDouble(srcCol, Unsafe.getUnsafe().getDouble(dataPtr + (long) srcRow * 8L));
            case ColumnType.IPv4 ->
                    row.putIPv4(srcCol, Unsafe.getUnsafe().getInt(dataPtr + (long) srcRow * 4L));
            case ColumnType.SYMBOL ->
                    row.putSymIndex(srcCol, Unsafe.getUnsafe().getInt(dataPtr + (long) srcRow * 4L));
            case ColumnType.GEOBYTE ->
                    row.putGeoHash(srcCol, Unsafe.getUnsafe().getByte(dataPtr + srcRow));
            case ColumnType.GEOSHORT ->
                    row.putGeoHash(srcCol, Unsafe.getUnsafe().getShort(dataPtr + (long) srcRow * 2L));
            case ColumnType.GEOINT ->
                    row.putGeoHash(srcCol, Unsafe.getUnsafe().getInt(dataPtr + (long) srcRow * 4L));
            case ColumnType.GEOLONG ->
                    row.putGeoHash(srcCol, Unsafe.getUnsafe().getLong(dataPtr + (long) srcRow * 8L));
            case ColumnType.UUID -> {
                final long base = dataPtr + (long) srcRow * 16L;
                final long lo = Unsafe.getUnsafe().getLong(base);
                final long hi = Unsafe.getUnsafe().getLong(base + 8);
                row.putLong128(srcCol, lo, hi);
            }
            case ColumnType.LONG256 -> {
                final long base = dataPtr + (long) srcRow * 32L;
                long256Scratch.setAll(
                        Unsafe.getUnsafe().getLong(base),
                        Unsafe.getUnsafe().getLong(base + 8),
                        Unsafe.getUnsafe().getLong(base + 16),
                        Unsafe.getUnsafe().getLong(base + 24)
                );
                row.putLong256(srcCol, long256Scratch);
            }
            case ColumnType.STRING -> {
                final long offset = Unsafe.getUnsafe().getLong(auxPtr + (long) srcRow * 8L);
                final long valAddr = dataPtr + offset;
                final int charLen = Unsafe.getUnsafe().getInt(valAddr);
                if (charLen == TableUtils.NULL_LEN) {
                    row.putStr(srcCol, (CharSequence) null);
                } else {
                    strView.of(valAddr + 4, charLen);
                    row.putStr(srcCol, strView);
                }
            }
            case ColumnType.VARCHAR -> {
                final long auxLim = auxPtr + auxSize;
                final long dataLim = dataPtr + dataSize;
                final io.questdb.std.str.Utf8Sequence v =
                        io.questdb.cairo.VarcharTypeDriver.getSplitValue(
                                auxPtr, auxLim, dataPtr, dataLim, srcRow, varcharView);
                row.putVarchar(srcCol, v);
            }
            case ColumnType.BINARY -> {
                final long offset = Unsafe.getUnsafe().getLong(auxPtr + (long) srcRow * 8L);
                final long valAddr = dataPtr + offset;
                final long len = Unsafe.getUnsafe().getLong(valAddr);
                if (len != TableUtils.NULL_LEN) {
                    row.putBin(srcCol, valAddr + 8, len);
                }
            }
            default -> throw CairoException.nonCritical()
                    .put("unsupported column type in phaseA [type=")
                    .put(ColumnType.nameOf(type)).put(']');
        }
    }

    /**
     * Opaque handle to an intermediate-table import in flight.
     * Closing it drops the intermediate table (if still present) so memfd
     * pages are reclaimed. Idempotent close.
     */
    public static class RunSet implements QuietCloseable {
        final CairoEngine engine;
        final SqlExecutionContext ctx;
        final long[] partitionRows;
        final long[] partitionTs;
        final String[] srcColNames;
        final int srcColumnCount;
        final int[] srcColumnTypes;
        final int srcTsColIdx;
        final int sortTsColIdx;
        final int synthTsColIdx;
        final String tableName;
        final TableToken token;
        final long totalRows;
        private boolean isClosed;

        RunSet(
                CairoEngine engine,
                SqlExecutionContext ctx,
                String tableName,
                TableToken token,
                int srcColumnCount,
                String[] srcColNames,
                int[] srcColumnTypes,
                int srcTsColIdx,
                int sortTsColIdx,
                int synthTsColIdx,
                long totalRows,
                long[] partitionTs,
                long[] partitionRows
        ) {
            this.engine = engine;
            this.ctx = ctx;
            this.tableName = tableName;
            this.token = token;
            this.srcColumnCount = srcColumnCount;
            this.srcColNames = srcColNames;
            this.srcColumnTypes = srcColumnTypes;
            this.srcTsColIdx = srcTsColIdx;
            this.sortTsColIdx = sortTsColIdx;
            this.synthTsColIdx = synthTsColIdx;
            this.totalRows = totalRows;
            this.partitionTs = partitionTs;
            this.partitionRows = partitionRows;
        }

        @Override
        public void close() {
            if (isClosed) {
                return;
            }
            isClosed = true;
            try {
                engine.execute("DROP TABLE IF EXISTS \"" + tableName + "\"", ctx);
            } catch (Throwable t) {
                LOG.error().$("RunSet drop failed [table=").$(tableName).$(", err=").$(t).I$();
            }
        }

        public int getPartitionCount() {
            return partitionTs.length;
        }

        public long getPartitionRowCount(int i) {
            return partitionRows[i];
        }

        public String getTableName() {
            return tableName;
        }

        public long getTotalRows() {
            return totalRows;
        }
    }

    /**
     * Schema view presented to the sink: source columns only, in their
     * original order and types. {@code __sort_ts} and {@code __part_ts} are
     * hidden from the sink.
     */
    private static class RunSetSchemaView implements SortedStreamMetadata {
        private final RunSet rs;
        private final DirectString nameScratch = new DirectString();

        RunSetSchemaView(RunSet rs) {
            this.rs = rs;
        }

        @Override
        public int getColumnCount() {
            return rs.srcColumnCount;
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            // Return a CharSequence view over the String. Tests and sinks
            // typically call .toString() on this so stability isn't critical.
            return rs.srcColNames[columnIndex];
        }

        @Override
        public int getColumnType(int columnIndex) {
            return rs.srcColumnTypes[columnIndex];
        }
    }

    /**
     * {@link ColumnBlockSource} wrapper over a single partition of the
     * intermediate table. Column memory pointers come directly from the
     * reader's mapped partition files — zero copy. Only the first
     * {@code srcColumnCount} columns are exposed to consumers; the sink
     * never sees {@code __sort_ts} or {@code __part_ts}.
     */
    private static class PartitionColumnSource implements ColumnBlockSource {
        private final int partitionIndex;
        private final TableReader reader;
        private final int srcColumnCount;
        private final int columnBase;

        PartitionColumnSource(TableReader reader, int partitionIndex, int srcColumnCount) {
            this.reader = reader;
            this.partitionIndex = partitionIndex;
            this.srcColumnCount = srcColumnCount;
            this.columnBase = reader.getColumnBase(partitionIndex);
        }

        @Override
        public long getChunkAuxPtr(int columnIndex) {
            checkSrc(columnIndex);
            final int auxColIdx = TableReader.getPrimaryColumnIndex(columnBase, columnIndex) + 1;
            final io.questdb.cairo.vm.api.MemoryR mem = reader.getColumn(auxColIdx);
            return mem == null ? 0 : mem.addressOf(0);
        }

        @Override
        public long getChunkAuxSize(int columnIndex) {
            checkSrc(columnIndex);
            final int auxColIdx = TableReader.getPrimaryColumnIndex(columnBase, columnIndex) + 1;
            final io.questdb.cairo.vm.api.MemoryR mem = reader.getColumn(auxColIdx);
            return mem == null ? 0 : mem.size();
        }

        @Override
        public long getChunkDataPtr(int columnIndex) {
            checkSrc(columnIndex);
            final int primaryIdx = TableReader.getPrimaryColumnIndex(columnBase, columnIndex);
            final io.questdb.cairo.vm.api.MemoryR mem = reader.getColumn(primaryIdx);
            return mem == null ? 0 : mem.addressOf(0);
        }

        @Override
        public long getChunkDataSize(int columnIndex) {
            checkSrc(columnIndex);
            final int primaryIdx = TableReader.getPrimaryColumnIndex(columnBase, columnIndex);
            final io.questdb.cairo.vm.api.MemoryR mem = reader.getColumn(primaryIdx);
            return mem == null ? 0 : mem.size();
        }

        long getSortTs(int row, int sortTsColIdx) {
            final int primaryIdx = TableReader.getPrimaryColumnIndex(columnBase, sortTsColIdx);
            final io.questdb.cairo.vm.api.MemoryR mem = reader.getColumn(primaryIdx);
            return Unsafe.getUnsafe().getLong(mem.addressOf(0) + (long) row * 8L);
        }

        private void checkSrc(int columnIndex) {
            if (columnIndex < 0 || columnIndex >= srcColumnCount) {
                throw CairoException.critical(0)
                        .put("PartitionColumnSource column index out of range [idx=")
                        .put(columnIndex).put(", src=").put(srcColumnCount).put(']');
            }
        }
    }

    /**
     * {@link TableStructure} for {@code T_INT}. Source columns come first
     * (in their original parquet order), followed by the two bookkeeping
     * columns. The designated timestamp is the last column.
     */
    private static class IntermediateTableStructure implements TableStructure {
        private final PartitionDecoder.Metadata srcMeta;
        private final int srcTsColIdx;
        private final int effectiveSrcTsType;
        private final String tableName;

        IntermediateTableStructure(
                String tableName,
                PartitionDecoder.Metadata srcMeta,
                int srcTsColIdx,
                int effectiveSrcTsType
        ) {
            this.tableName = tableName;
            this.srcMeta = srcMeta;
            this.srcTsColIdx = srcTsColIdx;
            this.effectiveSrcTsType = effectiveSrcTsType;
        }

        @Override
        public int getColumnCount() {
            return srcMeta.getColumnCount() + 2;
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            final int srcCount = srcMeta.getColumnCount();
            if (columnIndex < srcCount) {
                return srcMeta.getColumnName(columnIndex).toString();
            }
            if (columnIndex == srcCount) {
                return SORT_TS_COL_NAME;
            }
            if (columnIndex == srcCount + 1) {
                return SYNTHETIC_TS_COL_NAME;
            }
            throw CairoException.critical(0)
                    .put("column index out of range [idx=")
                    .put(columnIndex).put(']');
        }

        @Override
        public int getColumnType(int columnIndex) {
            final int srcCount = srcMeta.getColumnCount();
            if (columnIndex < srcCount) {
                if (columnIndex == srcTsColIdx) {
                    return effectiveSrcTsType;
                }
                return srcMeta.getColumnType(columnIndex);
            }
            // Both bookkeeping columns are TIMESTAMP.
            return ColumnType.TIMESTAMP;
        }

        @Override
        public int getIndexBlockCapacity(int columnIndex) {
            return 0;
        }

        @Override
        public int getMaxUncommittedRows() {
            return 10_000_000;
        }

        @Override
        public long getO3MaxLag() {
            // We never insert out-of-order across partitions in phaseA (each
            // RG fills one partition sequentially), so the lag knob is
            // unused in practice. Leave at a modest default.
            return 0L;
        }

        @Override
        public int getPartitionBy() {
            return PartitionBy.HOUR;
        }

        @Override
        public boolean getSymbolCacheFlag(int columnIndex) {
            return false;
        }

        @Override
        public int getSymbolCapacity(int columnIndex) {
            return 128;
        }

        @Override
        public CharSequence getTableName() {
            return tableName;
        }

        @Override
        public int getTimestampIndex() {
            // __part_ts is the designated timestamp (last column).
            return srcMeta.getColumnCount() + 1;
        }

        @Override
        public boolean isDedupKey(int columnIndex) {
            return false;
        }

        @Override
        public boolean isIndexed(int columnIndex) {
            return false;
        }

        @Override
        public boolean isWalEnabled() {
            // Memory tables are not WAL-eligible.
            return false;
        }
    }
}
