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

package io.questdb.griffin.engine.ops;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnarRowAppender;
import io.questdb.cairo.NativeFrameBatchAppender;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.InsertRowImpl;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;


import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

public class InsertAsSelectOperationImpl implements InsertOperation {
    private static final Log LOG = LogFactory.getLog(InsertAsSelectOperationImpl.class);
    private final long batchSize;
    private final RecordToRowCopier copier;
    private final IntList destinationWriterIndexes;
    private final InsertSelectOperationFuture doneFuture = new InsertSelectOperationFuture();
    private final CairoEngine engine;
    private final RecordCursorFactory factory;
    private final InsertSelectMethodImpl insertMethod = new InsertSelectMethodImpl();
    private final boolean isAdaptive;
    private final long metadataVersion;
    private final long o3MaxLag;
    private final TableToken tableToken;
    private final int timestampIndex;
    private long rowCount;

    public InsertAsSelectOperationImpl(
            CairoEngine engine,
            TableToken tableToken,
            RecordCursorFactory cursorFactory,
            RecordToRowCopier copier,
            IntList destinationWriterIndexes,
            long metadataVersion,
            int timestampIndex,
            boolean isAdaptive,
            long batchSize,
            long o3MaxLag
    ) {
        this.isAdaptive = isAdaptive;
        this.batchSize = batchSize;
        this.copier = copier;
        this.destinationWriterIndexes = destinationWriterIndexes;
        this.engine = engine;
        this.o3MaxLag = o3MaxLag;
        this.tableToken = tableToken;
        this.metadataVersion = metadataVersion;
        this.factory = cursorFactory;
        this.timestampIndex = timestampIndex;
    }

    public InsertAsSelectOperationImpl(
            CairoEngine engine,
            TableToken tableToken,
            RecordCursorFactory cursorFactory,
            RecordToRowCopier copier,
            IntList destinationWriterIndexes,
            long metadataVersion,
            int timestampIndex,
            long batchSize,
            long o3MaxLag
    ) {
        this(engine, tableToken, cursorFactory, copier, destinationWriterIndexes, metadataVersion, timestampIndex, true, batchSize, o3MaxLag);
    }

    public static boolean canCopyPageFrames(SqlExecutionContext context, RecordCursorFactory factory) {
        return factory.supportsPageFrameCursor()
                && factory.producesMaterializedPageFrames()
                && factory.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_FORWARD
                && !factory.usesIndex()
                && !factory.hasParquetConvertedColumns(context);
    }

    public static long copyPageFrames(
            SqlExecutionContext context,
            TableWriterAPI writer,
            RecordCursorFactory factory,
            PageFrameCursor cursor,
            IntList writerIndexes,
            int timestampIndex,
            boolean isAdaptiveBatching,
            long batchSize,
            long o3MaxLag
    ) {
        final RecordMetadata metadata = factory.getMetadata();
        final boolean ordered = timestampIndex >= 0 && metadata.getTimestampIndex() == timestampIndex
                && factory.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_FORWARD;
        final boolean isAdaptive = isAdaptiveBatching && batchSize > 0;
        // A source ordering guarantee applies only when this starts a fresh transaction.
        if (writer.getUncommittedRowCount() == 0) {
            writer.setBatchOrdered(ordered);
            writer.setBatchAdaptive(isAdaptive);
        }
        final long target = batchSize > 0 ? batchSize : Long.MAX_VALUE;
        long rows = 0;
        long pending = 0;
        try (
                PageFrameAddressCache cache = new PageFrameAddressCache();
                PageFrameMemoryPool pool = new PageFrameMemoryPool(context.getCairoEngine().getConfiguration());
                NativeFrameBatchAppender appender = new NativeFrameBatchAppender()
        ) {
            cache.of(metadata, cursor);
            pool.setMemoryTracker(context.getMemoryTracker());
            pool.of(cache);
            PageFrame frame;
            int frameIndex = 0;
            while ((frame = cursor.next()) != null) {
                cache.add(frameIndex, frame);
                final long frameRows = frame.getPartitionHi() - frame.getPartitionLo();
                if (frameRows > Integer.MAX_VALUE) {
                    throw CairoException.nonCritical().put("SQL batch frame exceeds row limit [rows=").put(frameRows).put(']');
                }
                final PageFrameMemory memory = pool.navigateTo(frameIndex++);
                for (int lo = 0, hi = (int) frameRows; lo < hi; ) {
                    context.getCircuitBreaker().statefulThrowExceptionIfTrippedOrYield();
                    int count = (int) Math.min(hi - lo, target - pending);
                    while (true) {
                        try {
                            final ColumnarRowAppender destination = writer.getColumnarRowAppender();
                            appender.append(memory, metadata, cursor, writerIndexes, lo, count, writer.getMetadata(), destination);
                            break;
                        } catch (CairoException e) {
                            if (!isAdaptive || !e.isOutOfMemory()) {
                                throw e;
                            }
                            if (writer.getUncommittedRowCount() > 0) {
                                writer.commitWithRoleFence(context.getCairoEngine(), false);
                                pending = 0;
                                writer.setBatchOrdered(ordered);
                                writer.setBatchAdaptive(true);
                            } else if (count > 1) {
                                count = (count + 1) >>> 1;
                            } else {
                                throw e;
                            }
                        }
                    }
                    lo += count;
                    rows += count;
                    pending += count;
                    if (pending == target) {
                        writer.commitWithRoleFence(context.getCairoEngine(), false);
                        pending = 0;
                        writer.setBatchOrdered(ordered);
                        writer.setBatchAdaptive(isAdaptive);
                    }
                }
                if (frameIndex == 1024) {
                    // Appends retain no frame addresses. Bound descriptor/decoder bookkeeping for long imports.
                    pool.releaseQueryResources();
                    cache.of(metadata, cursor);
                    pool.setMemoryTracker(context.getMemoryTracker());
                    pool.of(cache);
                    frameIndex = 0;
                }
            }
        }
        return rows;
    }

    public static boolean haveMatchingBatchTypes(RecordMetadata source, TableRecordMetadata target, IntList writerIndexes) {
        if (source.getColumnCount() != writerIndexes.size()) {
            return false;
        }
        for (int i = 0, n = writerIndexes.size(); i < n; i++) {
            final int targetIndex = target.getColumnIndexQuiet(source.getColumnName(i));
            // INSERT column order may differ from source names; find by immutable writer index.
            int destination = targetIndex;
            if (destination < 0 || target.getColumnMetadata(destination).getOriginalWriterIndex() != writerIndexes.getQuick(i)) {
                destination = -1;
                for (int j = 0, m = target.getColumnCount(); j < m; j++) {
                    if (target.getColumnMetadata(j).getOriginalWriterIndex() == writerIndexes.getQuick(i)) {
                        destination = j;
                        break;
                    }
                }
            }
            if (destination < 0 || source.getColumnType(i) != target.getColumnType(destination)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void addInsertRow(InsertRowImpl row) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        Misc.free(factory);
    }

    @Override
    public InsertMethod createMethod(SqlExecutionContext executionContext) throws SqlException {
        return createMethod(executionContext, engine);
    }

    @Override
    public InsertMethod createMethod(SqlExecutionContext executionContext, WriterSource writerSource) throws SqlException {
        SecurityContext securityContext = executionContext.getSecurityContext();
        securityContext.authorizeInsert(tableToken);

        if (insertMethod.writer == null) {
            final TableWriterAPI writer = writerSource.getTableWriterAPI(tableToken, "insertAsSelect");
            if (
                // when metadata changes the compiled insert may no longer be valid, we have to
                // recompile SQL text to ensure column indexes are correct
                    writer.getMetadataVersion() != metadataVersion
                            // when table names do not match, it means table was renamed (although our table token
                            // remains valid). We should not allow user to insert into new table name because they
                            // used "old" table name in SQL text
                            || !Chars.equals(tableToken.getTableName(), writer.getTableToken().getTableName())
            ) {
                LOG.error().$("insert-as-select table reference out of date [table=").$(tableToken.getTableName())
                        .$(", compileTableId=").$(tableToken.getTableId())
                        .$(", writerTableId=").$(writer.getTableToken().getTableId())
                        .$(", writerTableName=").$(writer.getTableToken().getTableName())
                        .$(", compileMetaVersion=").$(metadataVersion)
                        .$(", writerMetaVersion=").$(writer.getMetadataVersion())
                        .$(']').$();
                writer.close();
                throw TableReferenceOutOfDateException.of(tableToken.getTableName());
            }
            insertMethod.writer = writer;
        }
        insertMethod.intermediateCommitsAllowed = writerSource == engine;
        return insertMethod;
    }

    @Override
    public OperationFuture execute(SqlExecutionContext sqlExecutionContext) throws SqlException {
        try (InsertMethod insertMethod = createMethod(sqlExecutionContext)) {
            insertMethod.execute(sqlExecutionContext);
            insertMethod.commit();
            return doneFuture;
        }
    }

    private class InsertSelectMethodImpl implements InsertMethod {
        private boolean intermediateCommitsAllowed;
        private TableWriterAPI writer = null;

        @Override
        public void close() {
            writer = Misc.free(writer);
        }

        @Override
        public void commit() {
            try {
                writer.commitWithRoleFence(engine, false);
            } catch (CairoException e) {
                if (e.isReadOnlyAccessRefusal()) {
                    writer.rollback();
                }
                throw e;
            }
        }

        @Override
        public long execute(SqlExecutionContext executionContext) throws SqlException {
            executionContext.setUseSimpleCircuitBreaker(true);
            // The flag is restored even when getCursor() throws, as it does for a plan whose view
            // was redefined since it compiled. The caller recompiles and retries on this context,
            // and every statement after it would otherwise run against the simple circuit breaker.
            try {
                try {
                    if (writer.getColumnarRowAppender() != null
                            // Evaluated after the appender lookup: a pooled WAL writer switches its token to Delta there.
                            && writer.getTableToken().isDelta()
                            && canCopyPageFrames(executionContext, factory)
                            && haveMatchingBatchTypes(factory.getMetadata(), writer.getMetadata(), destinationWriterIndexes)) {
                        try (PageFrameCursor cursor = factory.getPageFrameCursor(executionContext, ORDER_ASC)) {
                            rowCount = copyPageFrames(executionContext, writer, factory, cursor, destinationWriterIndexes,
                                    intermediateCommitsAllowed ? timestampIndex : -1,
                                    intermediateCommitsAllowed && isAdaptive,
                                    intermediateCommitsAllowed ? batchSize : -1,
                                    o3MaxLag);
                        }
                    } else {
                        try (RecordCursor cursor = factory.getCursor(executionContext)) {
                            if (timestampIndex == -1) {
                                rowCount = SqlCompilerImpl.copyUnordered(executionContext, cursor, writer, copier);
                            } else {
                                if (intermediateCommitsAllowed && batchSize > 0) {
                                    rowCount = SqlCompilerImpl.copyOrderedBatched(
                                            executionContext,
                                            writer,
                                            factory.getMetadata(),
                                            cursor,
                                            copier,
                                            timestampIndex,
                                            batchSize,
                                            o3MaxLag
                                    );
                                } else {
                                    rowCount = SqlCompilerImpl.copyOrderedBatched(executionContext, writer, factory.getMetadata(), cursor, copier, timestampIndex, Long.MAX_VALUE, 0);
                                }
                            }
                        }
                    }
                } catch (Throwable e) {
                    // rollback data when system error occurs
                    try {
                        writer.rollback();
                    } catch (Throwable e2) {
                        // Writer is distressed, exception already logged, the pool will handle it when writer is returned
                        LOG.error().$("could not rollback, writer must be distressed [table=").$(tableToken).I$();
                    }
                    throw e;
                }
            } finally {
                executionContext.setUseSimpleCircuitBreaker(false);
            }
            return rowCount;
        }

        @Override
        public TableWriterAPI getWriter() {
            return writer;
        }

        @Override
        public TableWriterAPI popWriter() {
            TableWriterAPI w = writer;
            this.writer = null;
            return w;
        }
    }

    private class InsertSelectOperationFuture extends DoneOperationFuture {
        @Override
        public long getAffectedRowsCount() {
            return rowCount;
        }
    }
}
