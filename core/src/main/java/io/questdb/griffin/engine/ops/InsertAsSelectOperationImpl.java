/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.InsertRowImpl;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Misc;

public class InsertAsSelectOperationImpl implements InsertOperation {
    private static final Log LOG = LogFactory.getLog(InsertAsSelectOperationImpl.class);
    private final long batchSize;
    private final RecordToRowCopier copier;
    private final InsertSelectOperationFuture doneFuture = new InsertSelectOperationFuture();
    private final CairoEngine engine;
    private final RecordCursorFactory factory;
    private final InsertSelectMethodImpl insertMethod = new InsertSelectMethodImpl();
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
            long metadataVersion,
            int timestampIndex,
            long batchSize,
            long o3MaxLag
    ) {
        this.batchSize = batchSize;
        this.copier = copier;
        this.engine = engine;
        this.o3MaxLag = o3MaxLag;
        this.tableToken = tableToken;
        this.metadataVersion = metadataVersion;
        this.factory = cursorFactory;
        this.timestampIndex = timestampIndex;
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
    public InsertMethod createMethod(SqlExecutionContext executionContext) {
        return createMethod(executionContext, engine);
    }

    @Override
    public InsertMethod createMethod(SqlExecutionContext executionContext, WriterSource writerSource) {
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
                writer.close();
                throw TableReferenceOutOfDateException.of(tableToken.getTableName());
            }
            insertMethod.writer = writer;
        }
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
        private TableWriterAPI writer = null;

        @Override
        public void close() {
            writer = Misc.free(writer);
        }

        @Override
        public void commit() {
            writer.commit();
        }

        @Override
        public long execute(SqlExecutionContext executionContext) throws SqlException {
            executionContext.setUseSimpleCircuitBreaker(true);
            try (RecordCursor cursor = factory.getCursor(executionContext)) {
                try {
                    if (timestampIndex == -1) {
                        rowCount = SqlCompilerImpl.copyUnordered(executionContext, cursor, writer, copier);
                    } else {
                        if (batchSize != -1) {
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
                } catch (Throwable e) {
                    // rollback data when system error occurs
                    try {
                        writer.rollback();
                    } catch (Throwable e2) {
                        // Writer is distressed, exception already logged, the pool will handle it when writer is returned
                        LOG.error().$("could not rollback, writer must be distressed [table=").$(tableToken).I$();
                    }
                    throw e;
                } finally {
                    executionContext.setUseSimpleCircuitBreaker(false);
                }
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
