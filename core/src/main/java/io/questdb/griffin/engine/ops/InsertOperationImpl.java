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
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.InsertRowImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.util.concurrent.locks.Lock;

public class InsertOperationImpl implements InsertOperation {
    private final InsertOperationFuture doneFuture = new InsertOperationFuture();
    private final CairoEngine engine;
    private final InsertMethodImpl insertMethod = new InsertMethodImpl();
    private final ObjList<InsertRowImpl> insertRows = new ObjList<>();
    private final long metadataVersion;
    private final TableToken tableToken;

    public InsertOperationImpl(CairoEngine engine, TableToken tableToken, long metadataVersion) {
        this.engine = engine;
        this.tableToken = tableToken;
        this.metadataVersion = metadataVersion;
    }

    @Override
    public void addInsertRow(InsertRowImpl row) {
        insertRows.add(row);
    }

    @Override
    public void close() {
        Misc.free(insertMethod);
        Misc.freeObjList(insertRows);
    }

    @Override
    public InsertMethod createMethod(SqlExecutionContext executionContext, WriterSource writerSource) throws SqlException {
        SecurityContext securityContext = executionContext.getSecurityContext();
        securityContext.authorizeInsert(tableToken);

        initContext(executionContext);
        if (insertMethod.writer == null) {
            final TableWriterAPI writer = writerSource.getTableWriterAPI(tableToken, "insert");
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
    public InsertMethod createMethod(SqlExecutionContext executionContext) throws SqlException {
        return createMethod(executionContext, engine);
    }

    @Override
    public OperationFuture execute(SqlExecutionContext sqlExecutionContext) throws SqlException {
        try (InsertMethod insertMethod = createMethod(sqlExecutionContext)) {
            insertMethod.execute(sqlExecutionContext);
            insertMethod.commit();
            return doneFuture;
        }
    }

    private void initContext(SqlExecutionContext executionContext) throws SqlException {
        for (int i = 0, n = insertRows.size(); i < n; i++) {
            InsertRowImpl row = insertRows.get(i);
            row.initContext(executionContext);
        }
    }

    private class InsertMethodImpl implements InsertMethod {
        private TableWriterAPI writer = null;

        @Override
        public void close() {
            writer = Misc.free(writer);
        }

        @Override
        public void commit() {
            // Demote write-fence, mirrored from the ILP twin TableUpdateDetails.commit and the
            // pg-wire PGPipelineEntry.commit. The HTTP /exec path checks ReadOnlyStatementGate
            // before compiling, but that gate read is check-then-act: the writer is acquired while
            // the node is still PRIMARY and the rows are appended into its in-memory buffer; only
            // this commit() externalizes them (assigns a seqTxn, hands them to the WAL sequencer).
            // A PRIMARY->REPLICA flip anywhere between the gate read and this commit would otherwise
            // append a local txn on the already-demoting node and acknowledge an HTTP 200 for a write
            // no uploader will ever replicate. The append pump buffers everything; commit() is the
            // sole externalization point, so the in-lock re-check here closes the whole window.
            if (engine.isReadOnlyMode()) {
                writer.rollback();
                throw CairoException.readOnlyAccess();
            }
            final Lock lock = engine.getRoleSwitchReadLock();
            lock.lock();
            try {
                // Authoritative in-lock re-check against the role flip, which holds the WRITE side of
                // this lock around the REPLICA flag publish. Either the flip ran first (we see REPLICA
                // and refuse without committing) or we run first (we commit as PRIMARY and the flip's
                // write acquire waits for this read hold to release). Concurrent commits on other
                // tables/protocols share the read side and never serialize against each other.
                if (engine.isReadOnlyMode()) {
                    writer.rollback();
                    throw CairoException.readOnlyAccess();
                }
                writer.commit();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public long execute(SqlExecutionContext executionContext) {
            for (int i = 0, n = insertRows.size(); i < n; i++) {
                InsertRowImpl row = insertRows.get(i);
                row.append(executionContext, writer);
            }
            return insertRows.size();
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

    private class InsertOperationFuture extends DoneOperationFuture {
        @Override
        public long getAffectedRowsCount() {
            return insertRows.size();
        }
    }
}
