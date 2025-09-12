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
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.InsertRowImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

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
            writer.commit();
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
