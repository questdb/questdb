/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableStructureChangesException;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertStatement;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.mp.SCSequence;

public class CompiledQueryImpl implements CompiledQuery {
    private final CairoEngine engine;
    private final QueryFutureImpl queryFuture;
    private RecordCursorFactory recordCursorFactory;
    private InsertStatement insertStatement;
    private UpdateStatement updateStatement;
    private TextLoader textLoader;
    private AlterStatement alterStatement;
    private short type;
    private SqlExecutionContext sqlExecutionContext;
    //count of rows affected by this statement
    // currently works for update/insert as select/create table as insert
    private long affectedRowsCount;

    public CompiledQueryImpl(CairoEngine engine) {
        this.engine = engine;
        queryFuture = new QueryFutureImpl(engine);
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        return recordCursorFactory;
    }

    @Override
    public InsertStatement getInsertStatement() {
        return insertStatement;
    }

    @Override
    public TextLoader getTextLoader() {
        return textLoader;
    }

    @Override
    public AlterStatement getAlterStatement() {
        return alterStatement;
    }

    @Override
    public short getType() {
        return type;
    }

    @Override
    public UpdateStatement getUpdateStatement() {
        return updateStatement;
    }

    public CompiledQuery ofUpdate(UpdateStatement updateStatement) {
        this.updateStatement = updateStatement;
        this.type = UPDATE;
        return this;
    }

    @Override
    public QueryFuture execute(SCSequence eventSubSeq) throws SqlException {
        switch (type) {
            case INSERT:
                return executeInsert();
            case UPDATE:
                return executeUpdate(eventSubSeq);
            case ALTER:
                return executeAlter(eventSubSeq);
            default:
                return DONE;
        }
    }

    @Override
    public long getAffectedRowsCount() {
        return affectedRowsCount;
    }

    public CompiledQuery of(short type) {
        return of(type, null);
    }

    public CompiledQuery ofLock() {
        type = LOCK;
        return this;
    }

    public CompiledQuery ofUnlock() {
        type = UNLOCK;
        return this;
    }

    public CompiledQueryImpl withContext(SqlExecutionContext sqlExecutionContext) {
        this.sqlExecutionContext = sqlExecutionContext;
        if (updateStatement != null) {
            updateStatement.withContext(sqlExecutionContext);
        }
        return this;
    }

    private QueryFuture executeInsert() throws SqlException {
        try (InsertMethod insertMethod = insertStatement.createMethod(sqlExecutionContext)) {
            insertMethod.execute();
            insertMethod.commit();
            return DONE;
        }
    }

    private QueryFuture executeUpdate(SCSequence eventSubSeq) throws SqlException {
        try {
            try (
                    TableWriter writer = engine.getWriter(
                            sqlExecutionContext.getCairoSecurityContext(),
                            updateStatement.getTableName(),
                            "Update table execute"
                    )
            ) {
                affectedRowsCount = updateStatement.apply(writer);
                updateStatement.close();
                return DONE;
            } catch (EntryUnavailableException busyException) {
                if (eventSubSeq == null) {
                    throw busyException;
                }
                return executeAsync(updateStatement, eventSubSeq, false);
            } catch (TableStructureChangesException e) {
                assert false : "This must never happen for UPDATE, tableName=" + updateStatement.getTableName();
                return DONE;
            }
        } catch (Throwable th) {
            updateStatement.close();
            throw th;
        }
    }

    private QueryFuture executeAlter(SCSequence eventSubSeq) throws SqlException {
        try (
                TableWriter writer = engine.getWriter(
                        sqlExecutionContext.getCairoSecurityContext(),
                        alterStatement.getTableName(),
                        "Alter table execute"
                )
        ) {
            affectedRowsCount = alterStatement.apply(writer, true);
            return DONE;
        } catch (EntryUnavailableException busyException) {
            if (eventSubSeq == null) {
                throw busyException;
            }
            return executeAsync(alterStatement, eventSubSeq, true);
        } catch (TableStructureChangesException e) {
            assert false : "This must never happen when parameter acceptStructureChange=true, tableName=" + alterStatement.getTableName();
            return DONE;
        }
    }

    public QueryFuture executeAsync(
            AsyncWriterCommand asyncWriterCommand,
            SCSequence eventSubSeq,
            boolean acceptStructureChange
    ) throws SqlException {
        try {
            queryFuture.of(asyncWriterCommand, sqlExecutionContext, eventSubSeq, acceptStructureChange);
            return queryFuture;
        } catch (TableStructureChangesException e) {
            assert false : "This must never happen, command is either UPDATE or parameter acceptStructureChange=true";
            return DONE;
        }
    }

    CompiledQuery of(RecordCursorFactory recordCursorFactory) {
        return of(SELECT, recordCursorFactory);
    }

    private CompiledQuery of(short type, RecordCursorFactory factory) {
        this.type = type;
        this.recordCursorFactory = factory;
        this.affectedRowsCount = -1;
        return this;
    }

    CompiledQuery ofAlter(AlterStatement statement) {
        of(ALTER);
        alterStatement = statement;
        return this;
    }

    CompiledQuery ofBackupTable() {
        return of(BACKUP_TABLE);
    }

    CompiledQuery ofCopyLocal() {
        return of(COPY_LOCAL);
    }

    CompiledQuery ofCopyRemote(TextLoader textLoader) {
        this.textLoader = textLoader;
        return of(COPY_REMOTE);
    }

    CompiledQuery ofCreateTable() {
        return of(CREATE_TABLE);
    }

    CompiledQuery ofCreateTableAsSelect(long affectedRowsCount) {
        of(CREATE_TABLE_AS_SELECT);
        this.affectedRowsCount = affectedRowsCount;
        return this;
    }

    CompiledQuery ofDrop() {
        return of(DROP);
    }

    CompiledQuery ofInsert(InsertStatement insertStatement) {
        this.insertStatement = insertStatement;
        return of(INSERT);
    }

    CompiledQuery ofInsertAsSelect(long affectedRowsCount) {
        of(INSERT_AS_SELECT);
        this.affectedRowsCount = affectedRowsCount;
        return this;
    }

    CompiledQuery ofRenameTable() {
        return of(RENAME_TABLE);
    }

    CompiledQuery ofRepair() {
        return of(REPAIR);
    }

    CompiledQuery ofSet() {
        return of(SET);
    }

    CompiledQuery ofBegin() {
        return of(BEGIN);
    }

    CompiledQuery ofCommit() {
        return of(COMMIT);
    }

    CompiledQuery ofRollback() {
        return of(ROLLBACK);
    }

    CompiledQuery ofTruncate() {
        return of(TRUNCATE);
    }

    CompiledQuery ofVacuum() {
        return of(VACUUM);
    }

    CompiledQuery ofSnapshotPrepare() {
        return of(SNAPSHOT_DB_PREPARE);
    }

    CompiledQuery ofSnapshotComplete() {
        return of(SNAPSHOT_DB_COMPLETE);
    }

    private final QueryFuture DONE = new QueryFuture() {
        @Override
        public void await() {
        }

        @Override
        public int await(long timeout) {
            return QUERY_COMPLETE;
        }

        @Override
        public int getStatus() {
            return QUERY_COMPLETE;
        }

        @Override
        public long getAffectedRowsCount() {
            return affectedRowsCount;
        }

        @Override
        public void close() {
        }
    };
}
