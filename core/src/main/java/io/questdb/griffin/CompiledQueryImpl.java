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
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.griffin.engine.ops.*;
import io.questdb.mp.SCSequence;
import io.questdb.std.QuietClosable;

public class CompiledQueryImpl implements CompiledQuery {
    private RecordCursorFactory recordCursorFactory;
    private InsertOperation insertOperation;
    private UpdateOperation updateOperation;
    private AlterOperation alterOperation;
    private TextLoader textLoader;
    private short type;
    private SqlExecutionContext sqlExecutionContext;
    private final DoneOperationFuture doneFuture = new DoneOperationFuture();
    private final OperationDispatcher<UpdateOperation> updateOperationDispatcher;
    private final OperationDispatcher<AlterOperation> alterOperationDispatcher;

    // number of rows either returned by SELECT operation or affected by UPDATE or INSERT
    private long affectedRowsCount;

    public CompiledQueryImpl(CairoEngine engine) {
        updateOperationDispatcher = new OperationDispatcher<>(engine, "sync 'UPDATE' execution");
        alterOperationDispatcher = new OperationDispatcher<>(engine, "Alter table execute");
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        return recordCursorFactory;
    }

    @Override
    public InsertOperation getInsertOperation() {
        return insertOperation;
    }

    @Override
    public TextLoader getTextLoader() {
        return textLoader;
    }

    @Override
    public AlterOperation getAlterOperation() {
        return alterOperation;
    }

    @Override
    public short getType() {
        return type;
    }

    @Override
    public UpdateOperation getUpdateOperation() {
        return updateOperation;
    }

    public CompiledQuery ofUpdate(UpdateOperation updateOperation) {
        this.updateOperation = updateOperation;
        this.type = UPDATE;
        return this;
    }

    @Override
    public OperationFuture execute(SCSequence eventSubSeq) throws SqlException {
        switch (type) {
            case INSERT:
                return insertOperation.execute(sqlExecutionContext);
            case UPDATE:
                throw SqlException.$(0, "UPDATE execution is not supported via careless invocation. UpdateOperation is allocating.");
            case ALTER:
                return alterOperationDispatcher.execute(alterOperation, sqlExecutionContext, eventSubSeq);
            default:
                return doneFuture.of(0);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends AbstractOperation> OperationDispatcher<T> getDispatcher() {
        switch (type) {
            case ALTER:
                return (OperationDispatcher<T>) alterOperationDispatcher;
            case UPDATE:
                return (OperationDispatcher<T>) updateOperationDispatcher;
            default:
                return null;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends AbstractOperation> T getOperation() {
        switch (type) {
            case INSERT:
                return (T) insertOperation;
            case UPDATE:
                return (T) updateOperation;
            case ALTER:
                return (T) alterOperation;
            default:
                return null;
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
        return this;
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

    CompiledQuery ofAlter(AlterOperation statement) {
        of(ALTER);
        alterOperation = statement;
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

    CompiledQuery ofInsert(InsertOperation insertOperation) {
        this.insertOperation = insertOperation;
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
}
