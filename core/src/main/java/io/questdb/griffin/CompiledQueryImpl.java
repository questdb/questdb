/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.DoneOperationFuture;
import io.questdb.griffin.engine.ops.OperationDispatcher;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
import org.jetbrains.annotations.Nullable;

public class CompiledQueryImpl implements CompiledQuery {
    private final OperationDispatcher<AlterOperation> alterOperationDispatcher;
    private final DoneOperationFuture doneFuture = new DoneOperationFuture();
    private final OperationDispatcher<UpdateOperation> updateOperationDispatcher;
    // number of rows either returned by SELECT operation or affected by UPDATE or INSERT
    private long affectedRowsCount;
    private AlterOperation alterOp;
    private InsertOperation insertOp;
    private RecordCursorFactory recordCursorFactory;
    private SqlExecutionContext sqlExecutionContext;
    private String sqlStatement;
    // prepared statement name for DEALLOCATE operation
    private CharSequence statementName;
    private TextLoader textLoader;
    private short type;
    private UpdateOperation updateOp;

    public CompiledQueryImpl(CairoEngine engine) {
        updateOperationDispatcher = new OperationDispatcher<UpdateOperation>(engine, "sync 'UPDATE' execution") {
            @Override
            protected long apply(UpdateOperation operation, TableWriterAPI writerAPI) {
                return writerAPI.apply(operation);
            }
        };

        alterOperationDispatcher = new OperationDispatcher<AlterOperation>(engine, "Alter table execute") {
            @Override
            protected long apply(AlterOperation operation, TableWriterAPI writerAPI) {
                return writerAPI.apply(operation, true);
            }
        };
    }

    @Override
    public OperationFuture execute(SCSequence eventSubSeq) throws SqlException {
        return execute(sqlExecutionContext, eventSubSeq, true);
    }

    @Override
    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, SCSequence eventSubSeq, boolean closeOnDone) throws SqlException {
        switch (type) {
            case INSERT:
                return insertOp.execute(sqlExecutionContext);
            case UPDATE:
                updateOp.withSqlStatement(sqlStatement);
                return updateOperationDispatcher.execute(updateOp, sqlExecutionContext, eventSubSeq, closeOnDone);
            case ALTER:
                alterOp.withSqlStatement(sqlStatement);
                return alterOperationDispatcher.execute(alterOp, sqlExecutionContext, eventSubSeq, closeOnDone);
            default:
                return doneFuture.of(0);
        }
    }

    @Override
    public long getAffectedRowsCount() {
        return affectedRowsCount;
    }

    @Override
    public AlterOperation getAlterOperation() {
        return alterOp;
    }

    @Override
    public InsertOperation getInsertOperation() {
        return insertOp;
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        return recordCursorFactory;
    }

    @Override
    public String getSqlStatement() {
        return sqlStatement;
    }

    @Override
    public CharSequence getStatementName() {
        return statementName;
    }

    @Override
    public TextLoader getTextLoader() {
        return textLoader;
    }

    @Override
    public short getType() {
        return type;
    }

    @Override
    public UpdateOperation getUpdateOperation() {
        return updateOp;
    }

    public CompiledQuery of(short type) {
        return of(type, null);
    }

    public CompiledQuery ofAlter(AlterOperation alterOp) {
        of(ALTER);
        this.alterOp = alterOp;
        return this;
    }

    public CompiledQuery ofTableResume() {
        type = TABLE_RESUME;
        return this;
    }

    public CompiledQuery ofTableSetType() {
        type = TABLE_SET_TYPE;
        return this;
    }

    public CompiledQuery ofUpdate(UpdateOperation updateOperation) {
        this.updateOp = updateOperation;
        this.type = UPDATE;
        return this;
    }

    public CompiledQueryImpl withContext(SqlExecutionContext sqlExecutionContext) {
        this.sqlExecutionContext = sqlExecutionContext;
        return this;
    }

    public void withSqlStatement(String sqlStatement) {
        this.sqlStatement = sqlStatement;
    }

    private CompiledQuery of(short type, RecordCursorFactory factory) {
        this.type = type;
        this.recordCursorFactory = factory;
        this.affectedRowsCount = -1;
        return this;
    }

    CompiledQuery of(RecordCursorFactory recordCursorFactory) {
        return of(SELECT, recordCursorFactory);
    }

    CompiledQuery ofBackupTable() {
        return of(BACKUP_TABLE);
    }

    CompiledQuery ofBegin() {
        return of(BEGIN);
    }

    CompiledQuery ofCommit() {
        return of(COMMIT);
    }

    CompiledQuery ofCopyLocal(@Nullable RecordCursorFactory factory) {
        this.type = COPY_LOCAL;
        this.recordCursorFactory = factory;
        this.affectedRowsCount = -1;
        return this;
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

    CompiledQuery ofDeallocate(CharSequence statementName) {
        this.statementName = Chars.toString(statementName);
        return of(DEALLOCATE);
    }

    CompiledQuery ofDrop() {
        return of(DROP);
    }

    CompiledQuery ofExplain(RecordCursorFactory recordCursorFactory) {
        return of(EXPLAIN, recordCursorFactory);
    }

    CompiledQuery ofInsert(InsertOperation insertOperation) {
        this.insertOp = insertOperation;
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

    CompiledQuery ofRollback() {
        return of(ROLLBACK);
    }

    CompiledQuery ofSet() {
        return of(SET);
    }

    CompiledQuery ofSnapshotComplete() {
        return of(SNAPSHOT_DB_COMPLETE);
    }

    CompiledQuery ofSnapshotPrepare() {
        return of(SNAPSHOT_DB_PREPARE);
    }

    CompiledQuery ofTruncate() {
        return of(TRUNCATE);
    }

    CompiledQuery ofVacuum() {
        return of(VACUUM);
    }
}
