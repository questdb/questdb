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

package io.questdb.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.CreateMatViewOperation;
import io.questdb.griffin.engine.ops.CreateTableOperation;
import io.questdb.griffin.engine.ops.CreateViewOperation;
import io.questdb.griffin.engine.ops.DoneOperationFuture;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.griffin.engine.ops.OperationDispatcher;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import org.jetbrains.annotations.Nullable;

public class CompiledQueryImpl implements CompiledQuery, Mutable {
    private final OperationDispatcher<AlterOperation> alterOperationDispatcher;
    private final DoneOperationFuture doneFuture = new DoneOperationFuture();
    private final OperationDispatcher<UpdateOperation> updateOperationDispatcher;
    // number of rows either returned by SELECT operation or affected by UPDATE or INSERT
    private long affectedRowsCount;
    private AlterOperation alterOp;
    private boolean cacheable;
    private boolean done;
    private InsertOperation insertOp;
    private boolean isExecutedAtParseTime;
    private Operation operation;
    private RecordCursorFactory recordCursorFactory;
    private SqlExecutionContext sqlExecutionContext;
    private String sqlStatement;
    // prepared statement name for DEALLOCATE operation
    private CharSequence statementName;
    private short type;
    private UpdateOperation updateOp;

    public CompiledQueryImpl(CairoEngine engine) {
        updateOperationDispatcher = new OperationDispatcher<>(engine, "sync 'UPDATE' execution") {
            @Override
            protected long apply(UpdateOperation operation, TableWriterAPI writerAPI) {
                return writerAPI.apply(operation);
            }
        };
        alterOperationDispatcher = new OperationDispatcher<>(engine, "Alter table execute") {
            @Override
            protected long apply(AlterOperation operation, TableWriterAPI writerAPI) {
                try {
                    return writerAPI.apply(operation, true);
                } finally {
                    operation.clearSecurityContext();
                }
            }
        };
    }

    @Override
    public void clear() {
        this.type = NONE;
        this.recordCursorFactory = null;
        this.affectedRowsCount = -1;
        this.insertOp = null;
        this.alterOp = null;
        this.updateOp = null;
        this.statementName = null;
        this.operation = null;
        this.isExecutedAtParseTime = false;
        this.done = false;
    }

    @Override
    public void closeAllButSelect() {
        switch (getType()) {
            case CompiledQuery.INSERT:
            case CompiledQuery.INSERT_AS_SELECT:
                Misc.free(popInsertOperation());
                break;
            case CompiledQuery.UPDATE:
                Misc.free(updateOp);
                break;
            case CompiledQuery.ALTER:
                Misc.free(alterOp);
                break;
            default:
                break;
        }
    }

    public void done() {
        this.done = true;
    }

    @Override
    public OperationFuture execute(SCSequence eventSubSeq) throws SqlException {
        return execute(sqlExecutionContext, eventSubSeq, true);
    }

    @Override
    public OperationFuture execute(
            SqlExecutionContext sqlExecutionContext,
            SCSequence eventSubSeq,
            boolean closeOnDone
    ) throws SqlException {
        if (done) {
            return doneFuture.of(0);
        }

        switch (type) {
            case INSERT:
            case INSERT_AS_SELECT:
                OperationFuture future = insertOp.execute(sqlExecutionContext);
                if (closeOnDone) {
                    Misc.free(insertOp);
                }
                return future;
            case UPDATE:
                updateOp.withSqlStatement(sqlStatement);
                return updateOperationDispatcher.execute(updateOp, sqlExecutionContext, eventSubSeq, closeOnDone);
            case ALTER:
                alterOp.withSqlStatement(sqlStatement);
                return alterOperationDispatcher.execute(alterOp, sqlExecutionContext, eventSubSeq, closeOnDone);
            case CREATE_TABLE:
            case CREATE_MAT_VIEW:
            case CREATE_TABLE_AS_SELECT:
            case DROP:
                assert false;
                // fall through
            default:
                return doneFuture.of(0);
        }
    }

    @Override
    public boolean executedAtParseTime() {
        return isExecutedAtParseTime;
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
    public Operation getOperation() {
        return operation;
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        return recordCursorFactory;
    }

    @Override
    public String getSqlText() {
        return sqlStatement;
    }

    @Override
    public CharSequence getStatementName() {
        return statementName;
    }

    @Override
    public short getType() {
        return type;
    }

    @Override
    public UpdateOperation getUpdateOperation() {
        return updateOp;
    }

    @Override
    public boolean isCacheable() {
        return cacheable;
    }

    public void ofAlter(AlterOperation alterOp) {
        of(ALTER);
        this.alterOp = alterOp;
        this.isExecutedAtParseTime = false;
    }

    @SuppressWarnings("unused")
    public void ofAlterUser() {
        of(ALTER_USER);
        this.isExecutedAtParseTime = true;
    }

    public void ofAlterView() {
        of(ALTER_VIEW);
        this.isExecutedAtParseTime = true;
    }

    public void ofBegin() {
        of(BEGIN);
        this.isExecutedAtParseTime = false;
    }

    public void ofCancelQuery() {
        of(CANCEL_QUERY);
        this.isExecutedAtParseTime = true;
    }

    public void ofCheckpointCreate() {
        of(CHECKPOINT_CREATE);
        this.isExecutedAtParseTime = true;
    }

    public void ofCheckpointRelease() {
        of(CHECKPOINT_RELEASE);
        this.isExecutedAtParseTime = true;
    }

    public void ofCommit() {
        of(COMMIT);
        this.isExecutedAtParseTime = false;
    }

    public void ofCompileView() {
        of(COMPILE_VIEW);
        this.isExecutedAtParseTime = true;
    }

    public void ofCopyRemote() {
        of(COPY_REMOTE);
        this.isExecutedAtParseTime = true;
    }

    public void ofCreateMatView(CreateMatViewOperation createMatViewOp) {
        of(CREATE_MAT_VIEW);
        this.operation = createMatViewOp;
        this.isExecutedAtParseTime = false;
    }

    public void ofCreateTable(CreateTableOperation createTableOp) {
        of(createTableOp.getSelectText() == null ? CREATE_TABLE : CREATE_TABLE_AS_SELECT);
        this.operation = createTableOp;
        this.isExecutedAtParseTime = false;
    }

    @SuppressWarnings("unused")
    public void ofCreateUser() {
        of(CREATE_USER);
        this.isExecutedAtParseTime = true;
    }

    public void ofCreateView(CreateViewOperation createViewOp) {
        of(CREATE_VIEW);
        this.operation = createViewOp;
        this.isExecutedAtParseTime = false;
    }

    public void ofDeallocate(CharSequence statementName) {
        this.statementName = Chars.toString(statementName);
        of(DEALLOCATE);
        this.isExecutedAtParseTime = false;
    }

    public void ofDrop(Operation op) {
        of(DROP);
        this.operation = op;
        this.isExecutedAtParseTime = false;
    }

    public void ofEmpty() {
        of(EMPTY, new EmptyTableRecordCursorFactory(EmptyRecordMetadata.INSTANCE));
        this.isExecutedAtParseTime = false;
    }

    public void ofExplain(RecordCursorFactory recordCursorFactory) {
        of(EXPLAIN, recordCursorFactory);
        this.isExecutedAtParseTime = false;
    }

    public void ofInsert(InsertOperation insertOperation, boolean isInsectAsSelect) {
        this.insertOp = insertOperation;
        of(isInsectAsSelect ? INSERT_AS_SELECT : INSERT);
        this.isExecutedAtParseTime = false;
    }

    // although executor was there it had to fail back to the model
    // used in enterprise version . Do NOT remove.
    @SuppressWarnings("unused")
    public void ofNone() {
        of(NONE);
    }

    public void ofPseudoSelect(@Nullable RecordCursorFactory factory) {
        this.type = PSEUDO_SELECT;
        this.recordCursorFactory = factory;
        this.affectedRowsCount = -1;
        this.isExecutedAtParseTime = false;
    }

    public void ofRefreshMatView() {
        type = REFRESH_MAT_VIEW;
        this.isExecutedAtParseTime = true;
    }

    public void ofRenameTable() {
        of(RENAME_TABLE);
        this.isExecutedAtParseTime = true;
    }

    public void ofRepair() {
        of(REPAIR);
        this.isExecutedAtParseTime = true;
    }

    public void ofRollback() {
        of(ROLLBACK);
        this.isExecutedAtParseTime = false;
    }

    public void ofSelect(RecordCursorFactory recordCursorFactory, boolean cacheable) {
        of(SELECT, recordCursorFactory);
        this.isExecutedAtParseTime = false;
        this.cacheable = cacheable;
    }

    public void ofSet() {
        of(SET);
        this.isExecutedAtParseTime = true;
    }

    public void ofTableResume() {
        type = TABLE_RESUME;
        this.isExecutedAtParseTime = true;
    }

    public void ofTableSetType() {
        type = TABLE_SET_TYPE;
        this.isExecutedAtParseTime = true;
    }

    public void ofTableSuspend() {
        type = TABLE_SUSPEND;
        this.isExecutedAtParseTime = true;
    }

    public void ofTruncate() {
        of(TRUNCATE);
        this.isExecutedAtParseTime = true;
    }

    public void ofUpdate(UpdateOperation updateOperation) {
        this.updateOp = updateOperation;
        this.type = UPDATE;
        this.isExecutedAtParseTime = false;
    }

    public void ofVacuum() {
        of(VACUUM);
        this.isExecutedAtParseTime = true;
    }

    @Override
    public InsertOperation popInsertOperation() {
        InsertOperation op = insertOp;
        this.insertOp = null;
        return op;
    }

    @Override
    public CompiledQueryImpl withContext(SqlExecutionContext sqlExecutionContext) {
        this.sqlExecutionContext = sqlExecutionContext;
        return this;
    }

    @Override
    public void withSqlText(String sqlText) {
        this.sqlStatement = sqlText;
    }

    private CompiledQuery of(short type) {
        return of(type, null);
    }

    private CompiledQuery of(short type, RecordCursorFactory factory) {
        this.type = type;
        this.recordCursorFactory = factory;
        this.affectedRowsCount = -1;
        return this;
    }
}
