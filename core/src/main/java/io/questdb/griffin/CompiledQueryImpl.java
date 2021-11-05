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
import io.questdb.cairo.sql.AlterStatement;
import io.questdb.cairo.sql.InsertStatement;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.mp.SCSequence;

public class CompiledQueryImpl implements CompiledQuery {
    private final CairoEngine engine;
    private RecordCursorFactory recordCursorFactory;
    private InsertStatement insertStatement;
    private TextLoader textLoader;
    private AlterStatement alterStatement;
    private short type;
    private SqlExecutionContext defaultSqlExecutionContext;

    public CompiledQueryImpl(CairoEngine engine) {
        this.engine = engine;
    }

    public CompiledQueryImpl withDefaultContext(SqlExecutionContext executionContext) {
        defaultSqlExecutionContext = executionContext;
        return this;
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

    CompiledQuery of(RecordCursorFactory recordCursorFactory) {
        return of(SELECT, recordCursorFactory);
    }

    public CompiledQuery of(short type) {
        return of(type, null);
    }

    private CompiledQuery of(short type, RecordCursorFactory factory) {
        this.type = type;
        this.recordCursorFactory = factory;
        return this;
    }

    CompiledQuery ofAlter(AlterStatement statement) {
        of(ALTER);
        alterStatement = statement;
        return this;
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

    CompiledQuery ofDrop() {
        return of(DROP);
    }

    CompiledQuery ofInsert(InsertStatement insertStatement) {
        this.insertStatement = insertStatement;
        return of(INSERT);
    }

    CompiledQuery ofInsertAsSelect() {
        return of(INSERT_AS_SELECT);
    }

    CompiledQuery ofRepair() {
        return of(REPAIR);
    }

    CompiledQuery ofSet() {
        return of(SET);
    }

    CompiledQuery ofTruncate() {
        return of(TRUNCATE);
    }

    CompiledQuery ofRenameTable() {
        return of(RENAME_TABLE);
    }

    CompiledQuery ofBackupTable() {
        return of(BACKUP_TABLE);
    }

    @Override
    public long executeAlterNoWait() throws SqlException {
        return AlterCommandExecution.executeAlterCommandNoWait(
                engine,
                alterStatement,
                defaultSqlExecutionContext
        );
    }

    @Override
    public void executeAlter(SCSequence tempSequence) throws SqlException {
        AlterCommandExecution.executeAlterCommand(
                engine,
                alterStatement,
                defaultSqlExecutionContext,
                tempSequence
        );
    }

}
