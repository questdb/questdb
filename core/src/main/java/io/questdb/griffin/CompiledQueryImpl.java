/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.sql.InsertStatement;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.text.TextLoader;

public class CompiledQueryImpl implements CompiledQuery {
    private RecordCursorFactory recordCursorFactory;
    private InsertStatement insertStatement;
    private TextLoader textLoader;
    private int type;

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
    public int getType() {
        return type;
    }

    CompiledQuery of(RecordCursorFactory recordCursorFactory) {
        this.type = SELECT;
        this.recordCursorFactory = recordCursorFactory;
        return this;
    }

    CompiledQuery ofAlter() {
        this.type = ALTER;
        return this;
    }

    CompiledQuery ofCopyLocal() {
        this.type = COPY_LOCAL;
        return this;
    }

    CompiledQuery ofCopyRemote(TextLoader textLoader) {
        this.textLoader = textLoader;
        this.type = COPY_REMOTE;
        return this;
    }

    CompiledQuery ofCreateTable() {
        this.type = CREATE_TABLE;
        return this;
    }

    CompiledQuery ofDrop() {
        this.type = DROP;
        return this;
    }

    CompiledQuery ofInsert(InsertStatement insertStatement) {
        this.insertStatement = insertStatement;
        this.type = INSERT;
        return this;
    }

    CompiledQuery ofInsertAsSelect() {
        this.type = INSERT_AS_SELECT;
        return this;
    }

    CompiledQuery ofRepair() {
        this.type = REPAIR;
        return this;
    }

    CompiledQuery ofSet() {
        this.type = SET;
        return this;
    }

    CompiledQuery ofTruncate() {
        this.type = TRUNCATE;
        return this;
    }

    CompiledQuery ofRenameTable() {
        this.type = RENAME_TABLE;
        return this;
    }

    CompiledQuery ofBackupTable() {
        this.type = BACKUP_TABLE;
        return this;
    }

    CompiledQuery ofShowTables(RecordCursorFactory recordCursorFactory) {
        this.type = SHOW_TABLES;
        this.recordCursorFactory = recordCursorFactory;
        return this;
    }
}
