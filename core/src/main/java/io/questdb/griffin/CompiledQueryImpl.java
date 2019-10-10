/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
}
