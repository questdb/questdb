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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.str.CharSink;

public class NamedParameterLinkFunction implements ScalarFunction {
    private final String variableName;
    private final int type;
    private Function base;

    public NamedParameterLinkFunction(String variableName, int type) {
        this.variableName = variableName;
        this.type = type;
    }

    @Override
    public void close() {
        base = Misc.free(base);
    }

    @Override
    public char getChar(Record rec) {
        return getBase().getChar(rec);
    }

    @Override
    public BinarySequence getBin(Record rec) {
        return getBase().getBin(rec);
    }

    @Override
    public long getBinLen(Record rec) {
        return getBase().getBinLen(rec);
    }

    @Override
    public boolean getBool(Record rec) {
        return getBase().getBool(rec);
    }

    @Override
    public byte getByte(Record rec) {
        return getBase().getByte(rec);
    }

    @Override
    public long getDate(Record rec) {
        return getBase().getDate(rec);
    }

    @Override
    public double getDouble(Record rec) {
        return getBase().getDouble(rec);
    }

    @Override
    public float getFloat(Record rec) {
        return getBase().getFloat(rec);
    }

    @Override
    public int getInt(Record rec) {
        return getBase().getInt(rec);
    }

    @Override
    public long getLong(Record rec) {
        return getBase().getLong(rec);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        return getBase().getLong256A(rec);
    }

    @Override
    public Long256 getLong256B(Record rec) {
        return getBase().getLong256B(rec);
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        return getBase().getRecordCursorFactory();
    }

    @Override
    public short getShort(Record rec) {
        return getBase().getShort(rec);
    }

    @Override
    public CharSequence getStr(Record rec) {
        return getBase().getStr(rec);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        getBase().getStr(rec, sink);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return getBase().getStrB(rec);
    }

    @Override
    public int getStrLen(Record rec) {
        return getBase().getStrLen(rec);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return getBase().getSymbol(rec);
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return getBase().getSymbolB(rec);
    }

    @Override
    public long getTimestamp(Record rec) {
        return getBase().getTimestamp(rec);
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public void getLong256(Record rec, CharSink sink) {
        getBase().getLong256(rec, sink);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        base = executionContext.getBindVariableService().getFunction(variableName);
        if (base == null) {
            throw CairoException.instance(0).put("undefined bind variable: ").put(variableName);
        }
        assert base.getType() == type;
        base.init(symbolTableSource, executionContext);
    }

    private Function getBase() {
        assert base != null;
        return base;
    }
}
