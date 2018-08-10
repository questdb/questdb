/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.griffin.engine.functions.bind;

import com.questdb.cairo.CairoException;
import com.questdb.cairo.sql.*;
import com.questdb.std.BinarySequence;
import com.questdb.std.str.CharSink;

public class LinkFunction implements Function {
    private final String variableName;
    private final int type;
    private final int position;
    private Function base;

    public LinkFunction(String variableName, int type, int position) {
        this.variableName = variableName;
        this.type = type;
        this.position = position;
    }

    @Override
    public void close() {
        assert base != null;
        base.close();
    }

    @Override
    public BinarySequence getBin(Record rec) {
        assert base != null;
        return base.getBin(rec);
    }

    @Override
    public boolean getBool(Record rec) {
        assert base != null;
        return base.getBool(rec);
    }

    @Override
    public byte getByte(Record rec) {
        assert base != null;
        return base.getByte(rec);
    }

    @Override
    public long getDate(Record rec) {
        assert base != null;
        return base.getDate(rec);
    }

    @Override
    public double getDouble(Record rec) {
        assert base != null;
        return base.getDouble(rec);
    }

    @Override
    public float getFloat(Record rec) {
        assert base != null;
        return base.getFloat(rec);
    }

    @Override
    public int getInt(Record rec) {
        assert base != null;
        return base.getInt(rec);
    }

    @Override
    public long getLong(Record rec) {
        assert base != null;
        return base.getLong(rec);
    }

    @Override
    public RecordMetadata getMetadata() {
        assert base != null;
        return base.getMetadata();
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        assert base != null;
        return base.getRecordCursorFactory();
    }

    @Override
    public short getShort(Record rec) {
        assert base != null;
        return base.getShort(rec);
    }

    @Override
    public CharSequence getStr(Record rec) {
        assert base != null;
        return base.getStr(rec);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        assert base != null;
        base.getStr(rec, sink);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        assert base != null;
        return base.getStrB(rec);
    }

    @Override
    public int getStrLen(Record rec) {
        assert base != null;
        return base.getStrLen(rec);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        assert base != null;
        return base.getSymbol(rec);
    }

    @Override
    public long getTimestamp(Record rec) {
        assert base != null;
        return base.getTimestamp(rec);
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public void init(RecordCursor recordCursor, BindVariableService bindVariableService) {
        base = bindVariableService.getFunction(variableName);
        if (base == null) {
            throw CairoException.instance(0).put("undefined bind variable: ").put(variableName);
        }
        assert base.getType() == type;
        base.init(recordCursor, bindVariableService);
    }
}
