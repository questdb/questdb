/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.impl.analytic;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.AbstractRecord;
import com.questdb.ql.Record;
import com.questdb.std.CharSink;
import com.questdb.std.DirectInputStream;
import com.questdb.std.ObjList;

import java.io.OutputStream;

public class AnalyticRecord extends AbstractRecord {
    private final ObjList<AnalyticFunction> functions;
    private final int split;
    private Record base;

    public AnalyticRecord(RecordMetadata metadata, int split, ObjList<AnalyticFunction> functions) {
        super(metadata);
        this.functions = functions;
        this.split = split;
    }

    @Override
    public byte get(int col) {
        return col < split ? base.get(col) : functions.getQuick(col - split).get();
    }

    @Override
    public void getBin(int col, OutputStream s) {

    }

    @Override
    public DirectInputStream getBin(int col) {
        return null;
    }

    @Override
    public long getBinLen(int col) {
        return 0;
    }

    @Override
    public boolean getBool(int col) {
        return false;
    }

    @Override
    public long getDate(int col) {
        return 0;
    }

    @Override
    public double getDouble(int col) {
        return 0;
    }

    @Override
    public float getFloat(int col) {
        return 0;
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        return null;
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        return null;
    }

    @Override
    public int getInt(int col) {
        return col < split ? base.getInt(col) : functions.getQuick(col - split).getInt();
    }

    @Override
    public long getLong(int col) {
        return 0;
    }

    @Override
    public long getRowId() {
        return 0;
    }

    @Override
    public short getShort(int col) {
        return 0;
    }

    @Override
    public CharSequence getStr(int col) {
        return null;
    }

    @Override
    public void getStr(int col, CharSink sink) {
        if (col < split) {
            base.getStr(col, sink);
        } else {
            functions.getQuick(col - split).getStr(sink);
        }
    }

    @Override
    public int getStrLen(int col) {
        return 0;
    }

    @Override
    public String getSym(int col) {
        return null;
    }

    public void of(Record base) {
        this.base = base;
    }
}
