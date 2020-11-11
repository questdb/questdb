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

package io.questdb.griffin.engine.analytic.denserank;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.impl.RecordColumnMetadataImpl;
import com.questdb.ql.impl.analytic.AnalyticFunction;
import com.questdb.std.str.CharSink;
import com.questdb.store.ColumnType;
import com.questdb.store.MMappedSymbolTable;

public abstract class AbstractRankAnalyticFunction implements AnalyticFunction {

    private final RecordColumnMetadata metadata;
    protected long rank = -1;

    public AbstractRankAnalyticFunction(String name) {
        this.metadata = new RecordColumnMetadataImpl(name, ColumnType.LONG);
    }

    @Override
    public void add(Record record) {
    }

    @Override
    public byte get() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat() {
        throw new UnsupportedOperationException();
    }

    public CharSequence getStr() {
        throw new UnsupportedOperationException();
    }

    public CharSequence getStrB() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong() {
        return rank;
    }

    @Override
    public RecordColumnMetadata getMetadata() {
        return metadata;
    }

    @Override
    public short getShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getStr(CharSink sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStrLen() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSym() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MMappedSymbolTable getSymbolTable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getType() {
        return AnalyticFunction.STREAM;
    }

    @Override
    public void prepare(RecordCursor cursor) {
    }
}
