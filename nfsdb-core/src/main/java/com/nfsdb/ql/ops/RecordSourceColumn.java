/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.ops;

import com.nfsdb.collections.DirectInputStream;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.storage.SymbolTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.OutputStream;

public class RecordSourceColumn extends AbstractVirtualColumn {
    private final int index;

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    public RecordSourceColumn(String name, RecordMetadata metadata) {
        super(metadata.getColumn(name).getType());
        this.index = metadata.getColumnIndex(name);
        setName(name);
    }

    @Override
    public byte get() {
        return state.currentRecord().get(index);
    }

    @Override
    public void getBin(OutputStream s) {
        state.currentRecord().getBin(index, s);
    }

    @Override
    public DirectInputStream getBin() {
        return state.currentRecord().getBin(index);
    }

    @Override
    public boolean getBool() {
        return state.currentRecord().getBool(index);
    }

    @Override
    public long getDate() {
        return state.currentRecord().getDate(index);
    }

    @Override
    public double getDouble() {
        return state.currentRecord().getDouble(index);
    }

    @Override
    public float getFloat() {
        return state.currentRecord().getFloat(index);
    }

    @Override
    public CharSequence getFlyweightStr() {
        switch (getType()) {
            case SYMBOL:
                return state.currentRecord().getSym(index);
            default:
                return state.currentRecord().getFlyweightStr(index);
        }
    }

    @Override
    public int getInt() {
        return state.currentRecord().getInt(index);
    }

    @Override
    public long getLong() {
        return state.currentRecord().getLong(index);
    }

    @Override
    public short getShort() {
        return state.currentRecord().getShort(index);
    }

    @Override
    public CharSequence getStr() {
        return state.currentRecord().getStr(index);
    }

    @Override
    public void getStr(CharSink sink) {
        state.currentRecord().getStr(index, sink);
    }

    @Override
    public String getSym() {
        return state.currentRecord().getSym(index);
    }

    @Override
    public SymbolTable getSymbolTable() {
        return null;
    }

    @Override
    public boolean isConstant() {
        return false;
    }
}
