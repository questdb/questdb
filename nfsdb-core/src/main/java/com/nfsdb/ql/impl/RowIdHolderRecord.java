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

package com.nfsdb.ql.impl;

import com.nfsdb.collections.DirectInputStream;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.storage.SymbolTable;
import com.nfsdb.utils.Chars;

import java.io.OutputStream;

public class RowIdHolderRecord extends AbstractRecord {

    private final static String name = "KY_0C81_";

    private long rowId;

    public RowIdHolderRecord() {
        super(new RecordMetadata() {

            private final RecordColumnMetadata columnMetadata = new RecordColumnMetadata() {
                @Override
                public ColumnType getType() {
                    return ColumnType.LONG;
                }

                @Override
                public SymbolTable getSymbolTable() {
                    return null;
                }

                @Override
                public String getName() {
                    return name;
                }
            };

            @Override
            public RecordColumnMetadata getColumn(int index) {
                if (index == 0) {
                    return columnMetadata;
                }
                throw new JournalRuntimeException("Invalid column index: %d", index);
            }

            @Override
            public RecordColumnMetadata getColumn(CharSequence name) {
                return getColumn(getColumnIndex(name));
            }

            @Override
            public int getColumnCount() {
                return 1;
            }

            @Override
            public int getColumnIndex(CharSequence name) {
                if (Chars.equals(RowIdHolderRecord.name, name)) {
                    return 0;
                }
                throw new JournalRuntimeException("Invalid column name: %s", name);
            }

            @Override
            public boolean invalidColumn(CharSequence name) {
                return !RowIdHolderRecord.name.equals(name);
            }
        });
    }

    @Override
    public byte get(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getBin(int col, OutputStream s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DirectInputStream getBin(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDate(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int col) {
        return rowId;
    }

    @Override
    public long getRowId() {
        return rowId;
    }

    @Override
    public short getShort(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStr(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getStr(int col, CharSink sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSym(int col) {
        throw new UnsupportedOperationException();
    }

    public RowIdHolderRecord init(long rowid) {
        this.rowId = rowid;
        return this;
    }
}
