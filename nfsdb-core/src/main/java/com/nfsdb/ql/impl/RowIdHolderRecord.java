/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.ql.impl;

import com.nfsdb.collections.DirectInputStream;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.AbstractRecordMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.AbstractRecord;
import com.nfsdb.ql.collections.LongMetadata;
import com.nfsdb.utils.Chars;

import java.io.OutputStream;

public class RowIdHolderRecord extends AbstractRecord {

    private final static String name = "KY_0C81_";

    private long rowId;

    public RowIdHolderRecord() {
        super(new AbstractRecordMetadata() {
            @Override
            public RecordColumnMetadata getColumn(int index) {
                if (index == 0) {
                    return LongMetadata.INSTANCE;
                }
                throw new JournalRuntimeException("Invalid column index: %d", index);
            }

            @Override
            public int getColumnCount() {
                return 1;
            }

            @Override
            public RecordColumnMetadata getColumnQuick(int index) {
                return LongMetadata.INSTANCE;
            }

            @Override
            public RecordColumnMetadata getTimestampMetadata() {
                return null;
            }

            @Override
            protected int getLocalColumnIndex(CharSequence name) {
                return Chars.equals(RowIdHolderRecord.name, name) ? 0 : -1;
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
    public long getBinLen(int col) {
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
    public int getStrLen(int col) {
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
