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

package com.nfsdb.journal.lang.cst;

import com.nfsdb.journal.column.ColumnType;

import java.io.InputStream;
import java.io.OutputStream;

public abstract class AbstractDataRow implements DataRow {

    private ColumnType[] types;

    @Override
    public byte get(String column) {
        return get(getColumnIndex(column));
    }

    @Override
    public int getInt(String column) {
        return getInt(getColumnIndex(column));
    }

    @Override
    public long getLong(String column) {
        return getLong(getColumnIndex(column));
    }

    @Override
    public double getDouble(String column) {
        return getDouble(getColumnIndex(column));
    }

    @Override
    public CharSequence getStr(String column) {
        return getStr(getColumnIndex(column));
    }

    @Override
    public String getSym(String column) {
        return getSym(getColumnIndex(column));
    }

    @Override
    public boolean getBool(String column) {
        return getBool(getColumnIndex(column));
    }

    @Override
    public void getBin(String column, OutputStream s) {
        getBin(getColumnIndex(column), s);
    }

    @Override
    public InputStream getBin(String column) {
        return getBin(getColumnIndex(column));
    }

    @Override
    public ColumnType getColumnType(int x) {
        if (types == null) {
            types = new ColumnType[getColumnCount()];
        }

        if (types[x] != null) {
            return types[x];
        }


        return types[x] = getColumnTypeInternal(x);
    }

    protected abstract ColumnType getColumnTypeInternal(int x);
}
