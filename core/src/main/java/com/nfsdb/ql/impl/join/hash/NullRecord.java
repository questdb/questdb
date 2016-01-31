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

package com.nfsdb.ql.impl.join.hash;

import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.misc.Numbers;
import com.nfsdb.ql.AbstractRecord;
import com.nfsdb.std.DirectInputStream;

import java.io.OutputStream;

public class NullRecord extends AbstractRecord {

    public NullRecord(RecordMetadata metadata) {
        super(metadata);
    }

    @Override
    public byte get(int col) {
        return 0;
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
        return -1L;
    }

    @Override
    public boolean getBool(int col) {
        return false;
    }

    @Override
    public long getDate(int col) {
        return Numbers.LONG_NaN;
    }

    @Override
    public double getDouble(int col) {
        return Double.NaN;
    }

    @Override
    public float getFloat(int col) {
        return Float.NaN;
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        return null;
    }

    @Override
    public int getInt(int col) {
        return Numbers.INT_NaN;
    }

    @Override
    public long getLong(int col) {
        return Numbers.LONG_NaN;
    }

    @Override
    public long getRowId() {
        return -1;
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

    }

    @Override
    public int getStrLen(int col) {
        return -1;
    }

    @Override
    public String getSym(int col) {
        return null;
    }
}
