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

package io.questdb.cairo;

import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.LPSZ;

public class NullColumn implements ReadOnlyColumn {

    public static final NullColumn INSTANCE = new NullColumn();

    @Override
    public void of(FilesFacade ff, LPSZ name, long pageSize, long size) {
    }

    @Override
    public void close() {
    }

    @Override
    public BinarySequence getBin(long offset) {
        return null;
    }

    @Override
    public long getBinLen(long offset) {
        return TableUtils.NULL_LEN;
    }

    @Override
    public boolean getBool(long offset) {
        return false;
    }

    @Override
    public byte getByte(long offset) {
        return 0;
    }

    @Override
    public double getDouble(long offset) {
        return Double.NaN;
    }

    @Override
    public long getFd() {
        return -1;
    }

    @Override
    public float getFloat(long offset) {
        return Float.NaN;
    }

    @Override
    public int getInt(long offset) {
        return Numbers.INT_NaN;
    }

    @Override
    public long getLong(long offset) {
        return Numbers.LONG_NaN;
    }

    @Override
    public BinarySequence getRawBytes(long offset, int len) {
        return null;
    }

    @Override
    public short getShort(long offset) {
        return 0;
    }

    @Override
    public CharSequence getStr(long offset) {
        return null;
    }

    @Override
    public CharSequence getStr2(long offset) {
        return null;
    }

    @Override
    public char getChar(long offset) {
        return 0;
    }

    @Override
    public int getStrLen(long offset) {
        return TableUtils.NULL_LEN;
    }

    @Override
    public void grow(long size) {
    }

    @Override
    public void getLong256(long offset, CharSink sink) {
    }

    @Override
    public Long256 getLong256A(long offset) {
        return Long256Impl.NULL_LONG256;
    }

    @Override
    public Long256 getLong256B(long offset) {
        return Long256Impl.NULL_LONG256;
    }

    @Override
    public boolean isDeleted() {
        return true;
    }

    @Override
    public int getPageCount() {
        return 0;
    }

    @Override
    public long getPageSize(int pageIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPageAddress(int pageIndex) {
        throw new UnsupportedOperationException();
    }
}
