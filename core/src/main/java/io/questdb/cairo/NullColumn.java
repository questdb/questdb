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

import io.questdb.cairo.vm.MappedReadOnlyMemory;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.LPSZ;

import static io.questdb.griffin.engine.functions.constants.NullConstant.NULL;

public class NullColumn implements MappedReadOnlyMemory {

    public static final NullColumn INSTANCE = new NullColumn();

    @Override
    public void close() {
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long pageSize, long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long pageSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDeleted() {
        return true;
    }

    @Override
    public long getFd() {
        return -1;
    }

    @Override
    public void growToFileSize() {
    }

    @Override
    public BinarySequence getBin(long offset) {
        return NULL.getBin(null);
    }

    @Override
    public long getBinLen(long offset) {
        return NULL.getBinLen(null);
    }

    @Override
    public boolean getBool(long offset) {
        return NULL.getBool(null);
    }

    @Override
    public byte getByte(long offset) {
        return NULL.getByte(null);
    }

    @Override
    public double getDouble(long offset) {
        return NULL.getDouble(null);
    }

    @Override
    public float getFloat(long offset) {
        return NULL.getFloat(null);
    }

    @Override
    public int getInt(long offset) {
        return NULL.getInt(null);
    }

    @Override
    public long getLong(long offset) {
        return NULL.getLong(null);
    }

    @Override
    public long getPageAddress(int pageIndex) {
        return 0;
    }

    @Override
    public int getPageCount() {
        return 0;
    }

    @Override
    public long getPageSize(int pageIndex) {
        return 0;
    }

    @Override
    public short getShort(long offset) {
        return NULL.getShort(null);
    }

    @Override
    public CharSequence getStr(long offset) {
        return NULL.getStr(null);
    }

    @Override
    public CharSequence getStrB(long offset) {
        return NULL.getStrB(null);
    }

    @Override
    public Long256 getLong256A(long offset) {
        return NULL.getLong256A(null);
    }

    @Override
    public void getLong256(long offset, CharSink sink) {
    }

    @Override
    public Long256 getLong256B(long offset) {
        return NULL.getLong256B(null);
    }

    @Override
    public char getChar(long offset) {
        return NULL.getChar(null);
    }

    @Override
    public int getStrLen(long offset) {
        return NULL.getStrLen(null);
    }

    @Override
    public void grow(long size) {
    }

    @Override
    public long size() {
        return 0;
    }
}
