/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cairo.vm;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.LPSZ;

public class NullMemoryMR implements MemoryMR {

    public static final NullMemoryMR INSTANCE = new NullMemoryMR();

    @Override
    public long addressOf(long offset) {
        return 0;
    }

    @Override
    public void close() {
    }

    @Override
    public void extend(long size) {
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
    public char getChar(long offset) {
        return 0;
    }

    @Override
    public double getDouble(long offset) {
        return Double.NaN;
    }

    @Override
    public int getFd() {
        return -1;
    }

    @Override
    public FilesFacade getFilesFacade() {
        throw new UnsupportedOperationException();
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

    public long getLong128Hi(long offset) {
        return Numbers.LONG_NaN;
    }

    public long getLong128Lo(long offset) {
        return Numbers.LONG_NaN;
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
    public long getPageAddress(int pageIndex) {
        return 0;
    }

    @Override
    public int getPageCount() {
        return 0;
    }

    @Override
    public long getPageSize() {
        return 0;
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
    public int getStrLen(long offset) {
        return TableUtils.NULL_LEN;
    }

    @Override
    public boolean isDeleted() {
        return true;
    }

    @Override
    public boolean isMapped(long offset, long len) {
        return false;
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, long opts, int madviseOpts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long offsetInPage(long offset) {
        return offset;
    }

    @Override
    public int pageIndex(long offset) {
        return 0;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void wholeFile(FilesFacade ff, LPSZ name, int memoryTag) {
        throw new UnsupportedOperationException();
    }
}
