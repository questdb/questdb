/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.vm.api;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.std.BinarySequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.Long256;
import io.questdb.std.Long256Acceptor;
import io.questdb.std.Decimals;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

public class NullMemory implements MemoryMAR, MemoryCARW {

    public static final NullMemory INSTANCE = new NullMemory();

    @Override
    public long addressHi() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long addressOf(long offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long appendAddressFor(long bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long appendAddressFor(long offset, long bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close(boolean truncate, byte truncateMode) {
    }

    @Override
    public void close() {
    }

    @Override
    public long detachFdClose() {
        return -1;
    }

    @Override
    public void extend(long size) {
    }

    @Override
    public long getAppendOffset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ArrayView getArray(long offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BinarySequence getBin(long offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getExtendSegmentSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getFd() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FilesFacade getFilesFacade() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long256 getLong256A(long offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long256 getLong256B(long offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPageAddress(int pageIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getPageCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrA(long offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrB(long offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMapped(long offset, long len) {
        return false;
    }

    @Override
    public void jumpTo(long offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, int memoryTag, int opts) {
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, int opts, int madviseOpts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long offsetInPage(long offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int pageIndex(long offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putArray(ArrayView array) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long putBin(BinarySequence value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long putBin(long from, long len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putBlockOfBytes(long from, long len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putBool(boolean value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putBool(long offset, boolean value) {
    }

    @Override
    public void putByte(byte value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putByte(long offset, byte value) {
    }

    @Override
    public void putChar(char value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putChar(long offset, char value) {
    }

    @Override
    public void putDecimal128(long offset, long high, long low) {
    }

    @Override
    public void putDecimal256(long offset, long hh, long hl, long lh, long ll) {
    }

    @Override
    public void putDouble(double value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putDouble(long offset, double value) {
    }

    @Override
    public void putFloat(float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putFloat(long offset, float value) {
    }

    @Override
    public void putInt(int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putInt(long offset, int value) {
    }

    @Override
    public void putLong(long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putLong(long offset, long value) {
    }

    @Override
    public void putLong128(long lo, long hi) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putLong256(long l0, long l1, long l2, long l3) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putLong256(Long256 value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putLong256(CharSequence hexString) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putLong256(@NotNull CharSequence hexString, int start, int end) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putLong256(long offset, Long256 value) {
    }

    @Override
    public void putLong256(long offset, long l0, long l1, long l2, long l3) {
    }

    @Override
    public void putLong256(CharSequence hexString, int start, int end, Long256Acceptor acceptor) {
    }

    @Override
    public void putLong256Null() {
    }

    @Override
    public long putNullBin() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long putNullStr() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putNullStr(long offset) {
    }

    @Override
    public void putShort(short value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putShort(long offset, short value) {
    }

    @Override
    public long putStr(CharSequence value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long putStr(char value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long putStr(CharSequence value, int pos, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putStr(long offset, CharSequence value) {
    }

    @Override
    public void putStr(long offset, CharSequence value, int pos, int len) {
    }

    @Override
    public long putStrUnsafe(CharSequence value, int pos, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long putVarchar(@NotNull Utf8Sequence value, int lo, int hi) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long resize(long size) {
        return 0;
    }

    @Override
    public void shiftAddressRight(long shiftRightOffset) {
    }

    @Override
    public long size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skip(long bytes) {
    }

    @Override
    public void switchTo(FilesFacade ff, long fd, long extendSegmentSize, long offset, boolean truncate, byte truncateMode) {
    }

    @Override
    public void sync(boolean async) {
    }

    @Override
    public void truncate() {
    }

    @Override
    public void zero() {
    }
}
