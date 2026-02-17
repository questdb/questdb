/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.BinarySequence;
import io.questdb.std.DirectByteSequenceView;
import io.questdb.std.FilesFacade;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Mutable;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8SplitString;

// contiguous readable
public abstract class AbstractMemoryCR implements MemoryCR, Mutable {

    private final BorrowedArray borrowedArray = new BorrowedArray();
    private final DirectByteSequenceView bsview = new DirectByteSequenceView();
    private final DirectString csviewA = new DirectString();
    private final DirectString csviewB = new DirectString();
    private final Long256Impl long256A = new Long256Impl();
    private final Long256Impl long256B = new Long256Impl();
    private final DirectUtf8String utf8DirectView = new DirectUtf8String();
    private final Utf8SplitString utf8SplitViewA = new Utf8SplitString();
    private final Utf8SplitString utf8SplitViewB = new Utf8SplitString();
    protected FilesFacade ff;
    protected long lim;
    protected long pageAddress = 0;
    protected long size = 0;
    private long shiftAddressRight = 0;

    @Override
    public long addressOf(long offset) {
        offset -= shiftAddressRight;
        assert checkOffsetMapped(offset);
        return pageAddress + offset;
    }

    @Override
    public void clear() {
        // avoid debugger seg faulting when memory is closed
        csviewA.clear();
        csviewB.clear();
        bsview.clear();
    }

    @Override
    public final ArrayView getArray(long offset) {
        return getArray(offset, borrowedArray);
    }

    @Override
    public final BinarySequence getBin(long offset) {
        return getBin(offset, bsview);
    }

    @Override
    public DirectUtf8Sequence getDirectVarchar(long offset, int size, boolean ascii) {
        long addr = addressOf(offset);
        assert addr > 0;
        if (checkOffsetMapped(size + offset)) {
            return utf8DirectView.of(addr, addr + size, ascii);
        }
        throw CairoException.critical(0)
                .put("varchar is outside of file boundary [offset=")
                .put(offset)
                .put(", size=")
                .put(size)
                .put(", size()=")
                .put(size())
                .put(']');
    }

    public FilesFacade getFilesFacade() {
        return ff;
    }

    @Override
    public Long256 getLong256A(long offset) {
        getLong256(offset, long256A);
        return long256A;
    }

    @Override
    public Long256 getLong256B(long offset) {
        getLong256(offset, long256B);
        return long256B;
    }

    @Override
    public long getPageAddress(int pageIndex) {
        return pageAddress;
    }

    @Override
    public int getPageCount() {
        return pageAddress == 0 ? 0 : 1;
    }

    @Override
    public Utf8SplitString getSplitVarcharA(long auxLo, long dataLo, long dataLim, int size, boolean ascii) {
        return utf8SplitViewA.of(auxLo, dataLo, dataLim, size, ascii);
    }

    @Override
    public Utf8SplitString getSplitVarcharB(long auxLo, long dataLo, long dataLim, int size, boolean ascii) {
        return utf8SplitViewB.of(auxLo, dataLo, dataLim, size, ascii);
    }

    @Override
    public final CharSequence getStrA(long offset) {
        return getStr(offset, csviewA);
    }

    @Override
    public final CharSequence getStrB(long offset) {
        return getStr(offset, csviewB);
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
    public long resize(long size) {
        extend(size);
        return pageAddress;
    }

    public void shiftAddressRight(long shiftRightOffset) {
        this.shiftAddressRight = shiftRightOffset;
    }

    @Override
    public long size() {
        return size;
    }
}
