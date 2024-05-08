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

package io.questdb.cairo.vm;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.*;
import io.questdb.std.str.*;

// contiguous readable
public abstract class AbstractMemoryCR implements MemoryCR, Mutable {

    private final MemoryCR.ByteSequenceView bsview = new MemoryCR.ByteSequenceView();
    private final DirectString csviewA;
    private final DirectString csviewB;
    private final Long256Impl long256A = new Long256Impl();
    private final Long256Impl long256B = new Long256Impl();
    private final Utf8SplitString utf8SplitViewA;
    private final Utf8SplitString utf8SplitViewB;
    private final DirectUtf8String utf8ViewA;
    private final DirectUtf8String utf8ViewB;
    protected FilesFacade ff;
    protected long lim;
    protected long pageAddress = 0;
    protected long size = 0;
    private long shiftAddressRight = 0;

    public AbstractMemoryCR(boolean stableStrings) {
        if (stableStrings) {
            csviewA = new StableDirectString();
            csviewB = new StableDirectString();
        } else {
            csviewA = new DirectString();
            csviewB = new DirectString();
        }
        utf8SplitViewA = new Utf8SplitString(stableStrings);
        utf8SplitViewB = new Utf8SplitString(stableStrings);
        utf8ViewA = new DirectUtf8String(stableStrings);
        utf8ViewB = new DirectUtf8String(stableStrings);
    }

    public long addressOf(long offset) {
        offset -= shiftAddressRight;
        assert offset <= size : "offset=" + offset + ", size=" + size;
        return pageAddress + offset;
    }

    public void clear() {
        // avoid debugger seg faulting when memory is closed
        csviewA.clear();
        csviewB.clear();
        bsview.clear();
    }

    public final BinarySequence getBin(long offset) {
        return getBin(offset, bsview);
    }

    @Override
    public DirectUtf8Sequence getDirectVarcharA(long offset, int size, boolean ascii) {
        return getDirectVarchar(offset, size, utf8ViewA, ascii);
    }

    @Override
    public DirectUtf8Sequence getDirectVarcharB(long offset, int size, boolean ascii) {
        return getDirectVarchar(offset, size, utf8ViewB, ascii);
    }

    public FilesFacade getFilesFacade() {
        return ff;
    }

    public Long256 getLong256A(long offset) {
        getLong256(offset, long256A);
        return long256A;
    }

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
    public Utf8SplitString getSplitVarcharA(long auxLo, long dataLo, int size, boolean ascii) {
        return utf8SplitViewA.of(auxLo, dataLo, size, ascii);
    }

    @Override
    public Utf8SplitString getSplitVarcharB(long auxLo, long dataLo, int size, boolean ascii) {
        return utf8SplitViewB.of(auxLo, dataLo, size, ascii);
    }

    public final CharSequence getStrA(long offset) {
        return getStr(offset, csviewA);
    }

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

    private DirectUtf8String getDirectVarchar(long offset, int size, DirectUtf8String u8view, boolean ascii) {
        long addr = addressOf(offset);
        assert addr > 0;
        if (offset + size > size()) {
            throw CairoException.critical(0)
                    .put("String is outside of file boundary [offset=")
                    .put(offset)
                    .put(", size=")
                    .put(size)
                    .put(", size()=")
                    .put(size())
                    .put(']');
        }
        return u8view.of(addr, addr + size, ascii);
    }
}
