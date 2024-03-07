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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.*;
import io.questdb.std.str.DirectCharSequence;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;
import org.jetbrains.annotations.NotNull;

// contiguous readable
public abstract class AbstractMemoryCR implements MemoryCR, Mutable {

    private final MemoryCR.ByteSequenceView bsview = new MemoryCR.ByteSequenceView();
    private final DirectString csview = new DirectString();
    private final DirectString csview2 = new DirectString();
    private final Long256Impl long256 = new Long256Impl();
    private final Long256Impl long256B = new Long256Impl();
    private final Utf8SplitString utf8SplitViewA = new Utf8SplitString();
    private final Utf8SplitString utf8SplitViewB = new Utf8SplitString();
    private final DirectUtf8String utf8viewA = new DirectUtf8String();
    private final DirectUtf8String utf8viewB = new DirectUtf8String();
    protected int fd = -1;
    protected FilesFacade ff;
    protected long lim;
    protected long pageAddress = 0;
    protected long size = 0;
    private long shiftAddressRight = 0;

    public long addressOf(long offset) {
        offset -= shiftAddressRight;
        assert offset <= size : "offset=" + offset + ", size=" + size + ", fd=" + fd;
        return pageAddress + offset;
    }

    public void clear() {
        // avoid debugger seg faulting when memory is closed
        csview.clear();
        csview2.clear();
        bsview.clear();
    }

    public final BinarySequence getBin(long offset) {
        return getBin(offset, bsview);
    }

    @Override
    public DirectCharSequence getDirectStr(long offset) {
        return getStr(offset, csview);
    }

    public int getFd() {
        return fd;
    }

    public FilesFacade getFilesFacade() {
        return ff;
    }

    public Long256 getLong256A(long offset) {
        getLong256(offset, long256);
        return long256;
    }

    public Long256 getLong256B(long offset) {
        getLong256(offset, long256B);
        return long256B;
    }

    @Override
    public Utf8SplitString borrowUtf8SplitStringA() {
        return utf8SplitViewA;
    }

    @Override
    public Utf8SplitString borrowUtf8SplitStringB() {
        return utf8SplitViewB;
    }

    @Override
    public long getPageAddress(int pageIndex) {
        return pageAddress;
    }

    @Override
    public int getPageCount() {
        return pageAddress == 0 ? 0 : 1;
    }

    public final CharSequence getStr(long offset) {
        return getStr(offset, csview);
    }

    public final CharSequence getStr2(long offset) {
        return getStr(offset, csview2);
    }

    @Override
    @NotNull
    public Utf8Sequence getVarcharA(long offset, int size, boolean ascii) {
        return getVarchar(offset, size, utf8viewA, ascii);
    }

    @Override
    public @NotNull Utf8Sequence getVarcharB(long offset, int size, boolean ascii) {
        return getVarchar(offset, size, utf8viewB, ascii);
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

    private DirectUtf8String getVarchar(long offset, int size, DirectUtf8String u8view, boolean ascii) {
        long addr = addressOf(offset);
        assert addr > 0;
        if (size + offset <= size()) {
            return u8view.of(addr, addr + size, ascii);
        }
        throw CairoException.critical(0)
                .put("String is outside of file boundary [offset=")
                .put(offset)
                .put(", size=")
                .put(size)
                .put(", size()=")
                .put(size())
                .put(']');
    }
}
