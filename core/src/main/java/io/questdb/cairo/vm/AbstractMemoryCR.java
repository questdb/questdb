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

import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.*;

// contiguous readable
public abstract class AbstractMemoryCR implements MemoryCR, Mutable {

    private final MemoryCR.ByteSequenceView bsview = new MemoryCR.ByteSequenceView();
    private final MemoryCR.CharSequenceView csview = new MemoryCR.CharSequenceView();
    private final MemoryCR.CharSequenceView csview2 = new MemoryCR.CharSequenceView();
    private final Long256Impl long256 = new Long256Impl();
    private final Long256Impl long256B = new Long256Impl();
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
