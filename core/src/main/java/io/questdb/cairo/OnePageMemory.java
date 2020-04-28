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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.LPSZ;

import java.io.Closeable;

public class OnePageMemory implements ReadOnlyColumn, Closeable {
    private static final Log LOG = LogFactory.getLog(OnePageMemory.class);
    private final ByteSequenceView bsview = new ByteSequenceView();
    private final CharSequenceView csview = new CharSequenceView();
    private final CharSequenceView csview2 = new CharSequenceView();
    private final Long256Impl long256 = new Long256Impl();
    private final Long256Impl long256B = new Long256Impl();
    private long page = -1;
    private FilesFacade ff;
    private long fd = -1;
    private long size = 0;
    private long absolutePointer;

    public OnePageMemory(FilesFacade ff, LPSZ name, long size) {
        of(ff, name, 0, size);
    }

    public long addressOf(long offset) {
        assert offset < size : "offset=" + offset + ", size=" + size + ", fd=" + fd;
        return absolutePointer + offset;
    }

    @Override
    public void close() {
        if (page != -1) {
            ff.munmap(page, size);
        }
        if (fd != -1) {
            ff.close(fd);
            LOG.info().$("closed [fd=").$(fd).$(']').$();
            fd = -1;
            this.size = 0;
        }
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long pageSize, long size) {
        close();
        this.ff = ff;
        boolean exists = ff.exists(name);
        if (!exists) {
            throw CairoException.instance(0).put("File not found: ").put(name);
        }
        fd = ff.openRO(name);
        if (fd == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot open file: ").put(name);
        }

        this.size = size;
        this.page = ff.mmap(fd, size, 0, Files.MAP_RO);
        this.absolutePointer = page;
        LOG.info().$("open ").$(name).$(" [fd=").$(fd).$(", pageSize=").$(size).$(", size=").$(this.size).$(']').$();
    }

    @Override
    public final BinarySequence getBin(long offset) {
        final long len = getLong(offset);
        if (len == -1) {
            return null;
        }
        return bsview.of(offset + 8, len);
    }

    @Override
    public final long getBinLen(long offset) {
        return getLong(offset);
    }

    @Override
    public boolean getBool(long offset) {
        return getByte(offset) == 1;
    }

    @Override
    public final byte getByte(long offset) {
        return Unsafe.getUnsafe().getByte(addressOf(offset));
    }

    @Override
    public final double getDouble(long offset) {
        return Unsafe.getUnsafe().getDouble(addressOf(offset));
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public final float getFloat(long offset) {
        return Unsafe.getUnsafe().getFloat(addressOf(offset));
    }

    @Override
    public final int getInt(long offset) {
        return Unsafe.getUnsafe().getInt(addressOf(offset));
    }

    @Override
    public long getLong(long offset) {
        return Unsafe.getUnsafe().getLong(addressOf(offset));
    }

    @Override
    public final BinarySequence getRawBytes(long offset, int len) {
        return bsview.of(offset, len);
    }

    @Override
    public final short getShort(long offset) {
        return Unsafe.getUnsafe().getShort(absolutePointer + offset);
    }

    @Override
    public final CharSequence getStr(long offset) {
        return getStr0(offset, csview);
    }

    @Override
    public final CharSequence getStr2(long offset) {
        return getStr0(offset, csview2);
    }

    @Override
    public Long256 getLong256A(long offset) {
        getLong256(offset, long256);
        return long256;
    }

    @Override
    public void getLong256(long offset, CharSink sink) {
        final long a, b, c, d;
        a = Unsafe.getUnsafe().getLong(addressOf(offset));
        b = Unsafe.getUnsafe().getLong(addressOf(offset + Long.BYTES));
        c = Unsafe.getUnsafe().getLong(addressOf(offset + Long.BYTES * 2));
        d = Unsafe.getUnsafe().getLong(addressOf(offset + Long.BYTES * 3));
        Numbers.appendLong256(a, b, c, d, sink);
    }

    @Override
    public Long256 getLong256B(long offset) {
        getLong256(offset, long256B);
        return long256B;
    }

    @Override
    public final char getChar(long offset) {
        return Unsafe.getUnsafe().getChar(addressOf(offset));
    }

    @Override
    public final int getStrLen(long offset) {
        return getInt(offset);
    }

    @Override
    public void grow(long size) {
    }

    @Override
    public boolean isDeleted() {
        return !ff.exists(fd);
    }

    @Override
    public int getPageCount() {
        return 1;
    }

    @Override
    public long getPageSize(int pageIndex) {
        return size;
    }

    @Override
    public long getPageAddress(int pageIndex) {
        return absolutePointer;
    }

    public void getLong256(long offset, Long256Sink sink) {
        sink.setLong0(Unsafe.getUnsafe().getLong(addressOf(offset)));
        sink.setLong1(Unsafe.getUnsafe().getLong(addressOf(offset + Long.BYTES)));
        sink.setLong2(Unsafe.getUnsafe().getLong(addressOf(+offset + Long.BYTES * 2)));
        sink.setLong3(Unsafe.getUnsafe().getLong(addressOf(offset + Long.BYTES * 3)));
    }

    public final CharSequence getStr0(long offset, CharSequenceView view) {
        final int len = getInt(offset);
        if (offset + len + Integer.BYTES < size) {
            if (len == TableUtils.NULL_LEN) {
                return null;
            }

            if (len == 0) {
                return "";
            }
            return view.of(offset + VirtualMemory.STRING_LENGTH_BYTES, len);
        }
        throw CairoException.instance(0).put("String is outside of file boundary [offset=").put(offset).put(", len=").put(len).put(']');
    }

    public long size() {
        return size;
    }

    public class CharSequenceView extends AbstractCharSequence {
        private int len;
        private long offset;

        @Override
        public int length() {
            return len;
        }

        @Override
        public char charAt(int index) {
            return OnePageMemory.this.getChar(offset + index * 2L);
        }

        CharSequenceView of(long offset, int len) {
            this.offset = offset;
            this.len = len;
            return this;
        }
    }

    private class ByteSequenceView implements BinarySequence {
        private long offset;
        private long len = -1;
        private long readAddress;

        @Override
        public byte byteAt(long index) {
            return Unsafe.getUnsafe().getByte(readAddress++);
        }

        @Override
        public void copyTo(long address, final long start, final long length) {
            long bytesRemaining = Math.min(length, this.len - start);
            long offset = this.offset + start;
            Unsafe.getUnsafe().copyMemory(page + offset, address, bytesRemaining);
        }

        @Override
        public long length() {
            return len;
        }

        ByteSequenceView of(long offset, long len) {
            this.offset = offset;
            this.len = len;
            this.readAddress = page + offset;
            return this;
        }
    }
}
