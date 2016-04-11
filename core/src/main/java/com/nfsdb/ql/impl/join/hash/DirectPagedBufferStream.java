/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.impl.join.hash;

import com.nfsdb.misc.Unsafe;
import com.nfsdb.std.DirectInputStream;
import com.nfsdb.store.MemoryPages;

import java.io.IOException;

class DirectPagedBufferStream extends DirectInputStream {
    private final long length;
    private final MemoryPages buffer;
    private final long offset;
    private long blockStartAddress;
    private long blockEndOffset;
    private long blockStartOffset;
    private long position;

    DirectPagedBufferStream(MemoryPages buffer, long offset, long length) {
        this.buffer = buffer;
        this.offset = offset;
        this.blockStartAddress = buffer.addressOf(offset);
        this.blockStartOffset = 0;
        this.length = length;
    }

    @Override
    public long copyTo(final long address, long start, long len) {
        if (start < 0 || len < 0) {
            throw new IndexOutOfBoundsException();
        }

        long res;
        long rem = this.length - start;
        long size = res = len > rem ? rem : len;
        long offset = this.offset + start;

        long p = address;
        do {
            int remaining = buffer.pageRemaining(offset);
            int sz = size > remaining ? remaining : (int) size;
            Unsafe.getUnsafe().copyMemory(buffer.addressOf(offset), p, sz);
            p += sz;
            offset += sz;
            size -= sz;
        } while (size > 0);

        return res;
    }

    @Override
    public long size() {
        return (int) length - position;
    }

    @Override
    public int read() throws IOException {
        if (position < length) {
            if (position < blockEndOffset) {
                return Unsafe.getUnsafe().getByte(blockStartAddress + offset + position++ - blockStartOffset);
            }
            return readFromNextBlock();
        }
        return -1;
    }

    private int readFromNextBlock() {
        blockStartOffset = offset + position;
        blockStartAddress = buffer.addressOf(blockStartOffset);
        long blockLen = buffer.pageRemaining(blockStartOffset);
        if (blockLen < 0) {
            return -1;
        }

        blockEndOffset += blockLen;
        assert position < blockEndOffset;
        return Unsafe.getUnsafe().getByte(blockStartAddress + offset + position++ - blockStartOffset);
    }

}
