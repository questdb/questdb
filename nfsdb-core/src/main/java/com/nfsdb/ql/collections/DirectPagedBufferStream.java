/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.collections;

import com.nfsdb.collections.DirectInputStream;
import com.nfsdb.storage.SequentialMemory;
import com.nfsdb.utils.Unsafe;

import java.io.IOException;

public class DirectPagedBufferStream extends DirectInputStream {
    private final long length;
    private final SequentialMemory buffer;
    private final long offset;
    private long blockStartAddress;
    private long blockEndOffset;
    private long blockStartOffset;
    private long position;

    public DirectPagedBufferStream(SequentialMemory buffer, long offset, long length) {
        this.buffer = buffer;
        this.offset = offset;
        this.blockStartAddress = buffer.addressOf(offset);
        this.blockStartOffset = 0;
        this.length = length;
    }

    @Override
    public long copyTo(long address, long start, long len) {
        if (start < 0 || len < 0) {
            throw new IndexOutOfBoundsException();
        }

        long res;
        long rem = this.length - start;
        long size = res = len > rem ? rem : len;
        long offset = this.offset + start;

        do {
            int remaining = buffer.pageRemaining(offset);
            int sz = size > remaining ? remaining : (int) size;
            Unsafe.getUnsafe().copyMemory(buffer.addressOf(offset), address, sz);
            address += sz;
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
