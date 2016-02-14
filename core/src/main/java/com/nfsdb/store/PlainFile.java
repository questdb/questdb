/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.store;

import com.nfsdb.JournalMode;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Files;
import com.nfsdb.misc.Misc;
import com.nfsdb.std.ObjList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING", "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS", "EXS_EXCEPTION_SOFTENING_HAS_CHECKED"})
public class PlainFile implements Closeable {

    private static final Log LOGGER = LogFactory.getLog(PlainFile.class);
    private final File file;
    private final JournalMode mode;
    private final int bitHint;
    private FileChannel channel;
    private ObjList<MappedByteBuffer> buffers;
    private long cachedBufferLo = -1;
    private long cachedBufferHi = -1;
    private long cachedAddress;

    public PlainFile(File file, int bitHint, JournalMode mode) throws IOException {
        this.file = file;
        this.mode = mode;
        if (bitHint < 2) {
            LOGGER.info().$("BitHint is too small for ").$(file).$();
        }
        this.bitHint = bitHint;
        open();
        this.buffers = new ObjList<>((int) (size() >>> bitHint) + 1);
    }

    public long addressOf(long offset) {
        if (offset > cachedBufferLo && offset + 1 < cachedBufferHi) {
            return cachedAddress + offset - cachedBufferLo - 1;
        } else {
            return allocateAddress(offset);
        }
    }

    @Override
    public void close() {
        unmap();
        this.channel = Misc.free(channel);
    }

    public void compact(long size) throws IOException {
        close();
        openInternal("rw");
        try {
            LOGGER.debug().$("Compacting ").$(this).$(" to ").$(size).$(" bytes").$();
            channel.truncate(size).close();
        } finally {
            close();
        }
    }

    public void delete() {
        close();
        Files.delete(file);
    }

    public int pageRemaining(long offset) {
        if (offset > cachedBufferLo && offset < cachedBufferHi) {
            return (int) (cachedBufferHi - offset - 1);
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "[file=" + file + ']';
    }

    private long allocateAddress(long offset) {
        MappedByteBuffer buf = getBufferInternal(offset);
        cachedBufferLo = offset - buf.position() - 1;
        cachedBufferHi = cachedBufferLo + buf.limit() + 2;
        cachedAddress = ByteBuffers.getAddress(buf);
        return cachedAddress + buf.position();
    }

    private MappedByteBuffer getBufferInternal(long offset) {

        int bufferSize = 1 << bitHint;
        int index = (int) (offset >>> bitHint);
        long bufferOffset = ((long) index) << ((long) bitHint);
        int bufferPos = (int) (offset - bufferOffset);

        MappedByteBuffer buffer = buffers.getQuiet(index);

        if (buffer == null) {
            buffer = mapBufferInternal(bufferOffset, bufferSize);
            assert bufferSize > 0;
            buffers.extendAndSet(index, buffer);
            switch (mode) {
                case BULK_READ:
                case BULK_APPEND:
                    // for bulk operations unmap all buffers except for current one
                    // this is to prevent OS paging large files.
                    cachedBufferLo = cachedBufferHi = -1;
                    for (int i = index - 1; i > -1; i--) {
                        MappedByteBuffer b = buffers.getAndSetQuick(i, null);
                        if (b != null) {
                            ByteBuffers.release(b);
                        }
                    }
                    break;
                default:
                    break;
            }
        }
        buffer.position(bufferPos);
        return buffer;
    }

    private MappedByteBuffer mapBufferInternal(long offset, int size) {

        try {
            MappedByteBuffer buf;
            switch (mode) {
                case READ:
                case BULK_READ:
                    // make sure size does not extend beyond actual file size, otherwise
                    // java would assume we want to write and throw an exception
                    long sz;
                    if (offset + ((long) size) > channel.size()) {
                        sz = channel.size() - offset;
                    } else {
                        sz = size;
                    }
                    assert sz > 0;
                    buf = channel.map(FileChannel.MapMode.READ_ONLY, offset, sz);
                    break;
                default:
                    buf = channel.map(FileChannel.MapMode.READ_WRITE, offset, size);
                    break;
            }
            buf.order(ByteOrder.LITTLE_ENDIAN);
            return buf;
        } catch (IOException e) {
            throw new JournalRuntimeException("Failed to memory map: %s", e, file.getAbsolutePath());
        }
    }

    private void open() throws IOException {
        String m;
        switch (mode) {
            case READ:
            case BULK_READ:
                m = "r";
                break;
            default:
                m = "rw";
                break;
        }
        openInternal(m);
    }

    private void openInternal(String mode) throws IOException {
        File pf = file.getParentFile();
        if (pf == null) {
            throw new IOException("Expected a file: " + file);
        }

        if (!pf.exists() && !pf.mkdirs()) {
            throw new IOException("Could not create directories: " + file.getParentFile().getAbsolutePath());
        }

        this.channel = new RandomAccessFile(file, mode).getChannel();
    }

    private long size() throws IOException {
        return channel.size();
    }

    private void unmap() {
        for (int i = 0, k = buffers.size(); i < k; i++) {
            MappedByteBuffer b = buffers.getQuick(i);
            if (b != null) {
                ByteBuffers.release(b);
            }
        }
        cachedBufferLo = cachedBufferHi = -1;
        buffers.clear();
    }
}
