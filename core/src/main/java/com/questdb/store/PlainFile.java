/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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
 ******************************************************************************/

package com.questdb.store;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.ByteBuffers;
import com.questdb.std.Misc;
import com.questdb.std.ObjList;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class PlainFile implements Closeable {

    private static final Log LOGGER = LogFactory.getLog(PlainFile.class);
    private final File file;
    private final int journalMode;
    private final int bitHint;
    private FileChannel channel;
    private ObjList<MappedByteBuffer> buffers;
    private long cachedBufferLo = -1;
    private long cachedBufferHi = -1;
    private long cachedAddress;
    private boolean sequentialAccess = true;

    public PlainFile(File file, int bitHint, int journalMode) throws IOException {
        this.file = file;
        this.journalMode = journalMode;
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

    public boolean isSequentialAccess() {
        return sequentialAccess;
    }

    public void setSequentialAccess(boolean sequentialAccess) {
        this.sequentialAccess = sequentialAccess;
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
            if (sequentialAccess) {
                cachedBufferLo = cachedBufferHi = -1;
                for (int i = index - 1; i > -1; i--) {
                    MappedByteBuffer mbb = buffers.getQuick(i);
                    if (mbb == null) {
                        break;
                    }
                    buffers.setQuick(i, ByteBuffers.release(mbb));
                }
            }
        }
        buffer.position(bufferPos);
        return buffer;
    }

    private MappedByteBuffer mapBufferInternal(long offset, int size) {

        try {
            MappedByteBuffer buf;
            switch (journalMode) {
                case JournalMode.READ:
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
        openInternal(journalMode == JournalMode.READ ? "r" : "rw");
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
