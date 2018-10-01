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

package com.questdb.store.util;

import com.questdb.std.ByteBuffers;
import com.questdb.std.Unsafe;
import com.questdb.std.str.AbstractCharSink;
import com.questdb.std.str.CharSink;
import com.questdb.store.JournalRuntimeException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileSink extends AbstractCharSink implements Closeable {

    private final RandomAccessFile raf;
    private final FileChannel channel;
    private ByteBuffer buffer;
    private long pos;
    private long address;
    private long limit;

    public FileSink(File file) throws IOException {
        this.raf = new RandomAccessFile(file, "rw");
        this.channel = raf.getChannel();
        this.pos = 0L;
        this.address = this.limit = 0;
    }

    @Override
    public void close() throws IOException {
        ByteBuffers.release(buffer);
        if (address > 0 && pos > 0) {
            channel.truncate(pos - (limit - address));
        }
        channel.close();
        raf.close();
    }

    @Override
    public CharSink put(CharSequence cs) {
        if (cs == null) {
            return this;
        }

        for (int i = 0, l = cs.length(); i < l; i++) {
            if (address == limit) {
                map();
            }
            Unsafe.getUnsafe().putByte(address++, (byte) cs.charAt(i));
        }
        return this;
    }

    @Override
    public CharSink put(char c) {
        if (address == limit) {
            map();
        }
        Unsafe.getUnsafe().putByte(address++, (byte) c);
        return this;
    }

    private void map() {
        if (buffer != null) {
            ByteBuffers.release(buffer);
        }
        try {
            this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, this.pos, ByteBuffers.getMaxMappedBufferSize(Long.MAX_VALUE));
            pos += buffer.limit();
            address = ByteBuffers.getAddress(buffer);
            limit = address + buffer.remaining();
        } catch (IOException e) {
            throw new JournalRuntimeException(e);
        }
    }
}
