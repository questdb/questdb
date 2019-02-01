/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.std.str;

import com.questdb.std.ByteBuffers;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class FlexBufferSink extends AbstractCharSink implements Closeable {
    WritableByteChannel channel;
    private ByteBuffer buffer;
    private int capacity;

    public FlexBufferSink(WritableByteChannel channel, int bufferSize) {
        this(bufferSize);
        this.channel = channel;
    }

    FlexBufferSink() {
        this(1024);
    }

    private FlexBufferSink(int capacity) {
        this.buffer = ByteBuffer.allocateDirect(this.capacity = capacity);
    }

    @Override
    public void close() throws IOException {
        free();
    }

    @Override
    public void flush() throws IOException {
        buffer.flip();
        channel.write(buffer);
        buffer.clear();
    }

    @Override
    public CharSink put(CharSequence cs) {
        if (cs != null) {
            for (int i = 0, len = cs.length(); i < len; i++) {
                put(cs.charAt(i));
            }
        }
        return this;
    }

    @Override
    public CharSink put(char c) {
        if (!buffer.hasRemaining()) {
            resize();
        }
        buffer.put((byte) c);
        return this;
    }

    private void free() {
        buffer = ByteBuffers.release(buffer);
    }

    private void resize() {
        ByteBuffer buf = ByteBuffer.allocateDirect(capacity = capacity << 1);
        this.buffer.flip();
        ByteBuffers.copy(this.buffer, buf);
        ByteBuffers.release(this.buffer);
        this.buffer = buf;
    }
}
