/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.io.sink;

import com.nfsdb.utils.ByteBuffers;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class FlexBufferSink extends AbstractCharSink implements Closeable {
    protected WritableByteChannel channel;
    private ByteBuffer buffer;
    private int capacity;

    public FlexBufferSink(WritableByteChannel channel, int bufferSize) {
        this(bufferSize);
        this.channel = channel;
    }

    public FlexBufferSink() {
        this(1024);
    }

    protected FlexBufferSink(int capacity) {
        this.buffer = ByteBuffer.allocateDirect(this.capacity = capacity);
    }

    @Override
    public void close() throws IOException {
        free();
    }

    @Override
    public void flush() {
        try {
            buffer.flip();
            channel.write(buffer);
            buffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    public void free() {
        if (buffer != null) {
            ByteBuffers.release(buffer);
        }
    }

    private void resize() {
        ByteBuffer buf = ByteBuffer.allocateDirect(capacity = capacity << 1);
        this.buffer.flip();
        ByteBuffers.copy(this.buffer, buf);
        ByteBuffers.release(this.buffer);
        this.buffer = buf;
    }
}
