/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.ha;

import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.utils.ByteBuffers;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public abstract class AbstractObjectProducer<T> implements ChannelProducer {

    private ByteBuffer buffer;

    @Override
    public void free() {
        buffer = ByteBuffers.release(buffer);
    }

    @Override
    public final boolean hasContent() {
        return buffer != null && buffer.hasRemaining();
    }

    public void setValue(T value) {
        if (value != null) {
            int sz = getBufferSize(value);
            int bufSz = sz + 4;
            if (buffer == null || buffer.capacity() < bufSz) {
                ByteBuffers.release(buffer);
                buffer = ByteBuffer.allocateDirect(bufSz).order(ByteOrder.LITTLE_ENDIAN);
            }
            buffer.limit(bufSz);
            buffer.rewind();
            buffer.putInt(sz);
            write(value, buffer);
            buffer.flip();
        }
    }

    @Override
    public final void write(WritableByteChannel channel) throws JournalNetworkException {
        ByteBuffers.copy(buffer, channel);
    }

    public final void write(WritableByteChannel channel, T value) throws JournalNetworkException {
        setValue(value);
        write(channel);
    }

    abstract protected int getBufferSize(T value);

    abstract protected void write(T value, ByteBuffer buffer);
}
