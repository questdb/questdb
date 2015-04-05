/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.ha;

import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.utils.ByteBuffers;
import com.nfsdb.utils.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

public abstract class AbstractObjectConsumer extends AbstractChannelConsumer {

    private final ByteBuffer header = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
    private final long headerAddress = ((DirectBuffer) header).address();
    private ByteBuffer valueBuffer;

    @Override
    public void free() {
        valueBuffer = ByteBuffers.release(valueBuffer);
        ByteBuffers.release(header);
        super.free();
    }

    final ByteBuffer getValueBuffer() {
        return valueBuffer;
    }

    @Override
    protected final void doRead(ReadableByteChannel channel) throws JournalNetworkException {
        header.position(0);
        ByteBuffers.copy(channel, header);

        int bufSz = Unsafe.getUnsafe().getInt(headerAddress);
        if (valueBuffer == null || valueBuffer.capacity() < bufSz) {
            ByteBuffers.release(valueBuffer);
            valueBuffer = ByteBuffer.allocateDirect(bufSz).order(ByteOrder.LITTLE_ENDIAN);
        } else {
            valueBuffer.rewind();
        }
        valueBuffer.limit(bufSz);
        ByteBuffers.copy(channel, valueBuffer, bufSz);
    }
}
