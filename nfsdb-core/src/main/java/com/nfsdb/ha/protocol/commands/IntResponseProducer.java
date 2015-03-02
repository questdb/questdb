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

package com.nfsdb.ha.protocol.commands;

import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.ha.ChannelProducer;
import com.nfsdb.utils.ByteBuffers;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class IntResponseProducer implements ChannelProducer {

    private final ByteBuffer buffer = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);

    @Override
    public boolean hasContent() {
        return buffer.hasRemaining();
    }

    @Override
    public void write(WritableByteChannel channel) throws JournalNetworkException {
        ByteBuffers.copy(buffer, channel);
    }

    public void setValue(int value) {
        buffer.rewind();
        buffer.putInt(value);
        buffer.flip();
    }

    public void write(WritableByteChannel channel, int value) throws JournalNetworkException {
        setValue(value);
        write(channel);
    }
}
