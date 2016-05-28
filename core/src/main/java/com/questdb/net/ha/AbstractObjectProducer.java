/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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
 ******************************************************************************/

package com.questdb.net.ha;

import com.questdb.ex.JournalNetworkException;
import com.questdb.misc.ByteBuffers;

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

    @Override
    public final void write(WritableByteChannel channel) throws JournalNetworkException {
        ByteBuffers.copy(buffer, channel);
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

    public final void write(WritableByteChannel channel, T value) throws JournalNetworkException {
        setValue(value);
        write(channel);
    }

    abstract protected int getBufferSize(T value);

    abstract protected void write(T value, ByteBuffer buffer);
}
