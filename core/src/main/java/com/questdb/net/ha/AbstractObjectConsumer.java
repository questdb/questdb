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

package com.questdb.net.ha;

import com.questdb.std.ByteBuffers;
import com.questdb.std.Unsafe;
import com.questdb.std.ex.JournalNetworkException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

public abstract class AbstractObjectConsumer extends AbstractChannelConsumer {

    private final ByteBuffer header = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
    private final long headerAddress = ByteBuffers.getAddress(header);
    private ByteBuffer valueBuffer;

    @Override
    public void free() {
        valueBuffer = ByteBuffers.release(valueBuffer);
        ByteBuffers.release(header);
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

    final protected ByteBuffer getValueBuffer() {
        return valueBuffer;
    }
}
