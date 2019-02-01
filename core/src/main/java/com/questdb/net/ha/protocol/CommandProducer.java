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

package com.questdb.net.ha.protocol;

import com.questdb.net.ha.ChannelProducer;
import com.questdb.net.ha.model.Command;
import com.questdb.std.ByteBuffers;
import com.questdb.std.ex.JournalNetworkException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class CommandProducer implements ChannelProducer {

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

    public void setCommand(byte command) {
        int sz = Command.BUFFER_SIZE;
        int bufSz = sz + 4;
        if (buffer == null || buffer.capacity() < bufSz) {
            ByteBuffers.release(buffer);
            buffer = ByteBuffer.allocateDirect(bufSz).order(ByteOrder.LITTLE_ENDIAN);
        }
        buffer.limit(bufSz);
        buffer.rewind();
        buffer.putInt(sz);
        buffer.putChar(Command.AUTHENTICITY_KEY);
        buffer.put(command);
        buffer.flip();
    }

    public final void write(WritableByteChannel channel, byte command) throws JournalNetworkException {
        setCommand(command);
        write(channel);
    }
}
