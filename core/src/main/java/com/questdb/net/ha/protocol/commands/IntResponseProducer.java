/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.net.ha.protocol.commands;

import com.questdb.net.ha.ChannelProducer;
import com.questdb.std.ByteBuffers;
import com.questdb.std.ex.JournalNetworkException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class IntResponseProducer implements ChannelProducer {

    private final ByteBuffer buffer = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);

    @Override
    public void free() {
        ByteBuffers.release(buffer);
    }

    @Override
    public boolean hasContent() {
        return buffer.hasRemaining();
    }

    @Override
    public void write(WritableByteChannel channel) throws JournalNetworkException {
        ByteBuffers.copy(buffer, channel);
    }

    public void write(WritableByteChannel channel, int value) throws JournalNetworkException {
        setValue(value);
        write(channel);
    }

    private void setValue(int value) {
        buffer.rewind();
        buffer.putInt(value);
        buffer.flip();
    }
}
