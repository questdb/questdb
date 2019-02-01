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

package com.questdb.net.ha.comsumer;

import com.questdb.net.ha.ChannelConsumer;
import com.questdb.std.ByteBuffers;
import com.questdb.std.Unsafe;
import com.questdb.std.ex.JournalException;
import com.questdb.std.ex.JournalNetworkException;
import com.questdb.store.JournalMode;
import com.questdb.store.UnstructuredFile;
import com.questdb.store.factory.configuration.Constants;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

public class HugeBufferConsumer implements ChannelConsumer, Closeable {
    private final ByteBuffer header = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
    private final long headerAddress = ByteBuffers.getAddress(header);
    private final UnstructuredFile hb;

    public HugeBufferConsumer(File file) throws JournalException {
        hb = new UnstructuredFile(file, Constants.HB_HINT, JournalMode.APPEND);
    }

    @Override
    public void close() {
        free();
    }

    public void free() {
        hb.close();
    }

    @Override
    public void read(ReadableByteChannel channel) throws JournalNetworkException {
        try {
            header.position(0);
            channel.read(header);
            long target = Unsafe.getUnsafe().getLong(headerAddress);

            long pos = 0;
            while (pos < target) {
                pos += ByteBuffers.copy(channel, hb.getBuffer(pos), target - pos);
            }
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }

    public UnstructuredFile getHb() {
        return hb;
    }
}
