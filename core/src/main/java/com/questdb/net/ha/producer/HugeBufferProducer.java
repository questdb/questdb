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

package com.questdb.net.ha.producer;

import com.questdb.net.ha.ChannelProducer;
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
import java.nio.channels.WritableByteChannel;

public class HugeBufferProducer implements ChannelProducer, Closeable {
    private final ByteBuffer header = ByteBuffer.allocateDirect(8);
    private final long headerAddress = ByteBuffers.getAddress(header);
    private final UnstructuredFile hb;

    public HugeBufferProducer(File file) throws JournalException {
        hb = new UnstructuredFile(file, Constants.HB_HINT, JournalMode.READ);
    }

    @Override
    public void close() {
        free();
    }

    @Override
    public void free() {
        ByteBuffers.release(header);
        hb.close();
    }

    @Override
    public boolean hasContent() {
        return true;
    }

    @Override
    public void write(WritableByteChannel channel) throws JournalNetworkException {
        long target = hb.getAppendOffset();
        Unsafe.getUnsafe().putLong(headerAddress, target);
        header.position(0);
        try {
            channel.write(header);
            long pos = 0;
            while (pos < target) {
                pos += ByteBuffers.copy(hb.getBuffer(pos), channel, target - pos);
            }
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }
}
