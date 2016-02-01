/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.net.ha.comsumer;

import com.nfsdb.JournalMode;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalNetworkException;
import com.nfsdb.factory.configuration.Constants;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.net.ha.ChannelConsumer;
import com.nfsdb.store.UnstructuredFile;

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
                pos += ByteBuffers.copy(channel, hb.getBuffer(pos, 1), target - pos);
            }
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }

    public UnstructuredFile getHb() {
        return hb;
    }
}
