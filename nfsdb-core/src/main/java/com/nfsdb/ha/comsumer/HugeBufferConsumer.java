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

package com.nfsdb.ha.comsumer;

import com.nfsdb.JournalMode;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.factory.configuration.Constants;
import com.nfsdb.ha.ChannelConsumer;
import com.nfsdb.storage.HugeBuffer;
import com.nfsdb.utils.ByteBuffers;
import com.nfsdb.utils.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

public class HugeBufferConsumer implements ChannelConsumer {
    private final ByteBuffer header = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);
    private final long headerAddr = ((DirectBuffer) header).address();
    private final HugeBuffer hb;

    public HugeBufferConsumer(File file) throws JournalException {
        hb = new HugeBuffer(file, Constants.HB_HINT, JournalMode.APPEND);
    }

    @Override
    public void free() {

    }

    @Override
    public void read(ReadableByteChannel channel) throws JournalNetworkException {
        try {
            header.position(0);
            channel.read(header);
            long target = Unsafe.getUnsafe().getLong(headerAddr);

            long pos = 0;
            while (pos < target) {
                pos += ByteBuffers.copy(channel, hb.getBuffer(pos, 1), target - pos);
            }
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }

    @Override
    public void reset() {

    }

    public HugeBuffer getHb() {
        return hb;
    }
}
