/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.ha.producer;

import com.nfsdb.JournalMode;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.factory.configuration.Constants;
import com.nfsdb.ha.ChannelProducer;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.storage.UnstructuredFile;

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
                pos += ByteBuffers.copy(hb.getBuffer(pos, 1), channel, target - pos);
            }
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }
}
