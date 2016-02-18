/*
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
 */

package com.nfsdb.net;

import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Files;
import com.nfsdb.misc.Net;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NetworkChannelImpl implements NetworkChannel {
    private final long fd;
    private long totalWritten = 0;

    public NetworkChannelImpl(long fd) {
        this.fd = fd;
    }

    public long getFd() {
        return fd;
    }

    @Override
    public long getTotalWrittenAndReset() {
        long r = this.totalWritten;
        this.totalWritten = 0;
        return r;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void close() throws IOException {
        Files.close(fd);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int read = Net.recv(fd, ByteBuffers.getAddress(dst) + dst.position(), dst.remaining());
        if (read > 0) {
            dst.position(dst.position() + read);
        }
        return read;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int written = Net.send(fd, ByteBuffers.getAddress(src) + src.position(), src.remaining());
        if (written > 0) {
            totalWritten += written;
            src.position(src.position() + written);
        }
        return written;
    }
}
