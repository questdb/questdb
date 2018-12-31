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

package com.questdb.tuck;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.ByteBuffers;
import com.questdb.std.Net;
import com.questdb.std.NetworkChannel;
import com.questdb.std.Os;

import java.nio.ByteBuffer;

public class NetworkChannelImpl implements NetworkChannel {
    private static final Log LOG = LogFactory.getLog(NetworkChannelImpl.class);
    private final long fd;
    private final long ip;
    private long totalWritten = 0;

    public NetworkChannelImpl(long fd) {
        this.fd = fd;
        this.ip = Net.getPeerIP(fd);
    }

    public long getFd() {
        return fd;
    }

    @Override
    public long getIp() {
        return ip;
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
    public void close() {
        if (Net.close(fd) != 0) {
            LOG.error().$("failed to close [fd=").$(fd).$(", errno=").$(Os.errno()).$(']').$();
        }
    }

    @Override
    public int read(ByteBuffer dst) {
        int p = dst.position();
        int read = Net.recv(fd, ByteBuffers.getAddress(dst) + p, dst.remaining());
        if (read > 0) {
            dst.position(p + read);
        }

        if (read == 0) {
            // When running server and client on the same host without correct CPU pinning
            // it appears server is interacting with network socket to aggressively and
            // could starve client from CPU resource. This can result in silly situation
            // where epoll releases file descriptor with nothing to read from it and
            // EWOULDBLOCK flag set, which causes server to retry descriptor infinitely.
            // To alleviate CPU contention server will simply yield CPU to client whenever
            // there is 0 bytes to read.
            Thread.yield();
        }

        return read;
    }

    @Override
    public int write(ByteBuffer src) {
        int written = Net.send(fd, ByteBuffers.getAddress(src) + src.position(), src.remaining());
        if (written > 0) {
            totalWritten += written;
            src.position(src.position() + written);
        }
        return written;
    }
}
