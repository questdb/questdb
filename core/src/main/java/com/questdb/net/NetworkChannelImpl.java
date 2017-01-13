/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.net;

import com.questdb.misc.ByteBuffers;
import com.questdb.misc.Files;
import com.questdb.misc.Net;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NetworkChannelImpl implements NetworkChannel {
    private final long fd;
    private long totalWritten = 0;
    private long consecutiveBadReadCount = 0;
    private final long ip;

    public NetworkChannelImpl(long fd) {
        this.fd = fd;
        this.ip = Net.getPeerIP(fd);
    }

    @Override
    public long getConsecutiveBadReadCount() {
        return consecutiveBadReadCount;
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
    public long getIp() {
        return ip;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int read = Net.recv(fd, ByteBuffers.getAddress(dst) + dst.position(), dst.remaining());
        if (read > 0) {
            dst.position(dst.position() + read);
            if (consecutiveBadReadCount > 0) {
                consecutiveBadReadCount = 0;
            }
        } else {
            consecutiveBadReadCount++;
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
