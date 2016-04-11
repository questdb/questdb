/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

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
