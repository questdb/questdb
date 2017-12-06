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

package com.questdb.net.ha;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

class MockByteChannel extends ByteArrayOutputStream implements ByteChannel {

    private int offset = 0;
    private int cutoffIndex = -1;
    private boolean interrupted = false;

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public int read(ByteBuffer dst) {

        if (offset == buf.length) {
            return -1;
        }

        if (interrupted) {
            interrupted = false;
            cutoffIndex = -1;

            return 0;
        }

        // calculate cutoff point on first read
        // this is to simulate non-blocking socket mode, where there is
        // suddenly nothing to read from socket

        int oldOffset = offset;
        while (dst.remaining() > 0 && offset < buf.length) {

            // if we reached cutoff point - stop filling in buffer
            // subsequent call to read() method would return 0
            if (offset == cutoffIndex) {
                interrupted = true;
                break;
            }
            dst.put(buf[offset++]);
        }
        return offset - oldOffset;
    }

    @Override
    public String toString() {
        return "MockByteChannel{" +
                "offset=" + offset +
                '}';
    }

    @Override
    public void close() {

    }

    @Override
    public int write(ByteBuffer src) {
        int result = src.remaining();
        while (src.remaining() > 0) {
            write(src.get());
        }
        return result;
    }
}
