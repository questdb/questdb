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

package com.nfsdb.net.ha;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
    public int read(ByteBuffer dst) throws IOException {

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
    public void close() throws IOException {

    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int result = src.remaining();
        while (src.remaining() > 0) {
            write(src.get());
        }
        return result;
    }
}
