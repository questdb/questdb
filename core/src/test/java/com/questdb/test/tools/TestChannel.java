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

package com.questdb.test.tools;

import com.questdb.std.ByteBuffers;
import com.questdb.std.NetworkChannel;
import com.questdb.std.Unsafe;

import java.nio.ByteBuffer;

public class TestChannel implements NetworkChannel {
    private final long reqAddress;
    private final int reqLen;
    private final StringBuilder sb = new StringBuilder();
    private final StringBuilder lenBuilder = new StringBuilder();
    private boolean fullyRead = false;
    private boolean contentStarted = false;
    private int newLineCount = 0;
    private long contentLen = 0;
    private long contentReadLen = 0;
    private boolean outOfChunk = true;
    private boolean ignoreNext = false;

    public TestChannel(CharSequence request) {
        this.reqAddress = TestUtils.toMemory(request);
        this.reqLen = request.length();
    }

    public void free() {
        Unsafe.free(reqAddress, reqLen);
    }

    @Override
    public long getFd() {
        return 0;
    }

    @Override
    public long getIp() {
        return 0;
    }

    @Override
    public long getTotalWrittenAndReset() {
        return 0;
    }

    public CharSequence getOutput() {
        return sb;
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public void close() {
        reset();
    }

    @Override
    public int read(ByteBuffer dst) {
        if (!fullyRead) {
            Unsafe.getUnsafe().copyMemory(reqAddress, ByteBuffers.getAddress(dst), reqLen);
            dst.position(reqLen);
            fullyRead = true;
            return reqLen;
        }
        return 0;
    }

    public void reset() {
        sb.setLength(0);
        lenBuilder.setLength(0);
        fullyRead = false;
        contentStarted = false;
        newLineCount = 0;
        contentLen = 0;
        contentReadLen = 0;
        outOfChunk = true;
        ignoreNext = false;
    }

    @Override
    public int write(ByteBuffer src) {
        int count = src.remaining();

        while (src.hasRemaining()) {
            char c = (char) src.get();
            switch (c) {
                case '\r':
                    break;
                case '\n':
                    if (!contentStarted) {
                        newLineCount++;
                    }
                    if (newLineCount == 2) {
                        contentStarted = true;
                    }
                    break;
                default:
                    if (!contentStarted) {
                        newLineCount = 0;
                    }
                    break;
            }


            if (contentStarted) {

                if (ignoreNext) {
                    ignoreNext = false;
                    continue;
                }

                switch (c) {
                    case '\r':
                        if (outOfChunk) {
                            contentReadLen = 0;
                            contentLen = Long.parseLong(lenBuilder.toString().toUpperCase(), 16);
                            ignoreNext = true;
                            outOfChunk = false;
                            continue;
                        } else if (contentReadLen >= contentLen) {
                            lenBuilder.setLength(0);
                            outOfChunk = true;
                            ignoreNext = true;
                            continue;
                        }
                        break;
                    case '\n':
                        if (outOfChunk) {
                            continue;
                        }
                        break;
                    default:
                        break;
                }

                if (outOfChunk) {
                    lenBuilder.append(c);
                } else {
                    sb.append(c);
                    contentReadLen++;
                }
            }
        }
        return count;
    }
}
