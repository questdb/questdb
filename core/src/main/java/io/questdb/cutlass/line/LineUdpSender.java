/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cutlass.line;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacade;

public class LineUdpSender extends AbstractLineSender {

    private static final Log LOG = LogFactory.getLog(LineUdpSender.class);

    public LineUdpSender(int interfaceIPv4Address, int sendToIPv4Address, int sendToPort, int bufferCapacity, int ttl) {
        super(interfaceIPv4Address, sendToIPv4Address, sendToPort, bufferCapacity, ttl, LOG);
    }

    public LineUdpSender(NetworkFacade nf, int interfaceIPv4Address, int sendToIPv4Address, int sendToPort, int capacity, int ttl) {
        super(nf, interfaceIPv4Address, sendToIPv4Address, sendToPort, capacity, ttl, LOG);
    }

    @Override
    protected long createSocket(int interfaceIPv4Address, int ttl, long sockaddr) throws NetworkError {
        long fd = nf.socketUdp();

        if (fd == -1) {
            throw NetworkError.instance(nf.errno()).put("could not create UDP socket");
        }

        if (nf.setMulticastInterface(fd, interfaceIPv4Address) != 0) {
            final int errno = nf.errno();
            nf.close(fd, LOG);
            throw NetworkError.instance(errno).put("could not bind to ").ip(interfaceIPv4Address);
        }

        if (nf.setMulticastTtl(fd, ttl) != 0) {
            final int errno = nf.errno();
            nf.close(fd, LOG);
            throw NetworkError.instance(errno).put("could not set ttl [fd=").put(fd).put(", ttl=").put(ttl).put(']');
        }
        return fd;
    }

    @Override
    public CharSink put(char[] chars, int start, int len) {
        if (ptr + len < hi) {
            Chars.asciiCopyTo(chars, start, len, ptr);
        } else {
            send00();
            if (ptr + len < hi) {
                Chars.asciiCopyTo(chars, start, len, ptr);
            } else {
                throw CairoException.instance(0).put("value too long");
            }
        }
        ptr += len;
        return this;
    }

    public LineUdpSender metric(CharSequence metric) {
        if (hasMetric) {
            throw CairoException.instance(0).put("duplicate metric");
        }
        quoted = false;
        hasMetric = true;
        return put(metric);
    }

    @Override
    public LineUdpSender put(char c) {
        if (ptr >= hi) {
            send00();
        }
        Unsafe.getUnsafe().putByte(ptr++, (byte) c);
        return this;
    }

    public LineUdpSender tag(CharSequence tag, CharSequence value) {
        if (hasMetric) {
            put(',').encodeUtf8(tag).put('=').encodeUtf8(value);
            return this;
        }
        throw CairoException.instance(0).put("metric expected");
    }

    private CharSink field(CharSequence name) {
        if (hasMetric) {
            if (noFields) {
                put(' ');
                noFields = false;
            } else {
                put(',');
            }

            return encodeUtf8(name).put('=');
        }
        throw CairoException.instance(0).put("metric expected");
    }

    @Override
    public void putUtf8Special(char c) {
        switch (c) {
            case ' ':
            case ',':
            case '=':
                if (!quoted) {
                    put('\\');
                }
            default:
                put(c);
                break;
            case '"':
                if (quoted) {
                    put('\\');
                }
                put('\"');
                break;
            case '\\':
                put('\\').put('\\');
                break;
        }
    }

    private void send() {
        if (lo < lineStart) {
            int len = (int) (lineStart - lo);
            sendToSocket(fd, lo, sockaddr, len);
        }
    }

    protected void sendToSocket(long fd, long lo, long sockaddr, int len) throws NetworkError {
        if (nf.sendTo(fd, lo, len, sockaddr) != len) {
            throw NetworkError.instance(nf.errno()).put("send error");
        }
    }

    private void send00() {
        int len = (int) (ptr - lineStart);
        if (len == 0) {
            send();
            ptr = lineStart = lo;
        } else if (len < capacity) {
            long target = lo == bufA ? bufB : bufA;
            Vect.memcpy(target, lineStart, len);
            send();
            lineStart = lo = target;
            ptr = target + len;
            hi = lo + capacity;
        } else {
            throw NetworkError.instance(0).put("line too long");
        }
    }
}
