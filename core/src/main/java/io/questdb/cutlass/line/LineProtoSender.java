/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Chars;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.AbstractCharSink;
import io.questdb.std.str.CharSink;

import java.io.Closeable;

public class LineProtoSender extends AbstractCharSink implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineProtoSender.class);

    protected final int capacity;
    private final long bufA;
    private final long bufB;
    private final long sockaddr;
    protected final long fd;
    protected final NetworkFacade nf;

    private long lo;
    private long hi;
    private long ptr;
    private long lineStart;
    private boolean hasMetric = false;
    private boolean noFields = true;

    public LineProtoSender(
            int interfaceIPv4Address,
            int sendToIPv4Address,
            int sendToPort,
            int bufferCapacity,
            int ttl
    ) {
        this(NetworkFacadeImpl.INSTANCE, interfaceIPv4Address, sendToIPv4Address, sendToPort, bufferCapacity, ttl);
    }

    public LineProtoSender(
            NetworkFacade nf,
            int interfaceIPv4Address,
            int sendToIPv4Address,
            int sendToPort,
            int capacity,
            int ttl
    ) {
        this.nf = nf;
        this.capacity = capacity;
        sockaddr = nf.sockaddr(sendToIPv4Address, sendToPort);
        fd = createSocket(interfaceIPv4Address, ttl, sockaddr);

        bufA = Unsafe.malloc(capacity);
        bufB = Unsafe.malloc(capacity);

        lo = bufA;
        hi = lo + capacity;
        ptr = lo;
        lineStart = lo;
    }

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

    public void $(long timestamp) {
        put(' ').put(timestamp);
        $();
    }

    public void $() {
        put('\n');
        lineStart = ptr;
        hasMetric = false;
        noFields = true;
    }

    @Override
    public void close() {
        if (nf.close(fd) != 0) {
            LOG.error().$("could not close UDP socket [fd=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
        }
        nf.freeSockAddr(sockaddr);
        Unsafe.free(bufA, capacity);
        Unsafe.free(bufB, capacity);
    }

    public LineProtoSender field(CharSequence name, long value) {
        field(name).put(value).put('i');
        return this;
    }

    public LineProtoSender field(CharSequence name, CharSequence value) {
        field(name).putQuoted(value);
        return this;
    }

    public LineProtoSender field(CharSequence name, double value) {
        field(name).put(value);
        return this;
    }

    public LineProtoSender field(CharSequence name, boolean value) {
        field(name).put(value ? 't' : 'f');
        return this;
    }

    @Override
    public void flush() {
        send();
        ptr = lineStart = lo;
    }

    @Override
    public LineProtoSender put(CharSequence cs) {
        int l = cs.length();
        if (ptr + l < hi) {
            Chars.asciiStrCpy(cs, l, ptr);
        } else {
            send00();
            if (ptr + l < hi) {
                Chars.asciiStrCpy(cs, l, ptr);
            } else {
                throw CairoException.instance(0).put("value too long");
            }
        }
        ptr += l;
        return this;
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

    public LineProtoSender metric(CharSequence metric) {
        if (hasMetric) {
            throw CairoException.instance(0).put("duplicate metric");
        }
        hasMetric = true;
        return put(metric);
    }

    @Override
    public LineProtoSender put(char c) {
        if (ptr >= hi) {
            send00();
        }
        Unsafe.getUnsafe().putByte(ptr++, (byte) c);
        return this;
    }

    public LineProtoSender tag(CharSequence tag, CharSequence value) {
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
    protected void putUtf8Special(char c) {
        switch (c) {
            case ' ':
            case ',':
            case '=':
                put('\\');
            default:
                put(c);
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
            Vect.memcpy(lineStart, target, len);
            send();
            lineStart = lo = target;
            ptr = target + len;
            hi = lo + capacity;
        } else {
            throw NetworkError.instance(0).put("line too long");
        }
    }
}
