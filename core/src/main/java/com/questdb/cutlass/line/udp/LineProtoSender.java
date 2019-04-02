/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cutlass.line.udp;

import com.questdb.cairo.CairoException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.network.NetworkError;
import com.questdb.network.NetworkFacade;
import com.questdb.std.Chars;
import com.questdb.std.Unsafe;
import com.questdb.std.str.AbstractCharSink;
import com.questdb.std.str.CharSink;

import java.io.Closeable;

public class LineProtoSender extends AbstractCharSink implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineProtoSender.class);

    private final int capacity;
    private final long bufA;
    private final long bufB;
    private final long sockaddr;
    private final long fd;
    private final NetworkFacade nf;

    private long lo;
    private long hi;
    private long ptr;
    private long lineStart;
    private boolean hasMetric = false;
    private boolean noFields = true;

    public LineProtoSender(
            NetworkFacade nf,
            int interfaceIPv4Address,
            int sendToIPv4Address,
            int sendToPort,
            int capacity
    ) {
        this.nf = nf;
        this.capacity = capacity;
        fd = nf.socketUdp();

        if (fd == -1) {
            throw NetworkError.instance(nf.errno()).put("could not create UDP socket");
        }

        if (nf.setMulticastInterface(fd, interfaceIPv4Address) != 0) {
            final int errno = nf.errno();
            nf.close(fd, LOG);
            throw NetworkError.instance(errno).put("could not bind to ").ip(interfaceIPv4Address);
        }

        sockaddr = nf.sockaddr(sendToIPv4Address, sendToPort);
        bufA = Unsafe.malloc(capacity);
        bufB = Unsafe.malloc(capacity);

        lo = bufA;
        hi = lo + capacity;
        ptr = lo;
        lineStart = lo;
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
            LOG.error().$("failed to close UDP socket [fd=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
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

    public LineProtoSender field(CharSequence name, double value, int scale) {
        field(name).put(value, scale);
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
            Chars.strcpy(cs, l, ptr);
        } else {
            send00();
            if (ptr + l < hi) {
                Chars.strcpy(cs, l, ptr);
            } else {
                throw CairoException.instance(0).put("value too long");
            }
        }
        ptr += l;
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
            put(',').putNameEscaped(tag).put('=').encodeUtf8(value);
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

            return putNameEscaped(name).put('=');
        }
        throw CairoException.instance(0).put("metric expected");
    }

    private LineProtoSender putNameEscaped(CharSequence name) {
        for (int i = 0, n = name.length(); i < n; i++) {
            char c = name.charAt(i);
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
        return this;
    }

    private void send() {
        if (lo < lineStart) {
            int len = (int) (lineStart - lo);
            if (nf.sendTo(fd, lo, len, sockaddr) != len) {
                throw NetworkError.instance(nf.errno()).put("send error");
            }
        }
    }

    private void send00() {
        int len = (int) (ptr - lineStart);
        if (len == 0) {
            send();
            ptr = lineStart = lo;
        } else if (len < capacity) {
            long target = lo == bufA ? bufB : bufA;
            Unsafe.getUnsafe().copyMemory(lineStart, target, len);
            send();
            lineStart = lo = target;
            ptr = target + len;
            hi = lo + capacity;
        } else {
            throw NetworkError.instance(0).put("line too long");
        }
    }
}
