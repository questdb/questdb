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

import io.questdb.cairo.CairoException;
import io.questdb.cutlass.line.tcp.AuthDb;
import io.questdb.log.Log;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.AbstractCharSink;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Base64;

public abstract class AbstractLineSender extends AbstractCharSink implements Closeable {
    protected final int capacity;
    protected final long fd;
    protected final NetworkFacade nf;
    private final long bufA;
    private final long bufB;
    private final long sockaddr;
    private boolean quoted = false;

    private long lo;
    private long hi;
    private long ptr;
    private long lineStart;
    private boolean hasMetric = false;
    private boolean noFields = true;
    private final Log log;

    public AbstractLineSender(
            int interfaceIPv4Address,
            int sendToIPv4Address,
            int sendToPort,
            int bufferCapacity,
            int ttl,
            Log log
    ) {
        this(NetworkFacadeImpl.INSTANCE, interfaceIPv4Address, sendToIPv4Address, sendToPort, bufferCapacity, ttl, log);
    }

    public AbstractLineSender(
            NetworkFacade nf,
            int interfaceIPv4Address,
            int sendToIPv4Address,
            int sendToPort,
            int capacity,
            int ttl,
            Log log
    ) {
        this.nf = nf;
        this.capacity = capacity;
        this.log = log;
        sockaddr = nf.sockaddr(sendToIPv4Address, sendToPort);
        fd = createSocket(interfaceIPv4Address, ttl, sockaddr);

        bufA = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);
        bufB = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);

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
            log.error().$("could not close network socket [fd=").$(fd).$(", errno=").$(nf.errno()).$(']').$();
        }
        nf.freeSockAddr(sockaddr);
        Unsafe.free(bufA, capacity, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(bufB, capacity, MemoryTag.NATIVE_DEFAULT);
    }

    public AbstractLineSender field(CharSequence name, long value) {
        field(name).put(value).put('i');
        return this;
    }

    public AbstractLineSender field(CharSequence name, CharSequence value) {
        field(name).put('"');
        quoted = true;
        encodeUtf8(value);
        quoted = false;
        put('"');
        return this;
    }

    public AbstractLineSender field(CharSequence name, double value) {
        field(name).put(value);
        return this;
    }

    public AbstractLineSender field(CharSequence name, boolean value) {
        field(name).put(value ? 't' : 'f');
        return this;
    }

    @Override
    public void flush() {
        sendLine();
        ptr = lineStart = lo;
    }

    @Override
    public AbstractLineSender put(CharSequence cs) {
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
    public AbstractLineSender put(char c) {
        if (ptr >= hi) {
            send00();
        }
        Unsafe.getUnsafe().putByte(ptr++, (byte) c);
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

    public AbstractLineSender metric(CharSequence metric) {
        if (hasMetric) {
            throw CairoException.instance(0).put("duplicate metric");
        }
        quoted = false;
        hasMetric = true;
        encodeUtf8(metric);
        return this;
    }

    public AbstractLineSender tag(CharSequence tag, CharSequence value) {
        if (hasMetric) {
            put(',').encodeUtf8(tag).put('=').encodeUtf8(value);
            return this;
        }
        throw CairoException.instance(0).put("metric expected");
    }

    protected abstract long createSocket(int interfaceIPv4Address, int ttl, long sockaddr);

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
            case '\n':
            case '\r':
                put('\\').put(c);
                break;
            case '"':
                if (quoted) {
                    put('\\');
                }
                put(c);
                break;
            case '\\':
                put('\\').put('\\');
                break;
        }
    }

    private void sendLine() {
        if (lo < lineStart) {
            int len = (int) (lineStart - lo);
            sendToSocket(fd, lo, sockaddr, len);
        }
    }

    protected void send00() {
        int len = (int) (ptr - lineStart);
        if (len == 0) {
            sendLine();
            ptr = lineStart = lo;
        } else if (len < capacity) {
            long target = lo == bufA ? bufB : bufA;
            Vect.memcpy(target, lineStart, len);
            sendLine();
            lineStart = lo = target;
            ptr = target + len;
            hi = lo + capacity;
        } else {
            throw NetworkError.instance(0).put("line too long");
        }
    }

    protected final void authenticate(String authKey, PrivateKey privateKey) throws NetworkError {
        encodeUtf8(authKey).put('\n');
        sendAll();

        byte[] challengeBytes = receiveChallengeBytes();
        byte[] signature = signAndEncode(privateKey, challengeBytes);
        for (int n = 0; n < signature.length; n++) {
            put((char)signature[n]);
        }
        put('\n');
        sendAll();
    }

    protected void sendAll() {
        if (lo < ptr) {
            int len = (int) (ptr - lo);
            sendToSocket(fd, lo, sockaddr, len);
            lineStart = ptr = lo;
        }
    }

    protected abstract void sendToSocket(long fd, long lo, long sockaddr, int len);

    private static int findEOL(long ptr, int len) {
        for (int i = 0; i < len; i++) {
            byte b = Unsafe.getUnsafe().getByte(ptr + i);
            if (b == (byte) '\n') {
                return i;
            }
        }
        return -1;
    }

    @NotNull
    private byte[] receiveChallengeBytes() {
        int n = 0;
        for (;;) {
            int rc = nf.recv(fd, ptr + n, capacity - n);
            if (rc < 0) {
                close();
                throw NetworkError.instance(nf.errno()).put("disconnected during authentication");
            }
            int eol = findEOL(ptr + n, rc);
            if (eol != -1) {
                n += eol;
                break;
            }
            n += rc;
            if (n == capacity) {
                close();
                throw NetworkError.instance(0).put("challenge did not fit into buffer");
            }
        }
        int sz = n;
        byte[] challengeBytes = new byte[sz];
        for (n = 0; n < sz; n++) {
            challengeBytes[n] = Unsafe.getUnsafe().getByte(ptr + n);
        }
        return challengeBytes;
    }

    private byte[] signAndEncode(PrivateKey privateKey, byte[] challengeBytes) {
        byte[] rawSignature;
        try {
            Signature sig = Signature.getInstance(AuthDb.SIGNATURE_TYPE_DER);
            sig.initSign(privateKey);
            sig.update(challengeBytes);
            rawSignature = sig.sign();
        } catch (InvalidKeyException ex) {
            close();
            throw NetworkError.instance(0).put("invalid key");
        } catch (SignatureException ex) {
            close();
            throw NetworkError.instance(0).put("cannot sign challenge");
        } catch (NoSuchAlgorithmException ex) {
            close();
            throw NetworkError.instance(0).put("unsupported signing algorithm");
        }
        return Base64.getEncoder().encode(rawSignature);
    }
}
