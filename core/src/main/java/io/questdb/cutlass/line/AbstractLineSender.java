/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.TableUtils;
import io.questdb.client.Sender;
import io.questdb.cutlass.auth.AuthUtils;
import io.questdb.std.Decimal256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.bytes.DirectByteSlice;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Base64;

public abstract class AbstractLineSender implements Utf8Sink, Closeable, Sender {
    protected final int capacity;
    private final long bufA;
    private final long bufB;
    private final DirectByteSlice bufferView = new DirectByteSlice();
    private final int maxNameLength;
    protected long hi;
    protected LineChannel lineChannel;
    protected long ptr;
    private boolean closed;
    private boolean enableValidation;
    private boolean hasColumns;
    private boolean hasSymbols;
    private boolean hasTable;
    private long lineStart;
    private long lo;
    private boolean quoted = false;

    public AbstractLineSender(LineChannel lineChannel, int capacity, int maxNameLength) {
        this.lineChannel = lineChannel;
        this.capacity = capacity;
        this.enableValidation = true;
        this.maxNameLength = maxNameLength;

        bufA = Unsafe.malloc(capacity, MemoryTag.NATIVE_ILP_RSS);
        bufB = Unsafe.malloc(capacity, MemoryTag.NATIVE_ILP_RSS);

        lo = bufA;
        hi = lo + capacity;
        ptr = lo;
        lineStart = lo;
    }

    public void $(long timestamp) {
        putAsciiInternal(' ').put(timestamp);
        atNow();
    }

    public void $() {
        atNow();
    }

    @Override
    public final void atNow() {
        if (!hasColumns && !hasSymbols && enableValidation) {
            throw new LineSenderException("no symbols or columns were provided");
        }

        putAsciiInternal('\n');
        lineStart = ptr;
        hasTable = false;
        hasColumns = false;
        hasSymbols = false;
    }

    public final void authenticate(String keyId, PrivateKey privateKey) {
        validateNotClosed();
        put(keyId);
        putAsciiInternal('\n');
        sendAll();

        byte[] challengeBytes = receiveChallengeBytes();
        byte[] signature = signAndEncode(privateKey, challengeBytes);
        for (int n = 0, m = signature.length; n < m; n++) {
            putAsciiInternal((char) signature[n]);
        }
        putAsciiInternal('\n');
        sendAll();
    }

    @Override
    public final AbstractLineSender boolColumn(CharSequence name, boolean value) {
        return field(name, value);
    }

    public DirectByteSlice bufferView() {
        return bufferView.of(lo, (int) (ptr - lo));
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            flush();
        } finally {
            closed = true;
            lineChannel = Misc.free(lineChannel);
            Unsafe.free(bufA, capacity, MemoryTag.NATIVE_ILP_RSS);
            Unsafe.free(bufB, capacity, MemoryTag.NATIVE_ILP_RSS);
        }
    }

    /**
     * This is for testing only. Where we want to test server with a misbehaving client.
     */
    public void disableValidation() {
        enableValidation = false;
    }

    public AbstractLineSender field(CharSequence name, long value) {
        writeFieldName(name).put(value).put('i');
        return this;
    }

    public AbstractLineSender field(CharSequence name, CharSequence value) {
        writeFieldName(name).put('"');
        quoted = true;
        put(value);
        quoted = false;
        putAsciiInternal('"');
        return this;
    }

    public AbstractLineSender field(CharSequence name, double value) {
        writeFieldName(name).put(value);
        return this;
    }

    public AbstractLineSender field(CharSequence name, boolean value) {
        writeFieldName(name).putAsciiInternal(value ? 't' : 'f');
        return this;
    }

    @Override
    public void flush() {
        validateNotClosed();
        sendLine();
        ptr = lineStart = lo;
    }

    @Override
    public final AbstractLineSender longColumn(CharSequence name, long value) {
        return field(name, value);
    }

    public AbstractLineSender metric(CharSequence metric) {
        validateNotClosed();
        validateTableName(metric);
        if (hasTable) {
            throw new LineSenderException("duplicated table. call sender.at() or sender.atNow() to finish the current row first");
        }
        if (metric.isEmpty()) {
            throw new LineSenderException("table name cannot be empty");
        }
        quoted = false;
        hasTable = true;
        put(metric);
        return this;
    }

    @Override
    public AbstractLineSender put(@Nullable CharSequence cs) {
        Utf8Sink.super.put(cs);
        return this;
    }

    @Override
    public AbstractLineSender put(char c) {
        Utf8Sink.super.put(c);
        return this;
    }

    @Override
    public AbstractLineSender put(@Nullable Utf8Sequence us) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AbstractLineSender put(byte c) {
        validateNotClosed();
        if (ptr >= hi) {
            send00();
        }
        Unsafe.getUnsafe().putByte(ptr++, c);
        return this;
    }

    @Override
    public AbstractLineSender put(long value) {
        Numbers.append(this, value, false);
        return this;
    }

    @Override
    public AbstractLineSender putAscii(char @NotNull [] chars, int start, int len) {
        validateNotClosed();
        if (ptr + len < hi) {
            Utf8s.strCpyAscii(chars, start, len, ptr);
        } else {
            send00();
            if (ptr + len < hi) {
                Utf8s.strCpyAscii(chars, start, len, ptr);
            } else {
                throw new LineSenderException("value too long. increase buffer size.");
            }
        }
        ptr += len;
        return this;
    }

    @Override
    public AbstractLineSender putAscii(@Nullable CharSequence cs) {
        validateNotClosed();
        if (cs != null) {
            int l = cs.length();
            if (ptr + l < hi) {
                Utf8s.strCpyAscii(cs, l, ptr);
            } else {
                send00();
                if (ptr + l < hi) {
                    Utf8s.strCpyAscii(cs, l, ptr);
                } else {
                    throw new LineSenderException("value too long. increase buffer size.");
                }
            }
            ptr += l;
        }
        return this;
    }

    @Override
    public AbstractLineSender putAscii(char c) {
        validateNotClosed();
        switch (c) {
            case ' ':
            case ',':
            case '=':
                if (!quoted) {
                    put((byte) '\\');
                }
            default:
                put((byte) c);
                break;
            case '\n':
            case '\r':
                put((byte) '\\').put((byte) c);
                break;
            case '"':
                if (quoted) {
                    put((byte) '\\');
                }
                put((byte) c);
                break;
            case '\\':
                put((byte) '\\').put((byte) '\\');
                break;
        }
        return this;
    }

    // doesn't do special char checks
    public AbstractLineSender putAsciiInternal(char c) {
        return put((byte) c);
    }

    // doesn't do special char checks
    public AbstractLineSender putAsciiInternal(@Nullable CharSequence cs) {
        if (cs != null) {
            int l = cs.length();
            for (int i = 0; i < l; i++) {
                putAsciiInternal(cs.charAt(i));
            }
        }
        return this;
    }

    @Override
    public AbstractLineSender putNonAscii(long lo, long hi) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final AbstractLineSender stringColumn(CharSequence name, CharSequence value) {
        return field(name, value);
    }

    @Override
    public final AbstractLineSender symbol(CharSequence name, CharSequence value) {
        return tag(name, value);
    }

    @Override
    public final AbstractLineSender table(CharSequence table) {
        return metric(table);
    }

    public AbstractLineSender tag(CharSequence tag, CharSequence value) {
        if (!hasTable) {
            throw new LineSenderException("table expected");
        }
        if (hasColumns) {
            throw new LineSenderException("symbols must be written before any other column types");
        }
        validateColumnName(tag);
        putAsciiInternal(',').put(tag);
        putAsciiInternal('=').put(value);
        hasSymbols = true;
        return this;
    }

    private static int findEOL(long ptr, int len) {
        for (int i = 0; i < len; i++) {
            byte b = Unsafe.getUnsafe().getByte(ptr + i);
            if (b == (byte) '\n') {
                return i;
            }
        }
        return -1;
    }


    private byte[] receiveChallengeBytes() {
        int n = 0;
        for (; ; ) {
            int rc = lineChannel.receive(ptr + n, capacity - n);
            if (rc < 0) {
                int errno = lineChannel.errno();
                close();
                throw new LineSenderException("disconnected during authentication").errno(errno);
            }
            int eol = findEOL(ptr + n, rc);
            if (eol != -1) {
                n += eol;
                break;
            }
            n += rc;
            if (n == capacity) {
                close();
                throw new LineSenderException("challenge did not fit into buffer");
            }
        }
        int sz = n;
        byte[] challengeBytes = new byte[sz];
        for (n = 0; n < sz; n++) {
            challengeBytes[n] = Unsafe.getUnsafe().getByte(ptr + n);
        }
        return challengeBytes;
    }

    private void sendLine() {
        if (lo < lineStart) {
            int len = (int) (lineStart - lo);
            lineChannel.send(lo, len);
        }
    }

    private void validateColumnName(CharSequence name) {
        if (!enableValidation) {
            return;
        }
        if (!TableUtils.isValidColumnName(name, maxNameLength)) {
            if (name.length() > maxNameLength) {
                throw new LineSenderException("column name is too long: [name = ")
                        .putAsPrintable(name)
                        .put(", maxNameLength=")
                        .put(maxNameLength)
                        .put(']');
            }
            throw new LineSenderException("column name contains an illegal char: '\\n', '\\r', '?', '.', ','" +
                    ", ''', '\"', '\\', '/', ':', ')', '(', '+', '-', '*' '%%', '~', or a non-printable char: ").putAsPrintable(name);
        }
    }

    private void validateTableName(CharSequence name) {
        if (!enableValidation) {
            return;
        }
        if (!TableUtils.isValidTableName(name, maxNameLength)) {
            if (name.length() > maxNameLength) {
                throw new LineSenderException("table name is too long: [name = ")
                        .putAsPrintable(name)
                        .put(", maxNameLength=")
                        .put(maxNameLength)
                        .put(']');
            }
            throw new LineSenderException("table name contains an illegal char: '\\n', '\\r', '?', ',', ''', " +
                    "'\"', '\\', '/', ':', ')', '(', '+', '*' '%%', '~', or a non-printable char: ").putAsPrintable(name);
        }
    }

    protected void send00() {
        validateNotClosed();
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
            throw new LineSenderException("line too long. increase buffer size.");
        }
    }

    protected void sendAll() {
        validateNotClosed();
        if (lo < ptr) {
            int len = (int) (ptr - lo);
            lineChannel.send(lo, len);
            lineStart = ptr = lo;
        }
    }

    protected byte[] signAndEncode(PrivateKey privateKey, byte[] challengeBytes) {
        // protected for testing
        byte[] rawSignature;
        try {
            Signature sig = Signature.getInstance(AuthUtils.SIGNATURE_TYPE_DER);
            sig.initSign(privateKey);
            sig.update(challengeBytes);
            rawSignature = sig.sign();
        } catch (InvalidKeyException ex) {
            close();
            throw new LineSenderException("invalid key", ex);
        } catch (SignatureException ex) {
            close();
            throw new LineSenderException("cannot sign challenge", ex);
        } catch (NoSuchAlgorithmException ex) {
            close();
            throw new LineSenderException("unsupported signing algorithm", ex);
        }
        return Base64.getEncoder().encode(rawSignature);
    }

    protected final void validateNotClosed() {
        if (closed) {
            throw new LineSenderException("sender already closed");
        }
    }

    protected AbstractLineSender writeFieldName(CharSequence name) {
        validateNotClosed();
        validateColumnName(name);
        if (hasTable) {
            if (!hasColumns) {
                putAsciiInternal(' ');
                hasColumns = true;
            } else {
                putAsciiInternal(',');
            }
            return put(name).putAsciiInternal('=');
        }
        throw new LineSenderException("table expected");
    }
}
