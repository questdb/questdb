/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.*;
import io.questdb.std.str.AbstractCharSink;
import io.questdb.std.str.CharSink;

import java.io.Closeable;
import java.security.*;
import java.util.Base64;

public abstract class AbstractLineSender extends AbstractCharSink implements Closeable, Sender {
    protected final int capacity;
    private final long bufA;
    private final long bufB;
    protected LineChannel lineChannel;
    private boolean closed;
    private boolean enableValidation;
    private boolean hasColumns;
    private boolean hasSymbols;
    private boolean hasTable;
    private long hi;
    private long lineStart;
    private long lo;
    private long ptr;
    private boolean quoted = false;

    public AbstractLineSender(LineChannel lineChannel, int capacity) {
        this.lineChannel = lineChannel;
        this.capacity = capacity;
        this.enableValidation = true;

        bufA = Unsafe.malloc(capacity, MemoryTag.NATIVE_ILP_RSS);
        bufB = Unsafe.malloc(capacity, MemoryTag.NATIVE_ILP_RSS);

        lo = bufA;
        hi = lo + capacity;
        ptr = lo;
        lineStart = lo;
    }

    public void $(long timestamp) {
        put(' ').put(timestamp);
        atNow();
    }

    public void $() {
        atNow();
    }

    @Override
    public final void at(long timestamp) {
        put(' ').put(timestamp);
        atNow();
    }

    @Override
    public final void atNow() {
        if (!hasColumns && !hasSymbols && enableValidation) {
            throw new LineSenderException("no symbols or columns were provided");
        }

        put('\n');
        lineStart = ptr;
        hasTable = false;
        hasColumns = false;
        hasSymbols = false;
    }

    public final void authenticate(String keyId, PrivateKey privateKey) {
        validateNotClosed();
        encodeUtf8(keyId).put('\n');
        sendAll();

        byte[] challengeBytes = receiveChallengeBytes();
        byte[] signature = signAndEncode(privateKey, challengeBytes);
        for (int n = 0, m = signature.length; n < m; n++) {
            put((char) signature[n]);
        }
        put('\n');
        sendAll();
    }

    @Override
    public final AbstractLineSender boolColumn(CharSequence name, boolean value) {
        return field(name, value);
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

    @Override
    public final AbstractLineSender doubleColumn(CharSequence name, double value) {
        return field(name, value);
    }

    public AbstractLineSender field(CharSequence name, long value) {
        writeFieldName(name).put(value).put('i');
        return this;
    }

    public AbstractLineSender field(CharSequence name, CharSequence value) {
        writeFieldName(name).put('"');
        quoted = true;
        encodeUtf8(value);
        quoted = false;
        put('"');
        return this;
    }

    public AbstractLineSender field(CharSequence name, double value) {
        writeFieldName(name).put(value);
        return this;
    }

    public AbstractLineSender field(CharSequence name, boolean value) {
        writeFieldName(name).put(value ? 't' : 'f');
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
        if (metric.length() == 0) {
            throw new LineSenderException("table name cannot be empty");
        }
        quoted = false;
        hasTable = true;
        encodeUtf8(metric);
        return this;
    }

    @Override
    public AbstractLineSender put(CharSequence cs) {
        validateNotClosed();
        int l = cs.length();
        if (ptr + l < hi) {
            Chars.asciiStrCpy(cs, l, ptr);
        } else {
            send00();
            if (ptr + l < hi) {
                Chars.asciiStrCpy(cs, l, ptr);
            } else {
                throw new LineSenderException("value too long. increase buffer size.");
            }
        }
        ptr += l;
        return this;
    }

    @Override
    public AbstractLineSender put(char c) {
        validateNotClosed();
        if (ptr >= hi) {
            send00();
        }
        Unsafe.getUnsafe().putByte(ptr++, (byte) c);
        return this;
    }

    @Override
    public CharSink put(char[] chars, int start, int len) {
        validateNotClosed();
        if (ptr + len < hi) {
            Chars.asciiCopyTo(chars, start, len, ptr);
        } else {
            send00();
            if (ptr + len < hi) {
                Chars.asciiCopyTo(chars, start, len, ptr);
            } else {
                throw new LineSenderException("value too long. increase buffer size.");
            }
        }
        ptr += len;
        return this;
    }

    @Override
    public CharSink put(long value) {
        Numbers.append(this, value, false);
        return this;
    }

    @Override
    public void putUtf8Special(char c) {
        validateNotClosed();
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
        put(',').encodeUtf8(tag).put('=').encodeUtf8(value);
        hasSymbols = true;
        return this;
    }

    @Override
    public final AbstractLineSender timestampColumn(CharSequence name, long value) {
        writeFieldName(name).put(value).put('t');
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
        if (!TableUtils.isValidColumnName(name, Integer.MAX_VALUE)) {
            throw new LineSenderException("column name contains an illegal char: '\\n', '\\r', '?', '.', ','" +
                    ", ''', '\"', '\\', '/', ':', ')', '(', '+', '-', '*' '%%', '~', or a non-printable char: ").putAsPrintable(name);
        }
    }

    private void validateTableName(CharSequence name) {
        if (!enableValidation) {
            return;
        }
        if (!TableUtils.isValidTableName(name, Integer.MAX_VALUE)) {
            throw new LineSenderException("table name contains an illegal char: '\\n', '\\r', '?', ',', ''', " +
                    "'\"', '\\', '/', ':', ')', '(', '+', '*' '%%', '~', or a non-printable char: ").putAsPrintable(name);
        }
    }

    private CharSink writeFieldName(CharSequence name) {
        validateNotClosed();
        validateColumnName(name);
        if (hasTable) {
            if (!hasColumns) {
                put(' ');
                hasColumns = true;
            } else {
                put(',');
            }
            return encodeUtf8(name).put('=');
        }
        throw new LineSenderException("table expected");
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
}
