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

import io.questdb.cairo.TableUtils;
import io.questdb.cutlass.line.tcp.AuthDb;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.AbstractCharSink;
import io.questdb.std.str.CharSink;

import java.io.Closeable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Base64;

public abstract class AbstractLineSender extends AbstractCharSink implements Closeable, Sender {
    protected final int capacity;
    private final long bufA;
    private final long bufB;
    private boolean quoted = false;

    private long lo;
    private long hi;
    private long ptr;
    private long lineStart;
    private boolean hasTable;
    private boolean hasColumns;
    private boolean hasSymbols;
    protected final LineChannel lineChannel;
    private boolean enableValidation;

    public AbstractLineSender(LineChannel lineChannel, int capacity) {
        this.lineChannel = lineChannel;
        this.capacity = capacity;
        this.enableValidation = true;

        bufA = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);
        bufB = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);

        lo = bufA;
        hi = lo + capacity;
        ptr = lo;
        lineStart = lo;
    }

    public void $(long timestamp) {
        at(timestamp);
    }

    public void $() {
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

    @Override
    public final void at(long timestamp) {
        put(' ').put(timestamp);
        atNow();
    }

    @Override
    public void close() {
        Misc.free(lineChannel);
        Unsafe.free(bufA, capacity, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(bufB, capacity, MemoryTag.NATIVE_DEFAULT);
    }

    public AbstractLineSender field(CharSequence name, long value) {
        validateColumnName(name);
        field(name).put(value).put('i');
        return this;
    }

    @Override
    public final AbstractLineSender longColumn(CharSequence name, long value) {
        return field(name, value);
    }

    public AbstractLineSender field(CharSequence name, CharSequence value) {
        validateColumnName(name);
        field(name).put('"');
        quoted = true;
        encodeUtf8(value);
        quoted = false;
        put('"');
        return this;
    }

    @Override
    public final AbstractLineSender stringColumn(CharSequence name, CharSequence value) {
        return field(name, value);
    }

    public AbstractLineSender field(CharSequence name, double value) {
        validateColumnName(name);
        field(name).put(value);
        return this;
    }

    @Override
    public final AbstractLineSender doubleColumn(CharSequence name, double value) {
        return field(name, value);
    }

    public AbstractLineSender field(CharSequence name, boolean value) {
        validateColumnName(name);
        field(name).put(value ? 't' : 'f');
        return this;
    }

    @Override
    public final AbstractLineSender boolColumn(CharSequence name, boolean value) {
        return field(name, value);
    }

    @Override
    public final AbstractLineSender timestampColumn(CharSequence name, long value) {
        validateColumnName(name);
        field(name).put(value).put('t');
        return this;
    }

    @Override
    public final AbstractLineSender timestampColumn(CharSequence name, CharSequence value) throws LineSenderException {
        try {
            long value_micros = TimestampFormatUtils.parseUTCTimestamp(value);
            return timestampColumn(name, value_micros);
        } catch (NumericException e) {
            throw new LineSenderException("could not parse timestamp in UTC ISO 8601 format.");
        }
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
                throw new LineSenderException("value too long. increase buffer size.");
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
                throw new LineSenderException("value too long. increase buffer size.");
            }
        }
        ptr += len;
        return this;
    }

    public AbstractLineSender metric(CharSequence metric) {
        validateTableName(metric);
        if (hasTable) {
            throw new LineSenderException("duplicate table");
        }
        if (metric.length() == 0) {
            throw new LineSenderException("table name cannot be empty");
        }
        quoted = false;
        hasTable = true;
        encodeUtf8(metric);
        return this;
    }

    private void validateColumnName(CharSequence name) {
        if (!enableValidation) {
            return;
        }
        if (!TableUtils.isValidColumnName(name, Integer.MAX_VALUE)) {
            throw new LineSenderException("column name contains an illegal char: '\\n', '\\r', '?', '.', ','" +
                    ", ''', '\"', '\\', '/', ':', ')', '(', '+', '-', '*' '%%', '~', or a non-printable char: " + name);
        }
    }

    /**
     * This is for testing only. Where we want to test server with a misbehaving client.
     */
    public void disableValidation() {
        enableValidation = false;
    }

    private void validateTableName(CharSequence name) {
        if (!enableValidation) {
            return;
        }
        if (!TableUtils.isValidTableName(name, Integer.MAX_VALUE)) {
            throw new LineSenderException("table name contains an illegal char: '\\n', '\\r', '?', ',', ''', " +
                    "'\"', '\\', '/', ':', ')', '(', '+', '*' '%%', '~', or a non-printable char: " + name);
        }
    }

    @Override
    public final AbstractLineSender table(CharSequence table) {
        return metric(table);
    }

    public AbstractLineSender tag(CharSequence tag, CharSequence value) {
        validateColumnName(tag);
        if (hasTable) {
            put(',').encodeUtf8(tag).put('=').encodeUtf8(value);
            hasSymbols = true;
            return this;
        }
        throw new LineSenderException("table expected");
    }

    @Override
    public final AbstractLineSender symbol(CharSequence name, CharSequence value) {
        return tag(name, value);
    }

    private CharSink field(CharSequence name) {
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
            lineChannel.send(lo, len);
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
            throw new LineSenderException("line too long. increase buffer size.");
        }
    }

    public final void authenticate(String keyId, PrivateKey privateKey) {
        encodeUtf8(keyId).put('\n');
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
            lineChannel.send(lo, len);
            lineStart = ptr = lo;
        }
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
        for (;;) {
            int rc = lineChannel.receive(ptr + n, capacity - n);
            if (rc < 0) {
                close();
                throw new LineSenderException("disconnected during authentication [errno=" + lineChannel.errno() + "]");
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

    private byte[] signAndEncode(PrivateKey privateKey, byte[] challengeBytes) {
        byte[] rawSignature;
        try {
            Signature sig = Signature.getInstance(AuthDb.SIGNATURE_TYPE_DER);
            sig.initSign(privateKey);
            sig.update(challengeBytes);
            rawSignature = sig.sign();
        } catch (InvalidKeyException ex) {
            close();
            throw new LineSenderException("invalid key");
        } catch (SignatureException ex) {
            close();
            throw new LineSenderException("cannot sign challenge");
        } catch (NoSuchAlgorithmException ex) {
            close();
            throw new LineSenderException("unsupported signing algorithm");
        }
        return Base64.getEncoder().encode(rawSignature);
    }
}
