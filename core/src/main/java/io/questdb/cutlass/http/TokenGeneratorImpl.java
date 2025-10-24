package io.questdb.cutlass.http;

import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Transient;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import java.security.SecureRandom;

public class TokenGeneratorImpl implements TokenGenerator {
    private final ByteArraySequence byteSeq;
    private final int length;
    private final String prefix;
    private final SecureRandom srand = new SecureRandom();
    private final StringSink tokenSink = new StringSink();

    public TokenGeneratorImpl(String prefix, int length) {
        this.prefix = prefix;
        this.length = length;

        byteSeq = new ByteArraySequence(length);
    }

    @Transient
    @NotNull
    public CharSequence newToken() {
        srand.nextBytes(byteSeq.bytes);

        tokenSink.clear();
        tokenSink.putAscii(prefix);
        Chars.base64UrlEncode(byteSeq, length, tokenSink);

        return tokenSink;
    }

    private static class ByteArraySequence implements BinarySequence {
        final byte[] bytes;

        private ByteArraySequence(int length) {
            bytes = new byte[length];
        }

        @Override
        public byte byteAt(long index) {
            return bytes[(int) index];
        }

        @Override
        public long length() {
            return bytes.length;
        }
    }
}
