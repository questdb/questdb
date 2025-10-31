package io.questdb.cutlass.http;

import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Transient;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import java.security.SecureRandom;

/**
 * Generates cryptographically secure random tokens with a configurable prefix and length.
 * Tokens are Base64 URL-encoded.
 *
 * <p><b>Thread Safety:</b> This class is <b>NOT thread-safe</b>. External synchronization
 * is required when accessed from multiple threads. Internal buffers ({@code byteSeq} and
 * {@code tokenSink}) are reused across calls for performance.
 *
 * <p><b>Important:</b> The {@code CharSequence} returned by {@link #newToken()} is transient
 * and backed by a reusable buffer. Callers must copy the value if they need to retain it
 * beyond the next invocation.
 */
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

    /**
     * Generates a new cryptographically secure random token.
     *
     * <p><b>Warning:</b> The returned {@code CharSequence} is backed by a reusable buffer
     * and will be overwritten on the next call. Callers must copy the value (e.g., by calling
     * {@code .toString()}) if they need to retain it.
     *
     * @return a transient token string that must be copied if retained
     */
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
