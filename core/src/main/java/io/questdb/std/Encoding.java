package io.questdb.std;

import io.questdb.cairo.CairoException;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectByteCharSink;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class Encoding {
    static final char[] base64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray();
    static final byte[] base64Inverted = createInvertedAlphabet(base64);
    static final char[] base64Url = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".toCharArray();
    static final byte[] base64UrlInverted = createInvertedAlphabet(base64Url);

    private Encoding() {
    }

    /**
     * Decodes base64u encoded string into a byte buffer.
     * <p>
     * This method does not check for padding. It's up to the caller to ensure that the target
     * buffer has enough space to accommodate decoded data. Otherwise, {@link java.nio.BufferOverflowException}
     * will be thrown.
     *
     * @param encoded base64 encoded string
     * @param target  target buffer
     * @throws CairoException                   if encoded string is invalid
     * @throws java.nio.BufferOverflowException if target buffer is too small
     */
    public static void base64Decode(CharSequence encoded, ByteBuffer target) {
        base64Decode(encoded, target, base64Inverted);
    }

    public static void base64Decode(CharSequence encoded, DirectByteCharSink target) {
        base64Decode(encoded, target, base64Inverted);
    }

    public static void base64Encode(BinarySequence sequence, final int maxLength, CharSink buffer) {
        int pad = base64Encode(sequence, maxLength, buffer, base64);
        for (int j = 0; j < pad; j++) {
            buffer.put("=");
        }
    }

    /**
     * Decodes base64url encoded string into a byte buffer.
     * <p>
     * This method does not check for padding. It's up to the caller to ensure that the target
     * buffer has enough space to accommodate decoded data. Otherwise, {@link java.nio.BufferOverflowException}
     * will be thrown.
     *
     * @param encoded base64url encoded string
     * @param target  target buffer
     * @throws CairoException                   if encoded string is invalid
     * @throws java.nio.BufferOverflowException if target buffer is too small
     */
    public static void base64UrlDecode(CharSequence encoded, ByteBuffer target) {
        base64Decode(encoded, target, base64UrlInverted);
    }

    public static void base64UrlEncode(BinarySequence sequence, final int maxLength, CharSink buffer) {
        base64Encode(sequence, maxLength, buffer, base64Url);
        // base64 url does not use padding
    }

    private static void base64Decode(CharSequence encoded, DirectByteCharSink target, byte[] invertedAlphabet) {
        if (encoded == null) {
            return;
        }
        assert target != null;

        // skip trailing '=' they are just for padding and have no meaning
        int length = encoded.length();
        for (; length > 0; length--) {
            if (encoded.charAt(length - 1) != '=') {
                break;
            }
        }

        // we need at least 2 bytes to decode anything
        for (int i = 0, last = length - 1; i < last; ) {
            int wrk = invertedLookup(invertedAlphabet, encoded.charAt(i++)) << 18;
            wrk |= invertedLookup(invertedAlphabet, encoded.charAt(i++)) << 12;
            target.put((byte) (wrk >>> 16));
            if (i < length) {
                wrk |= invertedLookup(invertedAlphabet, encoded.charAt(i++)) << 6;
                target.put((byte) ((wrk >>> 8) & 0xFF));
                if (i < length) {
                    wrk |= invertedLookup(invertedAlphabet, encoded.charAt(i++));
                    target.put((byte) (wrk & 0xFF));
                }
            }
        }
    }

    private static void base64Decode(CharSequence encoded, ByteBuffer target, byte[] invertedAlphabet) {
        if (encoded == null) {
            return;
        }
        assert target != null;

        // skip trailing '=' they are just for padding and have no meaning
        int length = encoded.length();
        for (; length > 0; length--) {
            if (encoded.charAt(length - 1) != '=') {
                break;
            }
        }

        // we need at least 2 bytes to decode anything
        for (int i = 0, last = length - 1; i < last; ) {
            int wrk = invertedLookup(invertedAlphabet, encoded.charAt(i++)) << 18;
            wrk |= invertedLookup(invertedAlphabet, encoded.charAt(i++)) << 12;
            target.put((byte) (wrk >>> 16));
            if (i < length) {
                wrk |= invertedLookup(invertedAlphabet, encoded.charAt(i++)) << 6;
                target.put((byte) ((wrk >>> 8) & 0xFF));
                if (i < length) {
                    wrk |= invertedLookup(invertedAlphabet, encoded.charAt(i++));
                    target.put((byte) (wrk & 0xFF));
                }
            }
        }
    }

    private static int base64Encode(BinarySequence sequence, final int maxLength, CharSink buffer, char[] alphabet) {
        if (sequence == null) {
            return 0;
        }
        final long len = Math.min(maxLength, sequence.length());
        int pad = 0;
        for (int i = 0; i < len; i += 3) {

            int b = ((sequence.byteAt(i) & 0xFF) << 16) & 0xFFFFFF;
            if (i + 1 < len) {
                b |= (sequence.byteAt(i + 1) & 0xFF) << 8;
            } else {
                pad++;
            }
            if (i + 2 < len) {
                b |= (sequence.byteAt(i + 2) & 0xFF);
            } else {
                pad++;
            }

            for (int j = 0; j < 4 - pad; j++) {
                int c = (b & 0xFC0000) >> 18;
                buffer.put(alphabet[c]);
                b <<= 6;
            }
        }
        return pad;
    }

    private static byte[] createInvertedAlphabet(char[] alphabet) {
        byte[] inverted = new byte[128]; // ASCII only
        Arrays.fill(inverted, (byte) -1);
        int length = alphabet.length;
        for (int i = 0; i < length; i++) {
            char letter = alphabet[i];
            assert letter < 128;
            inverted[letter] = (byte) i;
        }
        return inverted;
    }

    private static byte invertedLookup(byte[] invertedAlphabet, char ch) {
        if (ch > 127) {
            throw CairoException.nonCritical().put("non-ascii character while decoding base64 [ch=").put((int) (ch)).put(']');
        }
        byte index = invertedAlphabet[ch];
        if (index == -1) {
            throw CairoException.nonCritical().put("invalid base64 character [ch=").put(ch).put(']');
        }
        return index;
    }
}
