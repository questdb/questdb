package io.questdb.std;

import io.questdb.cairo.CairoException;
import io.questdb.std.str.CharSink;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class Encoding {
    static final char[] base64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray();
    static final int[] base64Inverted = createInvertedAlphabet(base64);
    static final char[] base64Url = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".toCharArray();
    static final int[] base64UrlInverted = createInvertedAlphabet(base64Url);

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

    private static void base64Decode(CharSequence encoded, ByteBuffer target, int[] invertedAlphabet) {
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

        int remainder = length % 4;
        int sourcePos = 0;
        int targetPos = target.position();

        // first decode all 4 byte chunks. this is *the* hot loop, be careful when changing it
        for (int end = length - remainder; sourcePos < end; sourcePos += 4, targetPos += 3) {
            int b0 = invertedLookup(invertedAlphabet, encoded.charAt(sourcePos)) << 18;
            int b1 = invertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 1)) << 12;
            int b2 = invertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 2)) << 6;
            int b4 = invertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 3));

            int wrk = b0 | b1 | b2 | b4;
            // we use absolute positions to wrote to the byte buffer in the hot loop
            // benchmarking shows that it is faster than using relative positions
            target.put(targetPos, (byte) (wrk >>> 16));
            target.put(targetPos + 1, (byte) ((wrk >>> 8) & 0xFF));
            target.put(targetPos + 2, (byte) (wrk & 0xFF));
        }
        target.position(targetPos);
        // now decode remainder
        switch (remainder) {
            case 0:
                // nothing to do, yay!
                break;
            case 1:
                // invalid encoding, we can't have 1 byte remainder as
                // even 1 byte encodes to 2 chars
                throw CairoException.nonCritical().put("invalid base64 encoding [string=").put(encoded).put(']');
            case 2:
                int wrk = invertedLookup(invertedAlphabet, encoded.charAt(sourcePos)) << 18;
                wrk |= invertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 1)) << 12;
                target.put((byte) (wrk >>> 16));
                break;
            case 3:
                wrk = invertedLookup(invertedAlphabet, encoded.charAt(sourcePos)) << 18;
                wrk |= invertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 1)) << 12;
                wrk |= invertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 2)) << 6;
                target.put((byte) (wrk >>> 16));
                target.put((byte) ((wrk >>> 8) & 0xFF));
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

    private static int[] createInvertedAlphabet(char[] alphabet) {
        int[] inverted = new int[128]; // ASCII only
        Arrays.fill(inverted, (byte) -1);
        int length = alphabet.length;
        for (int i = 0; i < length; i++) {
            char letter = alphabet[i];
            assert letter < 128;
            inverted[letter] = (byte) i;
        }
        return inverted;
    }

    private static int invertedLookup(int[] invertedAlphabet, char ch) {
        if (ch > 127) {
            throw CairoException.nonCritical().put("non-ascii character while decoding base64 [ch=").put((int) (ch)).put(']');
        }
        int index = invertedAlphabet[ch];
        if (index == -1) {
            throw CairoException.nonCritical().put("invalid base64 character [ch=").put(ch).put(']');
        }
        return index;
    }
}
