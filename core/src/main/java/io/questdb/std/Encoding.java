package io.questdb.std;

import io.questdb.cairo.CairoException;
import io.questdb.std.str.CharSink;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class Encoding {
    static final char[] base64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray();
    static final byte[] base64Inverted = createInvertedAlphabet(base64);
    static final char[] base64Url = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".toCharArray();
    static final byte[] base64UrlInverted = createInvertedAlphabet(base64Url);

    private Encoding() {

    }

    public static void base64Decode(CharSequence encoded, ByteBuffer target) {
        base64Decode(encoded, target, base64Inverted);
    }

    public static void base64Encode(BinarySequence sequence, final int maxLength, CharSink buffer) {
        int pad = base64Encode(sequence, maxLength, buffer, base64);
        for (int j = 0; j < pad; j++) {
            buffer.put("=");
        }
    }

    public static void base64UrlDecode(CharSequence encoded, ByteBuffer target) {
        base64Decode(encoded, target, base64UrlInverted);
    }

    public static void base64UrlEncode(BinarySequence sequence, final int maxLength, CharSink buffer) {
        base64Encode(sequence, maxLength, buffer, base64Url);
        // base64 url does not use padding
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
        for (int i = 0; i < length; ) {
            byte b = invertLookup(invertedAlphabet, encoded.charAt(i++));
            assert b != -1;
            int wrk = b << 18;
            wrk |= invertedAlphabet[encoded.charAt(i++)] << 12;
            target.put((byte) (wrk >>> 16));
            if (i < length) {
                wrk |= invertedAlphabet[encoded.charAt(i++)] << 6;
                target.put((byte) ((wrk >>> 8) & 0xFF));
                if (i < length) {
                    wrk |= invertedAlphabet[encoded.charAt(i++)];
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

    private static byte invertLookup(byte[] invertedAlphabet, char ch) {
        if (ch > 127) {
            throw CairoException.nonCritical().put("not ascii letter while decoding base64 [ch=").put(Character.getNumericValue(ch)).put(']');
        }
        byte index = invertedAlphabet[ch];
        if (index == -1) {
            throw CairoException.nonCritical().put("invalid base64 character: ").put(ch);
        }
        return index;
    }
}
