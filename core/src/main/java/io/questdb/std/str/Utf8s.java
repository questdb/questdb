package io.questdb.std.str;

import io.questdb.cairo.CairoException;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * UTF-8 specific variant of the {@link Chars} utility.
 */
public final class Utf8s {

    private Utf8s() {
    }

    public static boolean containsAscii(Utf8Sequence sequence, CharSequence term) {
        return indexOfAscii(sequence, 0, sequence.size(), term) != -1;
    }

    public static int encodeUtf16Char(@NotNull Utf8Sink sink, @NotNull CharSequence cs, int hi, int i, char c) {
        if (c < 2048) {
            sink.put((byte) (192 | c >> 6));
            sink.put((byte) (128 | c & 63));
        } else if (Character.isSurrogate(c)) {
            i = encodeUtf16Surrogate(sink, c, cs, i, hi);
        } else {
            sink.put((byte) (224 | c >> 12));
            sink.put((byte) (128 | c >> 6 & 63));
            sink.put((byte) (128 | c & 63));
        }
        return i;
    }

    public static boolean endsWithAscii(@NotNull Utf8Sequence seq, @NotNull CharSequence endsAscii) {
        int l = endsAscii.length();
        if (l == 0) {
            return true;
        }

        int size = seq.size();
        return !(size == 0 || size < l) && equalsAscii(endsAscii, seq, size - l, size);
    }

    public static boolean endsWithAscii(@NotNull Utf8Sequence us, char asciiChar) {
        final int size = us.size();
        return size != 0 && asciiChar == us.byteAt(size - 1);
    }

    public static boolean equals(@NotNull DirectUtf8String l, @NotNull Utf8String r) {
        int size;
        if ((size = l.size()) != r.size()) {
            return false;
        }
        final long lo = l.lo();
        int i = 0;
        for (; i + 3 < size; i += 4) {
            if (Unsafe.getUnsafe().getInt(lo + i) != r.intAt(i)) {
                return false;
            }
        }
        for (; i < size; i++) {
            if (Unsafe.getUnsafe().getByte(lo + i) != r.byteAt(i)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(@NotNull Utf8String l, @NotNull Utf8String r) {
        if (l == r) {
            return true;
        }
        int size;
        if ((size = l.size()) != r.size()) {
            return false;
        }
        int i = 0;
        for (; i + 3 < size; i += 4) {
            if (l.intAt(i) != r.intAt(i)) {
                return false;
            }
        }
        for (; i < size; i++) {
            if (l.byteAt(i) != r.byteAt(i)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(@NotNull Utf8Sequence l, @NotNull Utf8Sequence r) {
        if (l == r) {
            return true;
        }
        if (l.size() != r.size()) {
            return false;
        }
        for (int index = 0, n = l.size(); index < n; index++) {
            if (l.byteAt(index) != r.byteAt(index)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(@NotNull Utf8Sequence l, int lLo, int lHi, @NotNull Utf8Sequence r, int rLo, int rHi) {
        if (l == r) {
            return true;
        }
        int ll = lHi - lLo;
        if (ll != rHi - rLo) {
            return false;
        }
        for (int i = 0; i < ll; i++) {
            if (l.byteAt(i + lLo) != r.byteAt(i + rLo)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(@NotNull DirectUtf8Sequence l, @NotNull DirectUtf8Sequence r) {
        if (l == r) {
            return true;
        }
        if (l.ptr() == r.ptr() && l.size() == r.size()) {
            return true;
        }
        return equals(l, (Utf8Sequence) r);
    }

    public static boolean equalsAscii(@NotNull CharSequence lAsciiSeq, int lLo, int lHi, @NotNull Utf8Sequence rSeq, int rLo, int rHi) {
        int ll = lHi - lLo;
        if (ll != rHi - rLo) {
            return false;
        }
        for (int i = 0; i < ll; i++) {
            if (lAsciiSeq.charAt(i + lLo) != rSeq.byteAt(i + rLo)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equalsAscii(@NotNull CharSequence asciiSeq, @NotNull Utf8Sequence seq) {
        int len;
        if ((len = asciiSeq.length()) != seq.size()) {
            return false;
        }
        for (int index = 0; index < len; index++) {
            if (asciiSeq.charAt(index) != seq.byteAt(index)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equalsAscii(@NotNull CharSequence lAsciiSeq, @NotNull Utf8Sequence rSeq, int rLo, int rHi) {
        int ll;
        if ((ll = lAsciiSeq.length()) != rHi - rLo) {
            return false;
        }

        for (int i = 0; i < ll; i++) {
            if (lAsciiSeq.charAt(i) != rSeq.byteAt(i + rLo)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equalsIgnoreCaseAscii(@NotNull Utf8Sequence lSeq, @NotNull Utf8Sequence rSeq) {
        int size;
        if ((size = lSeq.size()) != rSeq.size()) {
            return false;
        }
        for (int index = 0; index < size; index++) {
            if (toLowerCaseAscii(lSeq.byteAt(index)) != toLowerCaseAscii(rSeq.byteAt(index))) {
                return false;
            }
        }
        return true;
    }

    public static boolean equalsIgnoreCaseAscii(@NotNull Utf8Sequence lSeq, int lLo, int lHi, @NotNull Utf8Sequence rSeq, int rLo, int rHi) {
        if (lSeq == rSeq) {
            return true;
        }
        int ll = lHi - lLo;
        if (ll != rHi - rLo) {
            return false;
        }
        for (int i = 0; i < ll; i++) {
            if (toLowerCaseAscii(lSeq.byteAt(i + lLo)) != toLowerCaseAscii(rSeq.byteAt(i + rLo))) {
                return false;
            }
        }
        return true;
    }

    public static boolean equalsIgnoreCaseAscii(@NotNull CharSequence asciiSeq, @NotNull Utf8Sequence seq) {
        int len;
        if ((len = asciiSeq.length()) != seq.size()) {
            return false;
        }
        for (int index = 0; index < len; index++) {
            if (Chars.toLowerCaseAscii(asciiSeq.charAt(index)) != toLowerCaseAscii(seq.byteAt(index))) {
                return false;
            }
        }
        return true;
    }

    public static boolean equalsNcAscii(@NotNull CharSequence asciiSeq, @Nullable Utf8Sequence seq) {
        return seq != null && equalsAscii(asciiSeq, seq);
    }

    public static int hashCode(@NotNull Utf8Sequence value) {
        int size = value.size();
        if (size == 0) {
            return 0;
        }
        int h = 0;
        for (int p = 0; p < size; p++) {
            h = 31 * h + value.byteAt(p);
        }
        return h;
    }

    public static int hashCode(@NotNull Utf8Sequence value, int lo, int hi) {
        if (hi == lo) {
            return 0;
        }
        int h = 0;
        for (int p = lo; p < hi; p++) {
            h = 31 * h + value.byteAt(p);
        }
        return h;
    }

    public static int indexOfAscii(@NotNull Utf8Sequence seq, char asciiChar) {
        return indexOfAscii(seq, 0, asciiChar);
    }

    public static int indexOfAscii(@NotNull Utf8Sequence seq, int seqLo, char asciiChar) {
        return indexOfAscii(seq, seqLo, seq.size(), asciiChar);
    }

    public static int indexOfAscii(@NotNull Utf8Sequence seq, int seqLo, int seqHi, char asciiChar) {
        return indexOfAscii(seq, seqLo, seqHi, asciiChar, 1);
    }

    public static int indexOfAscii(@NotNull Utf8Sequence seq, int seqLo, int seqHi, @NotNull CharSequence asciiTerm) {
        int termLen = asciiTerm.length();
        if (termLen == 0) {
            return 0;
        }

        byte first = (byte) asciiTerm.charAt(0);
        int max = seqHi - termLen;

        for (int i = seqLo; i <= max; ++i) {
            if (seq.byteAt(i) != first) {
                do {
                    ++i;
                } while (i <= max && seq.byteAt(i) != first);
            }

            if (i <= max) {
                int j = i + 1;
                int end = j + termLen - 1;

                for (int k = 1; j < end && seq.byteAt(j) == asciiTerm.charAt(k); ++k) {
                    ++j;
                }

                if (j == end) {
                    return i;
                }
            }
        }

        return -1;
    }

    public static int indexOfAscii(@NotNull Utf8Sequence seq, int seqLo, int seqHi, @NotNull CharSequence asciiTerm, int occurrence) {
        int termLen = asciiTerm.length();
        if (termLen == 0) {
            return -1;
        }

        if (occurrence == 0) {
            return -1;
        }

        int foundIndex = -1;
        int count = 0;
        if (occurrence > 0) {
            for (int i = seqLo; i < seqHi; i++) {
                if (foundIndex == -1) {
                    if (seqHi - i < termLen) {
                        return -1;
                    }
                    if (seq.byteAt(i) == asciiTerm.charAt(0)) {
                        foundIndex = i;
                    }
                } else { // first character matched, try to match the rest of the term
                    if (seq.byteAt(i) != asciiTerm.charAt(i - foundIndex)) {
                        // start again from after where the first character was found
                        i = foundIndex;
                        foundIndex = -1;
                    }
                }

                if (foundIndex != -1 && i - foundIndex == termLen - 1) {
                    count++;
                    if (count == occurrence) {
                        return foundIndex;
                    } else {
                        foundIndex = -1;
                    }
                }
            }
        } else { // if occurrence is negative, search in reverse
            for (int i = seqHi - 1; i >= seqLo; i--) {
                if (foundIndex == -1) {
                    if (i - seqLo + 1 < termLen) {
                        return -1;
                    }
                    if (seq.byteAt(i) == asciiTerm.charAt(termLen - 1)) {
                        foundIndex = i;
                    }
                } else { // last character matched, try to match the rest of the term
                    if (seq.byteAt(i) != asciiTerm.charAt(termLen - 1 + i - foundIndex)) {
                        // start again from after where the first character was found
                        i = foundIndex;
                        foundIndex = -1;
                    }
                }

                if (foundIndex != -1 && foundIndex - i == termLen - 1) {
                    count--;
                    if (count == occurrence) {
                        return foundIndex + 1 - termLen;
                    } else {
                        foundIndex = -1;
                    }
                }
            }
        }

        return -1;
    }

    public static int indexOfAscii(@NotNull Utf8Sequence seq, int seqLo, int seqHi, char asciiChar, int occurrence) {
        if (occurrence == 0) {
            return -1;
        }

        int count = 0;
        if (occurrence > 0) {
            for (int i = seqLo; i < seqHi; i++) {
                if (seq.byteAt(i) == asciiChar) {
                    count++;
                    if (count == occurrence) {
                        return i;
                    }
                }
            }
        } else { // if occurrence is negative, search in reverse
            for (int i = seqHi - 1; i >= seqLo; i--) {
                if (seq.byteAt(i) == asciiChar) {
                    count--;
                    if (count == occurrence) {
                        return i;
                    }
                }
            }
        }

        return -1;
    }

    public static int lastIndexOfAscii(@NotNull Utf8Sequence seq, char asciiTerm) {
        for (int i = seq.size() - 1; i > -1; i--) {
            if (seq.byteAt(i) == asciiTerm) {
                return i;
            }
        }
        return -1;
    }

    public static int lowerCaseAsciiHashCode(@NotNull Utf8Sequence value) {
        int size = value.size();
        if (size == 0) {
            return 0;
        }
        int h = 0;
        for (int p = 0; p < size; p++) {
            h = 31 * h + toLowerCaseAscii(value.byteAt(p));
        }
        return h;
    }

    public static int lowerCaseAsciiHashCode(@NotNull Utf8Sequence value, int lo, int hi) {
        if (hi == lo) {
            return 0;
        }
        int h = 0;
        for (int p = lo; p < hi; p++) {
            h = 31 * h + toLowerCaseAscii(value.byteAt(p));
        }
        return h;
    }

    public static boolean startsWith(@NotNull Utf8Sequence seq, @NotNull Utf8Sequence term) {
        final int size = term.size();
        return seq.size() >= size && equalsBytes(seq, term, size);
    }

    public static boolean startsWithAscii(@NotNull Utf8Sequence seq, @NotNull CharSequence asciiTerm) {
        final int len = asciiTerm.length();
        return seq.size() >= len && equalsAscii(asciiTerm, seq, 0, len);
    }

    public static void strCpy(@NotNull Utf8Sequence src, int destLen, long destAddr) {
        for (int i = 0; i < destLen; i++) {
            Unsafe.getUnsafe().putByte(destAddr + i, src.byteAt(i));
        }
    }

    public static void strCpy(long srcLo, long srcHi, @NotNull Utf8Sink dest) {
        for (long i = srcLo; i < srcHi; i++) {
            dest.put(Unsafe.getUnsafe().getByte(i));
        }
    }

    public static void strCpyAscii(char @NotNull [] srcChars, int srcLo, int srcLen, long destAddr) {
        for (int i = 0; i < srcLen; i++) {
            Unsafe.getUnsafe().putByte(destAddr + i, (byte) srcChars[i + srcLo]);
        }
    }

    public static long strCpyAscii(@NotNull CharSequence asciiSrc, long destAddr) {
        strCpyAscii(asciiSrc, asciiSrc.length(), destAddr);
        return destAddr;
    }

    public static void strCpyAscii(@NotNull CharSequence asciiSrc, int srcLen, long destAddr) {
        strCpyAscii(asciiSrc, 0, srcLen, destAddr);
    }

    public static void strCpyAscii(@NotNull CharSequence asciiSrc, int srcLo, int srcLen, long destAddr) {
        for (int i = 0; i < srcLen; i++) {
            Unsafe.getUnsafe().putByte(destAddr + i, (byte) asciiSrc.charAt(srcLo + i));
        }
    }

    public static String stringFromUtf8Bytes(long lo, long hi) {
        if (hi == lo) {
            return "";
        }
        CharSink b = Misc.getThreadLocalSink();
        utf8ToUtf16(lo, hi, b);
        return b.toString();
    }

    public static String stringFromUtf8Bytes(@NotNull Utf8Sequence seq) {
        if (seq.size() == 0) {
            return "";
        }
        CharSink b = Misc.getThreadLocalSink();
        utf8ToUtf16(seq, b);
        return b.toString();
    }

    public static String toString(@Nullable Utf8Sequence s) {
        return s == null ? null : s.toString();
    }

    /**
     * A specialised function to decode a single UTF-8 character.
     * Used when it doesn't make sense to allocate a temporary sink
     *
     * @param lo character bytes start
     * @param hi character bytes end
     * @return an integer-encoded tuple (decoded number of bytes, character in UTF-16 encoding, stored as short type)
     */
    public static int utf8CharDecode(long lo, long hi) {
        if (lo < hi) {
            byte b1 = Unsafe.getUnsafe().getByte(lo);
            if (b1 < 0) {
                if (b1 >> 5 == -2 && (b1 & 30) != 0 && hi - lo > 1) {
                    byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
                    if (isNotContinuation(b2)) {
                        return 0;
                    }
                    return Numbers.encodeLowHighShorts((short) 2, (short) (b1 << 6 ^ b2 ^ 3968));
                }

                if (b1 >> 4 == -2 && hi - lo > 2) {
                    byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
                    byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
                    if (isMalformed3(b1, b2, b3)) {
                        return 0;
                    }

                    char c = (char) (b1 << 12 ^ b2 << 6 ^ b3 ^ -123008);
                    if (Character.isSurrogate(c)) {
                        return 0;
                    }
                    return Numbers.encodeLowHighShorts((short) 3, (short) c);
                }
                return 0;
            } else {
                return Numbers.encodeLowHighShorts((short) 1, b1);
            }
        }
        return 0;
    }

    public static int utf8DecodeMultiByte(long lo, long hi, int b, CharSinkBase<?> sink) {
        if (b >> 5 == -2 && (b & 30) != 0) {
            return utf8Decode2Bytes(lo, hi, b, sink);
        }
        if (b >> 4 == -2) {
            return utf8Decode3Bytes(lo, hi, b, sink);
        }
        return utf8Decode4Bytes(lo, hi, b, sink);
    }

    public static CharSequence utf8ToUtf16(
            @NotNull DirectUtf8Sequence utf8CharSeq,
            @NotNull MutableCharSink tempSink,
            boolean hasNonAsciiChars
    ) {
        if (hasNonAsciiChars) {
            utf8ToUtf16Unchecked(utf8CharSeq, tempSink);
            return tempSink;
        }
        return utf8CharSeq.asAsciiCharSequence();
    }

    /**
     * Decodes bytes between lo,hi addresses into sink.
     * Note: operation might fail in the middle and leave sink in inconsistent state.
     *
     * @return true if input is proper UTF-8 and false otherwise.
     */
    public static boolean utf8ToUtf16(long lo, long hi, @NotNull CharSinkBase<?> sink) {
        long p = lo;
        while (p < hi) {
            byte b = Unsafe.getUnsafe().getByte(p);
            if (b < 0) {
                int n = utf8DecodeMultiByte(p, hi, b, sink);
                if (n == -1) {
                    // UTF8 error
                    return false;
                }
                p += n;
            } else {
                sink.put((char) b);
                ++p;
            }
        }
        return true;
    }

    /**
     * Decodes bytes from the given UTF-8 sink into char sink.
     * Note: operation might fail in the middle and leave sink in inconsistent state.
     *
     * @return true if input is proper UTF-8 and false otherwise.
     */
    public static boolean utf8ToUtf16(@NotNull Utf8Sequence seq, int seqLo, int seqHi, @NotNull CharSinkBase<?> sink) {
        int i = seqLo;
        while (i < seqHi) {
            byte b = seq.byteAt(i);
            if (b < 0) {
                int n = utf8DecodeMultiByte(seq, i, b, sink);
                if (n == -1) {
                    // UTF-8 error
                    return false;
                }
                i += n;
            } else {
                sink.put((char) b);
                ++i;
            }
        }
        return true;
    }

    /**
     * Decodes bytes from the given UTF-8 sink into char sink.
     * Note: operation might fail in the middle and leave sink in inconsistent state.
     *
     * @return true if input is proper UTF-8 and false otherwise.
     */
    public static boolean utf8ToUtf16(@NotNull Utf8Sequence seq, @NotNull CharSinkBase<?> sink) {
        return utf8ToUtf16(seq, 0, seq.size(), sink);
    }

    /**
     * Translates UTF8 sequence into UTF-16 sequence and returns number of bytes read from the input sequence.
     * It terminates transcoding when it encounters one of the following:
     * <ul>
     *     <li>end of the input sequence</li>
     *     <li>terminator byte</li>
     *     <li>invalid UTF-8 sequence</li>
     * </ul>
     * The terminator byte must be a valid ASCII character.
     * <p>
     * It returns number of bytes consumed from the input sequence and does not include terminator byte.
     * <p>
     * When input sequence is invalid, it returns -1 and the sink is left in undefined state and should be cleared before
     * next use.
     *
     * @param seq        input sequence encoded in UTF-8
     * @param sink       sink to write UTF-16 characters to
     * @param terminator terminator byte, must be a valid ASCII character
     * @return number of bytes read or -1 if input sequence is invalid.
     */
    public static int utf8ToUtf16(@NotNull Utf8Sequence seq, @NotNull CharSinkBase<?> sink, byte terminator) {
        assert terminator >= 0 : "terminator must be ASCII character";

        int i = 0;
        int size = seq.size();
        while (i < size) {
            byte b = seq.byteAt(i);
            if (b == terminator) {
                return i;
            }
            if (b < 0) {
                int n = utf8DecodeMultiByte(seq, i, b, sink);
                if (n == -1) {
                    // UTF-8 error
                    return -1;
                }
                i += n;
            } else {
                sink.put((char) b);
                ++i;
            }
        }
        return i;
    }

    /**
     * Decodes bytes between lo,hi addresses into sink while replacing consecutive
     * quotes with a single one.
     * <p>
     * Note: operation might fail in the middle and leave sink in inconsistent state.
     *
     * @return true if input is proper UTF-8 and false otherwise.
     */
    public static boolean utf8ToUtf16EscConsecutiveQuotes(long lo, long hi, @NotNull CharSink sink) {
        long p = lo;
        int quoteCount = 0;

        while (p < hi) {
            byte b = Unsafe.getUnsafe().getByte(p);
            if (b < 0) {
                int n = utf8DecodeMultiByte(p, hi, b, sink);
                if (n == -1) {
                    // UTF-8 error
                    return false;
                }
                p += n;
            } else {
                if (b == '"') {
                    if (quoteCount++ % 2 == 0) {
                        sink.put('"');
                    }
                } else {
                    quoteCount = 0;
                    sink.put((char) b);
                }
                ++p;
            }
        }
        return true;
    }

    public static void utf8ToUtf16Unchecked(@NotNull DirectUtf8Sequence utf8CharSeq, @NotNull MutableCharSink tempSink) {
        tempSink.clear();
        if (!utf8ToUtf16(utf8CharSeq.lo(), utf8CharSeq.hi(), tempSink)) {
            throw CairoException.nonCritical().put("invalid UTF8 in value for ").put(utf8CharSeq);
        }
    }

    public static boolean utf8ToUtf16Z(long lo, CharSinkBase<?> sink) {
        long p = lo;
        while (true) {
            byte b = Unsafe.getUnsafe().getByte(p);
            if (b == 0) {
                break;
            }
            if (b < 0) {
                int n = utf8DecodeMultiByteZ(p, b, sink);
                if (n == -1) {
                    // UTF-8 error
                    return false;
                }
                p += n;
            } else {
                sink.put((char) b);
                ++p;
            }
        }
        return true;
    }

    private static int encodeUtf16Surrogate(@NotNull Utf8Sink sink, char c, @NotNull CharSequence in, int pos, int hi) {
        int dword;
        if (Character.isHighSurrogate(c)) {
            if (hi - pos < 1) {
                sink.putAscii('?');
                return pos;
            } else {
                char c2 = in.charAt(pos++);
                if (Character.isLowSurrogate(c2)) {
                    dword = Character.toCodePoint(c, c2);
                } else {
                    sink.putAscii('?');
                    return pos;
                }
            }
        } else if (Character.isLowSurrogate(c)) {
            sink.putAscii('?');
            return pos;
        } else {
            dword = c;
        }
        sink.put((byte) (240 | dword >> 18));
        sink.put((byte) (128 | dword >> 12 & 63));
        sink.put((byte) (128 | dword >> 6 & 63));
        sink.put((byte) (128 | dword & 63));
        return pos;
    }

    private static boolean equalsBytes(@NotNull Utf8Sequence l, @NotNull Utf8Sequence r, int size) {
        for (int i = 0; i < size; i++) {
            if (l.byteAt(i) != r.byteAt(i)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isMalformed3(int b1, int b2, int b3) {
        return b1 == -32 && (b2 & 224) == 128 || (b2 & 192) != 128 || (b3 & 192) != 128;
    }

    private static boolean isMalformed4(int b2, int b3, int b4) {
        return (b2 & 192) != 128 || (b3 & 192) != 128 || (b4 & 192) != 128;
    }

    private static boolean isNotContinuation(int b) {
        return (b & 192) != 128;
    }

    private static byte toLowerCaseAscii(byte b) {
        return b > 64 && b < 91 ? (byte) (b + 32) : b;
    }

    private static int utf8Decode2Bytes(@NotNull Utf8Sequence seq, int index, int b1, @NotNull CharSinkBase<?> sink) {
        if (seq.size() - index < 2) {
            return -1;
        }
        byte b2 = seq.byteAt(index + 1);
        if (isNotContinuation(b2)) {
            return -1;
        }
        sink.put((char) (b1 << 6 ^ b2 ^ 3968));
        return 2;
    }

    private static int utf8Decode2Bytes(long lo, long hi, int b1, @NotNull CharSinkBase<?> sink) {
        if (hi - lo < 2) {
            return -1;
        }
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        if (isNotContinuation(b2)) {
            return -1;
        }
        sink.put((char) (b1 << 6 ^ b2 ^ 3968));
        return 2;
    }

    private static int utf8Decode2BytesZ(long lo, int b1, @NotNull CharSinkBase<?> sink) {
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        if (b2 == 0) {
            return -1;
        }
        if (isNotContinuation(b2)) {
            return -1;
        }
        sink.put((char) (b1 << 6 ^ b2 ^ 3968));
        return 2;
    }

    private static int utf8Decode3Byte0(int b1, @NotNull CharSinkBase<?> sink, byte b2, byte b3) {
        if (isMalformed3(b1, b2, b3)) {
            return -1;
        }
        char c = (char) (b1 << 12 ^ b2 << 6 ^ b3 ^ -123008);
        if (Character.isSurrogate(c)) {
            return -1;
        }
        sink.put(c);
        return 3;
    }

    private static int utf8Decode3Bytes(long lo, long hi, int b1, @NotNull CharSinkBase<?> sink) {
        if (hi - lo < 3) {
            return -1;
        }
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
        return utf8Decode3Byte0(b1, sink, b2, b3);
    }

    private static int utf8Decode3Bytes(@NotNull Utf8Sequence seq, int index, int b1, @NotNull CharSinkBase<?> sink) {
        if (seq.size() - index < 3) {
            return -1;
        }
        byte b2 = seq.byteAt(index + 1);
        byte b3 = seq.byteAt(index + 2);
        return utf8Decode3Byte0(b1, sink, b2, b3);
    }

    private static int utf8Decode3BytesZ(long lo, int b1, @NotNull CharSinkBase<?> sink) {
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        if (b2 == 0) {
            return -1;
        }
        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
        if (b3 == 0) {
            return -1;
        }
        return utf8Decode3Byte0(b1, sink, b2, b3);
    }

    private static int utf8Decode4Bytes(long lo, long hi, int b, @NotNull CharSinkBase<?> sink) {
        if (b >> 3 != -2 || hi - lo < 4) {
            return -1;
        }
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
        byte b4 = Unsafe.getUnsafe().getByte(lo + 3);
        return utf8Decode4Bytes0(b, sink, b2, b3, b4);
    }

    private static int utf8Decode4Bytes(@NotNull Utf8Sequence seq, int index, int b, @NotNull CharSinkBase<?> sink) {
        if (b >> 3 != -2 || seq.size() - index < 4) {
            return -1;
        }
        byte b2 = seq.byteAt(index + 1);
        byte b3 = seq.byteAt(index + 2);
        byte b4 = seq.byteAt(index + 3);
        return utf8Decode4Bytes0(b, sink, b2, b3, b4);
    }

    private static int utf8Decode4Bytes0(int b, @NotNull CharSinkBase<?> sink, byte b2, byte b3, byte b4) {
        if (isMalformed4(b2, b3, b4)) {
            return -1;
        }
        final int codePoint = b << 18 ^ b2 << 12 ^ b3 << 6 ^ b4 ^ 3678080;
        if (Character.isSupplementaryCodePoint(codePoint)) {
            sink.put(Character.highSurrogate(codePoint));
            sink.put(Character.lowSurrogate(codePoint));
            return 4;
        }
        return -1;
    }

    private static int utf8Decode4BytesZ(long lo, int b, CharSinkBase<?> sink) {
        if (b >> 3 != -2) {
            return -1;
        }
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        if (b2 == 0) {
            return -1;
        }
        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
        if (b3 == 0) {
            return -1;
        }
        byte b4 = Unsafe.getUnsafe().getByte(lo + 3);
        if (b4 == 0) {
            return -1;
        }
        return utf8Decode4Bytes0(b, sink, b2, b3, b4);
    }

    private static int utf8DecodeMultiByte(Utf8Sequence seq, int index, int b, @NotNull CharSinkBase<?> sink) {
        if (b >> 5 == -2 && (b & 30) != 0) {
            return utf8Decode2Bytes(seq, index, b, sink);
        }
        if (b >> 4 == -2) {
            return utf8Decode3Bytes(seq, index, b, sink);
        }
        return utf8Decode4Bytes(seq, index, b, sink);
    }

    private static int utf8DecodeMultiByteZ(long lo, int b, @NotNull CharSinkBase<?> sink) {
        if (b >> 5 == -2 && (b & 30) != 0) {
            return utf8Decode2BytesZ(lo, b, sink);
        }
        if (b >> 4 == -2) {
            return utf8Decode3BytesZ(lo, b, sink);
        }
        return utf8Decode4BytesZ(lo, b, sink);
    }
}
