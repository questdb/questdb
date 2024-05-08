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

package io.questdb.std.str;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.griffin.engine.functions.str.TrimType;
import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_INLINED_PREFIX_BYTES;
import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_INLINED_PREFIX_MASK;

/**
 * UTF-8 specific variant of the {@link Chars} utility.
 */
public final class Utf8s {

    private final static io.questdb.std.ThreadLocal<StringSink> tlSink = new ThreadLocal<>(StringSink::new);

    private Utf8s() {
    }

    /**
     * Lexicographically compares two UTF-8 sequences.
     * <br>
     * The comparison is based on the codepoints of the characters in the sequences.
     * This definition differs from lexical comparison as defined in the Java language specification, where
     * the comparison is based values of char values of Strings. It may produce different results when comparing
     * sequences that contain characters outside the Basic Multilingual Plane (BMP).
     * <br>
     * This method assume that the sequences are valid UTF-8 sequences and does not perform any validation.
     *
     * @param l left sequence
     * @param r right sequence
     * @return a negative integer, zero, or a positive integer as the left sequence is less than, equal to, or greater than the right sequence
     */
    public static int compare(Utf8Sequence l, Utf8Sequence r) {
        if (l == r) {
            return 0;
        }

        if (l == null) {
            return -1;
        }

        if (r == null) {
            return 1;
        }

        final int ll = l.size();
        final int rl = r.size();
        final int min = Math.min(ll, rl);

        for (int i = 0; i < min; i++) {
            final int k = Numbers.compareUnsigned(l.byteAt(i), r.byteAt(i));
            if (k != 0) {
                return k;
            }
        }
        return Integer.compare(ll, rl);
    }

    public static boolean contains(@NotNull Utf8Sequence sequence, @NotNull Utf8Sequence term) {
        return indexOf(sequence, 0, sequence.size(), term) != -1;
    }

    public static boolean containsAscii(@NotNull Utf8Sequence sequence, @NotNull CharSequence asciiTerm) {
        return indexOfAscii(sequence, 0, sequence.size(), asciiTerm) != -1;
    }

    // Pattern has to be lower-case.
    public static boolean containsLowerCaseAscii(@NotNull Utf8Sequence sequence, @NotNull Utf8Sequence asciiTerm) {
        return indexOfLowerCaseAscii(sequence, 0, sequence.size(), asciiTerm) != -1;
    }

    public static CharSequence directUtf8ToUtf16(
            @NotNull DirectUtf8Sequence utf8CharSeq,
            @NotNull MutableUtf16Sink tempSink
    ) {
        if (utf8CharSeq.isAscii()) {
            return utf8CharSeq.asAsciiCharSequence();
        }
        utf8ToUtf16Unchecked(utf8CharSeq, tempSink);
        return tempSink;
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

    public static boolean endsWith(@NotNull Utf8Sequence seq, @NotNull Utf8Sequence endsWith) {
        int endsWithSize = endsWith.size();
        if (endsWithSize == 0) {
            return true;
        }
        int seqSize = seq.size();
        return seqSize >= endsWithSize && equalSuffixBytes(seq, endsWith, seqSize, endsWithSize);
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

    // Pattern has to be lower-case.
    public static boolean endsWithLowerCaseAscii(@NotNull Utf8Sequence seq, @NotNull Utf8Sequence asciiEnds) {
        final int size = asciiEnds.size();
        if (size == 0) {
            return true;
        }
        final int seqSize = seq.size();
        return !(seqSize == 0 || seqSize < size) && equalsAsciiLowerCase(asciiEnds, seq, seqSize - size, seqSize);
    }

    /**
     * Checks if the given UTF-8 sequences contain equal strings. Co-exists with
     * {@link #equals(Utf8Sequence, Utf8Sequence)} merely to avoid megamorphism
     * in {@link Utf8StringIntHashMap} and {@link Utf8StringObjHashMap}. Also, unlike
     * the general equals method, this one doesn't allow nulls.
     *
     * @param l left sequence to compare
     * @param r right sequence to compare
     * @return true if the sequences contain equal strings, false otherwise
     */
    public static boolean equals(@NotNull DirectUtf8Sequence l, @NotNull Utf8String r) {
        final int lSize = l.size();
        return lSize == r.size() && dataEquals(l, r, 0, lSize);
    }

    public static boolean equals(@NotNull Utf8Sequence l, long lSixPrefix, @NotNull Utf8Sequence r, long rSixPrefix) {
        final int lSize = l.size();
        return lSize == r.size() && lSixPrefix == rSixPrefix && dataEquals(l, r, VARCHAR_INLINED_PREFIX_BYTES, lSize);
    }

    public static boolean equals(@Nullable Utf8Sequence l, @Nullable Utf8Sequence r) {
        if (l == null && r == null) {
            return true;
        }
        if (l == null || r == null) {
            return false;
        }
        final int lSize = l.size();
        return lSize == r.size() && l.zeroPaddedSixPrefix() == r.zeroPaddedSixPrefix()
                && dataEquals(l, r, VARCHAR_INLINED_PREFIX_BYTES, lSize);
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
        int ll = lAsciiSeq.length();
        if (ll != rHi - rLo) {
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
        int size = lSeq.size();
        if (size != rSeq.size()) {
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
        int len = asciiSeq.length();
        if (len != seq.size()) {
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

    public static boolean equalsUtf16(CharSequence l, Utf8Sequence r) {
        return equalsUtf16(l, 0, l.length(), r, 0, r.size());
    }

    public static boolean equalsUtf16(CharSequence c, int ci, int cn, Utf8Sequence u, int ui, int un) {
        while (ui < un && ci < cn) {
            int bytes = utf16Equals(c, ci, cn, u, ui, un);
            switch (bytes) {
                case 4:
                    // 4 bytes decoded from UTF-8 sequence
                    ci++;
                    // fall through
                case 1:
                case 2:
                case 3:
                    // 1,2,3 bytes decoded from UTF-8 sequence
                    ci++;
                    ui += bytes;
                    break;
                default:
                    // Not equal or malformed
                    return false;
            }
        }
        return ui == un && ci == cn;
    }

    public static boolean equalsUtf16Nc(CharSequence l, Utf8Sequence r) {
        if (l == null || r == null) {
            return l == r;
        }

        return equalsUtf16(l, r);
    }

    public static int getUtf8Codepoint(int b1, int b2, int b3, int b4) {
        return b1 << 18 ^ b2 << 12 ^ b3 << 6 ^ b4 ^ 3678080;
    }

    /**
     * Validates if the bytes between lo,hi addresses belong to a valid UTF8 sequence.
     *
     * @return -1 if bytes are not a UTF8 sequence, 0 if this is ASCII sequence and 1 if it is non-ascii UTF8 sequence.
     */
    public static int getUtf8SequenceType(long lo, long hi) {
        long p = lo;
        int sequenceType = 0;
        while (p < hi) {
            byte b = Unsafe.getUnsafe().getByte(p);
            if (b < 0) {
                int n = validateUtf8MultiByte(p, hi, b);
                if (n == -1) {
                    // UTF8 error
                    return -1;
                }
                p += n;
                // non-ASCII sequence
                sequenceType = 1;
            } else {
                ++p;
            }
        }
        return sequenceType;
    }

    /**
     * Strictly greater than (&gt;) comparison of two UTF8 sequences in lexicographical
     * order. For example, for:
     * l = aaaaa
     * r = aaaaaaa
     * the l &gt; r will produce "false", however for:
     * l = bbbb
     * r = aaaaaaa
     * the l &gt; r will produce "true", because b &gt; a.
     *
     * @param l left sequence, can be null
     * @param r right sequence, can be null
     * @return if either l or r is "null", the return value false, otherwise sequences are compared lexicographically.
     */
    public static boolean greaterThan(@Nullable Utf8Sequence l, @Nullable Utf8Sequence r) {
        if (l == null || r == null) {
            return false;
        }

        final int ll = l.size();
        final int rl = r.size();
        final int min = Math.min(ll, rl);
        for (int i = 0; i < min; i++) {
            final int k = Numbers.compareUnsigned(l.byteAt(i), r.byteAt(i));
            if (k != 0) {
                return k > 0;
            }
        }
        return ll > rl;
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

    public static int indexOf(@NotNull Utf8Sequence seq, int seqLo, int seqHi, @NotNull Utf8Sequence term) {
        int termSize = term.size();
        if (termSize == 0) {
            return 0;
        }

        byte first = term.byteAt(0);
        int max = seqHi - termSize;

        for (int i = seqLo; i <= max; ++i) {
            if (seq.byteAt(i) != first) {
                do {
                    ++i;
                } while (i <= max && seq.byteAt(i) != first);
            }

            if (i <= max) {
                int j = i + 1;
                int end = j + termSize - 1;
                for (int k = 1; j < end && seq.byteAt(j) == term.byteAt(k); ++k) {
                    ++j;
                }
                if (j == end) {
                    return i;
                }
            }
        }

        return -1;
    }

    public static int indexOf(@NotNull Utf8Sequence seq, int seqLo, int seqHi, @NotNull Utf8Sequence term, int occurrence) {
        if (occurrence == 0) {
            return -1;
        }

        int termSize = term.size();
        if (termSize == 0) {
            return 0;
        }

        byte first = term.byteAt(0);
        int max = seqHi - termSize;

        int count = 0;
        if (occurrence > 0) {
            for (int i = seqLo; i <= max; i++) {
                if (seq.byteAt(i) != first) {
                    do {
                        ++i;
                    } while (i <= max && seq.byteAt(i) != first);
                }

                if (i <= max) {
                    int j = i + 1;
                    int end = j + termSize - 1;
                    for (int k = 1; j < end && seq.byteAt(j) == term.byteAt(k); ++k) {
                        ++j;
                    }
                    if (j == end) {
                        count++;
                        if (count == occurrence) {
                            return i;
                        }
                    }
                }
            }
        } else {    // if occurrence is negative, search in reverse
            for (int i = seqHi - termSize; i >= seqLo; i--) {
                if (seq.byteAt(i) != first) {
                    do {
                        --i;
                    } while (i >= seqLo && seq.byteAt(i) != first);
                }

                if (i >= seqLo) {
                    int j = i + 1;
                    int end = j + termSize - 1;
                    for (int k = 1; j < end && seq.byteAt(j) == term.byteAt(k); ++k) {
                        ++j;
                    }
                    if (j == end) {
                        count--;
                        if (count == occurrence) {
                            return i;
                        }
                    }
                }
            }
        }

        return -1;
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

    // Term has to be lower-case.
    public static int indexOfLowerCaseAscii(@NotNull Utf8Sequence seq, int seqLo, int seqHi, @NotNull Utf8Sequence termLC) {
        int termSize = termLC.size();
        if (termSize == 0) {
            return 0;
        }

        byte first = termLC.byteAt(0);
        int max = seqHi - termSize;

        for (int i = seqLo; i <= max; ++i) {
            if (toLowerCaseAscii(seq.byteAt(i)) != first) {
                do {
                    ++i;
                } while (i <= max && toLowerCaseAscii(seq.byteAt(i)) != first);
            }

            if (i <= max) {
                int j = i + 1;
                int end = j + termSize - 1;
                for (int k = 1; j < end && toLowerCaseAscii(seq.byteAt(j)) == termLC.byteAt(k); ++k) {
                    ++j;
                }
                if (j == end) {
                    return i;
                }
            }
        }

        return -1;
    }

    public static boolean isAscii(Utf8Sequence utf8) {
        boolean ascii = true;
        if (utf8 != null) {
            for (int k = 0, kl = utf8.size(); k < kl; k++) {
                if (utf8.byteAt(k) < 0) {
                    ascii = false;
                    break;
                }
            }
        }
        return ascii;
    }

    public static int lastIndexOfAscii(@NotNull Utf8Sequence seq, char asciiTerm) {
        for (int i = seq.size() - 1; i > -1; i--) {
            if (seq.byteAt(i) == asciiTerm) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the length of the UTF-8 sequence as the count of code points.
     * NOTE: this number is different from the length of the equivalent Java String,
     * which counts UTF-16 code words. A surrogate pair encodes one code point, but
     * counts as two in the length of a Java String.
     */
    public static int length(Utf8Sequence value) {
        if (value == null) {
            return TableUtils.NULL_LEN;
        }
        final int size = value.size();

        int continuationByteCount = 0;
        int i = 0;
        for (; i <= size - Long.BYTES; i += Long.BYTES) {
            long c = value.longAt(i);
            long x = c & 0x8080808080808080L;
            long y = ~c << 1;
            long swarDelta = x & y;
            int delta = Long.bitCount(swarDelta);
            continuationByteCount += delta;
        }
        for (; i < size; i++) {
            int c = value.byteAt(i);
            int x = c & 0x80;
            int y = ~c << 1;
            int delta = (x & y) >>> 7;
            continuationByteCount += delta;
        }
        return size - continuationByteCount;
    }

    /**
     * Strictly less than (&lt;) comparison of two UTF8 sequences in lexicographical
     * order. For example, for:
     * l = aaaaa
     * r = aaaaaaa
     * the l &lt; r will produce "true", however for:
     * l = bbbb
     * r = aaaaaaa
     * the l &lt; r will produce "false", because b &lt; a.
     *
     * @param l left sequence, can be null
     * @param r right sequence, can be null
     * @return if either l or r is "null", the return value false, otherwise sequences are compared lexicographically.
     */
    public static boolean lessThan(@Nullable Utf8Sequence l, @Nullable Utf8Sequence r) {
        if (l == null || r == null) {
            return false;
        }

        final int ll = l.size();
        final int rl = r.size();
        final int min = Math.min(ll, rl);
        for (int i = 0; i < min; i++) {
            final int k = Numbers.compareUnsigned(l.byteAt(i), r.byteAt(i));
            if (k != 0) {
                return k < 0;
            }
        }
        return ll < rl;
    }

    public static boolean lessThan(@Nullable Utf8Sequence l, @Nullable Utf8Sequence r, boolean negated) {
        final boolean eq = Utf8s.equals(l, r);
        return negated ? (eq || Utf8s.greaterThan(l, r)) : (!eq && Utf8s.lessThan(l, r));
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

    /**
     * Does not delegate to {@link #startsWith(Utf8Sequence, long, Utf8Sequence, long)} in order
     * to prevent unneeded calculation of six-prefix when an earlier check fails.
     */
    public static boolean startsWith(@NotNull Utf8Sequence seq, @NotNull Utf8Sequence startsWith) {
        final int startsWithSize = startsWith.size();
        return startsWithSize == 0 || seq.size() >= startsWithSize && equalPrefixBytes(
                seq, seq.zeroPaddedSixPrefix(), startsWith, startsWith.zeroPaddedSixPrefix(), startsWithSize
        );
    }

    public static boolean startsWith(
            @NotNull Utf8Sequence seq, long seqSixPrefix, @NotNull Utf8Sequence startsWith, long startsWithSixPrefix
    ) {
        final int startsWithSize = startsWith.size();
        return startsWithSize == 0 || seq.size() >= startsWithSize &&
                equalPrefixBytes(seq, seqSixPrefix, startsWith, startsWithSixPrefix, startsWithSize);
    }

    public static boolean startsWithAscii(@NotNull Utf8Sequence seq, @NotNull CharSequence asciiStarts) {
        final int len = asciiStarts.length();
        return seq.size() >= len && equalsAscii(asciiStarts, seq, 0, len);
    }

    // Pattern has to be lower-case.
    public static boolean startsWithLowerCaseAscii(@NotNull Utf8Sequence seq, @NotNull Utf8Sequence asciiStarts) {
        final int size = asciiStarts.size();
        if (size == 0) {
            return true;
        }
        return seq.size() >= size && equalsAsciiLowerCase(asciiStarts, seq, size);
    }

    public static void strCpy(@NotNull Utf8Sequence src, int destLen, long destAddr) {
        for (int i = 0; i < destLen; i++) {
            Unsafe.getUnsafe().putByte(destAddr + i, src.byteAt(i));
        }
    }

    public static void strCpy(long srcLo, long srcHi, @NotNull Utf8Sink dest) {
        for (long i = srcLo; i < srcHi; i++) {
            dest.putAny(Unsafe.getUnsafe().getByte(i));
        }
    }

    /**
     * Copies a substring of the given UTF-8 string.
     *
     * @param seq    input UTF-8 string
     * @param charLo character start (note: not in bytes, but in actual characters)
     * @param charHi character end (exclusive; note: not in bytes, but in actual characters)
     * @param sink   destination sink
     * @return number of copied bytes or -1 if the input is not valid UTF-8
     */
    public static int strCpy(@NotNull Utf8Sequence seq, int charLo, int charHi, @NotNull Utf8Sink sink) {
        if (seq.isAscii()) {
            for (int i = charLo; i < charHi; i++) {
                sink.putAscii((char) seq.byteAt(i));
            }
            return charHi - charLo;
        }

        return strCpyNonAscii(seq, charLo, charHi, sink);
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
        Utf16Sink b = getThreadLocalSink();
        utf8ToUtf16(lo, hi, b);
        return b.toString();
    }

    public static String stringFromUtf8Bytes(@NotNull Utf8Sequence seq) {
        if (seq.size() == 0) {
            return "";
        }
        Utf16Sink b = getThreadLocalSink();
        utf8ToUtf16(seq, b);
        return b.toString();
    }

    /**
     * Implements strpos() with SQL semantics. Returns the 1-based position of a non-null
     * needle within a non-null haystack, and 0 if needle doesn't occur within haystack. An
     * empty needle is specified to occur at position 1 of any haystack (even an empty one).
     */
    public static int strpos(@NotNull Utf8Sequence haystack, @NotNull Utf8Sequence needle) {
        final int substrSize = needle.size();
        if (substrSize < 1) {
            return 1;
        }
        final int strSize = haystack.size();
        if (strSize < 1) {
            return 0;
        }

        OUTER:
        for (int i = 0, strPos = 0, n = strSize - substrSize + 1; i < n; i++) {
            final byte c = haystack.byteAt(i);
            // Only advance strPos if c is not a continuation byte
            if ((c & 0b1100_0000) != 0b1000_0000) {
                strPos++;
            }
            if (c == needle.byteAt(0)) {
                for (int k = 1; k < substrSize; k++) {
                    if (haystack.byteAt(i + k) != needle.byteAt(k)) {
                        continue OUTER;
                    }
                }
                return strPos;
            }
        }
        return 0;
    }

    public static String toString(@NotNull Utf8Sequence us, int start, int end, byte unescapeAscii) {
        final Utf8Sink sink = Misc.getThreadLocalUtf8Sink();
        final int lastChar = end - 1;
        for (int i = start; i < end; i++) {
            byte b = us.byteAt(i);
            sink.putAny(b);
            if (b == unescapeAscii && i < lastChar && us.byteAt(i + 1) == unescapeAscii) {
                i++;
            }
        }
        return sink.toString();
    }

    public static String toString(@Nullable Utf8Sequence s) {
        return s == null ? null : s.toString();
    }

    public static Utf8String toUtf8String(@Nullable Utf8Sequence s) {
        return s == null ? null : Utf8String.newInstance(s);
    }

    public static void trim(TrimType type, Utf8Sequence source, Utf8Sink sink) {
        if (source == null || source.size() == 0) {
            return;
        }
        int start = 0;
        int limit = source.size();
        if (type != TrimType.RTRIM) {
            while (start < limit && source.byteAt(start) == ' ') {
                start++;
            }
        }
        if (type != TrimType.LTRIM) {
            while (limit > start && source.byteAt(limit - 1) == ' ') {
                limit--;
            }
        }
        sink.putAny(source, start, limit);
    }

    /**
     * A specialised function to decode a single UTF-8 character.
     * Used when it doesn't make sense to allocate a temporary sink.
     * Returns 0 in the case of a surrogate pair.
     *
     * @param seq input sequence
     * @return an integer-encoded tuple (decoded number of bytes, character in UTF-16 encoding, stored as short type)
     */
    public static int utf8CharDecode(Utf8Sequence seq) {
        return utf8CharDecode(seq, 0);
    }

    /**
     * A specialised function to decode a single UTF-8 character.
     * Used when it doesn't make sense to allocate a temporary sink.
     * Returns 0 in the case of a surrogate pair.
     *
     * @param seq    input sequence
     * @param offset offset into the sequence
     * @return an integer-encoded tuple (decoded number of bytes, character in UTF-16 encoding, stored as short type)
     */
    public static int utf8CharDecode(Utf8Sequence seq, int offset) {
        int size = seq.size() - offset;
        if (size > 0) {
            byte b1 = seq.byteAt(offset);
            if (b1 < 0) {
                if (b1 >> 5 == -2 && (b1 & 30) != 0 && size > 1) {
                    byte b2 = seq.byteAt(offset + 1);
                    if (isNotContinuation(b2)) {
                        return 0;
                    }
                    return Numbers.encodeLowHighShorts((short) 2, (short) (b1 << 6 ^ b2 ^ 3968));
                }

                if (b1 >> 4 == -2 && size > 2) {
                    byte b2 = seq.byteAt(offset + 1);
                    byte b3 = seq.byteAt(offset + 2);
                    if (isMalformed3(b1, b2, b3)) {
                        return 0;
                    }

                    final char c = utf8ToChar(b1, b2, b3);
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

    public static int utf8DecodeMultiByte(long lo, long hi, byte b, Utf16Sink sink) {
        if (b >> 5 == -2 && (b & 30) != 0) {
            return utf8Decode2Bytes(lo, hi, b, sink);
        }
        if (b >> 4 == -2) {
            return utf8Decode3Bytes(lo, hi, b, sink);
        }
        return utf8Decode4Bytes(lo, hi, b, sink);
    }

    public static char utf8ToChar(byte b1, byte b2, byte b3) {
        return (char) (b1 << 12 ^ b2 << 6 ^ b3 ^ -123008);
    }

    /**
     * Decodes bytes between lo,hi addresses into sink.
     * Note: operation might fail in the middle and leave sink in inconsistent state.
     *
     * @return true if input is proper UTF-8 and false otherwise.
     */
    public static boolean utf8ToUtf16(long lo, long hi, @NotNull Utf16Sink sink) {
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
     * @param seq   input sequence
     * @param seqLo character bytes start in input sequence
     * @param seqHi character bytes end in input sequence (exclusive)
     * @param sink  destination sink
     * @return true if input is proper UTF-8 and false otherwise.
     */
    public static boolean utf8ToUtf16(@NotNull Utf8Sequence seq, int seqLo, int seqHi, @NotNull Utf16Sink sink) {
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
    public static boolean utf8ToUtf16(@NotNull Utf8Sequence seq, @NotNull Utf16Sink sink) {
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
    public static int utf8ToUtf16(@NotNull Utf8Sequence seq, @NotNull Utf16Sink sink, byte terminator) {
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
    public static boolean utf8ToUtf16EscConsecutiveQuotes(long lo, long hi, @NotNull Utf16Sink sink) {
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

    public static void utf8ToUtf16Unchecked(@NotNull DirectUtf8Sequence utf8CharSeq, @NotNull MutableUtf16Sink tempSink) {
        tempSink.clear();
        if (!utf8ToUtf16(utf8CharSeq.lo(), utf8CharSeq.hi(), tempSink)) {
            throw CairoException.nonCritical().put("invalid UTF8 in value for ").put(utf8CharSeq);
        }
    }

    public static boolean utf8ToUtf16Z(long lo, Utf16Sink sink) {
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

    /**
     * Copies UTF8 null-terminated string into UTF8 sink excluding zero byte.
     *
     * @param addr pointer at the beginning of UTF8 null-terminated string
     * @param sink copy target
     */
    public static void utf8ZCopy(long addr, Utf8Sink sink) {
        long p = addr;
        while (true) {
            byte b = Unsafe.getUnsafe().getByte(p++);
            if (b == 0) {
                break;
            }
            sink.putAny(b);
        }
    }

    public static int validateUtf8(@NotNull Utf8Sequence seq) {
        if (seq.isAscii()) {
            return seq.size();
        }
        int len = 0;
        for (int i = 0, hi = seq.size(); i < hi; ) {
            byte b = seq.byteAt(i);
            if (b < 0) {
                int n = validateUtf8MultiByte(seq, i, b);
                if (n == -1) {
                    // UTF-8 error
                    return -1;
                }
                i += n;
            } else {
                ++i;
            }
            ++len;
        }
        return len;
    }

    public static int validateUtf8MultiByte(long lo, long hi, byte b) {
        if (b >> 5 == -2 && (b & 30) != 0) {
            return validateUtf8Decode2Bytes(lo, hi);
        }
        if (b >> 4 == -2) {
            return validateUtf8Decode3Bytes(lo, hi, b);
        }
        return validateUtf8Decode4Bytes(lo, hi, b);
    }

    /**
     * Returns up to 6 initial bytes of the given UTF-8 sequence (less if it's shorter)
     * packed into a zero-padded long value, in little-endian order. This prefix is
     * stored inline in the auxiliary vector of a VARCHAR column, so asking for it is a
     * matter of optimized data access. This is not a general access method, it
     * shouldn't be called unless looking to optimize the access of the VARCHAR column.
     *
     * @param seq UTF8 sequence
     * @return up to 6 initial bytes
     */
    public static long zeroPaddedSixPrefix(@NotNull Utf8Sequence seq) {
        final int size = seq.size();
        if (size >= Long.BYTES) {
            return seq.longAt(0) & VARCHAR_INLINED_PREFIX_MASK;
        }
        final long limit = Math.min(size, VARCHAR_INLINED_PREFIX_BYTES);
        long result = 0;
        for (int i = 0; i < limit; i++) {
            result |= (seq.byteAt(i) & 0xffL) << (8 * i);
        }
        return result;
    }

    private static boolean dataEquals(@NotNull Utf8Sequence l, @NotNull Utf8Sequence r, int start, int limit) {
        int i = start;
        for (; i <= limit - Long.BYTES; i += Long.BYTES) {
            if (l.longAt(i) != r.longAt(i)) {
                return false;
            }
        }
        for (; i < limit; i++) {
            if (l.byteAt(i) != r.byteAt(i)) {
                return false;
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

    private static boolean equalPrefixBytes(
            @NotNull Utf8Sequence l, long lSixPrefix, @NotNull Utf8Sequence r, long rSixPrefix, int prefixSize
    ) {
        long prefixMask = (1L << 8 * Math.min(VARCHAR_INLINED_PREFIX_BYTES, prefixSize)) - 1;
        return ((lSixPrefix ^ rSixPrefix) & prefixMask) == 0
                && dataEquals(l, r, VARCHAR_INLINED_PREFIX_BYTES, prefixSize);
    }

    private static boolean equalSuffixBytes(
            @NotNull Utf8Sequence seq, @NotNull Utf8Sequence suffix, int seqSize, int suffixSize
    ) {
        int seqLo = seqSize - suffixSize;
        int i = 0;
        for (; i <= suffixSize - Long.BYTES; i += Long.BYTES) {
            if (suffix.longAt(i) != seq.longAt(i + seqLo)) {
                return false;
            }
        }
        for (; i < suffixSize; i++) {
            if (suffix.byteAt(i) != seq.byteAt(i + seqLo)) {
                return false;
            }
        }
        return true;
    }

    // Left hand has to be lower-case.
    private static boolean equalsAsciiLowerCase(@NotNull Utf8Sequence lLC, @NotNull Utf8Sequence r, int size) {
        for (int i = 0; i < size; i++) {
            if (lLC.byteAt(i) != toLowerCaseAscii(r.byteAt(i))) {
                return false;
            }
        }
        return true;
    }

    // Left hand has to be lower-case.
    private static boolean equalsAsciiLowerCase(@NotNull Utf8Sequence lLC, @NotNull Utf8Sequence r, int rLo, int rHi) {
        int ls = lLC.size();
        if (ls != rHi - rLo) {
            return false;
        }

        for (int i = 0; i < ls; i++) {
            if (lLC.byteAt(i) != toLowerCaseAscii(r.byteAt(i + rLo))) {
                return false;
            }
        }
        return true;
    }

    private static StringSink getThreadLocalSink() {
        StringSink b = tlSink.get();
        b.clear();
        return b;
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

    private static int strCpyNonAscii(@NotNull Utf8Sequence seq, int charLo, int charHi, @NotNull Utf8Sink sink) {
        int charPos = 0;
        int bytesCopied = 0;
        for (int i = 0, hi = seq.size(); i < hi && charPos < charHi; charPos++) {
            byte b = seq.byteAt(i);
            if (b < 0) {
                int n = validateUtf8MultiByte(seq, i, b);
                if (n == -1) {
                    // UTF-8 error
                    return -1;
                }
                if (charPos >= charLo) {
                    sink.put(b);
                    for (int j = 1; j < n; j++) {
                        sink.put(seq.byteAt(i + j));
                    }
                    bytesCopied += n;
                }
                i += n;
            } else {
                if (charPos >= charLo) {
                    sink.putAscii((char) b);
                    bytesCopied++;
                }
                i++;
            }
        }
        return bytesCopied;
    }

    private static byte toLowerCaseAscii(byte b) {
        return b > 64 && b < 91 ? (byte) (b + 32) : b;
    }

    private static int utf16Equals(CharSequence c, int ci, int cn, Utf8Sequence u, int ui, int un) {
        byte b = u.byteAt(ui);
        if ((b & 0x80) == 0x00) {
            return c.charAt(ci) == b ? 1 : -1;
        } else if ((b & 0xE0) == 0xC0) {
            return utf16Equals2Bytes(c, ci, cn, b, u, ui + 1, un);
        } else if ((b & 0xF0) == 0xE0) {
            return utf16Equals3Bytes(c, ci, cn, b, u, ui + 1, un);
        }
        return utf16Equals4Bytes(c, ci, cn, b, u, ui + 1, un);
    }

    private static int utf16Equals2Bytes(CharSequence c, int ci, int cn, byte b1, Utf8Sequence u, int ui, int un) {
        if (ui < un && ci < cn) {
            byte b2 = u.byteAt(ui);
            char c1 = (char) (b1 << 6 ^ b2 ^ 3968);
            return c.charAt(ci) == c1 ? 2 : -1;
        }
        return -1;
    }

    private static int utf16Equals3Bytes(CharSequence c, int ci, int cn, byte b1, Utf8Sequence u, int ui, int un) {
        if (ui + 1 < un && ci < cn) {
            byte b2 = u.byteAt(ui++);
            byte b3 = u.byteAt(ui);
            char c1 = utf8ToChar(b1, b2, b3);
            return c.charAt(ci) == c1 ? 3 : -1;
        }
        return -1;
    }

    private static int utf16Equals4Bytes(CharSequence c, int ci, int cn, byte b1, Utf8Sequence u, int ui, int un) {
        if (ui + 2 < un && ci + 1 < cn) {
            byte b2 = u.byteAt(ui++);
            byte b3 = u.byteAt(ui++);
            byte b4 = u.byteAt(ui);
            if (isMalformed4(b2, b3, b4)) {
                return -1;
            }
            final int codePoint = getUtf8Codepoint(b1, b2, b3, b4);
            char c1 = c.charAt(ci++);
            char c2 = c.charAt(ci);

            if (Character.isSupplementaryCodePoint(codePoint)) {
                return c1 == Character.highSurrogate(codePoint) && c2 == Character.lowSurrogate(codePoint) ? 4 : -1;
            }
        }
        return -1;
    }

    private static int utf8Decode2Bytes(@NotNull Utf8Sequence seq, int index, int b1, @NotNull Utf16Sink sink) {
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

    private static int utf8Decode2Bytes(long lo, long hi, int b1, @NotNull Utf16Sink sink) {
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

    private static int utf8Decode2BytesZ(long lo, int b1, @NotNull Utf16Sink sink) {
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

    private static int utf8Decode3Byte0(byte b1, @NotNull Utf16Sink sink, byte b2, byte b3) {
        if (isMalformed3(b1, b2, b3)) {
            return -1;
        }
        char c = utf8ToChar(b1, b2, b3);
        if (Character.isSurrogate(c)) {
            return -1;
        }
        sink.put(c);
        return 3;
    }

    private static int utf8Decode3Bytes(long lo, long hi, byte b1, @NotNull Utf16Sink sink) {
        if (hi - lo < 3) {
            return -1;
        }
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
        return utf8Decode3Byte0(b1, sink, b2, b3);
    }

    private static int utf8Decode3Bytes(@NotNull Utf8Sequence seq, int index, byte b1, @NotNull Utf16Sink sink) {
        if (seq.size() - index < 3) {
            return -1;
        }
        byte b2 = seq.byteAt(index + 1);
        byte b3 = seq.byteAt(index + 2);
        return utf8Decode3Byte0(b1, sink, b2, b3);
    }

    private static int utf8Decode3BytesZ(long lo, byte b1, @NotNull Utf16Sink sink) {
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

    private static int utf8Decode4Bytes(long lo, long hi, int b, @NotNull Utf16Sink sink) {
        if (b >> 3 != -2 || hi - lo < 4) {
            return -1;
        }
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
        byte b4 = Unsafe.getUnsafe().getByte(lo + 3);
        return utf8Decode4Bytes0(b, sink, b2, b3, b4);
    }

    private static int utf8Decode4Bytes(@NotNull Utf8Sequence seq, int index, int b, @NotNull Utf16Sink sink) {
        if (b >> 3 != -2 || seq.size() - index < 4) {
            return -1;
        }
        byte b2 = seq.byteAt(index + 1);
        byte b3 = seq.byteAt(index + 2);
        byte b4 = seq.byteAt(index + 3);
        return utf8Decode4Bytes0(b, sink, b2, b3, b4);
    }

    private static int utf8Decode4Bytes0(int b, @NotNull Utf16Sink sink, byte b2, byte b3, byte b4) {
        if (isMalformed4(b2, b3, b4)) {
            return -1;
        }
        final int codePoint = getUtf8Codepoint(b, b2, b3, b4);
        if (Character.isSupplementaryCodePoint(codePoint)) {
            sink.put(Character.highSurrogate(codePoint));
            sink.put(Character.lowSurrogate(codePoint));
            return 4;
        }
        return -1;
    }

    private static int utf8Decode4BytesZ(long lo, int b, Utf16Sink sink) {
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

    private static int utf8DecodeMultiByte(Utf8Sequence seq, int index, byte b, @NotNull Utf16Sink sink) {
        if (b >> 5 == -2 && (b & 30) != 0) {
            // we should allow 11000001, as it is a valid UTF8 byte?
            return utf8Decode2Bytes(seq, index, b, sink);
        }
        if (b >> 4 == -2) {
            return utf8Decode3Bytes(seq, index, b, sink);
        }
        return utf8Decode4Bytes(seq, index, b, sink);
    }

    private static int utf8DecodeMultiByteZ(long lo, byte b, @NotNull Utf16Sink sink) {
        if (b >> 5 == -2 && (b & 30) != 0) {
            return utf8Decode2BytesZ(lo, b, sink);
        }
        if (b >> 4 == -2) {
            return utf8Decode3BytesZ(lo, b, sink);
        }
        return utf8Decode4BytesZ(lo, b, sink);
    }

    private static int validateUtf8Decode2Bytes(@NotNull Utf8Sequence seq, int index) {
        if (seq.size() - index < 2) {
            return -1;
        }
        byte b2 = seq.byteAt(index + 1);
        if (isNotContinuation(b2)) {
            return -1;
        }
        return 2;
    }

    private static int validateUtf8Decode2Bytes(long lo, long hi) {
        if (hi - lo < 2) {
            return -1;
        }
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        if (isNotContinuation(b2)) {
            return -1;
        }
        return 2;
    }

    private static int validateUtf8Decode3Bytes(long lo, long hi, byte b1) {
        if (hi - lo < 3) {
            return -1;
        }

        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);

        if (isMalformed3(b1, b2, b3)) {
            return -1;
        }

        char c = utf8ToChar(b1, b2, b3);
        if (Character.isSurrogate(c)) {
            return -1;
        }
        return 3;
    }

    private static int validateUtf8Decode3Bytes(@NotNull Utf8Sequence seq, int index, byte b1) {
        if (seq.size() - index < 3) {
            return -1;
        }
        byte b2 = seq.byteAt(index + 1);
        byte b3 = seq.byteAt(index + 2);

        if (isMalformed3(b1, b2, b3)) {
            return -1;
        }

        char c = utf8ToChar(b1, b2, b3);
        if (Character.isSurrogate(c)) {
            return -1;
        }
        return 3;
    }

    private static int validateUtf8Decode4Bytes(long lo, long hi, int b) {
        if (b >> 3 != -2 || hi - lo < 4) {
            return -1;
        }
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
        byte b4 = Unsafe.getUnsafe().getByte(lo + 3);

        if (isMalformed4(b2, b3, b4)) {
            return -1;
        }
        final int codePoint = getUtf8Codepoint(b, b2, b3, b4);
        if (!Character.isSupplementaryCodePoint(codePoint)) {
            return -1;
        }
        return 4;
    }

    private static int validateUtf8Decode4Bytes(@NotNull Utf8Sequence seq, int index, int b) {
        if (b >> 3 != -2 || seq.size() - index < 4) {
            return -1;
        }
        byte b2 = seq.byteAt(index + 1);
        byte b3 = seq.byteAt(index + 2);
        byte b4 = seq.byteAt(index + 3);

        if (isMalformed4(b2, b3, b4)) {
            return -1;
        }
        final int codePoint = getUtf8Codepoint(b, b2, b3, b4);
        if (!Character.isSupplementaryCodePoint(codePoint)) {
            return -1;
        }
        return 4;
    }

    private static int validateUtf8MultiByte(Utf8Sequence seq, int index, byte b) {
        if (b >> 5 == -2 && (b & 30) != 0) {
            // we should allow 11000001, as it is a valid UTF8 byte?
            return validateUtf8Decode2Bytes(seq, index);
        }
        if (b >> 4 == -2) {
            return validateUtf8Decode3Bytes(seq, index, b);
        }
        return validateUtf8Decode4Bytes(seq, index, b);
    }
}
