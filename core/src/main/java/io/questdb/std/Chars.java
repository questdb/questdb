/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.std;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.str.TrimType;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.questdb.std.Numbers.hexDigits;

public final class Chars {
    static final String[] CHAR_STRINGS;
    static final char[] base64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray();
    // inverted alphabets for base64 decoding could be just byte arrays. this would save 3 * 128 bytes per alphabet
    // but benchmarks show that int arrays make decoding faster.
    // it's probably because it does not require any type conversions in the hot decoding loop
    static final int[] base64Inverted = base64CreateInvertedAlphabet(base64);
    static final char[] base64Url = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".toCharArray();
    static final int[] base64UrlInverted = base64CreateInvertedAlphabet(base64Url);

    private Chars() {
    }

    /**
     * Avoid this when possible due to the allocation of a new char array
     * This should be only used when a JDK method forces you to use a byte array
     * <p>
     * It's responsibility of the caller to ensure that the input string is ASCII
     *
     * @param ascii ascii string to convert to byte array
     * @return byte array representation of the input string
     */
    public static byte[] asciiToByteArray(@NotNull CharSequence ascii) {
        byte[] dst = new byte[ascii.length()];
        for (int i = 0, n = ascii.length(); i < n; i++) {
            assert ascii.charAt(i) < 128;
            dst[i] = (byte) ascii.charAt(i);
        }
        return dst;
    }

    public static void base64Decode(CharSequence encoded, @NotNull Utf8Sink target) {
        base64Decode(encoded, target, base64Inverted);
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
    public static void base64Decode(CharSequence encoded, @NotNull ByteBuffer target) {
        base64Decode(encoded, target, base64Inverted);
    }

    public static void base64Encode(@Nullable BinarySequence sequence, int maxLength, @NotNull CharSink<?> buffer) {
        int pad = base64Encode(sequence, maxLength, buffer, base64);
        for (int j = 0; j < pad; j++) {
            buffer.putAscii("=");
        }
    }

    public static void base64UrlDecode(@Nullable CharSequence encoded, @NotNull Utf8Sink target) {
        base64Decode(encoded, target, base64UrlInverted);
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

    public static void base64UrlEncode(BinarySequence sequence, int maxLength, CharSink<?> buffer) {
        base64Encode(sequence, maxLength, buffer, base64Url);
        // base64 url does not use padding
    }

    public static int charBytes(char c) {
        if (c < 0x80) {
            return 1;
        } else if (c < 0x800) {
            return 2;
        } else if (Character.isSurrogate(c)) {
            return 1; // replaced with '?'
        } else {
            return 3;
        }
    }

    public static int compare(CharSequence l, CharSequence r) {
        if (l == r) {
            return 0;
        }

        if (l == null) {
            return -1;
        }

        if (r == null) {
            return 1;
        }

        final int ll = l.length();
        final int rl = r.length();
        final int min = Math.min(ll, rl);

        for (int i = 0; i < min; i++) {
            final int k = l.charAt(i) - r.charAt(i);
            if (k != 0) {
                return k;
            }
        }
        return Integer.compare(ll, rl);
    }

    public static int compareDescending(CharSequence l, CharSequence r) {
        return compare(r, l);
    }

    public static boolean contains(@NotNull CharSequence sequence, @NotNull CharSequence term) {
        return indexOf(sequence, 0, sequence.length(), term) != -1;
    }

    // Term has to be lower-case.
    public static boolean containsLowerCase(@NotNull CharSequence sequence, @NotNull CharSequence termLC) {
        return indexOfLowerCase(sequence, 0, sequence.length(), termLC) != -1;
    }

    public static boolean containsWordIgnoreCase(CharSequence seq, CharSequence term, char separator) {
        if (Chars.isBlank(seq)) {
            return false;
        }
        if (Chars.isBlank(term)) {
            return false;
        }

        int seqLen = seq.length();
        int i = Chars.indexOfIgnoreCase(seq, 0, seqLen, term);

        if (i < 0) {
            return false;
        }

        if (i > 0) {
            if (seq.charAt(i - 1) != separator) {
                return false;
            }
        }

        int termLen = term.length();
        if (i + termLen < seqLen) {
            return seq.charAt(i + termLen) == separator;
        }
        return true;
    }

    public static void copyStrChars(CharSequence value, int pos, int len, long address) {
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i + pos);
            Unsafe.getUnsafe().putChar(address + 2L * i, c);
        }
    }

    public static boolean empty(@Nullable CharSequence value) {
        return value == null || value.length() < 1;
    }

    public static boolean endsWith(CharSequence cs, CharSequence ends) {
        if (ends == null || cs == null) {
            return false;
        }

        int l = ends.length();
        if (l == 0) {
            return true;
        }

        int csl = cs.length();
        return !(csl == 0 || csl < l) && equals(ends, cs, csl - l, csl);
    }

    public static boolean endsWith(@Nullable CharSequence cs, char c) {
        if (cs == null) {
            return false;
        }
        final int csl = cs.length();
        return csl != 0 && c == cs.charAt(csl - 1);
    }

    // Pattern has to be in lower-case.
    public static boolean endsWithLowerCase(@Nullable CharSequence cs, @Nullable CharSequence endsLC) {
        if (endsLC == null || cs == null) {
            return false;
        }

        int l = endsLC.length();
        if (l == 0) {
            return true;
        }

        int csl = cs.length();
        return !(csl == 0 || csl < l) && equalsLowerCase(endsLC, cs, csl - l, csl);
    }

    public static boolean equals(@NotNull CharSequence l, @NotNull CharSequence r) {
        if (l == r) {
            return true;
        }

        int ll;
        if ((ll = l.length()) != r.length()) {
            return false;
        }

        return equalsChars(l, r, ll);
    }

    public static boolean equals(@NotNull String l, @NotNull String r) {
        return l.equals(r);
    }

    public static boolean equals(@NotNull CharSequence l, @NotNull CharSequence r, int rLo, int rHi) {
        if (l == r) {
            return true;
        }

        int ll = l.length();
        if (ll != rHi - rLo) {
            return false;
        }

        for (int i = 0; i < ll; i++) {
            if (l.charAt(i) != r.charAt(i + rLo)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(@NotNull CharSequence l, int lLo, int lHi, @NotNull CharSequence r, int rLo, int rHi) {
        if (l == r) {
            return true;
        }

        int ll = lHi - lLo;
        if (ll != rHi - rLo) {
            return false;
        }

        for (int i = 0; i < ll; i++) {
            if (l.charAt(i + lLo) != r.charAt(i + rLo)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(@NotNull CharSequence l, char r) {
        return l.length() == 1 && l.charAt(0) == r;
    }

    /**
     * Case-insensitive comparison of two char sequences.
     *
     * @param l left sequence
     * @param r right sequence
     * @return true if sequences match exactly (ignoring char case)
     */
    public static boolean equalsIgnoreCase(@NotNull CharSequence l, @NotNull CharSequence r) {
        if (l == r) {
            return true;
        }

        int ll = l.length();
        if (ll != r.length()) {
            return false;
        }

        return equalsCharsIgnoreCase(l, r, ll);
    }

    /**
     * Case-insensitive comparison of two char sequences, with subsequence over second.
     *
     * @param l   left sequence
     * @param r   right sequence
     * @param rLo right sequence lower bound
     * @param rHi right sequence upper bound
     * @return true if sequences match exactly (ignoring char case)
     */
    public static boolean equalsIgnoreCase(@NotNull CharSequence l, @NotNull CharSequence r, int rLo, int rHi) {
        if (l == r) {
            return true;
        }

        int ll = l.length();
        if (ll != rHi - rLo) {
            return false;
        }

        return equalsCharsIgnoreCase(l, r, ll, rLo, rHi);
    }

    public static boolean equalsIgnoreCaseNc(@NotNull CharSequence l, @Nullable CharSequence r) {
        return r != null && equalsIgnoreCase(l, r);
    }

    // Left side has to be lower-case.
    public static boolean equalsLowerCase(@NotNull CharSequence lLC, @NotNull CharSequence r, int rLo, int rHi) {
        if (lLC == r) {
            return true;
        }

        int ll = lLC.length();
        if (ll != rHi - rLo) {
            return false;
        }

        for (int i = 0; i < ll; i++) {
            if (lLC.charAt(i) != Character.toLowerCase(r.charAt(i + rLo))) {
                return false;
            }
        }
        return true;
    }

    public static boolean equalsLowerCase(@NotNull CharSequence l, int lLo, int lHi, @NotNull CharSequence r, int rLo, int rHi) {
        if (l == r) {
            return true;
        }

        int ll = lHi - lLo;
        if (ll != rHi - rLo) {
            return false;
        }

        for (int i = 0; i < ll; i++) {
            if (Character.toLowerCase(l.charAt(i + lLo)) != Character.toLowerCase(r.charAt(i + rLo))) {
                return false;
            }
        }
        return true;
    }

    // Left side has to be lower-case.
    public static boolean equalsLowerCaseAscii(@NotNull CharSequence lLC, @NotNull CharSequence r, int rLo, int rHi) {
        if (lLC == r) {
            return true;
        }

        int ll = lLC.length();
        if (ll != rHi - rLo) {
            return false;
        }

        for (int i = 0; i < ll; i++) {
            if (lLC.charAt(i) != toLowerCaseAscii(r.charAt(i + rLo))) {
                return false;
            }
        }
        return true;
    }

    public static boolean equalsLowerCaseAscii(@NotNull CharSequence l, int lLo, int lHi, @NotNull CharSequence r, int rLo, int rHi) {
        if (l == r) {
            return true;
        }

        int ll = lHi - lLo;
        if (ll != rHi - rLo) {
            return false;
        }

        for (int i = 0; i < ll; i++) {
            if (toLowerCaseAscii(l.charAt(i + lLo)) != toLowerCaseAscii(r.charAt(i + rLo))) {
                return false;
            }
        }
        return true;
    }

    public static boolean equalsLowerCaseAscii(@NotNull CharSequence l, @NotNull CharSequence r) {
        int ll = l.length();
        if (ll != r.length()) {
            return false;
        }

        for (int i = 0; i < ll; i++) {
            if (toLowerCaseAscii(l.charAt(i)) != toLowerCaseAscii(r.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    public static boolean equalsLowerCaseAsciiNc(@NotNull CharSequence l, @Nullable CharSequence r) {
        return r != null && equalsLowerCaseAscii(l, r);
    }

    public static boolean equalsNc(@Nullable CharSequence l, char r) {
        return (l == null && r == CharConstant.ZERO.getChar(null)) || (l != null && equals(l, r));
    }

    public static boolean equalsNc(@NotNull CharSequence l, @Nullable CharSequence r) {
        return r != null && equals(l, r);
    }

    public static boolean equalsNc(CharSequence l, CharSequence r, int rLo, int rHi) {
        return l != null && equals(l, r, rLo, rHi);
    }

    public static boolean equalsNullable(@Nullable CharSequence l, @Nullable CharSequence r) {
        if (l == null && r == null) {
            return true;
        }

        if (l == null || r == null) {
            return false;
        }

        int ll;
        if ((ll = l.length()) != r.length()) {
            return false;
        }

        return equalsChars(l, r, ll);
    }

    /**
     * Strictly greater than (&gt;) comparison of two UTF16 sequences in lexicographical
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
    public static boolean greaterThan(@Nullable CharSequence l, @Nullable CharSequence r) {
        if (l == null || r == null) {
            return false;
        }
        final int ll = l.length();
        final int rl = r.length();
        final int min = Math.min(ll, rl);

        for (int i = 0; i < min; i++) {
            final int k = l.charAt(i) - r.charAt(i);
            if (k != 0) {
                return k > 0;
            }
        }
        return ll > rl;
    }

    public static int hashCode(@NotNull CharSequence value, int lo, int hi) {
        if (hi == lo) {
            return 0;
        }

        int h = 0;
        for (int p = lo; p < hi; p++) {
            h = 31 * h + value.charAt(p);
        }
        return h;
    }

    public static int hashCode(char @NotNull [] value, int lo, int hi) {
        if (hi == lo) {
            return 0;
        }

        int h = 0;
        for (int p = lo; p < hi; p++) {
            h = 31 * h + value[p];
        }
        return h;
    }

    public static int hashCode(@NotNull CharSequence value) {
        if (value instanceof String) {
            return value.hashCode();
        }

        int len = value.length();
        if (len == 0) {
            return 0;
        }

        int h = 0;
        for (int p = 0; p < len; p++) {
            h = 31 * h + value.charAt(p);
        }
        return h;
    }

    public static int hashCode(@NotNull String value) {
        return value.hashCode();
    }

    /**
     * Searches for the first occurrence of a character sequence within the specified bounds of another character sequence.
     * This method performs a case-sensitive search using an optimized string matching algorithm.
     * <p>
     * The search bounds are defined as {@code [seqLo, seqHi)} where {@code seqLo} is inclusive and
     * {@code seqHi} is exclusive. The method searches for the complete term within these bounds.
     * <p>
     * If the term is empty (length 0), the method returns 0 as an empty string is considered to be
     * found at the beginning of any sequence.
     *
     * @param seq   the character sequence to search within (must not be null)
     * @param seqLo the lower bound (inclusive) of the search range
     * @param seqHi the upper bound (exclusive) of the search range
     * @param term  the character sequence to search for (must not be null)
     * @return the index of the first occurrence of the term, or -1 if not found within bounds
     * @throws StringIndexOutOfBoundsException if bounds are invalid for the given sequence
     */
    public static int indexOf(@NotNull CharSequence seq, int seqLo, int seqHi, @NotNull CharSequence term) {
        int termLen = term.length();
        if (termLen == 0) {
            return 0;
        }

        char first = term.charAt(0);
        int max = seqHi - termLen;

        for (int i = seqLo; i <= max; ++i) {
            if (seq.charAt(i) != first) {
                do {
                    ++i;
                } while (i <= max && seq.charAt(i) != first);
            }

            if (i <= max) {
                int j = i + 1;
                int end = j + termLen - 1;

                for (int k = 1; j < end && seq.charAt(j) == term.charAt(k); ++k) {
                    ++j;
                }

                if (j == end) {
                    return i;
                }
            }
        }

        return -1;
    }

    /**
     * Searches for the nth occurrence of a character sequence within the specified bounds of another character sequence.
     * This method supports both forward and reverse searching based on the occurrence parameter.
     * <p>
     * The search bounds are defined as {@code [seqLo, seqHi)} where {@code seqLo} is inclusive and
     * {@code seqHi} is exclusive.
     * <p>
     * <strong>Occurrence Parameter:</strong>
     * <ul>
     * <li>Positive values (1, 2, 3, ...): search forward for the 1st, 2nd, 3rd, ... occurrence</li>
     * <li>Negative values (-1, -2, -3, ...): search backward for the 1st, 2nd, 3rd, ... occurrence from the end</li>
     * <li>Zero (0): returns -1 immediately without searching</li>
     * </ul>
     * <p>
     * If the term is empty (length 0), the method returns 0 as an empty string is considered to be
     * found at the beginning of any sequence.
     *
     * @param seq        the character sequence to search within (must not be null)
     * @param seqLo      the lower bound (inclusive) of the search range
     * @param seqHi      the upper bound (exclusive) of the search range
     * @param term       the character sequence to search for (must not be null)
     * @param occurrence the occurrence number to find (positive for forward, negative for reverse, 0 returns -1)
     * @return the index of the specified occurrence of the term, or -1 if not found within bounds
     * @throws StringIndexOutOfBoundsException if bounds are invalid for the given sequence
     */
    public static int indexOf(@NotNull CharSequence seq, int seqLo, int seqHi, @NotNull CharSequence term, int occurrence) {
        int m = term.length();
        if (m == 0) {
            return 0;
        }

        if (occurrence == 0) {
            return -1;
        }

        int foundIndex = -1;
        int count = 0;
        if (occurrence > 0) {
            for (int i = seqLo; i < seqHi; i++) {
                if (foundIndex == -1) {
                    if (seqHi - i < m) {
                        return -1;
                    }
                    if (seq.charAt(i) == term.charAt(0)) {
                        foundIndex = i;
                    }
                } else { // first character matched, try to match the rest of the term
                    if (seq.charAt(i) != term.charAt(i - foundIndex)) {
                        // start again from after where the first character was found
                        i = foundIndex;
                        foundIndex = -1;
                    }
                }

                if (foundIndex != -1 && i - foundIndex == m - 1) {
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
                    if (i - seqLo + 1 < m) {
                        return -1;
                    }
                    if (seq.charAt(i) == term.charAt(m - 1)) {
                        foundIndex = i;
                    }
                } else { // last character matched, try to match the rest of the term
                    if (seq.charAt(i) != term.charAt(m - 1 + i - foundIndex)) {
                        // start again from after where the first character was found
                        i = foundIndex;
                        foundIndex = -1;
                    }
                }

                if (foundIndex != -1 && foundIndex - i == m - 1) {
                    count--;
                    if (count == occurrence) {
                        return foundIndex + 1 - m;
                    } else {
                        foundIndex = -1;
                    }
                }
            }
        }

        return -1;
    }

    /**
     * Searches for the first occurrence of a character in the entire character sequence.
     * This is a convenience method that searches from the beginning of the sequence.
     *
     * @param seq the character sequence to search within
     * @param c   the character to search for
     * @return the index of the first occurrence of the character, or -1 if not found
     */
    public static int indexOf(CharSequence seq, char c) {
        return indexOf(seq, 0, c);
    }

    /**
     * Searches for the first occurrence of a character starting from a specified position.
     * This is a convenience method that searches from the given starting position to the end of the sequence.
     *
     * @param seq   the character sequence to search within
     * @param seqLo the starting position (inclusive) to begin the search
     * @param c     the character to search for
     * @return the index of the first occurrence of the character, or -1 if not found
     */
    public static int indexOf(CharSequence seq, final int seqLo, char c) {
        return indexOf(seq, seqLo, seq.length(), c);
    }

    /**
     * Searches for the first occurrence of a character within the specified bounds of a character sequence.
     * This is a convenience method that searches for the 1st occurrence of the character.
     *
     * @param seq   the character sequence to search within
     * @param seqLo the lower bound (inclusive) of the search range
     * @param seqHi the upper bound (exclusive) of the search range
     * @param c     the character to search for
     * @return the index of the first occurrence of the character, or -1 if not found within bounds
     */
    public static int indexOf(CharSequence seq, int seqLo, int seqHi, char c) {
        return indexOf(seq, seqLo, seqHi, c, 1);
    }

    /**
     * Searches for the nth occurrence of a character within the specified bounds of a character sequence.
     * This method supports both forward and reverse searching based on the occurrence parameter.
     * <p>
     * The search bounds are defined as {@code [seqLo, seqHi)} where {@code seqLo} is inclusive and
     * {@code seqHi} is exclusive.
     * <p>
     * <strong>Occurrence Parameter:</strong>
     * <ul>
     * <li>Positive values (1, 2, 3, ...): search forward for the 1st, 2nd, 3rd, ... occurrence</li>
     * <li>Negative values (-1, -2, -3, ...): search backward for the 1st, 2nd, 3rd, ... occurrence from the end</li>
     * <li>Zero (0): returns -1 immediately without searching</li>
     * </ul>
     *
     * @param seq        the character sequence to search within
     * @param seqLo      the lower bound (inclusive) of the search range
     * @param seqHi      the upper bound (exclusive) of the search range
     * @param ch         the character to search for
     * @param occurrence the occurrence number to find (positive for forward, negative for reverse, 0 returns -1)
     * @return the index of the specified occurrence of the character, or -1 if not found within bounds
     * @throws StringIndexOutOfBoundsException if bounds are invalid for the given sequence
     */
    public static int indexOf(CharSequence seq, int seqLo, int seqHi, char ch, int occurrence) {
        if (occurrence == 0) {
            return -1;
        }

        int count = 0;
        if (occurrence > 0) {
            for (int i = seqLo; i < seqHi; i++) {
                if (seq.charAt(i) == ch) {
                    count++;
                    if (count == occurrence) {
                        return i;
                    }
                }
            }
        } else {    // if occurrence is negative, search in reverse
            for (int i = seqHi - 1; i >= seqLo; i--) {
                if (seq.charAt(i) == ch) {
                    count--;
                    if (count == occurrence) {
                        return i;
                    }
                }
            }
        }

        return -1;
    }

    /**
     * Searches for the first occurrence of a character sequence within the specified bounds of another character sequence,
     * ignoring case differences. This method performs a case-insensitive search using {@link Character#toLowerCase(char)}
     * for character comparison.
     * <p>
     * The search bounds are defined as {@code [seqLo, seqHi)} where {@code seqLo} is inclusive and
     * {@code seqHi} is exclusive. The method searches for the complete term within these bounds.
     * <p>
     * If the term is empty (length 0), the method returns 0 as an empty string is considered to be
     * found at the beginning of any sequence.
     *
     * @param seq   the character sequence to search within (must not be null)
     * @param seqLo the lower bound (inclusive) of the search range
     * @param seqHi the upper bound (exclusive) of the search range
     * @param term  the character sequence to search for (must not be null)
     * @return the index of the first occurrence of the term (case-insensitive), or -1 if not found within bounds
     * @throws StringIndexOutOfBoundsException if bounds are invalid for the given sequence
     */
    public static int indexOfIgnoreCase(@NotNull CharSequence seq, int seqLo, int seqHi, @NotNull CharSequence term) {
        int termLen = term.length();
        if (termLen == 0) {
            return 0;
        }

        char first = Character.toLowerCase(term.charAt(0));
        int max = seqHi - termLen;

        for (int i = seqLo; i <= max; ++i) {
            if (Character.toLowerCase(seq.charAt(i)) != first) {
                do {
                    ++i;
                } while (i <= max && Character.toLowerCase(seq.charAt(i)) != first);
            }

            if (i <= max) {
                int j = i + 1;
                int end = j + termLen - 1;
                for (int k = 1; j < end && Character.toLowerCase(seq.charAt(j)) == Character.toLowerCase(term.charAt(k)); ++k) {
                    ++j;
                }
                if (j == end) {
                    return i;
                }
            }
        }

        return -1;
    }

    /**
     * Searches for the last occurrence of a character that is not within quoted sections.
     * This method treats double quotes (") as quote delimiters and ignores characters within quoted sections.
     * This is a convenience method that searches the entire sequence.
     *
     * @param seq the character sequence to search within (must not be null)
     * @param ch  the character to search for
     * @return the index of the last unquoted occurrence of the character, or -1 if not found
     */
    public static int indexOfLastUnquoted(@NotNull CharSequence seq, char ch) {
        return indexOfLastUnquoted(seq, ch, 0, seq.length());
    }

    /**
     * Searches for the last occurrence of a character that is not within quoted sections,
     * within the specified bounds of a character sequence.
     * <p>
     * This method treats double quotes (") as quote delimiters and tracks whether the current
     * position is within a quoted section. Characters within quoted sections are ignored.
     * The search bounds are defined as {@code [seqLo, seqHi)} where {@code seqLo} is inclusive and
     * {@code seqHi} is exclusive.
     * <p>
     * The method scans forward through the sequence, toggling quote state when encountering
     * quote characters, and remembers the last position where the target character was found
     * outside of quotes.
     *
     * @param seq   the character sequence to search within (must not be null)
     * @param ch    the character to search for
     * @param seqLo the lower bound (inclusive) of the search range
     * @param seqHi the upper bound (exclusive) of the search range
     * @return the index of the last unquoted occurrence of the character, or -1 if not found
     * @throws StringIndexOutOfBoundsException if bounds are invalid for the given sequence
     */
    public static int indexOfLastUnquoted(@NotNull CharSequence seq, char ch, int seqLo, int seqHi) {
        boolean inQuotes = false;
        int last = -1;
        for (int i = seqLo; i < seqHi; i++) {
            if (seq.charAt(i) == '\"') {
                inQuotes = !inQuotes;
            }
            if (seq.charAt(i) == ch && !inQuotes) {
                last = i;
            }
        }

        return last;
    }

    /**
     * Searches for the first occurrence of a pre-lowercased character sequence within the specified bounds
     * of another character sequence, performing case-insensitive matching.
     * <p>
     * This method is optimized for cases where the search term is already in lowercase.
     * The input sequence is converted to lowercase on-the-fly using {@link Character#toLowerCase(char)}
     * for comparison with the pre-lowercased term.
     * <p>
     * The search bounds are defined as {@code [seqLo, seqHi)} where {@code seqLo} is inclusive and
     * {@code seqHi} is exclusive. The method searches for the complete term within these bounds.
     * <p>
     * If the term is empty (length 0), the method returns 0 as an empty string is considered to be
     * found at the beginning of any sequence.
     *
     * @param seq    the character sequence to search within (must not be null)
     * @param seqLo  the lower bound (inclusive) of the search range
     * @param seqHi  the upper bound (exclusive) of the search range
     * @param termLC the pre-lowercased character sequence to search for (must not be null and must be lowercase)
     * @return the index of the first occurrence of the term (case-insensitive), or -1 if not found within bounds
     * @throws StringIndexOutOfBoundsException if bounds are invalid for the given sequence
     */
    public static int indexOfLowerCase(@NotNull CharSequence seq, int seqLo, int seqHi, @NotNull CharSequence termLC) {
        int termLen = termLC.length();
        if (termLen == 0) {
            return 0;
        }

        char first = termLC.charAt(0);
        int max = seqHi - termLen;

        for (int i = seqLo; i <= max; ++i) {
            if (Character.toLowerCase(seq.charAt(i)) != first) {
                do {
                    ++i;
                } while (i <= max && Character.toLowerCase(seq.charAt(i)) != first);
            }

            if (i <= max) {
                int j = i + 1;
                int end = j + termLen - 1;
                for (int k = 1; j < end && Character.toLowerCase(seq.charAt(j)) == termLC.charAt(k); ++k) {
                    ++j;
                }
                if (j == end) {
                    return i;
                }
            }
        }

        return -1;
    }

    /**
     * Searches for the first non-whitespace character within the specified bounds of a character sequence.
     * This method allows efficient searching without creating substring objects, supporting both forward and
     * reverse search directions.
     * <p>
     * The search uses {@link Character#isWhitespace(char)} to identify whitespace characters. This includes
     * spaces, tabs, newlines, and other Unicode whitespace characters.
     * <p>
     * The search bounds are defined as {@code [seqLo, seqHi)} where {@code seqLo} is inclusive and
     * {@code seqHi} is exclusive. If {@code seqLo >= seqHi}, the method returns -1.
     * <p>
     * <strong>Search Direction:</strong>
     * <ul>
     * <li>Forward (1): searches from {@code seqLo} toward {@code seqHi - 1}</li>
     * <li>Reverse (-1): searches from {@code seqHi - 1} toward {@code seqLo}</li>
     * <li>None (0): returns -1 immediately without searching</li>
     * </ul>
     *
     * @param seq             the character sequence to search (must not be null)
     * @param seqLo           the lower bound (inclusive) of the search range
     * @param seqHi           the upper bound (exclusive) of the search range
     * @param searchDirection 1 for forward search, -1 for reverse search, 0 to skip searching
     * @return the index of the first non-whitespace character found, or -1 if none exists within bounds
     * @throws StringIndexOutOfBoundsException if bounds are invalid for the given sequence
     */
    public static int indexOfNonWhitespace(@NotNull CharSequence seq, int seqLo, int seqHi, int searchDirection) {
        if (searchDirection == 0) {
            return -1;
        }

        if (searchDirection > 0) {
            for (int i = seqLo; i < seqHi; i++) {
                if (!Character.isWhitespace(seq.charAt(i))) {
                    return i;
                }
            }
        } else { // if searchDirection is negative, search in reverse
            for (int i = seqHi - 1; i >= seqLo; i--) {
                if (!Character.isWhitespace(seq.charAt(i))) {
                    return i;
                }
            }
        }
        return -1;
    }

    public static boolean isAscii(@NotNull CharSequence cs) {
        for (int i = 0, n = cs.length(); i < n; i++) {
            if (cs.charAt(i) > 127) {
                return false;
            }
        }
        return true;
    }

    public static boolean isAsciiDigit(char c) {
        return c >= '0' && c <= '9';
    }

    public static boolean isAsciiLetter(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    public static boolean isAsciiWhitespace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f';
    }

    public static boolean isBlank(CharSequence s) {
        if (s == null) {
            return true;
        }

        int len = s.length();
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            if (!Character.isWhitespace(c)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isDoubleQuote(char c) {
        return c == '"';
    }

    public static boolean isDoubleQuoted(CharSequence s) {
        if (s == null || s.length() < 2) {
            return false;
        }

        return isDoubleQuote(s.charAt(0)) && isDoubleQuote(s.charAt(s.length() - 1));
    }

    public static boolean isOnlyDecimals(CharSequence s) {
        int len = s.length();
        for (int i = len - 1; i > -1; i--) {
            int digit = s.charAt(i);
            if (digit < '0' || digit > '9') {
                return false;
            }
        }
        return len > 0;
    }

    public static boolean isQuote(char c) {
        switch (c) {
            case '\'':
            case '"':
            case '`':
                return true;
            default:
                return false;
        }
    }

    public static boolean isQuoted(CharSequence s) {
        if (s == null || s.length() < 2) {
            return false;
        }

        char open = s.charAt(0);
        return isQuote(open) && open == s.charAt(s.length() - 1);
    }

    public static int lastIndexOf(@NotNull CharSequence sequence, int sequenceLo, int sequenceHi, @NotNull CharSequence term) {
        return indexOf(sequence, sequenceLo, sequenceHi, term, -1);
    }

    public static int lastIndexOf(@NotNull CharSequence sequence, int sequenceLo, int sequenceHi, char c) {
        return indexOf(sequence, sequenceLo, sequenceHi, c, -1);
    }

    /**
     * Returns the index of the last character that isn't c between sequenceLo and sequenceHi.
     *
     * @param sequence   the sequence to find the last index of.
     * @param sequenceLo the low limit to start searching through the sequence.
     * @param sequenceHi the hi limit to end searching through the sequence.
     * @param c          the character to stop matching.
     * @return the index of the last character that isn't c or -1 if there aren't any.
     */
    public static int lastIndexOfDifferent(@NotNull CharSequence sequence, int sequenceLo, int sequenceHi, char c) {
        for (int i = sequenceHi - 1; i >= sequenceLo; i--) {
            if (sequence.charAt(i) != c) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Strictly greater than (&lt;) comparison of two UTF16 sequences in lexicographical
     * order. For example, for:
     * l = aaaaa
     * r = aaaaaaa
     * the l &gt; r will produce "false", however for:
     * l = bbbb
     * r = aaaaaaa
     * the l &lt; r will produce "true", because b &lt; a.
     *
     * @param l left sequence, can be null
     * @param r right sequence, can be null
     * @return if either l or r is "null", the return value false, otherwise sequences are compared lexicographically.
     */
    public static boolean lessThan(@Nullable CharSequence l, @Nullable CharSequence r) {
        if (l == null || r == null) {
            return false;
        }
        final int ll = l.length();
        final int rl = r.length();
        final int min = Math.min(ll, rl);

        for (int i = 0; i < min; i++) {
            final int k = l.charAt(i) - r.charAt(i);
            if (k != 0) {
                return k < 0;
            }
        }
        return ll < rl;
    }

    public static boolean lessThan(@Nullable CharSequence l, @Nullable CharSequence r, boolean negated) {
        final boolean eq = Chars.equalsNullable(l, r);
        return negated ? (eq || Chars.greaterThan(l, r)) : (!eq && Chars.lessThan(l, r));
    }

    public static int lowerCaseAsciiHashCode(CharSequence value, int lo, int hi) {
        if (hi == lo) {
            return 0;
        }

        int h = 0;
        for (int p = lo; p < hi; p++) {
            h = 31 * h + toLowerCaseAscii(value.charAt(p));
        }
        return h;
    }

    public static int lowerCaseAsciiHashCode(CharSequence value) {
        int len = value.length();
        if (len == 0) {
            return 0;
        }

        int h = 0;
        for (int p = 0; p < len; p++) {
            h = 31 * h + toLowerCaseAscii(value.charAt(p));
        }
        return h;
    }

    public static int lowerCaseHashCode(CharSequence value, int lo, int hi) {
        if (hi == lo) {
            return 0;
        }

        int h = 0;
        for (int p = lo; p < hi; p++) {
            h = 31 * h + Character.toLowerCase(value.charAt(p));
        }
        return h;
    }

    public static int lowerCaseHashCode(CharSequence value) {
        int len = value.length();
        if (len == 0) {
            return 0;
        }

        int h = 0;
        for (int p = 0; p < len; p++) {
            h = 31 * h + Character.toLowerCase(value.charAt(p));
        }
        return h;
    }

    public static boolean noMatch(CharSequence l, int llo, int lhi, CharSequence r, int rlo, int rhi) {
        int lp = llo;
        int rp = rlo;
        while (lp < lhi && rp < rhi) {
            if (Character.toLowerCase(l.charAt(lp++)) != r.charAt(rp++)) {
                return true;
            }

        }
        return lp != lhi || rp != rhi;
    }

    public static CharSequence repeat(String s, int times) {
        return new CharSequence() {
            @Override
            public char charAt(int index) {
                return s.charAt(index % s.length());
            }

            @Override
            public int length() {
                return s.length() * times;
            }

            @Override
            @NotNull
            public CharSequence subSequence(int start, int end) {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Split character sequence into a list of lpsz strings. This function
     * uses space as a delimiter and it honours spaces in double quotes. Main
     * use for this code is to produce list of C-compatible argument values from
     * command line.
     *
     * @param args command line
     * @return list of 0-terminated strings
     */
    @SuppressWarnings("resource")
    public static ObjList<Path> splitLpsz(CharSequence args) {
        final ObjList<Path> paths = new ObjList<>();
        int n = args.length();
        int lastLen = 0;
        int lastIndex = 0;
        boolean inQuote = false;
        for (int i = 0; i < n; i++) {
            char b = args.charAt(i);

            switch (b) {
                case ' ':
                    // ab c
                    if (lastLen > 0) {
                        if (inQuote) {
                            lastLen++;
                        } else {
                            paths.add(new Path().of(args, lastIndex, lastLen + lastIndex));
                            lastLen = 0;
                        }
                    }
                    break;
                case '"':
                    inQuote = !inQuote;
                    break;
                default:
                    if (lastLen == 0) {
                        lastIndex = i;
                    }
                    lastLen++;
                    break;

            }
        }

        if (lastLen > 0) {
            paths.add(new Path().of(args, lastIndex, lastLen + lastIndex));
        }
        return paths;
    }

    public static boolean startsWith(@Nullable CharSequence cs, @Nullable CharSequence starts) {
        if (cs == null || starts == null) {
            return false;
        }
        int l = starts.length();
        return l == 0 || cs.length() >= l && equalsChars(cs, starts, l);
    }

    public static boolean startsWith(CharSequence _this, int thisLo, int thisHi, CharSequence that) {
        int len = that.length();
        int thisLen = thisHi - thisLo;
        if (thisLen < len) {
            return false;
        }

        for (int i = 0; i < len; i++) {
            if (_this.charAt(thisLo + i) != that.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    public static boolean startsWith(CharSequence _this, char c) {
        return _this.length() > 0 && _this.charAt(0) == c;
    }

    public static boolean startsWithIgnoreCase(CharSequence cs, CharSequence startsWith) {
        if (cs == null || startsWith == null) {
            return false;
        }

        int l = startsWith.length();
        if (l == 0) {
            return true;
        }

        return cs.length() >= l && equalsWithIgnoreCase(startsWith, cs, l);
    }

    // Pattern has to be lower-case.
    public static boolean startsWithLowerCase(@Nullable CharSequence cs, @Nullable CharSequence startsLC) {
        if (cs == null || startsLC == null) {
            return false;
        }

        int l = startsLC.length();
        if (l == 0) {
            return true;
        }

        return cs.length() >= l && equalsCharsLowerCase(startsLC, cs, l);
    }

    public static void toLowerCase(@Nullable CharSequence str, @NotNull CharSink<?> sink) {
        if (str != null) {
            toLowerCase(str, 0, str.length(), sink);
        }
    }

    public static void toLowerCase(@NotNull CharSequence str, int lo, int hi, @NotNull CharSink<?> sink) {
        for (int i = lo; i < hi; i++) {
            sink.put(Character.toLowerCase(str.charAt(i)));
        }
    }

    public static String toLowerCaseAscii(@Nullable CharSequence value) {
        if (value == null) {
            return null;
        }
        final int len = value.length();
        if (len == 0) {
            return "";
        }

        final Utf16Sink b = Misc.getThreadLocalSink();
        for (int i = 0; i < len; i++) {
            b.put(toLowerCaseAscii(value.charAt(i)));
        }
        return b.toString();
    }

    public static char toLowerCaseAscii(char character) {
        return character > 64 && character < 91 ? (char) (character + 32) : character;
    }

    public static void toSink(@Nullable BinarySequence sequence, @NotNull CharSink<?> sink) {
        if (sequence == null) {
            return;
        }

        // limit what we print
        int len = (int) sequence.length();
        for (int i = 0; i < len; i++) {
            if (i > 0) {
                if ((i % 16) == 0) {
                    sink.put('\n');
                    Numbers.appendHexPadded(sink, i);
                }
            } else {
                Numbers.appendHexPadded(sink, i);
            }
            sink.put(' ');

            final byte b = sequence.byteAt(i);
            final int v;
            if (b < 0) {
                v = 256 + b;
            } else {
                v = b;
            }

            if (v < 0x10) {
                sink.put('0');
                sink.put(hexDigits[b]);
            } else {
                sink.put(hexDigits[v / 0x10]);
                sink.put(hexDigits[v % 0x10]);
            }
        }
    }

    public static String toString(char value) {
        if (value < CHAR_STRINGS.length) {
            return CHAR_STRINGS[value];
        }
        return Character.toString(value);
    }

    public static String toString(CharSequence s) {
        return s == null ? null : s.toString();
    }

    public static String toString(CharSequence cs, int start, int end) {
        final Utf16Sink b = Misc.getThreadLocalSink();
        b.put(cs, start, end);
        return b.toString();
    }

    public static String toString(@NotNull CharSequence cs, int start, int end, char unescape) {
        final Utf16Sink b = Misc.getThreadLocalSink();
        unescape(cs, start, end, unescape, b);
        return b.toString();
    }

    public static void toUpperCase(@Nullable CharSequence str, @NotNull CharSink<?> sink) {
        if (str != null) {
            final int len = str.length();
            for (int i = 0; i < len; i++) {
                sink.put(Character.toUpperCase(str.charAt(i)));
            }
        }
    }

    public static void trim(TrimType type, CharSequence str, StringSink sink) {
        if (str == null) {
            return;
        }
        int startIdx = 0;
        int endIdx = str.length() - 1;
        if (type == TrimType.LTRIM || type == TrimType.TRIM) {
            while (startIdx < endIdx && str.charAt(startIdx) == ' ') {
                startIdx++;
            }
        }
        if (type == TrimType.RTRIM || type == TrimType.TRIM) {
            while (startIdx < endIdx && str.charAt(endIdx) == ' ') {
                endIdx--;
            }
        }
        sink.clear();
        if (startIdx != endIdx) {
            sink.put(str, startIdx, endIdx + 1);
        }
    }

    public static void unescape(@NotNull CharSequence cs, int start, int end, char unescape, @NotNull Utf16Sink sink) {
        final int lastChar = end - 1;
        for (int i = start; i < end; i++) {
            char c = cs.charAt(i);
            sink.put(c);
            if (c == unescape && i < lastChar && cs.charAt(i + 1) == unescape) {
                i++;
            }
        }
    }

    private static int[] base64CreateInvertedAlphabet(char[] alphabet) {
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

    private static void base64Decode(@Nullable CharSequence encoded, @NotNull Utf8Sink target, int[] invertedAlphabet) {
        if (encoded == null) {
            return;
        }

        // skip trailing '=' they are just for padding and have no meaning
        int length = encoded.length();
        for (; length > 0; length--) {
            if (encoded.charAt(length - 1) != '=') {
                break;
            }
        }

        int remainder = length % 4;
        int sourcePos = 0;

        // first decode all 4 byte chunks. this is *the* hot loop, be careful when changing it
        for (int end = length - remainder; sourcePos < end; sourcePos += 4) {
            int b0 = base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos)) << 18;
            int b1 = base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 1)) << 12;
            int b2 = base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 2)) << 6;
            int b4 = base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 3));

            int wrk = b0 | b1 | b2 | b4;
            // we use absolute positions to write to the byte buffer in the hot loop
            // benchmarking shows that it is faster than using relative positions
            target.putAny((byte) (wrk >>> 16));
            target.putAny((byte) ((wrk >>> 8) & 0xFF));
            target.putAny((byte) (wrk & 0xFF));
        }
        // now decode remainder
        int wrk;
        switch (remainder) {
            case 0:
                // nothing to do, yay!
                break;
            case 1:
                // invalid encoding, we can't have 1 byte remainder as
                // even 1 byte encodes to 2 chars
                throw CairoException.nonCritical().put("invalid base64 encoding [string=").putAsPrintable(encoded).put(']');
            case 2:
                wrk = base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos)) << 18;
                wrk |= base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 1)) << 12;
                target.putAny((byte) (wrk >>> 16));
                break;
            case 3:
                wrk = base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos)) << 18;
                wrk |= base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 1)) << 12;
                wrk |= base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 2)) << 6;
                target.putAny((byte) (wrk >>> 16));
                target.putAny((byte) ((wrk >>> 8) & 0xFF));
        }
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
            int b0 = base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos)) << 18;
            int b1 = base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 1)) << 12;
            int b2 = base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 2)) << 6;
            int b4 = base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 3));

            int wrk = b0 | b1 | b2 | b4;
            // we use absolute positions to write to the byte buffer in the hot loop
            // benchmarking shows that it is faster than using relative positions
            target.put(targetPos, (byte) (wrk >>> 16));
            target.put(targetPos + 1, (byte) ((wrk >>> 8) & 0xFF));
            target.put(targetPos + 2, (byte) (wrk & 0xFF));
        }
        target.position(targetPos);
        // now decode remainder
        int wrk;
        switch (remainder) {
            case 0:
                // nothing to do, yay!
                break;
            case 1:
                // invalid encoding, we can't have 1 byte remainder as
                // even 1 byte encodes to 2 chars
                throw CairoException.nonCritical().put("invalid base64 encoding [string=").putAsPrintable(encoded).put(']');
            case 2:
                wrk = base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos)) << 18;
                wrk |= base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 1)) << 12;
                target.put((byte) (wrk >>> 16));
                break;
            case 3:
                wrk = base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos)) << 18;
                wrk |= base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 1)) << 12;
                wrk |= base64InvertedLookup(invertedAlphabet, encoded.charAt(sourcePos + 2)) << 6;
                target.put((byte) (wrk >>> 16));
                target.put((byte) ((wrk >>> 8) & 0xFF));
        }
    }

    private static int base64Encode(@Nullable BinarySequence sequence, int maxLength, @NotNull CharSink<?> buffer, char @NotNull [] alphabet) {
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
                buffer.putAscii(alphabet[c]);
                b <<= 6;
            }
        }
        return pad;
    }

    private static int base64InvertedLookup(int[] invertedAlphabet, char ch) {
        if (ch > 127) {
            throw CairoException.nonCritical().put("non-ascii character while decoding base64 [ch=").put((int) (ch)).put(']');
        }
        int index = invertedAlphabet[ch];
        if (index == -1) {
            throw CairoException.nonCritical().put("invalid base64 character [ch=").put(ch).put(']');
        }
        return index;
    }

    private static boolean equalsChars(@NotNull CharSequence l, @NotNull CharSequence r, int len) {
        for (int i = 0; i < len; i++) {
            if (l.charAt(i) != r.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private static boolean equalsCharsIgnoreCase(@NotNull CharSequence l, @NotNull CharSequence r, int len) {
        for (int i = 0; i < len; i++) {
            if (Character.toLowerCase(l.charAt(i)) != Character.toLowerCase(r.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean equalsCharsIgnoreCase(@NotNull CharSequence l, @NotNull CharSequence r, int len, int rLo, int rHi) {
        assert len == (rHi - rLo);
        for (int i = 0; i < len; i++) {
            if (Character.toLowerCase(l.charAt(i)) != Character.toLowerCase(r.charAt(i + rLo))) {
                return false;
            }
        }
        return true;
    }

    // Left side has to be lower-case.
    private static boolean equalsCharsLowerCase(@NotNull CharSequence lLC, @NotNull CharSequence r, int len) {
        for (int i = 0; i < len; i++) {
            if (lLC.charAt(i) != Character.toLowerCase(r.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean equalsWithIgnoreCase(@NotNull CharSequence lLC, @NotNull CharSequence r, int len) {
        for (int i = 0; i < len; i++) {
            if (Character.toLowerCase(lLC.charAt(i)) != Character.toLowerCase(r.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    static {
        CHAR_STRINGS = new String[128];
        for (char c = 0; c < 128; c++) {
            CHAR_STRINGS[c] = Character.toString(c);
        }
    }
}
