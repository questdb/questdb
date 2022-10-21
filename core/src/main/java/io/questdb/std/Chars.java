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

package io.questdb.std;

import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.CharSinkBase;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.Numbers.hexDigits;

public final class Chars {
    static final char[] base64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray();

    private Chars() {
    }

    public static void asciiCopyTo(char[] chars, int start, int len, long dest) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(dest + i, (byte) chars[i + start]);
        }
    }

    public static long asciiStrCpy(final CharSequence value, final long address) {
        asciiStrCpy(value, value.length(), address);
        return address;
    }

    public static void asciiStrCpy(final CharSequence value, final int len, final long address) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(address + i, (byte) value.charAt(i));
        }
    }

    public static void asciiStrCpy(final CharSequence value, int lo, final int len, final long address) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(address + i, (byte) value.charAt(lo + i));
        }
    }

    public static void base64Encode(BinarySequence sequence, final int maxLength, CharSink buffer) {
        if (sequence == null) {
            return;
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
                buffer.put(base64[c]);
                b <<= 6;
            }
        }

        for (int j = 0; j < pad; j++) {
            buffer.put("=");
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

    public static boolean contains(CharSequence sequence, CharSequence term) {
        return indexOf(sequence, 0, sequence.length(), term) != -1;
    }

    public static int indexOf(CharSequence sequence, int sequenceLo, int sequenceHi, CharSequence term) {
        return indexOf(sequence, sequenceLo, sequenceHi, term, 1);
    }

    public static int indexOf(CharSequence sequence, int sequenceLo, int sequenceHi, CharSequence term, int occurrence) {
        int m = term.length();
        if (m == 0) {
            return -1;
        }

        if (occurrence == 0) {
            return -1;
        }

        int foundIndex = -1;
        int count = 0;
        if (occurrence > 0) {
            for (int i = sequenceLo; i < sequenceHi; i++) {
                if (foundIndex == -1) {
                    if (sequenceHi - i < m) {
                        return -1;
                    }
                    if (sequence.charAt(i) == term.charAt(0)) {
                        foundIndex = i;
                    }
                } else {    // first character matched, try to match the rest of the term
                    if (sequence.charAt(i) != term.charAt(i - foundIndex)) {
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
        } else {    // if occurrence is negative, search in reverse
            for (int i = sequenceHi - 1; i >= sequenceLo; i--) {
                if (foundIndex == -1) {
                    if (i - sequenceLo + 1 < m) {
                        return -1;
                    }
                    if (sequence.charAt(i) == term.charAt(m - 1)) {
                        foundIndex = i;
                    }
                } else {    // last character matched, try to match the rest of the term
                    if (sequence.charAt(i) != term.charAt(m - 1 + i - foundIndex)) {
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

    public static void copyStrChars(CharSequence value, int pos, int len, long address) {
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i + pos);
            Unsafe.getUnsafe().putChar(address + 2L * i, c);
        }
    }

    public static boolean endsWith(CharSequence cs, CharSequence ends) {
        if (ends == null || cs == null) {
            return false;
        }

        int l = ends.length();
        if (l == 0) {
            return false;
        }

        int csl = cs.length();
        return !(csl == 0 || csl < l) && equals(ends, cs, csl - l, csl);

    }

    public static boolean endsWith(CharSequence cs, char c) {
        if (cs == null) {
            return false;
        }
        final int csl = cs.length();
        return csl != 0 && c == cs.charAt(csl - 1);
    }

    public static boolean equals(CharSequence l, CharSequence r) {
        if (l == r) {
            return true;
        }

        int ll;
        if ((ll = l.length()) != r.length()) {
            return false;
        }

        return equalsChars(l, r, ll);
    }

    public static boolean equals(CharSequence l, CharSequence r, int rLo, int rHi) {
        if (l == r) {
            return true;
        }

        int ll;
        if ((ll = l.length()) != rHi - rLo) {
            return false;
        }

        for (int i = 0; i < ll; i++) {
            if (l.charAt(i) != r.charAt(i + rLo)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(CharSequence l, int lLo, int lHi, CharSequence r, int rLo, int rHi) {
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

    public static boolean equals(CharSequence l, char r) {
        return l.length() == 1 && l.charAt(0) == r;
    }

    /**
     * Compares two char sequences on assumption and right value is always lower case.
     * Methods converts every char of right sequence before comparing to left sequence.
     *
     * @param l left sequence
     * @param r right sequence
     * @return true if sequences match exactly (ignoring char case)
     */
    public static boolean equalsIgnoreCase(CharSequence l, CharSequence r) {
        int ll;
        if ((ll = l.length()) != r.length()) {
            return false;
        }

        for (int i = 0; i < ll; i++) {
            if (Character.toLowerCase(l.charAt(i)) != Character.toLowerCase(r.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    public static boolean equalsIgnoreCaseNc(CharSequence l, CharSequence r) {
        return l != null && equalsIgnoreCase(l, r);
    }

    public static boolean equalsLowerCase(CharSequence l, int lLo, int lHi, CharSequence r, int rLo, int rHi) {
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

    public static boolean equalsLowerCaseAscii(CharSequence l, int lLo, int lHi, CharSequence r, int rLo, int rHi) {
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

    public static boolean equalsLowerCaseAscii(@NotNull CharSequence l, CharSequence r) {
        int ll;
        if ((ll = l.length()) != r.length()) {
            return false;
        }

        for (int i = 0; i < ll; i++) {
            if (toLowerCaseAscii(l.charAt(i)) != toLowerCaseAscii(r.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    public static boolean equalsLowerCaseAsciiNc(@Nullable CharSequence l, @NotNull CharSequence r) {
        return l != null && equalsLowerCaseAscii(l, r);
    }

    public static boolean equalsNc(CharSequence l, char r) {
        return (l == null && r == CharConstant.ZERO.getChar(null)) || (l != null && equals(l, r));
    }

    public static boolean equalsNc(CharSequence l, CharSequence r) {
        return r != null && equals(l, r);
    }

    public static int hashCode(CharSequence value, int lo, int hi) {

        if (hi == lo) {
            return 0;
        }

        int h = 0;
        for (int p = lo; p < hi; p++) {
            h = 31 * h + value.charAt(p);
        }
        return h;
    }

    public static int hashCode(CharSequence value) {
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

    public static int hashCode(String value) {
        return value.hashCode();
    }

    public static int hashCode(DirectByteCharSequence value) {
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

    public static int indexOf(CharSequence s, char c) {
        return indexOf(s, 0, c);
    }

    public static int indexOf(CharSequence s, final int lo, char c) {
        return indexOf(s, lo, s.length(), c);
    }

    public static int indexOf(CharSequence s, final int lo, int hi, char c) {
        return indexOf(s, lo, hi, c, 1);
    }

    public static int indexOf(CharSequence sequence, int sequenceLo, int sequenceHi, char ch, int occurrence) {
        if (occurrence == 0) {
            return -1;
        }

        int count = 0;
        if (occurrence > 0) {
            for (int i = sequenceLo; i < sequenceHi; i++) {
                if (sequence.charAt(i) == ch) {
                    count++;
                    if (count == occurrence) {
                        return i;
                    }
                }
            }
        } else {    // if occurrence is negative, search in reverse
            for (int i = sequenceHi - 1; i >= sequenceLo; i--) {
                if (sequence.charAt(i) == ch) {
                    count--;
                    if (count == occurrence) {
                        return i;
                    }
                }
            }
        }

        return -1;
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

    public static boolean isMalformed3(int b1, int b2, int b3) {
        return b1 == -32 && (b2 & 224) == 128 || (b2 & 192) != 128 || (b3 & 192) != 128;
    }

    public static boolean isMalformed4(int b2, int b3, int b4) {
        return (b2 & 192) != 128 || (b3 & 192) != 128 || (b4 & 192) != 128;
    }

    public static boolean isNotContinuation(int b) {
        return (b & 192) != 128;
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

    public static int lastIndexOf(CharSequence s, char c) {
        for (int i = s.length() - 1; i > -1; i--) {
            if (s.charAt(i) == c) {
                return i;
            }
        }
        return -1;
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

    public static boolean empty(final CharSequence value) {
        return value == null || value.length() < 1;
    }

    public static CharSequence repeat(String s, int times) {
        return new CharSequence() {
            @Override
            public int length() {
                return s.length() * times;
            }

            @Override
            public char charAt(int index) {
                return s.charAt(index % s.length());
            }

            @Override
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
                            paths.add(new Path().of(args, lastIndex, lastLen + lastIndex).$());
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
            paths.add(new Path().of(args, lastIndex, lastLen + lastIndex).$());
        }
        return paths;
    }

    public static boolean startsWith(CharSequence _this, CharSequence that) {
        final int len = that.length();
        return _this.length() >= len && equalsChars(_this, that, len);
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

    public static String stringFromUtf8Bytes(long lo, long hi) {
        if (hi == lo) {
            return "";
        }

        CharSink b = Misc.getThreadLocalBuilder();
        utf8Decode(lo, hi, b);
        return b.toString();
    }

    public static void toLowerCase(@Nullable final CharSequence str, final CharSink sink) {
        if (str != null) {
            final int len = str.length();
            for (int i = 0; i < len; i++) {
                sink.put(Character.toLowerCase(str.charAt(i)));
            }
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

        final CharSink b = Misc.getThreadLocalBuilder();
        for (int i = 0; i < len; i++) {
            b.put(toLowerCaseAscii(value.charAt(i)));
        }
        return b.toString();
    }

    public static char toLowerCaseAscii(char character) {
        return character > 64 && character < 91 ? (char) (character + 32) : character;
    }

    public static void toSink(BinarySequence sequence, CharSink sink) {
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

    public static String toString(CharSequence s) {
        return s == null ? null : s.toString();
    }

    public static String toString(CharSequence cs, int start, int end) {
        final CharSink b = Misc.getThreadLocalBuilder();
        b.put(cs, start, end);
        return b.toString();
    }

    public static String toString(CharSequence cs, int start, int end, char unescape) {
        final CharSink b = Misc.getThreadLocalBuilder();
        final int lastChar = end - 1;
        for (int i = start; i < end; i++) {
            char c = cs.charAt(i);
            b.put(c);
            if (c == unescape && i < lastChar && cs.charAt(i + 1) == unescape) {
                i++;
            }
        }
        return b.toString();
    }

    public static void toUpperCase(@Nullable final CharSequence str, final CharSink sink) {
        if (str != null) {
            final int len = str.length();
            for (int i = 0; i < len; i++) {
                sink.put(Character.toUpperCase(str.charAt(i)));
            }
        }
    }

    /* Decodes bytes between lo,hi addresses into sink.
     *  Note: operation might fail in the middle and leave sink in inconsistent  state .
     *  @return true if input is proper utf8 and false otherwise . */
    public static boolean utf8Decode(long lo, long hi, CharSinkBase sink) {
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

    public static int utf8DecodeMultiByte(long lo, long hi, int b, CharSinkBase sink) {
        if (b >> 5 == -2 && (b & 30) != 0) {
            return utf8Decode2Bytes(lo, hi, b, sink);
        }

        if (b >> 4 == -2) {
            return utf8Decode3Bytes(lo, hi, b, sink);
        }

        return utf8Decode4Bytes(lo, b, hi, sink);
    }

    public static int utf8DecodeMultiByteZ(long lo, int b, CharSink sink) {
        if (b >> 5 == -2 && (b & 30) != 0) {
            return utf8Decode2BytesZ(lo, b, sink);
        }

        if (b >> 4 == -2) {
            return utf8Decode3BytesZ(lo, b, sink);
        }

        return utf8Decode4BytesZ(lo, b, sink);
    }

    public static boolean utf8DecodeZ(long lo, CharSink sink) {
        long p = lo;

        while (true) {
            byte b = Unsafe.getUnsafe().getByte(p);

            if (b == 0) {
                break;
            }

            if (b < 0) {
                int n = utf8DecodeMultiByteZ(p, b, sink);
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

    private static boolean equalsChars(CharSequence l, CharSequence r, int len) {
        for (int i = 0; i < len; i++) {
            if (l.charAt(i) != r.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private static int utf8error() {
        return -1;
    }

    private static int utf8Decode4Bytes(long lo, int b, long hi, CharSinkBase sink) {
        if (b >> 3 != -2 || hi - lo < 4) {
            return utf8error();
        }

        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
        byte b4 = Unsafe.getUnsafe().getByte(lo + 3);

        return utf8Decode4Bytes0(b, sink, b2, b3, b4);
    }

    private static int utf8Decode4Bytes0(int b, CharSinkBase sink, byte b2, byte b3, byte b4) {
        if (isMalformed4(b2, b3, b4)) {
            return utf8error();
        }

        final int codePoint = b << 18 ^ b2 << 12 ^ b3 << 6 ^ b4 ^ 3678080;
        if (Character.isSupplementaryCodePoint(codePoint)) {
            sink.put(Character.highSurrogate(codePoint));
            sink.put(Character.lowSurrogate(codePoint));
            return 4;
        }
        return utf8error();
    }

    private static int utf8Decode4BytesZ(long lo, int b, CharSink sink) {
        if (b >> 3 != -2) {
            return utf8error();
        }

        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        if (b2 == 0) {
            return utf8error();
        }

        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
        if (b3 == 0) {
            return utf8error();
        }

        byte b4 = Unsafe.getUnsafe().getByte(lo + 3);
        if (b4 == 0) {
            return utf8error();
        }

        return utf8Decode4Bytes0(b, sink, b2, b3, b4);
    }

    private static int utf8Decode3Bytes(long lo, long hi, int b1, CharSinkBase sink) {
        if (hi - lo < 3) {
            return utf8error();
        }

        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);

        return utf8Decode3Byte0(b1, sink, b2, b3);
    }

    private static int utf8Decode3BytesZ(long lo, int b1, CharSink sink) {
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        if (b2 == 0) {
            return utf8error();
        }

        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
        if (b3 == 0) {
            return utf8error();
        }

        return utf8Decode3Byte0(b1, sink, b2, b3);
    }

    private static int utf8Decode3Byte0(int b1, CharSinkBase sink, byte b2, byte b3) {
        if (isMalformed3(b1, b2, b3)) {
            return utf8error();
        }

        char c = (char) (b1 << 12 ^ b2 << 6 ^ b3 ^ -123008);
        if (Character.isSurrogate(c)) {
            return utf8error();
        }

        sink.put(c);
        return 3;
    }

    private static int utf8Decode2Bytes(long lo, long hi, int b1, CharSinkBase sink) {
        if (hi - lo < 2) {
            return utf8error();
        }

        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        if (isNotContinuation(b2)) {
            return utf8error();
        }

        sink.put((char) (b1 << 6 ^ b2 ^ 3968));
        return 2;
    }

    private static int utf8Decode2BytesZ(long lo, int b1, CharSink sink) {
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        if (b2 == 0) {
            return utf8error();
        }
        if (isNotContinuation(b2)) {
            return utf8error();
        }

        sink.put((char) (b1 << 6 ^ b2 ^ 3968));
        return 2;
    }
}
