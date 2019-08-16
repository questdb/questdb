/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std;

import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectBytes;
import com.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static com.questdb.std.Numbers.hexDigits;

public final class Chars {
    private Chars() {
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

        int ll = l.length();
        int rl = r.length();

        for (int i = 0, n = Math.min(ll, rl); i < n; i++) {
            int k = l.charAt(i) - r.charAt(i);
            if (k != 0) {
                return k;
            }
        }
        return Integer.compare(ll, rl);
    }

    public static boolean contains(CharSequence sequence, CharSequence term) {
        int m = term.length();
        if (m == 0) {
            return false;
        }

        for (int i = 0, n = sequence.length(); i < n; i++) {
            if (sequence.charAt(i) == term.charAt(0)) {
                if (n - i < m) {
                    return false;
                }
                boolean found = true;
                for (int k = 1; k < m && k + i < n; k++) {
                    if (sequence.charAt(i + k) != term.charAt(k)) {
                        found = false;
                        break;
                    }
                }
                if (found) {
                    return true;
                }
            }
        }

        return false;
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

        for (int i = 0; i < ll; i++) {
            if (l.charAt(i) != r.charAt(i)) {
                return false;
            }
        }
        return true;
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

    public static boolean equalsLowerCaseAscii(CharSequence l, int lLo, int lHi, CharSequence r, int rLo, int rHi) {
        if (l == r) {
            return true;
        }

        int ll = lHi - lLo;
        if (ll != rHi - rLo) {
            return false;
        }

        for (int i = 0; i < ll; i++) {
            if (toLowerCaseAscii(l.charAt(i + lLo)) != r.charAt(i + rLo)) {
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
            if (toLowerCaseAscii(l.charAt(i)) != r.charAt(i)) {
                return false;
            }
        }

        return true;
    }

    public static boolean equalsLowerCaseAsciiNc(@Nullable CharSequence l, @NotNull CharSequence r) {
        return l != null && equalsLowerCaseAscii(l, r);
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

    public static int hashCode(DirectBytes value) {
        int len = value.byteLength();
        if (len == 0) {
            return 0;
        }

        int h = 0;
        long address = value.address();
        for (int p = 0, n = len / 2; p < n; p++) {
            h = 31 * h + Unsafe.getUnsafe().getChar(address + (p << 1));
        }
        return h;
    }

    public static int indexOf(CharSequence s, char c) {
        return indexOf(s, 0, c);
    }

    public static int indexOf(CharSequence s, final int lo, char c) {
        int i = lo;
        for (int n = s.length(); i < n; i++) {
            if (s.charAt(i) == c) {
                return i;
            }
        }
        return -1;
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

    public static boolean isQuoted(CharSequence s) {
        if (s == null || s.length() == 0) {
            return false;
        }

        switch (s.charAt(0)) {
            case '\'':
            case '"':
            case '`':
                return true;
            default:
                return false;
        }
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
        int len = that.length();
        if (_this.length() < len) {
            return false;
        }

        for (int i = 0; i < len; i++) {
            if (_this.charAt(i) != that.charAt(i)) {
                return false;
            }
        }
        return true;
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

    public static void strcpy(final CharSequence value, final int len, final long address) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(address + i, (byte) value.charAt(i));
        }
    }

    public static void strcpy(final CharSequence value, final int lo, final int hi, final long address) {
        for (int i = lo; i < hi; i++) {
            Unsafe.getUnsafe().putByte(address + i - lo, (byte) value.charAt(i));
        }
    }

    public static int strcpyw(CharSequence value, long address) {
        int len = value.length();
        Unsafe.getUnsafe().putInt(address, len);
        strcpyw(value, len, address + 4);
        return (len << 1) + 4;
    }

    public static void strcpyw(final CharSequence value, final int len, final long address) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putChar(address + (i << 1), value.charAt(i));
        }
    }

    public static String stringOf(CharSequence charSequence) {
        if (charSequence instanceof String) {
            return (String) charSequence;
        }

        if (charSequence == null) {
            return null;
        }
        return charSequence.toString();
    }

    public static String stripQuotes(String s) {
        int l;
        if (s == null || (l = s.length()) == 0) {
            return s;
        }

        switch (s.charAt(0)) {
            case '\'':
            case '"':
            case '`':
                return s.substring(1, l - 1);
            default:
                return s;
        }
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

    public static String toUtf8String(long lo, long hi) {
        if (hi == lo) {
            return "";
        }

        CharSink b = Misc.getThreadLocalBuilder();
        utf8Decode(lo, hi, b);
        return b.toString();
    }

    public static boolean utf8Decode(long lo, long hi, CharSink sink) {
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

    public static int utf8DecodeMultiByte(long lo, long hi, int b, CharSink sink) {
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

    private static int utf8error() {
        return -1;
    }

    private static int utf8Decode4Bytes(long lo, int b, long hi, CharSink sink) {
        if (b >> 3 != -2 || hi - lo < 4) {
            return utf8error();
        }

        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
        byte b4 = Unsafe.getUnsafe().getByte(lo + 3);

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

    private static int utf8Decode3Bytes(long lo, long hi, int b1, CharSink sink) {
        if (hi - lo < 3) {
            return utf8error();
        }

        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);

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

    private static int utf8Decode3BytesZ(long lo, int b1, CharSink sink) {
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        if (b2 == 0) {
            return utf8error();
        }

        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
        if (b3 == 0) {
            return utf8error();
        }

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

    private static int utf8Decode2Bytes(long lo, long hi, int b1, CharSink sink) {
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
