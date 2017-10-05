/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.misc;

import com.questdb.ex.JournalRuntimeException;
import com.questdb.std.ObjList;
import com.questdb.std.str.ByteSequence;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectBytes;
import com.questdb.std.str.Path;

public final class Chars {
    private final static ThreadLocal<char[]> builder = new ThreadLocal<>();

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

        for (int i = 0, n = ll < rl ? ll : rl; i < n; i++) {
            int k = l.charAt(i) - r.charAt(i);
            if (k != 0) {
                return k;
            }
        }
        return Integer.compare(ll, rl);
    }

    public static boolean contains(CharSequence _this, CharSequence that) {
        int m = that.length();
        if (m == 0) {
            return false;
        }

        for (int i = 0, n = _this.length(); i < n; i++) {
            if (_this.charAt(i) == that.charAt(0)) {
                if (n - i < m) {
                    return false;
                }
                boolean found = true;
                for (int k = 1; k < m && k + i < n; k++) {
                    if (_this.charAt(i + k) != that.charAt(k)) {
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
            if (Character.toLowerCase(l.charAt(i)) != r.charAt(i)) {
                return false;
            }
        }

        return true;
    }

    public static boolean equalsIgnoreCase(CharSequence l, int llo, int lhi, CharSequence r, int rlo, int rhi) {
        int lp = llo;
        int rp = rlo;
        while (lp < lhi && rp < rhi) {
            if (Character.toLowerCase(l.charAt(lp++)) != r.charAt(rp++)) {
                return false;
            }

        }
        return lp == lhi && rp == rhi;
    }

    public static boolean equalsNc(CharSequence l, CharSequence r) {
        return r != null && equals(l, r);
    }

    public static String getFileName(CharSequence path) {
        int pos = -1;
        for (int i = 0, k = path.length(); i < k; i++) {
            char c = path.charAt(i);
            if (c == '\\' || c == '/') {
                pos = i;
            }
        }

        int l = path.length() - pos - 1;
        if (l == 0) {
            throw new JournalRuntimeException("Invalid path: %s", path);
        }

        char buf[] = builder.get();
        if (buf == null || buf.length < l) {
            builder.set(buf = new char[l]);
        }

        int p = 0;
        for (int i = pos + 1, k = path.length(); i < k; i++) {
            buf[p++] = path.charAt(i);
        }

        return new String(buf, 0, l);
    }

    public static int hashCode(CharSequence value) {
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

    public static int lastIndexOf(CharSequence s, char c) {
        for (int i = s.length() - 1; i > -1; i--) {
            if (s.charAt(i) == c) {
                return i;
            }
        }
        return -1;
    }

    public static void putCharsOnly(long address, CharSequence value) {
        strcpyw(value, value.length(), address);
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
                            paths.add(new Path().of(args, lastIndex, lastLen));
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
            paths.add(new Path().of(args, lastIndex, lastLen));
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

    public static boolean startsWith(CharSequence _this, char c) {
        return _this.length() > 0 && _this.charAt(0) == c;
    }

    public static void strcpy(final CharSequence value, final int len, final long address) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(address + i, (byte) value.charAt(i));
        }
    }

    public static void strcpy(final ByteSequence value, final int len, final long address) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(address + i, value.byteAt(i));
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

    public static String toString(CharSequence s) {
        return s == null ? null : s.toString();
    }

    public static String toUtf8String(ByteSequence in) {
        if (in == null) {
            return null;
        }

        CharSink b = Misc.getThreadLocalBuilder();
        utf8Decode(in, b);
        return b.toString();
    }

    public static void utf8Decode(ByteSequence in, CharSink builder) {
        int index = 0;
        int len = in.length();

        while (index < len) {
            byte b = in.byteAt(index);
            if (b < 0) {
                index += utf8DecodeMultiByte(in, b, index, len, builder);
            } else {
                builder.put((char) b);
                ++index;
            }
        }
    }

    private static int utf8error(int index, int len) {
        return len - index;
    }

    private static int utf8DecodeMultiByte(ByteSequence in, int b, int index, int len, CharSink builder) {
        if (b >> 5 == -2 && (b & 30) != 0) {
            return utf8Decode2Bytes(in, b, index, len, builder);
        }

        if (b >> 4 == -2) {
            return utf8Decode3Bytes(in, b, index, len, builder);
        }

        return utf8Decode4Bytes(in, b, index, len, builder);
    }

    private static int utf8Decode4Bytes(ByteSequence in, int b, int index, int len, CharSink builder) {
        if (b >> 3 != -2) {
            return utf8error(index, len);
        }

        if (len - index > 3) {
            byte b2 = in.byteAt(index + 1);
            byte b3 = in.byteAt(index + 2);
            byte b4 = in.byteAt(index + 3);

            int codePoint = b << 18 ^ b2 << 12 ^ b3 << 6 ^ b4 ^ 3678080;
            if (!isMalformed4(b2, b3, b4) && Character.isSupplementaryCodePoint(codePoint)) {
                builder.put(Character.highSurrogate(codePoint));
                builder.put(Character.lowSurrogate(codePoint));
                return 4;
            }
        }
        return utf8error(index, len);
    }

    private static int utf8Decode3Bytes(ByteSequence in, int b, int index, int len, CharSink builder) {
        if (len - index < 3) {
            return utf8error(index, len);
        }

        byte b2 = in.byteAt(index + 1);
        byte b3 = in.byteAt(index + 2);

        if (isMalformed3(b, b2, b3)) {
            return utf8error(index, len);
        }

        char c = (char) (b << 12 ^ b2 << 6 ^ b3 ^ -123008);
        if (Character.isSurrogate(c)) {
            return utf8error(index, len);
        }

        builder.put(c);
        return 3;
    }

    private static int utf8Decode2Bytes(ByteSequence in, int b, int index, int len, CharSink builder) {
        if (len - index < 2) {
            return utf8error(index, len);
        }

        byte b2 = in.byteAt(index + 1);
        if (isNotContinuation(b2)) {
            return utf8error(index, len);
        }

        builder.put((char) (b << 6 ^ b2 ^ 3968));
        return 2;
    }

    private static boolean isNotContinuation(int b) {
        return (b & 192) != 128;
    }

    private static boolean isMalformed3(int b1, int b2, int b3) {
        return b1 == -32 && (b2 & 224) == 128 || (b2 & 192) != 128 || (b3 & 192) != 128;
    }

    private static boolean isMalformed4(int b2, int b3, int b4) {
        return (b2 & 192) != 128 || (b3 & 192) != 128 || (b4 & 192) != 128;
    }
}
