/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.misc;

import com.nfsdb.exceptions.JournalRuntimeException;

public final class Chars {
    private final static ThreadLocal<char[]> builder = new ThreadLocal<>();

    private Chars() {
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
        if (value.length() == 0) {
            return 0;
        }

        int h = 0;
        for (int p = 0, n = value.length(); p < n; p++) {
            h = 31 * h + value.charAt(p);
        }
        return h;
    }

    public static int indexOf(CharSequence s, char c) {
        for (int i = 0, n = s.length(); i < n; i++) {
            if (s.charAt(i) == c) {
                return i;
            }
        }
        return -1;
    }

    public static int put(long address, CharSequence value) {
        int len = value.length();
        Unsafe.getUnsafe().putInt(address, len);
        address += 4;
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putChar(address + (i << 1), value.charAt(i));
        }
        return (len << 1) + 4;
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
}
