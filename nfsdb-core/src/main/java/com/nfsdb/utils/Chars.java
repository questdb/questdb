/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.utils;

public final class Chars {
    private Chars() {
    }

    public static boolean equals(CharSequence l, CharSequence r) {

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

    public static String stripQuotes(String s) {
        int l;
        if (s == null || (l = s.length()) == 0) {
            return s;
        }

        char c = s.charAt(0);
        if (c == '\'' || c == '"') {
            return s.substring(1, l - 1);
        }

        return s;
    }

}
