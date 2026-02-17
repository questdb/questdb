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

package io.questdb.cutlass.http;

import io.questdb.std.Unsafe;

public class HttpSemantics {
    /**
     * Check whether c is a <a href="https://www.rfc-editor.org/rfc/rfc9110.html#section-5.6.2-3">delimiter</a>.
     *
     * @param c the character to check
     * @return true if c is a delimiter
     */
    public static boolean isDelimiter(char c) {
        switch (c) {
            case '"':
            case '(':
            case ')':
            case ',':
            case '/':
            case ':':
            case ';':
            case '<':
            case '=':
            case '>':
            case '?':
            case '@':
            case '[':
            case '\\':
            case ']':
            case '{':
            case '}':
                return true;
            default:
                return false;
        }
    }

    /**
     * Check whether c is a <a href="https://www.rfc-editor.org/rfc/rfc9110.html#section-5.6.2">token</a>.
     *
     * @param c the character to check
     * @return true if c is a token
     */
    public static boolean isToken(char c) {
        return c > 32 && c < 127 && !isDelimiter(c);
    }

    /**
     * Swallow the next character if it's a delimiter.
     *
     * @param lo the ptr to start swallowing from.
     * @param hi the last ptr to check.
     * @return the address of the next character if c is a delimiter.
     */
    public static long swallowNextDelimiter(long lo, long hi) {
        if (lo < hi && isDelimiter((char) Unsafe.getUnsafe().getByte(lo))) {
            return lo + 1;
        }
        return lo;
    }

    /**
     * Swallow optional whitespaces.
     *
     * @param lo the ptr to start swallowing from.
     * @param hi the last ptr to check.
     * @return the address of the first character that isn't a whitespace.
     */
    public static long swallowOWS(long lo, long hi) {
        while (lo < hi && (char) Unsafe.getUnsafe().getByte(lo) == ' ') {
            lo++;
        }
        return lo;
    }

    /**
     * Swallow <a href="https://www.rfc-editor.org/rfc/rfc9110.html#section-5.6.2">token</a>.
     *
     * @param lo the ptr to start swallowing from.
     * @param hi the last ptr to check.
     * @return the address of the first character that isn't a token.
     */
    public static long swallowTokens(long lo, long hi) {
        while (lo < hi && isToken((char) Unsafe.getUnsafe().getByte(lo))) {
            lo++;
        }
        return lo;
    }
}
