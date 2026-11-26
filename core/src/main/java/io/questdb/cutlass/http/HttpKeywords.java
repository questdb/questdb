/*+*****************************************************************************
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

import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

public class HttpKeywords {

    public static boolean isAbort(@Nullable Utf8Sequence tok) {
        return tok != null && tok.size() == 5
                && (tok.byteAt(0) | 32) == 'a'
                && (tok.byteAt(1) | 32) == 'b'
                && (tok.byteAt(2) | 32) == 'o'
                && (tok.byteAt(3) | 32) == 'r'
                && (tok.byteAt(4) | 32) == 't';
    }

    public static boolean isChunked(@Nullable Utf8Sequence tok) {
        return tok != null && tok.size() == 7
                && (tok.byteAt(0) | 32) == 'c'
                && (tok.byteAt(1) | 32) == 'h'
                && (tok.byteAt(2) | 32) == 'u'
                && (tok.byteAt(3) | 32) == 'n'
                && (tok.byteAt(4) | 32) == 'k'
                && (tok.byteAt(5) | 32) == 'e'
                && (tok.byteAt(6) | 32) == 'd';
    }

    public static boolean isClose(@Nullable Utf8Sequence tok) {
        return tok != null && tok.size() == 5
                && (tok.byteAt(0) | 32) == 'c'
                && (tok.byteAt(1) | 32) == 'l'
                && (tok.byteAt(2) | 32) == 'o'
                && (tok.byteAt(3) | 32) == 's'
                && (tok.byteAt(4) | 32) == 'e';
    }

    public static boolean isCon(@Nullable Utf8Sequence tok) {
        return tok != null && tok.size() == 3
                && (tok.byteAt(0) | 32) == 'c'
                && (tok.byteAt(1) | 32) == 'o'
                && (tok.byteAt(2) | 32) == 'n';
    }

    /**
     * Returns true when the value of a Connection header lists the close option, with which
     * the client asks the server to close the connection after the response (RFC 9112, 9.6).
     * The value is a comma-separated list of case-insensitive tokens, with optional
     * whitespace around each, and empty list elements, which are skipped (RFC 9110, 5.6.1).
     */
    public static boolean isConnectionClose(@Nullable Utf8Sequence connection) {
        if (connection == null) {
            return false;
        }
        final int size = connection.size();
        int lo = 0;
        while (lo < size) {
            final byte b = connection.byteAt(lo);
            if (b == ',' || b == ' ' || b == '\t') {
                lo++;
                continue;
            }
            int hi = lo + 1;
            while (hi < size && connection.byteAt(hi) != ',') {
                hi++;
            }
            int tokenHi = hi;
            // the token starts with a byte other than whitespace, so trimming stops before lo
            while (connection.byteAt(tokenHi - 1) == ' ' || connection.byteAt(tokenHi - 1) == '\t') {
                tokenHi--;
            }
            if (tokenHi - lo == 5
                    && (connection.byteAt(lo) | 32) == 'c'
                    && (connection.byteAt(lo + 1) | 32) == 'l'
                    && (connection.byteAt(lo + 2) | 32) == 'o'
                    && (connection.byteAt(lo + 3) | 32) == 's'
                    && (connection.byteAt(lo + 4) | 32) == 'e') {
                return true;
            }
            lo = hi;
        }
        return false;
    }

    public static boolean isContentTypeMultipartFormData(@Nullable Utf8Sequence tok) {
        return tok != null && tok.size() == 19
                && (tok.byteAt(0) | 32) == 'm'
                && (tok.byteAt(1) | 32) == 'u'
                && (tok.byteAt(2) | 32) == 'l'
                && (tok.byteAt(3) | 32) == 't'
                && (tok.byteAt(4) | 32) == 'i'
                && (tok.byteAt(5) | 32) == 'p'
                && (tok.byteAt(6) | 32) == 'a'
                && (tok.byteAt(7) | 32) == 'r'
                && (tok.byteAt(8) | 32) == 't'
                && (tok.byteAt(9)) == '/'
                && (tok.byteAt(10) | 32) == 'f'
                && (tok.byteAt(11) | 32) == 'o'
                && (tok.byteAt(12) | 32) == 'r'
                && (tok.byteAt(13) | 32) == 'm'
                && (tok.byteAt(14)) == '-'
                && (tok.byteAt(15) | 32) == 'd'
                && (tok.byteAt(16) | 32) == 'a'
                && (tok.byteAt(17) | 32) == 't'
                && (tok.byteAt(18) | 32) == 'a';
    }

    public static boolean isContentTypeMultipartMixed(@Nullable Utf8Sequence tok) {
        return tok != null && tok.size() == 15
                && (tok.byteAt(0) | 32) == 'm'
                && (tok.byteAt(1) | 32) == 'u'
                && (tok.byteAt(2) | 32) == 'l'
                && (tok.byteAt(3) | 32) == 't'
                && (tok.byteAt(4) | 32) == 'i'
                && (tok.byteAt(5) | 32) == 'p'
                && (tok.byteAt(6) | 32) == 'a'
                && (tok.byteAt(7) | 32) == 'r'
                && (tok.byteAt(8) | 32) == 't'
                && (tok.byteAt(9)) == '/'
                && (tok.byteAt(10) | 32) == 'm'
                && (tok.byteAt(11) | 32) == 'i'
                && (tok.byteAt(12) | 32) == 'x'
                && (tok.byteAt(13) | 32) == 'e'
                && (tok.byteAt(14) | 32) == 'd';
    }

    public static boolean isFalse(@Nullable Utf8Sequence tok) {
        return tok != null && tok.size() == 5
                && (tok.byteAt(0) | 32) == 'f'
                && (tok.byteAt(1) | 32) == 'a'
                && (tok.byteAt(2) | 32) == 'l'
                && (tok.byteAt(3) | 32) == 's'
                && (tok.byteAt(4) | 32) == 'e';
    }

    public static boolean isGET(@Nullable Utf8Sequence tok) {
        return tok != null && tok.size() == 3
                && (tok.byteAt(0)) == 'G'
                && (tok.byteAt(1)) == 'E'
                && (tok.byteAt(2)) == 'T';
    }

    public static boolean isHeaderSetCookie(@Nullable Utf8Sequence tok) {
        return tok != null && tok.size() == 10
                && (tok.byteAt(0) | 32) == 's'
                && (tok.byteAt(1) | 32) == 'e'
                && (tok.byteAt(2) | 32) == 't'
                && (tok.byteAt(3)) == '-'
                && (tok.byteAt(4) | 32) == 'c'
                && (tok.byteAt(5) | 32) == 'o'
                && (tok.byteAt(6) | 32) == 'o'
                && (tok.byteAt(7) | 32) == 'k'
                && (tok.byteAt(8) | 32) == 'i'
                && (tok.byteAt(9) | 32) == 'e';
    }

    public static boolean isJson(@Nullable Utf8Sequence tok) {
        return tok != null && tok.size() == 4
                && (tok.byteAt(0) | 32) == 'j'
                && (tok.byteAt(1) | 32) == 's'
                && (tok.byteAt(2) | 32) == 'o'
                && (tok.byteAt(3) | 32) == 'n';
    }

    public static boolean isPOST(@Nullable Utf8Sequence tok) {
        return tok != null && tok.size() == 4
                && (tok.byteAt(0)) == 'P'
                && (tok.byteAt(1)) == 'O'
                && (tok.byteAt(2)) == 'S'
                && (tok.byteAt(3)) == 'T';
    }

    public static boolean isPUT(@Nullable Utf8Sequence tok) {
        return tok != null && tok.size() == 3
                && (tok.byteAt(0)) == 'P'
                && (tok.byteAt(1)) == 'U'
                && (tok.byteAt(2)) == 'T';
    }

    public static boolean isSkipRow(@Nullable Utf8Sequence tok) {
        return tok != null && tok.size() == 7
                && (tok.byteAt(0) | 32) == 's'
                && (tok.byteAt(1) | 32) == 'k'
                && (tok.byteAt(2) | 32) == 'i'
                && (tok.byteAt(3) | 32) == 'p'
                && (tok.byteAt(4) | 32) == 'r'
                && (tok.byteAt(5) | 32) == 'o'
                && (tok.byteAt(6) | 32) == 'w';
    }

    public static boolean isTrue(@Nullable Utf8Sequence tok) {
        return tok != null && tok.size() == 4
                && (tok.byteAt(0) | 32) == 't'
                && (tok.byteAt(1) | 32) == 'r'
                && (tok.byteAt(2) | 32) == 'u'
                && (tok.byteAt(3) | 32) == 'e';
    }

}
