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

package io.questdb.cutlass.qwp.server.egress;

import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.std.str.Utf8Sequence;

import java.nio.charset.StandardCharsets;

/**
 * Parses the {@code X-QWP-Accept-Encoding} handshake header and picks the
 * first codec the server supports. The header follows HTTP's
 * {@code Accept-Encoding} grammar loosely:
 * <pre>
 *     X-QWP-Accept-Encoding: zstd;level=1, raw
 * </pre>
 * Tokens are matched case-insensitively and separated by commas. Parameters
 * (the {@code ;level=N} segment) are recognised only for {@code zstd}. The
 * level is clamped to {@link QwpConstants#COMPRESSION_ZSTD_MIN_LEVEL} ..
 * {@link QwpConstants#COMPRESSION_ZSTD_MAX_LEVEL} to bound server CPU cost.
 * <p>
 * The parsed result is returned as a single {@code long} to avoid allocating
 * a result record on the handshake path: the low byte carries the chosen
 * codec ({@code COMPRESSION_NONE} / {@code COMPRESSION_ZSTD}), the next byte
 * carries the clamped level.
 */
public final class QwpEgressCompressionNegotiator {

    /**
     * Sentinel returned when the header is absent or no supported codec is listed.
     */
    public static final long RESULT_NONE = 0L;
    // Precomputed X-QWP-Content-Encoding header bytes per zstd level so the
    // handshake response writer avoids a per-call String concatenation +
    // getBytes allocation. Levels outside [COMPRESSION_ZSTD_MIN_LEVEL,
    // COMPRESSION_ZSTD_MAX_LEVEL] are never produced by negotiate() and any
    // out-of-band lookup will trip ArrayIndexOutOfBoundsException -- a loud
    // failure is preferred over silently returning a wrong/raw header value.
    private static final byte[][] ZSTD_LEVEL_BYTES = buildZstdLevelBytes();

    private QwpEgressCompressionNegotiator() {
    }

    public static byte codec(long result) {
        return (byte) (result & 0xFF);
    }

    public static byte level(long result) {
        return (byte) ((result >>> 8) & 0xFF);
    }

    /**
     * Walks comma-separated tokens in preference order and returns the first one
     * whose codec the server supports. Unknown tokens are skipped rather than
     * rejected so clients can safely advertise codecs the server hasn't shipped
     * yet (e.g. {@code br, zstd, raw}).
     */
    public static long negotiate(Utf8Sequence header) {
        if (header == null) {
            return RESULT_NONE;
        }
        int size = header.size();
        int i = 0;
        while (i < size) {
            while (i < size && isAsciiWhitespace(header.byteAt(i))) {
                i++;
            }
            int tokenStart = i;
            while (i < size && header.byteAt(i) != ',' && header.byteAt(i) != ';') {
                i++;
            }
            int tokenEnd = i;
            int paramStart = -1;
            int paramEnd = -1;
            if (i < size && header.byteAt(i) == ';') {
                paramStart = i + 1;
                while (i < size && header.byteAt(i) != ',') {
                    i++;
                }
                paramEnd = i;
            }
            if (i < size && header.byteAt(i) == ',') {
                i++;
            }
            // Trim trailing whitespace on the token name.
            while (tokenEnd > tokenStart && isAsciiWhitespace(header.byteAt(tokenEnd - 1))) {
                tokenEnd--;
            }
            if (matchesAsciiIgnoreCase(header, tokenStart, tokenEnd, "zstd")) {
                int level = parseLevel(header, paramStart, paramEnd);
                if (level < QwpConstants.COMPRESSION_ZSTD_MIN_LEVEL) {
                    level = QwpConstants.COMPRESSION_ZSTD_MIN_LEVEL;
                } else if (level > QwpConstants.COMPRESSION_ZSTD_MAX_LEVEL) {
                    level = QwpConstants.COMPRESSION_ZSTD_MAX_LEVEL;
                }
                return pack(QwpConstants.COMPRESSION_ZSTD, (byte) level);
            }
            if (matchesAsciiIgnoreCase(header, tokenStart, tokenEnd, "raw")
                    || matchesAsciiIgnoreCase(header, tokenStart, tokenEnd, "identity")) {
                return RESULT_NONE;
            }
        }
        return RESULT_NONE;
    }

    /**
     * Applies the operator-side forced-level override, if any, on top of the
     * client-negotiated zstd level. Returns the level the server should
     * actually use for the connection's encoder and echo back in the
     * {@code X-QWP-Content-Encoding} response header.
     * <ul>
     *   <li>{@code forcedLevel == 0} -- no override; honor the client's
     *       choice verbatim.</li>
     *   <li>{@code negotiatedCodec != COMPRESSION_ZSTD} -- override is a
     *       no-op; raw transport stays raw.</li>
     *   <li>Otherwise the forced level wins, clamped into
     *       {@code [COMPRESSION_ZSTD_MIN_LEVEL, COMPRESSION_ZSTD_MAX_LEVEL]}
     *       as defense-in-depth so a misconfigured property can't escape the
     *       wire-allowed range.</li>
     * </ul>
     * The operator override deliberately ignores the client's request: an
     * operator capping a misbehaving client at level 1 must beat a client
     * that asks for level 9.
     */
    public static byte resolveEffectiveZstdLevel(byte negotiatedCodec, byte negotiatedLevel, int forcedLevel) {
        if (forcedLevel == 0 || negotiatedCodec != QwpConstants.COMPRESSION_ZSTD) {
            return negotiatedLevel;
        }
        if (forcedLevel < QwpConstants.COMPRESSION_ZSTD_MIN_LEVEL) {
            return (byte) QwpConstants.COMPRESSION_ZSTD_MIN_LEVEL;
        }
        if (forcedLevel > QwpConstants.COMPRESSION_ZSTD_MAX_LEVEL) {
            return (byte) QwpConstants.COMPRESSION_ZSTD_MAX_LEVEL;
        }
        return (byte) forcedLevel;
    }

    /**
     * Returns the precomputed {@code X-QWP-Content-Encoding} response header
     * bytes for the negotiated codec, or {@code null} when the server chose
     * raw transport (the header is then omitted entirely). The returned array
     * is interned in a static table -- callers must not mutate it.
     */
    public static byte[] responseHeaderValue(byte codec, byte level) {
        if (codec == QwpConstants.COMPRESSION_ZSTD) {
            return ZSTD_LEVEL_BYTES[level & 0xFF];
        }
        return null;
    }

    private static byte[][] buildZstdLevelBytes() {
        byte[][] table = new byte[QwpConstants.COMPRESSION_ZSTD_MAX_LEVEL + 1][];
        for (int level = QwpConstants.COMPRESSION_ZSTD_MIN_LEVEL; level <= QwpConstants.COMPRESSION_ZSTD_MAX_LEVEL; level++) {
            table[level] = ("zstd;level=" + level).getBytes(StandardCharsets.US_ASCII);
        }
        return table;
    }

    private static boolean isAsciiWhitespace(byte b) {
        return b == ' ' || b == '\t';
    }

    private static boolean matchesAsciiIgnoreCase(Utf8Sequence seq, int from, int to, String target) {
        int len = to - from;
        if (len != target.length()) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            byte a = seq.byteAt(from + i);
            char b = target.charAt(i);
            if (toLowerAscii(a) != toLowerAscii((byte) b)) {
                return false;
            }
        }
        return true;
    }

    private static long pack(byte codec, byte level) {
        return ((long) (level & 0xFF) << 8) | ((long) codec & 0xFF);
    }

    /**
     * Accepts either {@code level=N} or {@code N} (bare integer). Returns 1 as
     * the default when the parameter is missing or unparseable -- the cheapest
     * zstd level, picked so a client that opts in to compression without
     * naming a level doesn't pay surprise server-side CPU.
     */
    private static int parseLevel(Utf8Sequence seq, int start, int end) {
        if (start < 0 || end <= start) {
            return 1;
        }
        while (start < end && isAsciiWhitespace(seq.byteAt(start))) {
            start++;
        }
        while (end > start && isAsciiWhitespace(seq.byteAt(end - 1))) {
            end--;
        }
        // Skip optional "level=" prefix.
        int keyLen = 6;
        if (end - start >= keyLen) {
            boolean hasPrefix = true;
            for (int i = 0; i < keyLen; i++) {
                byte a = seq.byteAt(start + i);
                char b = "level=".charAt(i);
                if (toLowerAscii(a) != toLowerAscii((byte) b)) {
                    hasPrefix = false;
                    break;
                }
            }
            if (hasPrefix) {
                start += keyLen;
            }
        }
        int value = 0;
        boolean anyDigit = false;
        while (start < end) {
            byte b = seq.byteAt(start);
            if (b < '0' || b > '9') {
                break;
            }
            anyDigit = true;
            value = value * 10 + (b - '0');
            if (value > 1000) {
                value = 1000;
                break;
            }
            start++;
        }
        return anyDigit ? value : 1;
    }

    private static byte toLowerAscii(byte b) {
        return (b >= 'A' && b <= 'Z') ? (byte) (b + 32) : b;
    }
}
