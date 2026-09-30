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

package io.questdb.cutlass.qwp.server;

import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;

import java.util.Locale;

/**
 * An immutable, pre-encoded snapshot of the browser origins allowed on QWP WebSockets.
 * <p>
 * Entries are compared byte for byte with the Origin header, so each entry must
 * be written exactly as a browser serializes an origin: a lowercase http or https
 * scheme, a lowercase host (underscores allowed), no default port, and IP
 * literals in their canonical form. Entries a browser could never send are
 * rejected rather than normalized, so the configured value always equals the
 * value on the wire.
 */
public final class QwpBrowserAllowedOrigins {
    public static final QwpBrowserAllowedOrigins EMPTY = new QwpBrowserAllowedOrigins(new ObjList<>());

    private final ObjList<Utf8String> origins;

    private QwpBrowserAllowedOrigins(ObjList<Utf8String> origins) {
        this.origins = origins;
    }

    public static QwpBrowserAllowedOrigins parse(String value) {
        if (value == null || value.isBlank()) {
            return EMPTY;
        }
        ObjList<Utf8String> origins = new ObjList<>();
        for (String part : value.split(",", -1)) {
            String origin = part.trim();
            boolean hasControl = false;
            for (int i = 0; i < part.length(); i++) {
                char c = part.charAt(i);
                if (c < ' ' && c != '\t') {
                    hasControl = true;
                    break;
                }
            }
            final String error = hasControl ? "control character" : validateSerializedOrigin(origin);
            if (error != null) {
                throw new IllegalArgumentException("expected comma-separated http(s) origins written exactly as browsers send them in the Origin header, without paths or wildcards; "
                        + error + ": " + origin);
            }
            origins.add(new Utf8String(origin));
        }
        return new QwpBrowserAllowedOrigins(origins);
    }

    public boolean isAllowed(Utf8Sequence origin) {
        if (origin == null) {
            return false;
        }
        for (int i = 0, n = origins.size(); i < n; i++) {
            if (Utf8s.equals(origin, origins.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    private static int hexDigit(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        }
        if (c >= 'A' && c <= 'F') {
            return c - 'A' + 10;
        }
        return -1;
    }

    private static boolean isCanonicalIpv4(String value, int lo, int hi) {
        int parts = 0;
        for (int partLo = lo; partLo <= hi; ) {
            int partHi = value.indexOf('.', partLo);
            if (partHi < 0 || partHi > hi) {
                partHi = hi;
            }
            final int length = partHi - partLo;
            if (length == 0 || length > 3 || (length > 1 && value.charAt(partLo) == '0')) {
                return false;
            }
            int part = 0;
            for (int i = partLo; i < partHi; i++) {
                final char c = value.charAt(i);
                if (c < '0' || c > '9') {
                    return false;
                }
                part = part * 10 + c - '0';
            }
            if (part > 255) {
                return false;
            }
            parts++;
            partLo = partHi + 1;
        }
        return parts == 4;
    }

    // WHATWG URL "ends in a number": a host whose last label parses as an IPv4
    // number is an IPv4 address, which browsers serialize as dotted decimal.
    private static boolean isIpv4Host(String value, int lo, int hi) {
        if (hi > lo && value.charAt(hi - 1) == '.') {
            hi--;
        }
        final int lastLo = value.lastIndexOf('.', hi - 1) + 1;
        final int labelLo = Math.max(lastLo, lo);
        if (labelLo == hi) {
            return false;
        }
        if (hi - labelLo >= 2 && value.charAt(labelLo) == '0' && value.charAt(labelLo + 1) == 'x') {
            for (int i = labelLo + 2; i < hi; i++) {
                if (hexDigit(value.charAt(i)) < 0) {
                    return false;
                }
            }
            return true;
        }
        for (int i = labelLo; i < hi; i++) {
            final char c = value.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    // Returns the WHATWG URL serialization of an IPv6 literal (without brackets),
    // or null when the text is not an IPv6 address in hexadecimal notation.
    private static @Nullable String serializeIpv6(String value, int lo, int hi) {
        final int[] pieces = new int[8];
        int pieceIndex = 0;
        int compress = -1;
        int p = lo;
        if (p < hi && value.charAt(p) == ':') {
            if (p + 1 >= hi || value.charAt(p + 1) != ':') {
                return null;
            }
            p += 2;
            compress = ++pieceIndex;
        }
        while (p < hi) {
            if (pieceIndex == 8) {
                return null;
            }
            if (value.charAt(p) == ':') {
                if (compress != -1) {
                    return null;
                }
                p++;
                compress = ++pieceIndex;
                continue;
            }
            int piece = 0;
            int length = 0;
            while (length < 4 && p < hi && hexDigit(value.charAt(p)) >= 0) {
                piece = piece * 16 + hexDigit(value.charAt(p++));
                length++;
            }
            if (length == 0) {
                return null;
            }
            if (p < hi) {
                if (value.charAt(p) != ':' || ++p == hi) {
                    return null;
                }
            }
            pieces[pieceIndex++] = piece;
        }
        if (compress != -1) {
            int swaps = pieceIndex - compress;
            pieceIndex = 7;
            while (pieceIndex != 0 && swaps > 0) {
                final int swapIndex = compress + swaps - 1;
                final int tmp = pieces[pieceIndex];
                pieces[pieceIndex] = pieces[swapIndex];
                pieces[swapIndex] = tmp;
                pieceIndex--;
                swaps--;
            }
        } else if (pieceIndex != 8) {
            return null;
        }

        // compress the first longest run of two or more zero pieces
        int compressLo = -1;
        int compressLength = 1;
        for (int i = 0; i < 8; ) {
            if (pieces[i] != 0) {
                i++;
                continue;
            }
            int j = i;
            while (j < 8 && pieces[j] == 0) {
                j++;
            }
            if (j - i > compressLength) {
                compressLo = i;
                compressLength = j - i;
            }
            i = j;
        }
        final StringBuilder sink = new StringBuilder(39);
        for (int i = 0; i < 8; i++) {
            if (i == compressLo) {
                sink.append(i == 0 ? "::" : ":");
                i += compressLength - 1;
                continue;
            }
            sink.append(Integer.toHexString(pieces[i]));
            if (i != 7) {
                sink.append(':');
            }
        }
        return sink.toString();
    }

    // Returns null when the value is a browser-serialized http(s) origin, or the
    // reason it is not. A browser sends a lowercase scheme and host, omits the
    // default port and writes IP literals canonically; an entry in any other
    // form could never match the Origin header byte for byte.
    private static @Nullable String validateSerializedOrigin(String value) {
        final int hostLo;
        final int defaultPort;
        if (value.startsWith("https://")) {
            hostLo = 8;
            defaultPort = 443;
        } else if (value.startsWith("http://")) {
            hostLo = 7;
            defaultPort = 80;
        } else {
            return "the scheme must be http or https";
        }
        final int n = value.length();
        for (int i = hostLo; i < n; i++) {
            final char c = value.charAt(i);
            if (c <= ' ' || c > '~') {
                return "unexpected character";
            }
        }

        final int hostHi;
        if (hostLo < n && value.charAt(hostLo) == '[') {
            final int close = value.indexOf(']', hostLo);
            if (close < 0) {
                return "unterminated IPv6 address";
            }
            final String canonical = serializeIpv6(value, hostLo + 1, close);
            if (canonical == null) {
                return "not an IPv6 address in hexadecimal notation";
            }
            if (!value.regionMatches(hostLo + 1, canonical, 0, close - hostLo - 1) || canonical.length() != close - hostLo - 1) {
                return "the IPv6 address is not canonical, browsers send [" + canonical + ']';
            }
            hostHi = close + 1;
        } else {
            final int colon = value.indexOf(':', hostLo);
            hostHi = colon < 0 ? n : colon;
            if (hostHi == hostLo) {
                return "empty host";
            }
            boolean hasUppercase = false;
            for (int i = hostLo; i < hostHi; i++) {
                final char c = value.charAt(i);
                if (c >= 'A' && c <= 'Z') {
                    hasUppercase = true;
                } else if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.')) {
                    return "the host may contain only letters, digits, '-', '_' and '.'";
                }
                // labels are separated by single dots; only the last may be empty (a trailing dot)
                if (c == '.' && (i == hostLo || value.charAt(i - 1) == '.')) {
                    return "empty host label";
                }
            }
            final String host = hasUppercase ? value.substring(hostLo, hostHi).toLowerCase(Locale.ROOT) : value;
            final int lo = hasUppercase ? 0 : hostLo;
            final int hi = hasUppercase ? host.length() : hostHi;
            if (isIpv4Host(host, lo, hi) && !isCanonicalIpv4(host, lo, hi)) {
                return "the IPv4 address is not in canonical dotted-decimal form";
            }
            if (hasUppercase) {
                return "the host is not lowercase, browsers send " + value.substring(0, hostLo) + host + value.substring(hostHi);
            }
        }

        if (hostHi == n) {
            return null;
        }
        if (value.charAt(hostHi) != ':' || hostHi + 1 == n) {
            return "unexpected character after the host";
        }
        final int portLo = hostHi + 1;
        if (n - portLo > 5 || value.charAt(portLo) == '0') {
            return "invalid port";
        }
        int port = 0;
        for (int i = portLo; i < n; i++) {
            final char c = value.charAt(i);
            if (c < '0' || c > '9') {
                return "invalid port";
            }
            port = port * 10 + c - '0';
        }
        if (port > 65_535) {
            return "invalid port";
        }
        if (port == defaultPort) {
            return "the default port is not sent, browsers send " + value.substring(0, hostHi);
        }
        return null;
    }
}
