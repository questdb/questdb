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

import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.Nullable;

import java.util.Locale;

/**
 * An immutable snapshot of the host names an HTTP server serves when it runs
 * without authentication, which protects it from DNS rebinding: a web page whose
 * host name its owner re-points at the server's address afterwards reads the
 * server's responses as same-origin ones, and its requests carry that host name
 * in the Host header.
 * <p>
 * An attacker can make public DNS point only names they can register at the
 * server, so the server always serves IP addresses, single-label names such as
 * localhost or a Docker service name, and the special-use domains localhost,
 * local, internal, home.arpa and test. The configuration adds host names, and
 * whole domains written with a leading dot, such as {@code .example.com} for
 * example.com and its subdomains; {@code *} serves any host name.
 */
public final class HttpAllowedHosts {
    public static final HttpAllowedHosts ANY = new HttpAllowedHosts(true, new ObjList<>(), new ObjList<>());
    public static final HttpAllowedHosts DEFAULT = new HttpAllowedHosts(false, new ObjList<>(), new ObjList<>());
    // no public DNS server answers for these domains (RFC 6761, RFC 6762, RFC 8375, ICANN)
    private static final String[] SPECIAL_USE_DOMAINS = {".home.arpa", ".internal", ".local", ".localhost", ".test"};
    // configured domains, each with its leading dot
    private final ObjList<String> domains;
    private final boolean isAnyHostAllowed;
    private final ObjList<String> names;

    private HttpAllowedHosts(boolean isAnyHostAllowed, ObjList<String> names, ObjList<String> domains) {
        this.isAnyHostAllowed = isAnyHostAllowed;
        this.names = names;
        this.domains = domains;
    }

    public static HttpAllowedHosts parse(@Nullable String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT;
        }
        final ObjList<String> names = new ObjList<>();
        final ObjList<String> domains = new ObjList<>();
        boolean isAnyHostAllowed = false;
        for (String part : value.split(",", -1)) {
            final String entry = part.trim();
            final String reason = validate(part, entry);
            if (reason != null) {
                throw new IllegalArgumentException("expected comma-separated host names, domains written with a leading dot such as .example.com, or *; "
                        + reason + ": " + entry);
            }
            if (entry.equals("*")) {
                isAnyHostAllowed = true;
                continue;
            }
            // a fully qualified name may end with a dot, the Host header may omit it
            final String name = (entry.endsWith(".") ? entry.substring(0, entry.length() - 1) : entry).toLowerCase(Locale.ROOT);
            if (name.charAt(0) == '.') {
                domains.add(name);
            } else {
                names.add(name);
            }
        }
        return isAnyHostAllowed ? ANY : new HttpAllowedHosts(false, names, domains);
    }

    /**
     * Returns true when the server serves the given Host header value: a host
     * name, with or without a port. A request without Host is served, a browser
     * always sends one.
     */
    public boolean isAllowed(@Nullable Utf8Sequence host) {
        if (isAnyHostAllowed || host == null) {
            return true;
        }
        final int size = host.size();
        if (size == 0 || host.byteAt(0) == '[') {
            // an IPv6 address
            return true;
        }
        int hi = 0;
        while (hi < size && host.byteAt(hi) != ':') {
            hi++;
        }
        if (hi > 0 && host.byteAt(hi - 1) == '.') {
            hi--;
        }
        int lastDot = hi - 1;
        while (lastDot > -1 && host.byteAt(lastDot) != '.') {
            lastDot--;
        }
        // a browser takes a name whose last label is a number for an IPv4 address
        if (lastDot < 0 || isNumber(host, lastDot + 1, hi)) {
            return true;
        }
        for (String domain : SPECIAL_USE_DOMAINS) {
            if (isInDomain(host, hi, domain)) {
                return true;
            }
        }
        for (int i = 0, n = names.size(); i < n; i++) {
            if (equalsIgnoreCase(host, 0, hi, names.getQuick(i), 0)) {
                return true;
            }
        }
        for (int i = 0, n = domains.size(); i < n; i++) {
            final String domain = domains.getQuick(i);
            if (isInDomain(host, hi, domain) || equalsIgnoreCase(host, 0, hi, domain, 1)) {
                return true;
            }
        }
        return false;
    }

    // compares host[lo, hi) with name[nameLo, name.length()), the latter lowercase ASCII
    private static boolean equalsIgnoreCase(Utf8Sequence host, int lo, int hi, String name, int nameLo) {
        if (hi - lo != name.length() - nameLo) {
            return false;
        }
        for (int i = lo, j = nameLo; i < hi; i++, j++) {
            if (toLowerAscii(host.byteAt(i)) != name.charAt(j)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isHexDigit(byte b) {
        return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F');
    }

    // whether host[0, hi) ends with the domain, a lowercase ASCII suffix that starts with a dot
    private static boolean isInDomain(Utf8Sequence host, int hi, String domain) {
        return hi > domain.length() && equalsIgnoreCase(host, hi - domain.length(), hi, domain, 0);
    }

    // WHATWG URL "ends in a number": decimal digits, or 0x followed by hexadecimal digits
    private static boolean isNumber(Utf8Sequence host, int lo, int hi) {
        if (lo == hi) {
            return false;
        }
        if (hi - lo > 1 && host.byteAt(lo) == '0' && (host.byteAt(lo + 1) == 'x' || host.byteAt(lo + 1) == 'X')) {
            for (int i = lo + 2; i < hi; i++) {
                if (!isHexDigit(host.byteAt(i))) {
                    return false;
                }
            }
            return true;
        }
        for (int i = lo; i < hi; i++) {
            final byte b = host.byteAt(i);
            if (b < '0' || b > '9') {
                return false;
            }
        }
        return true;
    }

    private static char toLowerAscii(byte b) {
        return (char) (b >= 'A' && b <= 'Z' ? b + ('a' - 'A') : b & 0xff);
    }

    // Returns null when the entry is valid, or the reason it is not.
    private static @Nullable String validate(String part, String entry) {
        for (int i = 0, n = part.length(); i < n; i++) {
            final char c = part.charAt(i);
            if (c < ' ' && c != '\t') {
                return "control character";
            }
        }
        if (entry.isEmpty()) {
            return "empty entry, check for a stray comma";
        }
        if (entry.equals("*")) {
            return null;
        }
        if (entry.startsWith("*.")) {
            return "write .example.com to serve example.com and its subdomains, no other wildcard is supported";
        }
        if (entry.charAt(0) == '[') {
            return "IP addresses are always served, list host names only";
        }
        if (entry.indexOf(':') > -1) {
            return "host names are listed without a port";
        }
        for (int i = 0, n = entry.length(); i < n; i++) {
            final char c = entry.charAt(i);
            if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.')) {
                return "a host name may contain only letters, digits, '-', '_' and '.'";
            }
        }
        // a leading dot marks a domain, a trailing one a fully qualified name; labels are not empty
        final int lo = entry.charAt(0) == '.' ? 1 : 0;
        final int hi = entry.length() > lo && entry.charAt(entry.length() - 1) == '.' ? entry.length() - 1 : entry.length();
        if (lo >= hi) {
            return "empty host name";
        }
        int labelLo = lo;
        for (int i = lo; i <= hi; i++) {
            if (i == hi || entry.charAt(i) == '.') {
                if (i == labelLo) {
                    return "empty label";
                }
                labelLo = i + 1;
            }
        }
        if (isNumber(new Utf8String(entry), Math.max(entry.lastIndexOf('.', hi - 1) + 1, lo), hi)) {
            return "IP addresses are always served, list host names only";
        }
        return null;
    }
}
