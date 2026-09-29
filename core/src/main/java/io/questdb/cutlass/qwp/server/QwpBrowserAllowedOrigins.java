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

import java.net.URI;
import java.net.URISyntaxException;

/** An immutable, pre-encoded snapshot of the browser origins allowed on QWP WebSockets. */
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
            if (hasControl || !isSerializedOrigin(origin)) {
                throw new IllegalArgumentException("expected comma-separated, exact http(s) origins without paths or wildcards: " + origin);
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

    private static boolean isSerializedOrigin(String value) {
        // An Origin header contains a serialized, ASCII scheme and authority,
        // never a URL with a path, credentials, query or fragment. Do not
        // normalize: the configured value must match the wire value exactly.
        if ((!value.startsWith("http://") && !value.startsWith("https://")) || value.indexOf('*') >= 0) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) <= ' ' || value.charAt(i) > '~') {
                return false;
            }
        }
        try {
            URI uri = new URI(value);
            if (uri.getHost() == null || uri.getRawUserInfo() != null
                    || !uri.getRawPath().isEmpty() || uri.getRawQuery() != null || uri.getRawFragment() != null) {
                return false;
            }
            int port = uri.getPort();
            return port <= 65_535 && port != 0 && uri.getRawAuthority().equals(
                    port == -1 ? uri.getHost() : uri.getHost() + ":" + port);
        } catch (URISyntaxException e) {
            return false;
        }
    }
}
