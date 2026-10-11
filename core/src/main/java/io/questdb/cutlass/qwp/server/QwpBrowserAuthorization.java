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

import io.questdb.std.Numbers;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;

import java.util.Arrays;
import java.util.Base64;

/**
 * Extracts a browser's Authorization value without materializing the secret as a String.
 */
public final class QwpBrowserAuthorization {
    private static final String PREFIX = "questdb.qwp.authorization.";

    private QwpBrowserAuthorization() {
    }

    /**
     * Decodes exactly one unpadded base64url credential; rejects ambiguous or malformed offers.
     */
    public static boolean decode(Utf8Sequence protocols, DirectUtf8Sink out) {
        boolean isCredentialDecoded = false;
        for (long token = QwpIngressHttpProcessor.nextWebSocketProtocolToken(protocols, 0);
             token != -1;
             token = QwpIngressHttpProcessor.nextWebSocketProtocolToken(protocols, Numbers.decodeHighInt(token))) {
            final int start = Numbers.decodeLowInt(token);
            final int end = Numbers.decodeHighInt(token);
            if (hasPrefix(protocols, start, end)) {
                if (isCredentialDecoded || !decodeToken(protocols, start + PREFIX.length(), end, out)) {
                    return false;
                }
                isCredentialDecoded = true;
            }
        }
        return isCredentialDecoded;
    }

    public static boolean hasCredential(Utf8Sequence protocols) {
        if (protocols == null) {
            return false;
        }
        for (long token = QwpIngressHttpProcessor.nextWebSocketProtocolToken(protocols, 0);
             token != -1;
             token = QwpIngressHttpProcessor.nextWebSocketProtocolToken(protocols, Numbers.decodeHighInt(token))) {
            if (hasPrefix(protocols, Numbers.decodeLowInt(token), Numbers.decodeHighInt(token))) {
                return true;
            }
        }
        return false;
    }

    private static boolean decodeToken(Utf8Sequence protocols, int lo, int hi, DirectUtf8Sink out) {
        int length = hi - lo;
        // '=' is not a WebSocket subprotocol token character: browsers must
        // use unpadded base64url. Refuse padding and malformed tokens.
        if (length == 0 || length % 4 == 1) {
            return false;
        }
        byte[] encoded = new byte[length];
        byte[] decoded = null;
        try {
            for (int i = 0; i < length; i++) {
                byte b = protocols.byteAt(lo + i);
                if (!((b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
                        || (b >= '0' && b <= '9') || b == '-' || b == '_')) {
                    return false;
                }
                encoded[i] = b;
            }
            decoded = Base64.getUrlDecoder().decode(encoded);
            if (decoded.length == 0 || decoded[0] <= ' ' || decoded[decoded.length - 1] <= ' ') {
                return false;
            }
            for (byte b : decoded) {
                if (b < ' ' || b >= 0x7f) {
                    return false;
                }
            }
            for (byte b : decoded) {
                out.putAny(b);
            }
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        } finally {
            Arrays.fill(encoded, (byte) 0);
            if (decoded != null) {
                Arrays.fill(decoded, (byte) 0);
            }
        }
    }

    private static boolean hasPrefix(Utf8Sequence protocols, int start, int end) {
        if (end - start < PREFIX.length()) {
            return false;
        }
        for (int i = 0; i < PREFIX.length(); i++) {
            if (protocols.byteAt(start + i) != PREFIX.charAt(i)) {
                return false;
            }
        }
        return true;
    }
}
