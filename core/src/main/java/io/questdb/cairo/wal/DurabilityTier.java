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

package io.questdb.cairo.wal;

import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

/**
 * Durability tiers for the QWP durable-ack. Ordered by strength:
 * {@link #LOCAL} (power-loss-safe, adaptive fdatasync) is weaker than
 * {@link #REPLICATED} (failover-safe, object-store upload). {@link #NONE}
 * means durable-ack is off; {@link #DEFAULT} is the legacy {@code "true"}
 * request intent, resolved to the server's strongest available tier.
 */
public final class DurabilityTier {
    public static final int NONE = -1;
    public static final int LOCAL = 0;
    public static final int REPLICATED = 1;
    public static final int DEFAULT = 2;

    private static final Utf8String TOKEN_TRUE = new Utf8String("true");
    private static final Utf8String TOKEN_LOCAL = new Utf8String("local");
    private static final Utf8String TOKEN_REPLICATED = new Utf8String("replicated");

    private DurabilityTier() {
    }

    /** Parse the X-QWP-Request-Durable-Ack header value into a request intent. */
    public static int fromHeaderValue(Utf8Sequence v) {
        if (v == null) {
            return NONE;
        }
        if (Utf8s.equalsIgnoreCaseAscii(v, TOKEN_TRUE)) {
            return DEFAULT;
        }
        if (Utf8s.equalsIgnoreCaseAscii(v, TOKEN_LOCAL)) {
            return LOCAL;
        }
        if (Utf8s.equalsIgnoreCaseAscii(v, TOKEN_REPLICATED)) {
            return REPLICATED;
        }
        return NONE;
    }

    /** The confirmation token echoed for an explicitly-granted tier, or null. */
    public static Utf8String responseToken(int tier) {
        switch (tier) {
            case LOCAL:
                return TOKEN_LOCAL;
            case REPLICATED:
                return TOKEN_REPLICATED;
            default:
                return null;
        }
    }
}
