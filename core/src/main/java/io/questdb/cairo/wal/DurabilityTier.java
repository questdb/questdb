/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import org.jetbrains.annotations.Nullable;

/**
 * Durability tiers for the QWP durable-ack, encoded as a bitmask so a
 * connection can be granted more than one independent ack stream:
 * <ul>
 *   <li>{@link #LOCAL} — the sequencer record is fdatasync-durable; the server
 *       emits {@code STATUS_LOCAL_DURABLE_ACK} frames (power-loss-safe).</li>
 *   <li>{@link #REPLICATED} — the commit is uploaded to the replica store; the
 *       server emits {@code STATUS_DURABLE_ACK} frames (failover-safe).</li>
 * </ul>
 * {@link #NONE} means durable-ack is off. {@link #LEGACY_TRUE} is a modifier
 * bit recording that the request arrived as the shipped literal {@code "true"},
 * which means {@link #REPLICATED} and must be confirmed with the historical
 * {@code "enabled"} token so released clients see byte-identical responses.
 * <p>
 * The grant is all-or-nothing: the server confirms the full requested set or
 * denies the whole request (no confirmation header). A request for
 * {@code replicated} never silently degrades to a local-only guarantee.
 */
public final class DurabilityTier {
    public static final int NONE = 0;
    public static final int LOCAL = 1;
    public static final int REPLICATED = 2;
    // Modifier bit, only ever combined with REPLICATED: the request used the
    // legacy "true" token, so the confirmation must echo "enabled".
    public static final int LEGACY_TRUE = 4;
    public static final int TIERS_MASK = LOCAL | REPLICATED;

    private static final Utf8String TOKEN_TRUE = new Utf8String("true");
    private static final Utf8String TOKEN_ENABLED = new Utf8String("enabled");
    private static final Utf8String TOKEN_LOCAL = new Utf8String("local");
    private static final Utf8String TOKEN_REPLICATED = new Utf8String("replicated");
    private static final Utf8String TOKEN_LOCAL_REPLICATED = new Utf8String("local,replicated");
    private static final Utf8String TOKEN_REPLICATED_LOCAL = new Utf8String("replicated,local");

    private DurabilityTier() {
    }

    /**
     * Parse the X-QWP-Request-Durable-Ack header value into a requested tier
     * set. Returns {@link #NONE} for an absent header and for any value that
     * is not exactly one of the known tokens — an opted-in client then gets
     * no confirmation header and fails loudly at the handshake.
     */
    public static int fromHeaderValue(@Nullable Utf8Sequence v) {
        if (v == null) {
            return NONE;
        }
        if (Utf8s.equalsIgnoreCaseAscii(v, TOKEN_TRUE)) {
            return REPLICATED | LEGACY_TRUE;
        }
        if (Utf8s.equalsIgnoreCaseAscii(v, TOKEN_LOCAL)) {
            return LOCAL;
        }
        if (Utf8s.equalsIgnoreCaseAscii(v, TOKEN_REPLICATED)) {
            return REPLICATED;
        }
        if (Utf8s.equalsIgnoreCaseAscii(v, TOKEN_LOCAL_REPLICATED)
                || Utf8s.equalsIgnoreCaseAscii(v, TOKEN_REPLICATED_LOCAL)) {
            return LOCAL | REPLICATED;
        }
        return NONE;
    }

    /**
     * True when the tier set includes the {@link #LOCAL} stream.
     */
    public static boolean hasLocal(int tiers) {
        return (tiers & LOCAL) != 0;
    }

    /**
     * True when the tier set includes the {@link #REPLICATED} stream.
     */
    public static boolean hasReplicated(int tiers) {
        return (tiers & REPLICATED) != 0;
    }

    /**
     * The X-QWP-Durable-Ack confirmation token for a granted tier set, or null
     * when nothing was granted. A {@link #LEGACY_TRUE} grant echoes the
     * historical {@code "enabled"} token, byte-identical to pre-tier servers.
     */
    public static @Nullable Utf8String responseToken(int granted) {
        if ((granted & LEGACY_TRUE) != 0) {
            return TOKEN_ENABLED;
        }
        return switch (granted & TIERS_MASK) {
            case LOCAL -> TOKEN_LOCAL;
            case REPLICATED -> TOKEN_REPLICATED;
            case LOCAL | REPLICATED -> TOKEN_LOCAL_REPLICATED;
            default -> null;
        };
    }
}
