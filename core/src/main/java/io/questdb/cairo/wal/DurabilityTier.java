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
