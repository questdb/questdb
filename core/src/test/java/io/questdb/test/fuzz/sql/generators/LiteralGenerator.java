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

package io.questdb.test.fuzz.sql.generators;

import io.questdb.std.Rnd;
import io.questdb.test.fuzz.sql.GeneratorContext;

/**
 * Generates SQL literal values of various types.
 * <p>
 * Supports:
 * <ul>
 *   <li>Integer literals (including edge cases: 0, negatives, Long.MAX_VALUE)</li>
 *   <li>Float literals (including NaN, Infinity, scientific notation)</li>
 *   <li>String literals (including empty, escaped quotes, unicode)</li>
 *   <li>Boolean literals (true, false)</li>
 *   <li>Null literal</li>
 *   <li>Timestamp literals</li>
 * </ul>
 * <p>
 * QuestDB-specific types:
 * <ul>
 *   <li>Geohash char literals (e.g., #sp052w)</li>
 *   <li>Geohash bit literals (e.g., ##01110)</li>
 *   <li>IPv4 literals (e.g., '192.168.1.1')</li>
 *   <li>UUID literals</li>
 *   <li>Long256 literals</li>
 * </ul>
 */
public final class LiteralGenerator {

    // Literal type weights (relative probability)
    private static final int WEIGHT_INTEGER = 25;
    private static final int WEIGHT_FLOAT = 18;
    private static final int WEIGHT_STRING = 22;
    private static final int WEIGHT_BOOLEAN = 8;
    private static final int WEIGHT_NULL = 8;
    private static final int WEIGHT_TIMESTAMP = 5;
    // QuestDB-specific types
    private static final int WEIGHT_GEOHASH = 5;
    private static final int WEIGHT_IPV4 = 3;
    private static final int WEIGHT_UUID = 3;
    private static final int WEIGHT_LONG256 = 3;
    private static final int TOTAL_WEIGHT = WEIGHT_INTEGER + WEIGHT_FLOAT + WEIGHT_STRING
            + WEIGHT_BOOLEAN + WEIGHT_NULL + WEIGHT_TIMESTAMP + WEIGHT_GEOHASH
            + WEIGHT_IPV4 + WEIGHT_UUID + WEIGHT_LONG256;

    // Geohash base32 characters
    private static final String GEOHASH_CHARS = "0123456789bcdefghjkmnpqrstuvwxyz";

    private LiteralGenerator() {
        // Static utility class
    }

    /**
     * Generates a random literal and appends it to the context.
     */
    public static void generate(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();
        int roll = rnd.nextInt(TOTAL_WEIGHT);
        int cumulative = 0;

        cumulative += WEIGHT_INTEGER;
        if (roll < cumulative) {
            generateInteger(ctx);
            return;
        }

        cumulative += WEIGHT_FLOAT;
        if (roll < cumulative) {
            generateFloat(ctx);
            return;
        }

        cumulative += WEIGHT_STRING;
        if (roll < cumulative) {
            generateString(ctx);
            return;
        }

        cumulative += WEIGHT_BOOLEAN;
        if (roll < cumulative) {
            generateBoolean(ctx);
            return;
        }

        cumulative += WEIGHT_NULL;
        if (roll < cumulative) {
            generateNull(ctx);
            return;
        }

        cumulative += WEIGHT_TIMESTAMP;
        if (roll < cumulative) {
            generateTimestamp(ctx);
            return;
        }

        // QuestDB-specific types
        cumulative += WEIGHT_GEOHASH;
        if (roll < cumulative) {
            generateGeohash(ctx);
            return;
        }

        cumulative += WEIGHT_IPV4;
        if (roll < cumulative) {
            generateIPv4(ctx);
            return;
        }

        cumulative += WEIGHT_UUID;
        if (roll < cumulative) {
            generateUUID(ctx);
            return;
        }

        generateLong256(ctx);
    }

    /**
     * Generates a random literal and returns it as a string.
     */
    public static String generateValue(GeneratorContext ctx) {
        Rnd rnd = ctx.rnd();
        int roll = rnd.nextInt(TOTAL_WEIGHT);
        int cumulative = 0;

        cumulative += WEIGHT_INTEGER;
        if (roll < cumulative) {
            return generateIntegerValue(rnd);
        }

        cumulative += WEIGHT_FLOAT;
        if (roll < cumulative) {
            return generateFloatValue(rnd);
        }

        cumulative += WEIGHT_STRING;
        if (roll < cumulative) {
            return generateStringValue(rnd);
        }

        cumulative += WEIGHT_BOOLEAN;
        if (roll < cumulative) {
            return generateBooleanValue(rnd);
        }

        cumulative += WEIGHT_NULL;
        if (roll < cumulative) {
            return "null";
        }

        return generateTimestampValue(rnd);
    }

    // --- Integer literals ---

    /**
     * Generates an integer literal.
     */
    public static void generateInteger(GeneratorContext ctx) {
        ctx.literal(generateIntegerValue(ctx.rnd()));
    }

    /**
     * Generates an integer literal value.
     */
    public static String generateIntegerValue(Rnd rnd) {
        int variant = rnd.nextInt(10);

        switch (variant) {
            case 0:  // Zero
                return "0";
            case 1:  // Small positive
                return String.valueOf(rnd.nextInt(100));
            case 2:  // Small negative
                return String.valueOf(-rnd.nextInt(100));
            case 3:  // Medium positive
                return String.valueOf(rnd.nextInt(1_000_000));
            case 4:  // Medium negative
                return String.valueOf(-rnd.nextInt(1_000_000));
            case 5:  // Large positive
                return String.valueOf(rnd.nextLong(1_000_000_000_000L));
            case 6:  // Large negative
                return String.valueOf(-rnd.nextLong(1_000_000_000_000L));
            case 7:  // Long.MAX_VALUE
                return String.valueOf(Long.MAX_VALUE);
            case 8:  // Long.MIN_VALUE
                return String.valueOf(Long.MIN_VALUE);
            default:  // Random long
                return String.valueOf(rnd.nextLong());
        }
    }

    // --- Float literals ---

    /**
     * Generates a float literal.
     */
    public static void generateFloat(GeneratorContext ctx) {
        ctx.literal(generateFloatValue(ctx.rnd()));
    }

    /**
     * Generates a float literal value.
     */
    public static String generateFloatValue(Rnd rnd) {
        int variant = rnd.nextInt(12);

        switch (variant) {
            case 0:  // Zero
                return "0.0";
            case 1:  // Negative zero
                return "-0.0";
            case 2:  // Small decimal
                return String.format("%.6f", rnd.nextDouble());
            case 3:  // Larger decimal
                return String.format("%.2f", rnd.nextDouble() * 1000);
            case 4:  // Negative decimal
                return String.format("%.4f", -rnd.nextDouble() * 100);
            case 5:  // Scientific notation positive
                return String.format("%.2e", rnd.nextDouble() * 1e10);
            case 6:  // Scientific notation negative
                return String.format("%.2e", -rnd.nextDouble() * 1e10);
            case 7:  // Very small number
                return String.format("%.10f", rnd.nextDouble() * 1e-8);
            case 8:  // NaN
                return "NaN";
            case 9:  // Positive Infinity
                return "Infinity";
            case 10:  // Negative Infinity
                return "-Infinity";
            default:  // Random double
                return String.valueOf(rnd.nextDouble() * (rnd.nextBoolean() ? 1 : -1) * 1e6);
        }
    }

    // --- String literals ---

    /**
     * Generates a string literal.
     */
    public static void generateString(GeneratorContext ctx) {
        ctx.literal(generateStringValue(ctx.rnd()));
    }

    /**
     * Generates a string literal value (with quotes).
     */
    public static String generateStringValue(Rnd rnd) {
        int variant = rnd.nextInt(10);

        switch (variant) {
            case 0:  // Empty string
                return "''";
            case 1:  // Single character
                return "'" + (char) ('a' + rnd.nextInt(26)) + "'";
            case 2:  // Simple word
                return "'" + generateSimpleWord(rnd) + "'";
            case 3:  // With spaces
                return "'" + generateSimpleWord(rnd) + " " + generateSimpleWord(rnd) + "'";
            case 4:  // Escaped single quote
                return "'" + generateSimpleWord(rnd) + "''" + generateSimpleWord(rnd) + "'";
            case 5:  // Multiple escaped quotes
                return "''''" + generateSimpleWord(rnd) + "''''";
            case 6:  // Numeric string
                return "'" + rnd.nextInt(10000) + "'";
            case 7:  // With special characters
                return "'" + generateSimpleWord(rnd) + "_" + rnd.nextInt(100) + "'";
            case 8:  // Longer string
                return "'" + generateSimpleWord(rnd) + " " + generateSimpleWord(rnd) + " " + generateSimpleWord(rnd) + "'";
            default:  // Simple word
                return "'" + generateSimpleWord(rnd) + "'";
        }
    }

    /**
     * Generates a simple word (lowercase letters only).
     */
    private static String generateSimpleWord(Rnd rnd) {
        int len = 3 + rnd.nextInt(8);  // 3-10 characters
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append((char) ('a' + rnd.nextInt(26)));
        }
        return sb.toString();
    }

    // --- Boolean literals ---

    /**
     * Generates a boolean literal.
     */
    public static void generateBoolean(GeneratorContext ctx) {
        ctx.literal(generateBooleanValue(ctx.rnd()));
    }

    /**
     * Generates a boolean literal value.
     */
    public static String generateBooleanValue(Rnd rnd) {
        return rnd.nextBoolean() ? "true" : "false";
    }

    // --- Null literal ---

    /**
     * Generates a null literal.
     */
    public static void generateNull(GeneratorContext ctx) {
        ctx.literal("null");
    }

    // --- Timestamp literals ---

    /**
     * Generates a timestamp literal.
     */
    public static void generateTimestamp(GeneratorContext ctx) {
        ctx.literal(generateTimestampValue(ctx.rnd()));
    }

    /**
     * Generates a timestamp literal value.
     */
    public static String generateTimestampValue(Rnd rnd) {
        int variant = rnd.nextInt(5);

        // Generate random date components
        int year = 1970 + rnd.nextInt(60);  // 1970-2029
        int month = 1 + rnd.nextInt(12);
        int day = 1 + rnd.nextInt(28);  // Safe for all months
        int hour = rnd.nextInt(24);
        int minute = rnd.nextInt(60);
        int second = rnd.nextInt(60);

        switch (variant) {
            case 0:  // Date only
                return String.format("'%04d-%02d-%02d'", year, month, day);
            case 1:  // Date and time
                return String.format("'%04d-%02d-%02dT%02d:%02d:%02d'",
                        year, month, day, hour, minute, second);
            case 2:  // With milliseconds
                return String.format("'%04d-%02d-%02dT%02d:%02d:%02d.%03dZ'",
                        year, month, day, hour, minute, second, rnd.nextInt(1000));
            case 3:  // With microseconds
                return String.format("'%04d-%02d-%02dT%02d:%02d:%02d.%06dZ'",
                        year, month, day, hour, minute, second, rnd.nextInt(1_000_000));
            default:  // ISO format
                return String.format("'%04d-%02d-%02dT%02d:%02d:%02dZ'",
                        year, month, day, hour, minute, second);
        }
    }

    // --- Specific type generators for targeted testing ---

    /**
     * Generates only integer literals.
     */
    public static void generateIntegerOnly(GeneratorContext ctx) {
        generateInteger(ctx);
    }

    /**
     * Generates only float literals.
     */
    public static void generateFloatOnly(GeneratorContext ctx) {
        generateFloat(ctx);
    }

    /**
     * Generates only string literals.
     */
    public static void generateStringOnly(GeneratorContext ctx) {
        generateString(ctx);
    }

    /**
     * Generates only numeric literals (integer or float).
     */
    public static void generateNumeric(GeneratorContext ctx) {
        if (ctx.rnd().nextBoolean()) {
            generateInteger(ctx);
        } else {
            generateFloat(ctx);
        }
    }

    // --- QuestDB-specific literal generators ---

    /**
     * Generates a geohash literal.
     * QuestDB geohash literals start with # for char literals or ## for bit literals.
     */
    public static void generateGeohash(GeneratorContext ctx) {
        ctx.literal(generateGeohashValue(ctx.rnd()));
    }

    /**
     * Generates a geohash literal value.
     */
    public static String generateGeohashValue(Rnd rnd) {
        int variant = rnd.nextInt(3);

        switch (variant) {
            case 0:
                // Char geohash: #sp052w (1-12 base32 chars)
                return generateGeohashCharValue(rnd);
            case 1:
                // Bit geohash: ##01110 (binary representation)
                return generateGeohashBitValue(rnd);
            default:
                // Char geohash with precision: #sp052w/25
                return generateGeohashCharValue(rnd) + "/" + (rnd.nextInt(60) + 1);
        }
    }

    /**
     * Generates a char-based geohash literal (e.g., #sp052w).
     */
    private static String generateGeohashCharValue(Rnd rnd) {
        int len = 1 + rnd.nextInt(12);  // 1-12 characters
        StringBuilder sb = new StringBuilder(len + 1);
        sb.append('#');
        for (int i = 0; i < len; i++) {
            sb.append(GEOHASH_CHARS.charAt(rnd.nextInt(GEOHASH_CHARS.length())));
        }
        return sb.toString();
    }

    /**
     * Generates a bit-based geohash literal (e.g., ##01110).
     */
    private static String generateGeohashBitValue(Rnd rnd) {
        int len = 1 + rnd.nextInt(60);  // 1-60 bits
        StringBuilder sb = new StringBuilder(len + 2);
        sb.append("##");
        for (int i = 0; i < len; i++) {
            sb.append(rnd.nextBoolean() ? '1' : '0');
        }
        return sb.toString();
    }

    /**
     * Generates an IPv4 literal.
     */
    public static void generateIPv4(GeneratorContext ctx) {
        ctx.literal(generateIPv4Value(ctx.rnd()));
    }

    /**
     * Generates an IPv4 literal value.
     */
    public static String generateIPv4Value(Rnd rnd) {
        int variant = rnd.nextInt(6);

        switch (variant) {
            case 0:  // Private Class A
                return String.format("'10.%d.%d.%d'",
                        rnd.nextInt(256), rnd.nextInt(256), rnd.nextInt(256));
            case 1:  // Private Class B
                return String.format("'172.%d.%d.%d'",
                        16 + rnd.nextInt(16), rnd.nextInt(256), rnd.nextInt(256));
            case 2:  // Private Class C
                return String.format("'192.168.%d.%d'",
                        rnd.nextInt(256), rnd.nextInt(256));
            case 3:  // Localhost
                return "'127.0.0.1'";
            case 4:  // Broadcast
                return "'255.255.255.255'";
            default:  // Random public IP
                return String.format("'%d.%d.%d.%d'",
                        rnd.nextInt(224), rnd.nextInt(256), rnd.nextInt(256), rnd.nextInt(256));
        }
    }

    /**
     * Generates a UUID literal.
     */
    public static void generateUUID(GeneratorContext ctx) {
        ctx.literal(generateUUIDValue(ctx.rnd()));
    }

    /**
     * Generates a UUID literal value.
     */
    public static String generateUUIDValue(Rnd rnd) {
        // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        return String.format("'%08x-%04x-%04x-%04x-%012x'",
                rnd.nextInt(),
                rnd.nextInt(0xFFFF),
                rnd.nextInt(0xFFFF),
                rnd.nextInt(0xFFFF),
                rnd.nextLong() & 0xFFFFFFFFFFFFL);
    }

    /**
     * Generates a Long256 literal.
     * Long256 is QuestDB's 256-bit integer type, represented as hex.
     */
    public static void generateLong256(GeneratorContext ctx) {
        ctx.literal(generateLong256Value(ctx.rnd()));
    }

    /**
     * Generates a Long256 literal value.
     */
    public static String generateLong256Value(Rnd rnd) {
        // Long256 as hex: 0x followed by up to 64 hex digits
        int variant = rnd.nextInt(4);

        switch (variant) {
            case 0:  // Small value
                return String.format("0x%x", rnd.nextLong() & 0xFFFFFFFFL);
            case 1:  // Medium value (64-bit)
                return String.format("0x%016x", rnd.nextLong());
            case 2:  // Large value (128-bit)
                return String.format("0x%016x%016x", rnd.nextLong(), rnd.nextLong());
            default:  // Full 256-bit value
                return String.format("0x%016x%016x%016x%016x",
                        rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
        }
    }

    // --- Specific QuestDB type generators for targeted testing ---

    /**
     * Generates only geohash literals.
     */
    public static void generateGeohashOnly(GeneratorContext ctx) {
        generateGeohash(ctx);
    }

    /**
     * Generates only IPv4 literals.
     */
    public static void generateIPv4Only(GeneratorContext ctx) {
        generateIPv4(ctx);
    }

    /**
     * Generates only UUID literals.
     */
    public static void generateUUIDOnly(GeneratorContext ctx) {
        generateUUID(ctx);
    }

    /**
     * Generates only Long256 literals.
     */
    public static void generateLong256Only(GeneratorContext ctx) {
        generateLong256(ctx);
    }
}
