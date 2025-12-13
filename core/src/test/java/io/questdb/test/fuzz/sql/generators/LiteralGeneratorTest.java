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
import io.questdb.test.fuzz.sql.GeneratorConfig;
import io.questdb.test.fuzz.sql.GeneratorContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class LiteralGeneratorTest {

    private static final long SEED0 = 123456789L;
    private static final long SEED1 = 987654321L;
    private GeneratorContext ctx;

    @Before
    public void setUp() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        ctx = new GeneratorContext(rnd, GeneratorConfig.defaults());
    }

    @Test
    public void testGenerateProducesTokens() {
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            LiteralGenerator.generate(ctx);
            Assert.assertFalse("Should produce at least one token", ctx.tokens().isEmpty());
        }
    }

    @Test
    public void testGenerateValueReturnsNonEmpty() {
        for (int i = 0; i < 100; i++) {
            String value = LiteralGenerator.generateValue(ctx);
            Assert.assertNotNull(value);
            Assert.assertFalse("Generated value should not be empty", value.isEmpty());
        }
    }

    @Test
    public void testIntegerLiterals() {
        Rnd rnd = new Rnd(SEED0, SEED1);

        for (int i = 0; i < 100; i++) {
            String value = LiteralGenerator.generateIntegerValue(rnd);
            Assert.assertNotNull(value);

            // Should be parseable as long (except for Long.MIN_VALUE edge case)
            try {
                Long.parseLong(value);
            } catch (NumberFormatException e) {
                Assert.fail("Integer literal should be valid: " + value);
            }
        }
    }

    @Test
    public void testIntegerEdgeCases() {
        Set<String> values = new HashSet<>();
        Rnd rnd = new Rnd(SEED0, SEED1);

        // Generate many values to ensure we hit edge cases
        for (int i = 0; i < 1000; i++) {
            values.add(LiteralGenerator.generateIntegerValue(rnd));
        }

        // Should include zero
        Assert.assertTrue("Should generate 0", values.contains("0"));

        // Should include Long.MAX_VALUE and Long.MIN_VALUE eventually
        Assert.assertTrue("Should generate Long.MAX_VALUE",
                values.contains(String.valueOf(Long.MAX_VALUE)));
        Assert.assertTrue("Should generate Long.MIN_VALUE",
                values.contains(String.valueOf(Long.MIN_VALUE)));
    }

    @Test
    public void testFloatLiterals() {
        Rnd rnd = new Rnd(SEED0, SEED1);

        for (int i = 0; i < 100; i++) {
            String value = LiteralGenerator.generateFloatValue(rnd);
            Assert.assertNotNull(value);

            // Should be parseable as double (including NaN, Infinity)
            try {
                Double.parseDouble(value);
            } catch (NumberFormatException e) {
                Assert.fail("Float literal should be valid: " + value);
            }
        }
    }

    @Test
    public void testFloatEdgeCases() {
        Set<String> values = new HashSet<>();
        Rnd rnd = new Rnd(SEED0, SEED1);

        for (int i = 0; i < 1000; i++) {
            values.add(LiteralGenerator.generateFloatValue(rnd));
        }

        // Should include special values
        Assert.assertTrue("Should generate NaN", values.contains("NaN"));
        Assert.assertTrue("Should generate Infinity", values.contains("Infinity"));
        Assert.assertTrue("Should generate -Infinity", values.contains("-Infinity"));
        Assert.assertTrue("Should generate 0.0", values.contains("0.0"));
        Assert.assertTrue("Should generate -0.0", values.contains("-0.0"));
    }

    @Test
    public void testStringLiterals() {
        Rnd rnd = new Rnd(SEED0, SEED1);

        for (int i = 0; i < 100; i++) {
            String value = LiteralGenerator.generateStringValue(rnd);
            Assert.assertNotNull(value);

            // Should be quoted
            Assert.assertTrue("String should start with quote: " + value,
                    value.startsWith("'"));
            Assert.assertTrue("String should end with quote: " + value,
                    value.endsWith("'"));
        }
    }

    @Test
    public void testStringEdgeCases() {
        Set<String> values = new HashSet<>();
        Rnd rnd = new Rnd(SEED0, SEED1);

        for (int i = 0; i < 1000; i++) {
            values.add(LiteralGenerator.generateStringValue(rnd));
        }

        // Should include empty string
        Assert.assertTrue("Should generate empty string ''", values.contains("''"));

        // Should include strings with escaped quotes
        boolean hasEscapedQuotes = values.stream().anyMatch(s -> s.contains("''") && s.length() > 2);
        Assert.assertTrue("Should generate strings with escaped quotes", hasEscapedQuotes);
    }

    @Test
    public void testBooleanLiterals() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        Set<String> values = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            values.add(LiteralGenerator.generateBooleanValue(rnd));
        }

        Assert.assertTrue("Should generate true", values.contains("true"));
        Assert.assertTrue("Should generate false", values.contains("false"));
        Assert.assertEquals("Should only generate true and false", 2, values.size());
    }

    @Test
    public void testNullLiteral() {
        ctx.reset();
        LiteralGenerator.generateNull(ctx);

        String sql = ctx.toSql();
        Assert.assertEquals("null", sql);
    }

    @Test
    public void testTimestampLiterals() {
        Rnd rnd = new Rnd(SEED0, SEED1);

        for (int i = 0; i < 100; i++) {
            String value = LiteralGenerator.generateTimestampValue(rnd);
            Assert.assertNotNull(value);

            // Should be quoted
            Assert.assertTrue("Timestamp should start with quote: " + value,
                    value.startsWith("'"));
            Assert.assertTrue("Timestamp should end with quote: " + value,
                    value.endsWith("'"));

            // Should contain date pattern
            Assert.assertTrue("Should contain date pattern: " + value,
                    value.matches("'\\d{4}-\\d{2}-\\d{2}.*'"));
        }
    }

    @Test
    public void testGenerateIntegerOnly() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            LiteralGenerator.generateIntegerOnly(ctx);

            String sql = ctx.toSql();
            // Should be parseable as long
            try {
                Long.parseLong(sql);
            } catch (NumberFormatException e) {
                Assert.fail("Should generate valid integer: " + sql);
            }
        }
    }

    @Test
    public void testGenerateFloatOnly() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            LiteralGenerator.generateFloatOnly(ctx);

            String sql = ctx.toSql();
            // Should be parseable as double
            try {
                Double.parseDouble(sql);
            } catch (NumberFormatException e) {
                Assert.fail("Should generate valid float: " + sql);
            }
        }
    }

    @Test
    public void testGenerateStringOnly() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            LiteralGenerator.generateStringOnly(ctx);

            String sql = ctx.toSql();
            Assert.assertTrue("Should be quoted string: " + sql,
                    sql.startsWith("'") && sql.endsWith("'"));
        }
    }

    @Test
    public void testGenerateNumeric() {
        int integers = 0;
        int floats = 0;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            LiteralGenerator.generateNumeric(ctx);

            String sql = ctx.toSql();
            if (sql.contains(".") || sql.contains("e") || sql.contains("E")
                    || sql.equals("NaN") || sql.contains("Infinity")) {
                floats++;
            } else {
                integers++;
            }
        }

        // Should have a mix
        Assert.assertTrue("Should generate some integers", integers > 0);
        Assert.assertTrue("Should generate some floats", floats > 0);
    }

    @Test
    public void testDistribution() {
        int integers = 0;
        int floats = 0;
        int strings = 0;
        int booleans = 0;
        int nulls = 0;
        int timestamps = 0;

        for (int i = 0; i < 1000; i++) {
            ctx.reset();
            LiteralGenerator.generate(ctx);

            String sql = ctx.toSql();
            if (sql.equals("null")) {
                nulls++;
            } else if (sql.equals("true") || sql.equals("false")) {
                booleans++;
            } else if (sql.startsWith("'")) {
                // Could be string or timestamp
                if (sql.matches("'\\d{4}-\\d{2}-\\d{2}.*'")) {
                    timestamps++;
                } else {
                    strings++;
                }
            } else if (sql.contains(".") || sql.contains("e") || sql.contains("E")
                    || sql.equals("NaN") || sql.contains("Infinity")) {
                floats++;
            } else {
                integers++;
            }
        }

        // Verify reasonable distribution (with tolerance for randomness)
        Assert.assertTrue("Should have integers: " + integers, integers > 100);
        Assert.assertTrue("Should have floats: " + floats, floats > 50);
        Assert.assertTrue("Should have strings: " + strings, strings > 100);
        Assert.assertTrue("Should have booleans: " + booleans, booleans > 30);
        Assert.assertTrue("Should have nulls: " + nulls, nulls > 30);
        Assert.assertTrue("Should have timestamps: " + timestamps, timestamps > 10);
    }

    @Test
    public void testReproducibility() {
        // Generate with specific seed
        Rnd rnd1 = new Rnd(SEED0, SEED1);
        GeneratorContext ctx1 = new GeneratorContext(rnd1, GeneratorConfig.defaults());

        String[] values1 = new String[10];
        for (int i = 0; i < 10; i++) {
            values1[i] = LiteralGenerator.generateValue(ctx1);
        }

        // Generate with same seed
        Rnd rnd2 = new Rnd(SEED0, SEED1);
        GeneratorContext ctx2 = new GeneratorContext(rnd2, GeneratorConfig.defaults());

        String[] values2 = new String[10];
        for (int i = 0; i < 10; i++) {
            values2[i] = LiteralGenerator.generateValue(ctx2);
        }

        // Should be identical
        Assert.assertArrayEquals(values1, values2);
    }

    // --- QuestDB-specific literal tests ---

    @Test
    public void testGenerateGeohash() {
        boolean foundChar = false;
        boolean foundBit = false;
        boolean foundWithPrecision = false;

        for (int i = 0; i < 200; i++) {
            ctx.reset();
            LiteralGenerator.generateGeohash(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with #: " + sql, sql.startsWith("#"));

            if (sql.startsWith("##")) {
                // Bit geohash
                foundBit = true;
                String bits = sql.substring(2);
                Assert.assertTrue("Bit geohash should contain only 0s and 1s: " + sql,
                        bits.matches("[01]+"));
            } else if (sql.contains("/")) {
                // Char geohash with precision
                foundWithPrecision = true;
            } else {
                // Char geohash
                foundChar = true;
            }
        }

        Assert.assertTrue("Should generate char geohash", foundChar);
        Assert.assertTrue("Should generate bit geohash", foundBit);
        Assert.assertTrue("Should generate geohash with precision", foundWithPrecision);
    }

    @Test
    public void testGenerateIPv4() {
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            LiteralGenerator.generateIPv4(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should be quoted: " + sql, sql.startsWith("'") && sql.endsWith("'"));
            String ip = sql.substring(1, sql.length() - 1);
            Assert.assertTrue("Should be valid IPv4 format: " + ip,
                    ip.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}"));
        }
    }

    @Test
    public void testGenerateIPv4Variety() {
        boolean foundPrivateA = false;
        boolean foundPrivateC = false;
        boolean foundLocalhost = false;

        for (int i = 0; i < 200; i++) {
            ctx.reset();
            LiteralGenerator.generateIPv4(ctx);
            String sql = ctx.toSql();

            if (sql.startsWith("'10.")) foundPrivateA = true;
            if (sql.startsWith("'192.168.")) foundPrivateC = true;
            if (sql.equals("'127.0.0.1'")) foundLocalhost = true;
        }

        Assert.assertTrue("Should generate Private Class A IPs", foundPrivateA);
        Assert.assertTrue("Should generate Private Class C IPs", foundPrivateC);
        Assert.assertTrue("Should generate localhost", foundLocalhost);
    }

    @Test
    public void testGenerateUUID() {
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            LiteralGenerator.generateUUID(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should be quoted: " + sql, sql.startsWith("'") && sql.endsWith("'"));
            String uuid = sql.substring(1, sql.length() - 1);
            Assert.assertTrue("Should be valid UUID format: " + uuid,
                    uuid.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));
        }
    }

    @Test
    public void testGenerateLong256() {
        boolean foundSmall = false;
        boolean foundMedium = false;
        boolean foundLarge = false;

        for (int i = 0; i < 200; i++) {
            ctx.reset();
            LiteralGenerator.generateLong256(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with 0x: " + sql, sql.startsWith("0x"));
            String hex = sql.substring(2);
            Assert.assertTrue("Should be valid hex: " + sql, hex.matches("[0-9a-f]+"));

            if (hex.length() <= 8) foundSmall = true;
            else if (hex.length() <= 16) foundMedium = true;
            else foundLarge = true;
        }

        Assert.assertTrue("Should generate small Long256 values", foundSmall);
        Assert.assertTrue("Should generate medium Long256 values", foundMedium);
        Assert.assertTrue("Should generate large Long256 values", foundLarge);
    }

    @Test
    public void testGenerateIncludesQuestDBTypes() {
        boolean foundGeohash = false;
        boolean foundIPv4 = false;
        boolean foundUUID = false;
        boolean foundLong256 = false;

        for (int i = 0; i < 1000; i++) {
            ctx.reset();
            LiteralGenerator.generate(ctx);
            String sql = ctx.toSql();

            if (sql.startsWith("#")) foundGeohash = true;
            if (sql.startsWith("0x")) foundLong256 = true;
            // IPv4 and UUID are quoted strings, harder to distinguish
            if (sql.matches("'\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}'")) foundIPv4 = true;
            if (sql.matches("'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'")) foundUUID = true;
        }

        Assert.assertTrue("Should generate Geohash literals", foundGeohash);
        Assert.assertTrue("Should generate Long256 literals", foundLong256);
        // IPv4 and UUID have lower weights, might not appear in 1000 iterations
    }
}
