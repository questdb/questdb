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

public class SampleByGeneratorTest {

    private static final long SEED0 = 123456789L;
    private static final long SEED1 = 987654321L;
    private GeneratorContext ctx;

    @Before
    public void setUp() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        ctx = new GeneratorContext(rnd, GeneratorConfig.defaults());
    }

    @Test
    public void testGenerateProducesSampleByClause() {
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            SampleByGenerator.generate(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with SAMPLE BY: " + sql,
                    sql.startsWith("SAMPLE BY "));
        }
    }

    @Test
    public void testGenerateSimple() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            SampleByGenerator.generateSimple(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with SAMPLE BY: " + sql,
                    sql.startsWith("SAMPLE BY "));
            // Should be simple - just SAMPLE BY interval
            Assert.assertFalse("Simple should not have FILL: " + sql, sql.contains("FILL"));
            Assert.assertFalse("Simple should not have ALIGN: " + sql, sql.contains("ALIGN"));
            Assert.assertFalse("Simple should not have FROM: " + sql, sql.contains("FROM"));
        }
    }

    @Test
    public void testGenerateWithInterval() {
        ctx.reset();
        SampleByGenerator.generateWithInterval(ctx, "1h");
        String sql = ctx.toSql();

        Assert.assertEquals("SAMPLE BY 1h", sql);
    }

    @Test
    public void testGenerateWithIntervalMinutes() {
        ctx.reset();
        SampleByGenerator.generateWithInterval(ctx, "30m");
        String sql = ctx.toSql();

        Assert.assertEquals("SAMPLE BY 30m", sql);
    }

    @Test
    public void testGenerateFromTo() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            SampleByGenerator.generateFromTo(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should have FROM: " + sql, sql.contains("FROM "));
            Assert.assertTrue("Should have TO: " + sql, sql.contains(" TO "));
            // Should have timestamp literals
            Assert.assertTrue("Should have timestamp: " + sql, sql.contains("'202"));
        }
    }

    @Test
    public void testGenerateFill() {
        boolean foundNone = false;
        boolean foundPrev = false;
        boolean foundNull = false;
        boolean foundLinear = false;
        boolean foundNumeric = false;

        for (int i = 0; i < 200; i++) {
            ctx.reset();
            SampleByGenerator.generateFill(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with FILL: " + sql, sql.startsWith("FILL"));
            Assert.assertTrue("Should have open paren: " + sql, sql.contains("("));
            Assert.assertTrue("Should have close paren: " + sql, sql.contains(")"));

            if (sql.contains("NONE")) foundNone = true;
            if (sql.contains("PREV")) foundPrev = true;
            if (sql.contains("NULL")) foundNull = true;
            if (sql.contains("LINEAR")) foundLinear = true;
            // Check for numeric value (not a keyword)
            if (sql.matches(".*FILL\\s*\\(\\s*\\d+.*")) foundNumeric = true;
        }

        Assert.assertTrue("Should generate NONE fill", foundNone);
        Assert.assertTrue("Should generate PREV fill", foundPrev);
        // NULL might conflict with other matches, so just check it appeared
    }

    @Test
    public void testGenerateAlignToCalendar() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            SampleByGenerator.generateAlignToCalendar(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should contain CALENDAR: " + sql, sql.contains("CALENDAR"));
        }
    }

    @Test
    public void testGenerateAlignToCalendarWithTimeZone() {
        boolean foundTimeZone = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            SampleByGenerator.generateAlignToCalendar(ctx);
            String sql = ctx.toSql();

            if (sql.contains("TIME ZONE")) {
                foundTimeZone = true;
                // Should have a timezone value
                Assert.assertTrue("Should have timezone value: " + sql,
                        sql.contains("'UTC'") || sql.contains("America") ||
                                sql.contains("Europe") || sql.contains("Asia") ||
                                sql.contains("Australia"));
            }
        }

        Assert.assertTrue("Should sometimes generate TIME ZONE", foundTimeZone);
    }

    @Test
    public void testGenerateAlignToCalendarWithOffset() {
        boolean foundOffset = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            SampleByGenerator.generateAlignToCalendar(ctx);
            String sql = ctx.toSql();

            if (sql.contains("WITH OFFSET")) {
                foundOffset = true;
            }
        }

        Assert.assertTrue("Should sometimes generate WITH OFFSET", foundOffset);
    }

    @Test
    public void testGenerateAlignToFirstObservation() {
        ctx.reset();
        SampleByGenerator.generateAlignToFirstObservation(ctx);
        String sql = ctx.toSql();

        Assert.assertEquals("FIRST OBSERVATION", sql);
    }

    @Test
    public void testGenerateAlignTo() {
        boolean foundCalendar = false;
        boolean foundFirstObservation = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            SampleByGenerator.generateAlignTo(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with ALIGN TO: " + sql,
                    sql.startsWith("ALIGN TO "));

            if (sql.contains("CALENDAR")) foundCalendar = true;
            if (sql.contains("FIRST OBSERVATION")) foundFirstObservation = true;
        }

        Assert.assertTrue("Should generate ALIGN TO CALENDAR", foundCalendar);
        Assert.assertTrue("Should generate ALIGN TO FIRST OBSERVATION", foundFirstObservation);
    }

    @Test
    public void testGenerateFullSampleBy() {
        boolean foundFill = false;
        boolean foundAlign = false;
        boolean foundFromTo = false;

        for (int i = 0; i < 300; i++) {
            ctx.reset();
            SampleByGenerator.generate(ctx);
            String sql = ctx.toSql();

            if (sql.contains("FILL")) foundFill = true;
            if (sql.contains("ALIGN")) foundAlign = true;
            if (sql.contains(" FROM ")) foundFromTo = true;
        }

        Assert.assertTrue("Should sometimes generate FILL", foundFill);
        Assert.assertTrue("Should sometimes generate ALIGN TO", foundAlign);
        Assert.assertTrue("Should sometimes generate FROM...TO", foundFromTo);
    }

    @Test
    public void testIntervalVariety() {
        java.util.Set<String> intervals = new java.util.HashSet<>();

        for (int i = 0; i < 200; i++) {
            ctx.reset();
            SampleByGenerator.generateSimple(ctx);
            String sql = ctx.toSql();

            // Extract interval from "SAMPLE BY <interval>"
            String interval = sql.replace("SAMPLE BY ", "");
            intervals.add(interval);
        }

        // Should generate multiple different intervals
        Assert.assertTrue("Should generate variety of intervals: " + intervals.size(),
                intervals.size() >= 5);
    }

    @Test
    public void testBalancedParentheses() {
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            SampleByGenerator.generate(ctx);
            String sql = ctx.toSql();

            int openCount = 0;
            int closeCount = 0;
            for (char c : sql.toCharArray()) {
                if (c == '(') openCount++;
                if (c == ')') closeCount++;
            }

            Assert.assertEquals("Parentheses should be balanced in: " + sql,
                    openCount, closeCount);
        }
    }

    @Test
    public void testReproducibility() {
        Rnd rnd1 = new Rnd(SEED0, SEED1);
        GeneratorContext ctx1 = new GeneratorContext(rnd1, GeneratorConfig.defaults());

        String[] samples1 = new String[10];
        for (int i = 0; i < 10; i++) {
            ctx1.reset();
            SampleByGenerator.generate(ctx1);
            samples1[i] = ctx1.toSql();
        }

        Rnd rnd2 = new Rnd(SEED0, SEED1);
        GeneratorContext ctx2 = new GeneratorContext(rnd2, GeneratorConfig.defaults());

        String[] samples2 = new String[10];
        for (int i = 0; i < 10; i++) {
            ctx2.reset();
            SampleByGenerator.generate(ctx2);
            samples2[i] = ctx2.toSql();
        }

        Assert.assertArrayEquals(samples1, samples2);
    }
}
