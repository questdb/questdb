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

public class LatestOnGeneratorTest {

    private static final long SEED0 = 123456789L;
    private static final long SEED1 = 987654321L;
    private GeneratorContext ctx;

    @Before
    public void setUp() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        ctx = new GeneratorContext(rnd, GeneratorConfig.defaults());
    }

    @Test
    public void testGenerateProducesLatestOnClause() {
        for (int i = 0; i < 100; i++) {
            ctx.reset();
            LatestOnGenerator.generate(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with LATEST ON: " + sql,
                    sql.startsWith("LATEST ON "));
            Assert.assertTrue("Should have PARTITION BY: " + sql,
                    sql.contains(" PARTITION BY "));
        }
    }

    @Test
    public void testGenerateWithTimestamp() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            LatestOnGenerator.generateWithTimestamp(ctx, "my_ts");
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with LATEST ON my_ts: " + sql,
                    sql.startsWith("LATEST ON my_ts"));
            Assert.assertTrue("Should have PARTITION BY: " + sql,
                    sql.contains(" PARTITION BY "));
        }
    }

    @Test
    public void testGenerateExplicit() {
        ctx.reset();
        LatestOnGenerator.generateExplicit(ctx, "created_at", "user_id", "device_id");
        String sql = ctx.toSql();

        Assert.assertEquals("LATEST ON created_at PARTITION BY user_id, device_id", sql);
    }

    @Test
    public void testGenerateExplicitSinglePartition() {
        ctx.reset();
        LatestOnGenerator.generateExplicit(ctx, "timestamp", "symbol");
        String sql = ctx.toSql();

        Assert.assertEquals("LATEST ON timestamp PARTITION BY symbol", sql);
    }

    @Test
    public void testGenerateDeprecated() {
        for (int i = 0; i < 50; i++) {
            ctx.reset();
            LatestOnGenerator.generateDeprecated(ctx);
            String sql = ctx.toSql();

            Assert.assertTrue("Should start with LATEST BY: " + sql,
                    sql.startsWith("LATEST BY "));
            // Deprecated syntax doesn't have PARTITION BY or ON
            Assert.assertFalse("Deprecated should not have PARTITION BY: " + sql,
                    sql.contains("PARTITION BY"));
            Assert.assertFalse("Deprecated should not have ON: " + sql,
                    sql.contains(" ON "));
        }
    }

    @Test
    public void testPartitionColumnCount() {
        boolean foundOne = false;
        boolean foundMultiple = false;

        for (int i = 0; i < 100; i++) {
            ctx.reset();
            LatestOnGenerator.generate(ctx);
            String sql = ctx.toSql();

            // Extract partition by part
            int partitionByIdx = sql.indexOf("PARTITION BY ");
            String partitionPart = sql.substring(partitionByIdx + "PARTITION BY ".length());

            // Count commas to determine number of columns
            int commaCount = 0;
            for (char c : partitionPart.toCharArray()) {
                if (c == ',') commaCount++;
            }

            if (commaCount == 0) {
                foundOne = true;
            } else {
                foundMultiple = true;
            }
        }

        Assert.assertTrue("Should sometimes have single partition column", foundOne);
        Assert.assertTrue("Should sometimes have multiple partition columns", foundMultiple);
    }

    @Test
    public void testTimestampColumnVariety() {
        java.util.Set<String> timestampCols = new java.util.HashSet<>();

        for (int i = 0; i < 200; i++) {
            ctx.reset();
            LatestOnGenerator.generate(ctx);
            String sql = ctx.toSql();

            // Extract timestamp column (between "LATEST ON " and " PARTITION")
            int start = "LATEST ON ".length();
            int end = sql.indexOf(" PARTITION");
            String tsCol = sql.substring(start, end);
            timestampCols.add(tsCol);
        }

        // Should generate multiple different timestamp column names
        Assert.assertTrue("Should generate variety of timestamp columns: " + timestampCols.size(),
                timestampCols.size() >= 3);
    }

    @Test
    public void testReproducibility() {
        Rnd rnd1 = new Rnd(SEED0, SEED1);
        GeneratorContext ctx1 = new GeneratorContext(rnd1, GeneratorConfig.defaults());

        String[] latestOns1 = new String[10];
        for (int i = 0; i < 10; i++) {
            ctx1.reset();
            LatestOnGenerator.generate(ctx1);
            latestOns1[i] = ctx1.toSql();
        }

        Rnd rnd2 = new Rnd(SEED0, SEED1);
        GeneratorContext ctx2 = new GeneratorContext(rnd2, GeneratorConfig.defaults());

        String[] latestOns2 = new String[10];
        for (int i = 0; i < 10; i++) {
            ctx2.reset();
            LatestOnGenerator.generate(ctx2);
            latestOns2[i] = ctx2.toSql();
        }

        Assert.assertArrayEquals(latestOns1, latestOns2);
    }
}
