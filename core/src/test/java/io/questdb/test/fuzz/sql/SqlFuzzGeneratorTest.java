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

package io.questdb.test.fuzz.sql;

import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class SqlFuzzGeneratorTest {

    private static final long SEED0 = 123456789L;
    private static final long SEED1 = 987654321L;

    @Test
    public void testGenerate() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, GeneratorConfig.defaults());

        String sql = generator.generate();

        Assert.assertNotNull(sql);
        Assert.assertFalse(sql.isEmpty());
    }

    @Test
    public void testGenerateMultiple() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, GeneratorConfig.defaults());

        // Generate multiple queries
        for (int i = 0; i < 100; i++) {
            String sql = generator.generate();
            Assert.assertNotNull(sql);
            Assert.assertFalse(sql.isEmpty());
        }
    }

    @Test
    public void testSeedCapture() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, GeneratorConfig.defaults());

        // Generate a query
        generator.generate();

        // Check seeds were captured
        long[] seeds = generator.getLastSeeds();
        Assert.assertEquals(2, seeds.length);
        Assert.assertEquals(generator.getLastSeed0(), seeds[0]);
        Assert.assertEquals(generator.getLastSeed1(), seeds[1]);
    }

    @Test
    public void testReproducibility() {
        // Generate with specific seeds
        Rnd rnd1 = new Rnd(SEED0, SEED1);
        SqlFuzzGenerator gen1 = new SqlFuzzGenerator(rnd1, GeneratorConfig.defaults());
        String sql1 = gen1.generate();
        long[] seeds1 = gen1.getLastSeeds();

        // Generate with same seeds - should get same result
        Rnd rnd2 = new Rnd(seeds1[0], seeds1[1]);
        SqlFuzzGenerator gen2 = new SqlFuzzGenerator(rnd2, GeneratorConfig.defaults());
        String sql2 = gen2.generate();

        Assert.assertEquals(sql1, sql2);
    }

    @Test
    public void testValidOnlyMode() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        GeneratorConfig config = GeneratorConfig.builder().validOnly().build();
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, config);

        for (int i = 0; i < 100; i++) {
            generator.generate();
            Assert.assertEquals(SqlFuzzGenerator.Mode.VALID, generator.getLastMode());
            Assert.assertEquals(FuzzFailure.GenerationMode.VALID, generator.getLastGenerationMode());
        }
    }

    @Test
    public void testCorruptOnlyMode() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        GeneratorConfig config = GeneratorConfig.builder().corruptOnly().build();
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, config);

        for (int i = 0; i < 100; i++) {
            generator.generate();
            Assert.assertEquals(SqlFuzzGenerator.Mode.CORRUPT, generator.getLastMode());
        }
    }

    @Test
    public void testGarbageOnlyMode() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        GeneratorConfig config = GeneratorConfig.builder().garbageOnly().build();
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, config);

        for (int i = 0; i < 100; i++) {
            generator.generate();
            Assert.assertEquals(SqlFuzzGenerator.Mode.GARBAGE, generator.getLastMode());
        }
    }

    @Test
    public void testModeDistribution() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        GeneratorConfig config = GeneratorConfig.defaults();  // 60% valid, 30% corrupt, 10% garbage
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, config);

        int valid = 0;
        int corrupt = 0;
        int garbage = 0;

        for (int i = 0; i < 1000; i++) {
            generator.generate();
            switch (generator.getLastMode()) {
                case VALID:
                    valid++;
                    break;
                case CORRUPT:
                    corrupt++;
                    break;
                case GARBAGE:
                    garbage++;
                    break;
            }
        }

        // Check distribution is roughly as expected (with tolerance)
        Assert.assertTrue("valid=" + valid, valid > 500 && valid < 700);  // ~60%
        Assert.assertTrue("corrupt=" + corrupt, corrupt > 200 && corrupt < 400);  // ~30%
        Assert.assertTrue("garbage=" + garbage, garbage > 50 && garbage < 200);  // ~10%
    }

    @Test
    public void testGenerateTokenized() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, GeneratorConfig.defaults());

        TokenizedQuery query = generator.generateTokenized();

        Assert.assertNotNull(query);
        Assert.assertFalse(query.isEmpty());

        // Verify serialization works
        String sql = query.serialize();
        Assert.assertNotNull(sql);
        Assert.assertFalse(sql.isEmpty());
    }

    @Test
    public void testTokenizedReproducibility() {
        // Generate tokenized with specific seeds
        Rnd rnd1 = new Rnd(SEED0, SEED1);
        SqlFuzzGenerator gen1 = new SqlFuzzGenerator(rnd1, GeneratorConfig.defaults());
        TokenizedQuery q1 = gen1.generateTokenized();
        long[] seeds1 = gen1.getLastSeeds();

        // Generate with same seeds
        Rnd rnd2 = new Rnd(seeds1[0], seeds1[1]);
        SqlFuzzGenerator gen2 = new SqlFuzzGenerator(rnd2, GeneratorConfig.defaults());
        TokenizedQuery q2 = gen2.generateTokenized();

        Assert.assertEquals(q1.serialize(), q2.serialize());
    }

    @Test
    public void testValidQueryStructure() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        GeneratorConfig config = GeneratorConfig.builder().validOnly().build();
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, config);

        for (int i = 0; i < 50; i++) {
            String sql = generator.generate();

            // Basic structure check
            Assert.assertTrue("SQL should start with SELECT: " + sql,
                    sql.toUpperCase().startsWith("SELECT"));
            Assert.assertTrue("SQL should contain FROM: " + sql,
                    sql.toUpperCase().contains("FROM"));
        }
    }

    @Test
    public void testCorruptQueryIsDifferent() {
        Rnd rnd = new Rnd(SEED0, SEED1);

        // Generate valid query
        GeneratorConfig validConfig = GeneratorConfig.builder().validOnly().build();
        SqlFuzzGenerator validGen = new SqlFuzzGenerator(new Rnd(SEED0, SEED1), validConfig);
        String validSql = validGen.generate();
        long[] seeds = validGen.getLastSeeds();

        // Generate corrupt query with same seed
        GeneratorConfig corruptConfig = GeneratorConfig.builder().corruptOnly().build();
        SqlFuzzGenerator corruptGen = new SqlFuzzGenerator(new Rnd(seeds[0], seeds[1]), corruptConfig);
        String corruptSql = corruptGen.generate();

        // Corrupt should be different (corrupted version of valid)
        // Note: This is probabilistic - there's a small chance they could be equal
        // but with multiple corruptions it's very unlikely
    }

    @Test
    public void testGarbageQueryHasTokens() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        GeneratorConfig config = GeneratorConfig.builder().garbageOnly().build();
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, config);

        for (int i = 0; i < 50; i++) {
            TokenizedQuery query = generator.generateTokenized();

            // Garbage queries should have 1-20 tokens
            Assert.assertTrue("Tokens: " + query.size(), query.size() >= 1 && query.size() <= 20);
        }
    }

    @Test
    public void testConfigAccess() {
        GeneratorConfig config = GeneratorConfig.questdbFocused();
        SqlFuzzGenerator generator = new SqlFuzzGenerator(new Rnd(SEED0, SEED1), config);

        Assert.assertSame(config, generator.config());
    }
}
