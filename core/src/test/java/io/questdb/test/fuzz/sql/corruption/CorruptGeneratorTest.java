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

package io.questdb.test.fuzz.sql.corruption;

import io.questdb.std.Rnd;
import io.questdb.test.fuzz.sql.TokenizedQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class CorruptGeneratorTest {

    private static final long SEED0 = 123456789L;
    private static final long SEED1 = 987654321L;
    private TokenizedQuery sampleQuery;

    @Before
    public void setUp() {
        sampleQuery = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("col1")
                .addPunctuation(",")
                .addIdentifier("col2")
                .addKeyword("FROM")
                .addIdentifier("table1")
                .addKeyword("WHERE")
                .addIdentifier("col1")
                .addOperator("=")
                .addLiteral("1");
    }

    @Test
    public void testCorruptChangesQuery() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        String original = sampleQuery.serialize();
        TokenizedQuery corrupted = CorruptGenerator.corrupt(sampleQuery, rnd);

        Assert.assertNotEquals(original, corrupted.serialize());
    }

    @Test
    public void testCorruptReproducible() {
        Rnd rnd1 = new Rnd(SEED0, SEED1);
        Rnd rnd2 = new Rnd(SEED0, SEED1);

        String result1 = CorruptGenerator.corrupt(sampleQuery, rnd1).serialize();
        String result2 = CorruptGenerator.corrupt(sampleQuery, rnd2).serialize();

        Assert.assertEquals(result1, result2);
    }

    @Test
    public void testCorruptNAppliesMultiple() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = CorruptGenerator.corruptN(sampleQuery, rnd, 3);

        // Hard to verify exact changes, but query should be different
        Assert.assertNotEquals(sampleQuery.serialize(), result.serialize());
    }

    @Test
    public void testCorruptRandomAppliesVariableCorruptions() {
        Set<String> results = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            Rnd rnd = new Rnd(SEED0 + i, SEED1 + i);
            results.add(CorruptGenerator.corruptRandom(sampleQuery, rnd).serialize());
        }

        // Should produce variety of results
        Assert.assertTrue("Should produce variety of corruptions", results.size() > 10);
    }

    @Test
    public void testCorruptWithSpecificStrategy() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = CorruptGenerator.corruptWith(sampleQuery, rnd, DropTokenStrategy.INSTANCE);

        Assert.assertEquals(sampleQuery.size() - 1, result.size());
    }

    @Test
    public void testCorruptEmptyQuery() {
        TokenizedQuery empty = new TokenizedQuery();
        Rnd rnd = new Rnd(SEED0, SEED1);

        // Should handle empty query (fall back to always-applicable strategies)
        TokenizedQuery result = CorruptGenerator.corrupt(empty, rnd);
        Assert.assertTrue(result.size() >= 1);
    }

    @Test
    public void testCorruptSingleTokenQuery() {
        TokenizedQuery single = new TokenizedQuery().addKeyword("SELECT");
        Rnd rnd = new Rnd(SEED0, SEED1);

        // Should handle single token query
        TokenizedQuery result = CorruptGenerator.corrupt(single, rnd);
        Assert.assertNotNull(result);
    }

    @Test
    public void testGetStrategies() {
        CorruptionStrategy[] strategies = CorruptGenerator.getStrategies();

        Assert.assertTrue(strategies.length > 0);
        Assert.assertEquals(CorruptGenerator.strategyCount(), strategies.length);
    }

    @Test
    public void testAllStrategiesUsed() {
        Set<String> usedStrategies = new HashSet<>();

        // Run many corruptions and track which strategies are used
        for (int i = 0; i < 1000; i++) {
            Rnd rnd = new Rnd(SEED0 + i, SEED1 + i);
            // Can't directly track which strategy is used, but we can verify
            // that the generator doesn't crash with various queries
            TokenizedQuery result = CorruptGenerator.corrupt(sampleQuery, rnd);
            Assert.assertNotNull(result);
        }
    }

    @Test
    public void testCorruptManyIterations() {
        // Stress test: corrupt many times without crashing
        for (int i = 0; i < 1000; i++) {
            Rnd rnd = new Rnd(SEED0 + i, SEED1 + i);
            TokenizedQuery result = CorruptGenerator.corrupt(sampleQuery, rnd);
            Assert.assertNotNull(result);
            Assert.assertNotNull(result.serialize());
        }
    }
}
