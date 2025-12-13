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
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class GarbageGeneratorTest {

    private static final long SEED0 = 123456789L;
    private static final long SEED1 = 987654321L;

    @Test
    public void testGenerateProducesTokens() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery query = GarbageGenerator.generate(rnd);

        Assert.assertNotNull(query);
        Assert.assertTrue(query.size() >= 1);
    }

    @Test
    public void testGenerateWithCountProducesExactTokens() {
        Rnd rnd = new Rnd(SEED0, SEED1);

        for (int count = 1; count <= 10; count++) {
            TokenizedQuery query = GarbageGenerator.generateWithCount(rnd, count);
            Assert.assertEquals(count, query.size());
        }
    }

    @Test
    public void testGenerateReproducible() {
        Rnd rnd1 = new Rnd(SEED0, SEED1);
        Rnd rnd2 = new Rnd(SEED0, SEED1);

        String result1 = GarbageGenerator.generate(rnd1).serialize();
        String result2 = GarbageGenerator.generate(rnd2).serialize();

        Assert.assertEquals(result1, result2);
    }

    @Test
    public void testGenerateVariety() {
        Set<String> results = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            Rnd rnd = new Rnd(SEED0 + i, SEED1 + i);
            results.add(GarbageGenerator.generate(rnd).serialize());
        }

        // Should produce variety
        Assert.assertTrue("Should produce variety of garbage", results.size() > 50);
    }

    @Test
    public void testGenerateRawProducesString() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        String raw = GarbageGenerator.generateRaw(rnd);

        Assert.assertNotNull(raw);
    }

    @Test
    public void testGenerateEmptyOrWhitespace() {
        Set<String> results = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            Rnd rnd = new Rnd(SEED0 + i, SEED1 + i);
            results.add(GarbageGenerator.generateEmptyOrWhitespace(rnd));
        }

        // Should include empty string
        Assert.assertTrue(results.contains(""));

        // Should include whitespace
        boolean hasWhitespace = results.stream().anyMatch(s -> !s.isEmpty() && s.trim().isEmpty());
        Assert.assertTrue("Should generate whitespace", hasWhitespace);
    }

    @Test
    public void testGenerateRandomChars() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        String result = GarbageGenerator.generateRandomChars(rnd);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.length() >= 1);
    }

    @Test
    public void testGenerateEdgeCases() {
        Set<String> results = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            Rnd rnd = new Rnd(SEED0 + i, SEED1 + i);
            results.add(GarbageGenerator.generateEdgeCase(rnd));
        }

        // Should include various edge cases
        boolean hasNullByte = results.stream().anyMatch(s -> s.contains("\0"));
        boolean hasRepeatedParens = results.stream().anyMatch(s -> s.contains("((((") || s.contains("))))"));

        Assert.assertTrue("Should generate null bytes", hasNullByte);
        Assert.assertTrue("Should generate repeated parens", hasRepeatedParens);
    }

    @Test
    public void testGenerateManyIterations() {
        // Stress test
        for (int i = 0; i < 1000; i++) {
            Rnd rnd = new Rnd(SEED0 + i, SEED1 + i);
            TokenizedQuery query = GarbageGenerator.generate(rnd);
            Assert.assertNotNull(query);
            Assert.assertNotNull(query.serialize());
        }
    }

    @Test
    public void testGenerateRawManyIterations() {
        // Stress test for raw generation
        for (int i = 0; i < 1000; i++) {
            Rnd rnd = new Rnd(SEED0 + i, SEED1 + i);
            String raw = GarbageGenerator.generateRaw(rnd);
            Assert.assertNotNull(raw);
        }
    }

    @Test
    public void testGenerateCanSerialize() {
        // All generated garbage should be serializable
        for (int i = 0; i < 100; i++) {
            Rnd rnd = new Rnd(SEED0 + i, SEED1 + i);
            TokenizedQuery query = GarbageGenerator.generate(rnd);
            String serialized = query.serialize();
            Assert.assertNotNull(serialized);
        }
    }
}
