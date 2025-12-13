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

public class CorruptionStrategyTest {

    private static final long SEED0 = 123456789L;
    private static final long SEED1 = 987654321L;
    private TokenizedQuery sampleQuery;

    @Before
    public void setUp() {
        // Create a sample query: SELECT col1, col2 FROM table1 WHERE col1 = 1
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

    // --- DropTokenStrategy tests ---

    @Test
    public void testDropTokenReducesSize() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = DropTokenStrategy.INSTANCE.apply(sampleQuery, rnd);

        Assert.assertEquals(sampleQuery.size() - 1, result.size());
    }

    @Test
    public void testDropTokenCannotApplyToSingleToken() {
        TokenizedQuery single = new TokenizedQuery().addKeyword("SELECT");
        Assert.assertFalse(DropTokenStrategy.INSTANCE.canApply(single));
    }

    @Test
    public void testDropTokenReproducible() {
        Rnd rnd1 = new Rnd(SEED0, SEED1);
        Rnd rnd2 = new Rnd(SEED0, SEED1);

        String result1 = DropTokenStrategy.INSTANCE.apply(sampleQuery, rnd1).serialize();
        String result2 = DropTokenStrategy.INSTANCE.apply(sampleQuery, rnd2).serialize();

        Assert.assertEquals(result1, result2);
    }

    // --- SwapTokensStrategy tests ---

    @Test
    public void testSwapTokensKeepsSize() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = SwapTokensStrategy.INSTANCE.apply(sampleQuery, rnd);

        Assert.assertEquals(sampleQuery.size(), result.size());
    }

    @Test
    public void testSwapTokensChangesQuery() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = SwapTokensStrategy.INSTANCE.apply(sampleQuery, rnd);

        Assert.assertNotEquals(sampleQuery.serialize(), result.serialize());
    }

    @Test
    public void testSwapTokensCannotApplyToSingleToken() {
        TokenizedQuery single = new TokenizedQuery().addKeyword("SELECT");
        Assert.assertFalse(SwapTokensStrategy.INSTANCE.canApply(single));
    }

    // --- DuplicateTokenStrategy tests ---

    @Test
    public void testDuplicateTokenIncreasesSize() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = DuplicateTokenStrategy.INSTANCE.apply(sampleQuery, rnd);

        Assert.assertEquals(sampleQuery.size() + 1, result.size());
    }

    @Test
    public void testDuplicateTokenCanApplyToAny() {
        TokenizedQuery single = new TokenizedQuery().addKeyword("SELECT");
        Assert.assertTrue(DuplicateTokenStrategy.INSTANCE.canApply(single));
    }

    // --- InjectKeywordStrategy tests ---

    @Test
    public void testInjectKeywordIncreasesSize() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = InjectKeywordStrategy.INSTANCE.apply(sampleQuery, rnd);

        Assert.assertEquals(sampleQuery.size() + 1, result.size());
    }

    @Test
    public void testInjectKeywordCanApplyToEmpty() {
        TokenizedQuery empty = new TokenizedQuery();
        Assert.assertTrue(InjectKeywordStrategy.INSTANCE.canApply(empty));

        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = InjectKeywordStrategy.INSTANCE.apply(empty, rnd);
        Assert.assertEquals(1, result.size());
    }

    // --- TruncateStrategy tests ---

    @Test
    public void testTruncateReducesSize() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = TruncateStrategy.INSTANCE.apply(sampleQuery, rnd);

        Assert.assertTrue(result.size() < sampleQuery.size());
        Assert.assertTrue(result.size() >= 1);
    }

    @Test
    public void testTruncateCannotApplyToSingleToken() {
        TokenizedQuery single = new TokenizedQuery().addKeyword("SELECT");
        Assert.assertFalse(TruncateStrategy.INSTANCE.canApply(single));
    }

    // --- CharacterInsertStrategy tests ---

    @Test
    public void testCharacterInsertIncreasesSize() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = CharacterInsertStrategy.INSTANCE.apply(sampleQuery, rnd);

        Assert.assertEquals(sampleQuery.size() + 1, result.size());
    }

    @Test
    public void testCharacterInsertCanApplyToEmpty() {
        TokenizedQuery empty = new TokenizedQuery();
        Assert.assertTrue(CharacterInsertStrategy.INSTANCE.canApply(empty));

        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = CharacterInsertStrategy.INSTANCE.apply(empty, rnd);
        Assert.assertEquals(1, result.size());
    }

    // --- UnbalanceParensStrategy tests ---

    @Test
    public void testUnbalanceParensChangesQuery() {
        // Query with parens
        TokenizedQuery withParens = new TokenizedQuery()
                .addKeyword("SELECT")
                .addIdentifier("func")
                .addPunctuation("(")
                .addIdentifier("col")
                .addPunctuation(")")
                .addKeyword("FROM")
                .addIdentifier("t");

        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = UnbalanceParensStrategy.INSTANCE.apply(withParens, rnd);

        // Should be different (either added or removed a paren)
        Assert.assertNotEquals(withParens.serialize(), result.serialize());
    }

    // --- DuplicateClauseStrategy tests ---

    @Test
    public void testDuplicateClauseChangesQuery() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = DuplicateClauseStrategy.INSTANCE.apply(sampleQuery, rnd);

        Assert.assertNotEquals(sampleQuery.serialize(), result.serialize());
    }

    // --- MixKeywordsStrategy tests ---

    @Test
    public void testMixKeywordsChangesQuery() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = MixKeywordsStrategy.INSTANCE.apply(sampleQuery, rnd);

        Assert.assertNotEquals(sampleQuery.serialize(), result.serialize());
    }

    @Test
    public void testMixKeywordsKeepsSize() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = MixKeywordsStrategy.INSTANCE.apply(sampleQuery, rnd);

        // Either same size (replacement) or +1 (injection when no keywords)
        Assert.assertTrue(result.size() >= sampleQuery.size());
    }

    // --- RemoveRangeStrategy tests ---

    @Test
    public void testRemoveRangeReducesSize() {
        Rnd rnd = new Rnd(SEED0, SEED1);
        TokenizedQuery result = RemoveRangeStrategy.INSTANCE.apply(sampleQuery, rnd);

        Assert.assertTrue(result.size() < sampleQuery.size());
    }

    @Test
    public void testRemoveRangeCannotApplyToSingleToken() {
        TokenizedQuery single = new TokenizedQuery().addKeyword("SELECT");
        Assert.assertFalse(RemoveRangeStrategy.INSTANCE.canApply(single));
    }

    // --- All strategies have names ---

    @Test
    public void testAllStrategiesHaveNames() {
        for (CorruptionStrategy strategy : CorruptGenerator.getStrategies()) {
            Assert.assertNotNull(strategy.name());
            Assert.assertFalse(strategy.name().isEmpty());
        }
    }
}
