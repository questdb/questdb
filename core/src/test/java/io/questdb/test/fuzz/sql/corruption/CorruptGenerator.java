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

/**
 * Applies random corruption strategies to valid TokenizedQuery instances.
 * This is the main entry point for the corruption layer.
 */
public final class CorruptGenerator {

    // All available corruption strategies
    private static final CorruptionStrategy[] STRATEGIES = {
            // Token-level corruptions (more common)
            DropTokenStrategy.INSTANCE,
            SwapTokensStrategy.INSTANCE,
            DuplicateTokenStrategy.INSTANCE,
            InjectKeywordStrategy.INSTANCE,
            TruncateStrategy.INSTANCE,
            CharacterInsertStrategy.INSTANCE,
            RemoveRangeStrategy.INSTANCE,
            // Structural corruptions
            UnbalanceParensStrategy.INSTANCE,
            DuplicateClauseStrategy.INSTANCE,
            MixKeywordsStrategy.INSTANCE
    };

    // Strategies that always work (no minimum requirements)
    private static final CorruptionStrategy[] ALWAYS_APPLICABLE = {
            InjectKeywordStrategy.INSTANCE,
            CharacterInsertStrategy.INSTANCE
    };

    private CorruptGenerator() {
    }

    /**
     * Applies a single random corruption to the query.
     *
     * @param query the valid query to corrupt
     * @param rnd   random source
     * @return a new TokenizedQuery with corruption applied
     */
    public static TokenizedQuery corrupt(TokenizedQuery query, Rnd rnd) {
        return corruptN(query, rnd, 1);
    }

    /**
     * Applies multiple random corruptions to the query.
     *
     * @param query the valid query to corrupt
     * @param rnd   random source
     * @param count number of corruptions to apply
     * @return a new TokenizedQuery with corruptions applied
     */
    public static TokenizedQuery corruptN(TokenizedQuery query, Rnd rnd, int count) {
        TokenizedQuery result = query;

        for (int i = 0; i < count; i++) {
            CorruptionStrategy strategy = selectStrategy(result, rnd);
            result = strategy.apply(result, rnd);
        }

        return result;
    }

    /**
     * Applies a random number of corruptions (1-3) to the query.
     */
    public static TokenizedQuery corruptRandom(TokenizedQuery query, Rnd rnd) {
        int count = 1 + rnd.nextInt(3);
        return corruptN(query, rnd, count);
    }

    /**
     * Applies a specific corruption strategy.
     */
    public static TokenizedQuery corruptWith(TokenizedQuery query, Rnd rnd, CorruptionStrategy strategy) {
        return strategy.apply(query, rnd);
    }

    /**
     * Selects a random applicable corruption strategy.
     */
    private static CorruptionStrategy selectStrategy(TokenizedQuery query, Rnd rnd) {
        // Try up to 10 times to find an applicable strategy
        for (int attempt = 0; attempt < 10; attempt++) {
            CorruptionStrategy strategy = STRATEGIES[rnd.nextInt(STRATEGIES.length)];
            if (strategy.canApply(query)) {
                return strategy;
            }
        }
        // Fall back to a strategy that always works
        return ALWAYS_APPLICABLE[rnd.nextInt(ALWAYS_APPLICABLE.length)];
    }

    /**
     * Returns all available corruption strategies.
     */
    public static CorruptionStrategy[] getStrategies() {
        return STRATEGIES.clone();
    }

    /**
     * Returns the number of available strategies.
     */
    public static int strategyCount() {
        return STRATEGIES.length;
    }
}
