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
 * Interface for corruption strategies that modify valid TokenizedQuery instances
 * to create intentionally malformed SQL for parser error handling testing.
 * <p>
 * Each strategy implements a specific type of corruption, such as:
 * <ul>
 *     <li>Dropping tokens</li>
 *     <li>Swapping adjacent tokens</li>
 *     <li>Duplicating tokens</li>
 *     <li>Injecting random keywords</li>
 *     <li>Truncating the query</li>
 *     <li>Inserting random characters</li>
 *     <li>Unbalancing parentheses</li>
 * </ul>
 * <p>
 * Strategies should be deterministic given the same Rnd state, to allow
 * reproduction of specific corruptions for debugging.
 */
public interface CorruptionStrategy {

    /**
     * Applies this corruption strategy to the given query.
     *
     * @param query the valid query to corrupt
     * @param rnd   random source for deterministic corruption
     * @return a new TokenizedQuery with the corruption applied
     */
    TokenizedQuery apply(TokenizedQuery query, Rnd rnd);

    /**
     * Returns a human-readable name for this strategy.
     * Used for logging and debugging.
     */
    String name();

    /**
     * Returns true if this strategy can be applied to the given query.
     * Some strategies require a minimum token count or specific token types.
     */
    default boolean canApply(TokenizedQuery query) {
        return query != null && !query.isEmpty();
    }
}
