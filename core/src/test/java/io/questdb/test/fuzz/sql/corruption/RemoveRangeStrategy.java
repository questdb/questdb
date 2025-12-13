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
 * Removes a consecutive range of tokens from the query.
 * This simulates accidentally deleting a portion of a query.
 */
public final class RemoveRangeStrategy implements CorruptionStrategy {

    public static final RemoveRangeStrategy INSTANCE = new RemoveRangeStrategy();

    private RemoveRangeStrategy() {
    }

    @Override
    public TokenizedQuery apply(TokenizedQuery query, Rnd rnd) {
        if (!canApply(query)) {
            return query;
        }

        int size = query.size();
        // Remove 1-3 tokens, but leave at least 1
        int removeCount = Math.min(1 + rnd.nextInt(3), size - 1);
        int start = rnd.nextInt(size - removeCount + 1);

        return query.removeRange(start, start + removeCount);
    }

    @Override
    public String name() {
        return "RemoveRange";
    }

    @Override
    public boolean canApply(TokenizedQuery query) {
        return query != null && query.size() >= 2;
    }
}
