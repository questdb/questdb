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
 * Truncates the query at a random position.
 * This simulates incomplete queries.
 */
public final class TruncateStrategy implements CorruptionStrategy {

    public static final TruncateStrategy INSTANCE = new TruncateStrategy();

    private TruncateStrategy() {
    }

    @Override
    public TokenizedQuery apply(TokenizedQuery query, Rnd rnd) {
        if (!canApply(query)) {
            return query;
        }
        // Truncate at position 1 to size-1 (keep at least 1 token, remove at least 1)
        int position = 1 + rnd.nextInt(query.size() - 1);
        return query.truncateAt(position);
    }

    @Override
    public String name() {
        return "Truncate";
    }

    @Override
    public boolean canApply(TokenizedQuery query) {
        return query != null && query.size() >= 2;
    }
}
