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
import io.questdb.test.fuzz.sql.SqlToken;
import io.questdb.test.fuzz.sql.TokenizedQuery;

/**
 * Duplicates a clause keyword to create invalid syntax like "SELECT SELECT".
 * This tests parser handling of duplicate clauses.
 */
public final class DuplicateClauseStrategy implements CorruptionStrategy {

    public static final DuplicateClauseStrategy INSTANCE = new DuplicateClauseStrategy();

    private static final String[] CLAUSE_KEYWORDS = {
            "SELECT", "FROM", "WHERE", "JOIN", "ORDER", "GROUP",
            "HAVING", "LIMIT", "OFFSET", "WITH", "UNION", "EXCEPT",
            "INTERSECT", "SAMPLE", "LATEST"
    };

    private DuplicateClauseStrategy() {
    }

    @Override
    public TokenizedQuery apply(TokenizedQuery query, Rnd rnd) {
        if (!canApply(query)) {
            return query;
        }

        // Find all positions where clause keywords appear
        for (int i = 0; i < query.size(); i++) {
            SqlToken token = query.get(i);
            if (token.isKeyword() && isClauseKeyword(token.value())) {
                // Duplicate this keyword
                if (rnd.nextBoolean()) {
                    return query.duplicateToken(i);
                }
            }
        }

        // No clause keyword found, just pick a random clause and insert it
        String keyword = CLAUSE_KEYWORDS[rnd.nextInt(CLAUSE_KEYWORDS.length)];
        int position = rnd.nextInt(query.size() + 1);
        return query.insertToken(position, SqlToken.keyword(keyword));
    }

    private boolean isClauseKeyword(String value) {
        String upper = value.toUpperCase();
        for (String keyword : CLAUSE_KEYWORDS) {
            if (keyword.equals(upper)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String name() {
        return "DuplicateClause";
    }
}
