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
 * Replaces keywords with other keywords to create semantic errors.
 * E.g., "SELECT" becomes "DELETE", "FROM" becomes "INTO".
 */
public final class MixKeywordsStrategy implements CorruptionStrategy {

    public static final MixKeywordsStrategy INSTANCE = new MixKeywordsStrategy();

    private static final String[] REPLACEMENT_KEYWORDS = {
            "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
            "FROM", "INTO", "SET", "VALUES", "TABLE", "INDEX", "VIEW",
            "WHERE", "HAVING", "ON", "USING", "AS", "BY", "TO",
            "AND", "OR", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE",
            "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "NATURAL",
            "ORDER", "GROUP", "PARTITION", "OVER", "WINDOW", "ROWS", "RANGE",
            "LIMIT", "OFFSET", "FETCH", "FIRST", "NEXT", "ONLY",
            "UNION", "EXCEPT", "INTERSECT", "ALL", "DISTINCT",
            "CASE", "WHEN", "THEN", "ELSE", "END", "IF", "NULLIF", "COALESCE",
            "CAST", "CONVERT", "EXTRACT", "SUBSTRING", "TRIM",
            "ASC", "DESC", "NULLS", "FIRST", "LAST",
            "PRIMARY", "FOREIGN", "KEY", "REFERENCES", "UNIQUE", "CHECK",
            "DEFAULT", "CONSTRAINT", "CASCADE", "RESTRICT", "NO", "ACTION",
            // QuestDB-specific
            "SAMPLE", "LATEST", "TIMESTAMP", "ASOF", "LT", "SPLICE",
            "TOLERANCE", "FILL", "PREV", "LINEAR", "NONE", "CALENDAR",
            "OBSERVATION", "ZONE", "DEDUP"
    };

    private MixKeywordsStrategy() {
    }

    @Override
    public TokenizedQuery apply(TokenizedQuery query, Rnd rnd) {
        if (!canApply(query)) {
            return query;
        }

        // Find a keyword to replace
        int keywordCount = 0;
        for (int i = 0; i < query.size(); i++) {
            if (query.get(i).isKeyword()) {
                keywordCount++;
            }
        }

        if (keywordCount == 0) {
            // No keywords, just insert one
            String keyword = REPLACEMENT_KEYWORDS[rnd.nextInt(REPLACEMENT_KEYWORDS.length)];
            int position = rnd.nextInt(query.size() + 1);
            return query.insertToken(position, SqlToken.keyword(keyword));
        }

        // Pick a random keyword position to replace
        int targetKeyword = rnd.nextInt(keywordCount);
        int currentKeyword = 0;

        for (int i = 0; i < query.size(); i++) {
            if (query.get(i).isKeyword()) {
                if (currentKeyword == targetKeyword) {
                    String replacement = REPLACEMENT_KEYWORDS[rnd.nextInt(REPLACEMENT_KEYWORDS.length)];
                    return query.replaceToken(i, SqlToken.keyword(replacement));
                }
                currentKeyword++;
            }
        }

        return query;
    }

    @Override
    public String name() {
        return "MixKeywords";
    }
}
