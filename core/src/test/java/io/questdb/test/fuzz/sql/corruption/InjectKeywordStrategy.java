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
 * Injects a random SQL keyword at a random position.
 * This tests parser handling of unexpected keywords.
 */
public final class InjectKeywordStrategy implements CorruptionStrategy {

    public static final InjectKeywordStrategy INSTANCE = new InjectKeywordStrategy();

    private static final String[] KEYWORDS = {
            "SELECT", "FROM", "WHERE", "AND", "OR", "NOT",
            "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS",
            "ON", "USING", "AS", "ORDER", "BY", "GROUP", "HAVING",
            "LIMIT", "OFFSET", "DISTINCT", "ALL", "UNION", "EXCEPT",
            "INTERSECT", "IN", "EXISTS", "BETWEEN", "LIKE", "IS",
            "NULL", "TRUE", "FALSE", "CASE", "WHEN", "THEN", "ELSE",
            "END", "CAST", "WITH", "RECURSIVE", "INSERT", "INTO",
            "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "TABLE",
            "DROP", "ALTER", "INDEX", "PRIMARY", "KEY", "FOREIGN",
            "REFERENCES", "UNIQUE", "CHECK", "DEFAULT", "CONSTRAINT",
            // QuestDB-specific
            "SAMPLE", "LATEST", "PARTITION", "TIMESTAMP", "ASOF",
            "LT", "SPLICE", "TOLERANCE", "FILL", "ALIGN", "CALENDAR",
            "OBSERVATION", "ZONE"
    };

    private InjectKeywordStrategy() {
    }

    @Override
    public TokenizedQuery apply(TokenizedQuery query, Rnd rnd) {
        if (!canApply(query)) {
            return new TokenizedQuery()
                    .addKeyword(KEYWORDS[rnd.nextInt(KEYWORDS.length)]);
        }
        String keyword = KEYWORDS[rnd.nextInt(KEYWORDS.length)];
        int position = rnd.nextInt(query.size() + 1);
        return query.insertToken(position, SqlToken.keyword(keyword));
    }

    @Override
    public String name() {
        return "InjectKeyword";
    }

    @Override
    public boolean canApply(TokenizedQuery query) {
        return true; // Can always inject a keyword
    }
}
