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
 * Unbalances parentheses by removing or adding unmatched parens.
 * This tests parser handling of unbalanced parentheses.
 */
public final class UnbalanceParensStrategy implements CorruptionStrategy {

    public static final UnbalanceParensStrategy INSTANCE = new UnbalanceParensStrategy();

    private UnbalanceParensStrategy() {
    }

    @Override
    public TokenizedQuery apply(TokenizedQuery query, Rnd rnd) {
        if (!canApply(query)) {
            return query;
        }

        int action = rnd.nextInt(4);
        switch (action) {
            case 0:
                // Remove an opening paren
                return removeFirstMatchingToken(query, "(");
            case 1:
                // Remove a closing paren
                return removeFirstMatchingToken(query, ")");
            case 2:
                // Add extra opening paren
                int openPos = rnd.nextInt(query.size() + 1);
                return query.insertToken(openPos, SqlToken.punctuation("("));
            default:
                // Add extra closing paren
                int closePos = rnd.nextInt(query.size() + 1);
                return query.insertToken(closePos, SqlToken.punctuation(")"));
        }
    }

    private TokenizedQuery removeFirstMatchingToken(TokenizedQuery query, String value) {
        for (int i = 0; i < query.size(); i++) {
            if (query.get(i).value().equals(value)) {
                return query.removeToken(i);
            }
        }
        // No matching token found, just add the opposite paren
        if (value.equals("(")) {
            return query.insertToken(0, SqlToken.punctuation(")"));
        } else {
            return query.insertToken(query.size(), SqlToken.punctuation("("));
        }
    }

    @Override
    public String name() {
        return "UnbalanceParens";
    }
}
