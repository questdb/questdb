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
 * Inserts random characters as a token at a random position.
 * This tests parser handling of unexpected characters.
 */
public final class CharacterInsertStrategy implements CorruptionStrategy {

    public static final CharacterInsertStrategy INSTANCE = new CharacterInsertStrategy();

    private static final String[] SPECIAL_CHARS = {
            "@", "#", "$", "%", "^", "&", "~", "`", "\\", "|",
            "!", "?", ":", ";", "\"", "'", "[", "]", "{", "}",
            "<", ">", "/", "=", "+", "-", "*", ".", ",",
            "@@", "##", "$$", "%%", "^^", "&&", "~~", "``",
            "->", "<-", "=>", "<=", ">=", "!=", "<>", "==",
            "||", "&&", "++", "--", "**", "//", "::", "..",
            "/*", "*/", "--", "//", ";;", ",,", "((", "))",
            "\u0000", "\u0001", "\n", "\r", "\t"
    };

    private CharacterInsertStrategy() {
    }

    @Override
    public TokenizedQuery apply(TokenizedQuery query, Rnd rnd) {
        String chars = SPECIAL_CHARS[rnd.nextInt(SPECIAL_CHARS.length)];

        if (!canApply(query)) {
            return new TokenizedQuery()
                    .add(SqlToken.punctuation(chars));
        }

        int position = rnd.nextInt(query.size() + 1);
        return query.insertToken(position, SqlToken.punctuation(chars));
    }

    @Override
    public String name() {
        return "CharacterInsert";
    }

    @Override
    public boolean canApply(TokenizedQuery query) {
        return true; // Can always insert characters
    }
}
