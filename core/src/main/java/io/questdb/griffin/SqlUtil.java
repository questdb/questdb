/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.std.*;

public class SqlUtil {

    static final CharSequenceHashSet disallowedAliases = new CharSequenceHashSet();

    public static CharSequence fetchNext(GenericLexer lexer) {
        int blockCount = 0;
        boolean lineComment = false;
        while (lexer.hasNext()) {
            CharSequence cs = lexer.next();

            if (lineComment) {
                if (Chars.equals(cs, '\n') || Chars.equals(cs, '\r')) {
                    lineComment = false;
                }
                continue;
            }

            if (Chars.equals("--", cs)) {
                lineComment = true;
                continue;
            }

            if (Chars.equals("/*", cs)) {
                blockCount++;
                continue;
            }

            if (Chars.equals("*/", cs) && blockCount > 0) {
                blockCount--;
                continue;
            }

            if (blockCount == 0 && GenericLexer.WHITESPACE.excludes(cs)) {
                return cs;
            }
        }
        return null;
    }

    public static boolean isNotBindVariable(CharSequence token) {
        int len = token.length();
        if (len < 1) {
            return true;
        }

        final char first = token.charAt(0);
        if (first == ':') {
            return false;
        }

        if (first == '?' && len == 1) {
            return false;
        }

        if (first == '$') {
            try {
                Numbers.parseInt(token, 1, len);
                return false;
            } catch (NumericException e) {
                return true;
            }
        }

        return true;
    }

    static ExpressionNode nextLiteral(ObjectPool<ExpressionNode> pool, CharSequence token, int position) {
        return pool.next().of(ExpressionNode.LITERAL, token, 0, position);
    }

    static CharSequence createColumnAlias(
            CharacterStore store,
            CharSequence base,
            int indexOfDot,
            LowerCaseCharSequenceObjHashMap<QueryColumn> aliasToColumnMap
    ) {
        final boolean disallowed = disallowedAliases.contains(base);

        // short and sweet version
        if (indexOfDot == -1 && !disallowed && aliasToColumnMap.excludes(base)) {
            return base;
        }

        final CharacterStoreEntry characterStoreEntry = store.newEntry();

        if (indexOfDot == -1) {
            if (disallowed) {
                characterStoreEntry.put("column");
            } else {
                characterStoreEntry.put(base);
            }
        } else {
            if (indexOfDot + 1 == base.length()) {
                characterStoreEntry.put("column");
            } else {
                characterStoreEntry.put(base, indexOfDot + 1, base.length());
            }
        }


        int len = characterStoreEntry.length();
        int sequence = 0;
        while (true) {
            if (sequence > 0) {
                characterStoreEntry.trimTo(len);
                characterStoreEntry.put(sequence);
            }
            sequence++;
            CharSequence alias = characterStoreEntry.toImmutable();
            if (aliasToColumnMap.excludes(alias)) {
                return alias;
            }
        }
    }

    static QueryColumn nextColumn(
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<ExpressionNode> sqlNodePool,
            CharSequence alias,
            CharSequence column
    ) {
        return queryColumnPool.next().of(alias, nextLiteral(sqlNodePool, column, 0));
    }

    static {
        for (int i = 0, n = OperatorExpression.operators.size(); i < n; i++) {
            SqlUtil.disallowedAliases.add(OperatorExpression.operators.getQuick(i).token);
        }
    }
}
