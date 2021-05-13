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

    static long expectMicros(CharSequence tok, int position) throws SqlException {
        int k = -1;

        final int len = tok.length();

        // look for end of digits
        for (int i = 0; i < len; i++) {
            char c = tok.charAt(i);
            if (c < '0' || c > '9') {
                k = i;
                break;
            }
        }

        if (k == -1) {
            throw SqlException.$(position + len, "expected interval qualifier in ").put(tok);
        }

        try {
            long interval = Numbers.parseLong(tok, 0, k);
            int nChars = len - k;
            if (nChars > 2) {
                throw SqlException.$(position + k, "expected 1/2 letter interval qualifier in ").put(tok);
            }

            switch (tok.charAt(k)) {
                case 's':
                    if (nChars == 1) {
                        // seconds
                        return interval * 1_000_000L;
                    }
                    break;
                case 'm':
                    if (nChars == 1) {
                        // minutes
                        return interval * 60_000_000L;
                    } else {
                        if (tok.charAt(k + 1) == 's') {
                            // millis
                            return interval * 1_000;
                        }
                    }
                    break;
                case 'h':
                    if (nChars == 1) {
                        // hours
                        return interval * 3_600_000_000L;
                    }
                    break;
                case 'd':
                    if (nChars == 1) {
                        // days
                        return interval * 86_400_000_000L;
                    }
                    break;
            }
        } catch (NumericException ex) {
            // Ignored
        }

        throw SqlException.$(position + len, "invalid interval qualifier ").put(tok);
    }
}
