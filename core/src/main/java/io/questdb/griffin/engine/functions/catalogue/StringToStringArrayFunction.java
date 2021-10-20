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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.StrArrayFunction;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

class StringToStringArrayFunction extends StrArrayFunction {
    private static final int BRANCH_BEFORE_ITEM = 0;
    private static final int BRANCH_ITEM = 1;
    private static final int BRANCH_AFTER_ITEM = 2;
    private static final int BRANCH_AFTER_LAST_ITEM = 3;
    private static final int BRANCH_DOUBLE_QUOTE = 4;

    private final ObjList<CharSequence> items = new ObjList<>();

    StringToStringArrayFunction(int position, CharSequence type) throws SqlException {
        if (type == null) {
            throw SqlException.$(position, "NULL is not allowed");
        }

        int charIndex = findArrayOpeningBracketIndex(position, type);
        int branch = BRANCH_BEFORE_ITEM;
        int stringStartIndex = -1;
        int stringEndIndex = -1;
        int lastBackslashIndex = -1;
        StringSink sink = Misc.getThreadLocalBuilder();
        int len = type.length();

        out:
        for (charIndex++; charIndex < len; charIndex++) {
            if (lastBackslashIndex == charIndex - 1) {
                if (branch == BRANCH_ITEM || branch == BRANCH_DOUBLE_QUOTE) {
                    sink.put(type, stringStartIndex, lastBackslashIndex);
                } else {
                    branch = BRANCH_ITEM;
                }
                stringStartIndex = stringEndIndex = charIndex;
                continue;
            }

            char ch = type.charAt(charIndex);
            switch (ch) {
                case '\\':
                    if (branch == BRANCH_AFTER_ITEM) {
                        throw SqlException.$(position, "unexpected character after '\"'");
                    }
                    lastBackslashIndex = charIndex;
                    break;
                case '"':
                    if (branch == BRANCH_AFTER_ITEM) {
                        throw SqlException.$(position, "unexpected character after '\"'");
                    }
                    if (branch == BRANCH_ITEM) {
                        throw SqlException.$(position, "unexpected '\"' character");
                    }
                    if (branch == BRANCH_BEFORE_ITEM) {
                        stringStartIndex = charIndex + 1;
                        branch = BRANCH_DOUBLE_QUOTE;
                    } else {
                        stringEndIndex = charIndex - 1;
                        branch = BRANCH_AFTER_ITEM;
                    }
                    break;
                case '{':
                    if (branch != BRANCH_DOUBLE_QUOTE) {
                        throw SqlException.$(position, "unexpected '{' character");
                    }
                    break;
                case '}':
                    if (branch == BRANCH_DOUBLE_QUOTE) {
                        break;
                    }
                    if (branch == BRANCH_BEFORE_ITEM && items.size() > 0) {
                        throw SqlException.$(position, "unexpected '}' character");
                    }
                    if (branch == BRANCH_ITEM || branch == BRANCH_AFTER_ITEM) {
                        commit(type, stringStartIndex, stringEndIndex, sink);
                    }
                    branch = BRANCH_AFTER_LAST_ITEM;
                    break out;
                case ',':
                    if (branch == BRANCH_DOUBLE_QUOTE) {
                        break;
                    }
                    if (branch == BRANCH_BEFORE_ITEM) {
                        throw SqlException.$(position, "unexpected ',' character");
                    }
                    commit(type, stringStartIndex, stringEndIndex, sink);
                    branch = BRANCH_BEFORE_ITEM;
                    break;
                default:
                    if (!GenericLexer.WHITESPACE_CH.contains(ch)) {
                        if (branch == BRANCH_AFTER_ITEM) {
                            throw SqlException.$(position, "unexpected character after '\"'");
                        }
                        if (branch == BRANCH_BEFORE_ITEM) {
                            stringStartIndex = charIndex;
                            branch = BRANCH_ITEM;
                        }
                        stringEndIndex = charIndex;
                    }
            }
        }

        if (branch != BRANCH_AFTER_LAST_ITEM) {
            throw SqlException.$(position, "array must end with '}'");
        }

        for (charIndex++; charIndex < len; charIndex++) {
            char ch = type.charAt(charIndex);
            if (!GenericLexer.WHITESPACE_CH.contains(ch)) {
                throw SqlException.$(position, "unexpected character after '}'");
            }
        }
    }

    private int findArrayOpeningBracketIndex(int position, CharSequence type) throws SqlException {
        int charIndex = 0;
        for (int len = type.length(); charIndex < len; charIndex++) {
            char ch = type.charAt(charIndex);
            if (ch == '{') {
                return charIndex;
            }
            if (!GenericLexer.WHITESPACE_CH.contains(ch)) {
                break;
            }
        }
        throw SqlException.$(position, "array must start with '{'");
    }

    private void commit(CharSequence type, int stringStartIndex, int stringEndIndex, StringSink sink) {
        sink.put(type, stringStartIndex, stringEndIndex + 1);
        items.add(Chars.toString(sink));
        sink.clear();
    }

    @Override
    public int getArrayLength() {
        return items.size();
    }

    @Override
    public CharSequence getStr(Record rec, int arrayIndex) {
        return items.getQuick(arrayIndex);
    }

    @Override
    public void getStr(Record rec, CharSink sink, int arrayIndex) {
        sink.put(getStr(rec, arrayIndex));
    }

    @Override
    public CharSequence getStrB(Record rec, int arrayIndex) {
        return getStr(rec, arrayIndex);
    }

    @Override
    public int getStrLen(Record rec, int arrayIndex) {
        return getStr(rec, arrayIndex).length();
    }

    @Override
    public boolean isConstant() {
        return true;
    }
}
