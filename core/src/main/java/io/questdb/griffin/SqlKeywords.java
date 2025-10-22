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

package io.questdb.griffin;

import io.questdb.std.Chars;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

public class SqlKeywords {
    public static final int CAST_KEYWORD_LENGTH = 4;
    public static final String CONCAT_FUNC_NAME = "concat";
    public static final int DECIMAL_KEYWORD_LENGTH = 7;
    public static final int GEOHASH_KEYWORD_LENGTH = 7;
    protected static final LowerCaseCharSequenceHashSet KEYWORDS = new LowerCaseCharSequenceHashSet();
    private static final LowerCaseCharSequenceHashSet TIMESTAMP_PART_SET = new LowerCaseCharSequenceHashSet();

    public static boolean isAddKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 'a'
                && (tok.charAt(1) | 32) == 'd'
                && (tok.charAt(2) | 32) == 'd';
    }

    public static boolean isAlignKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'a'
                && (tok.charAt(1) | 32) == 'l'
                && (tok.charAt(2) | 32) == 'i'
                && (tok.charAt(3) | 32) == 'g'
                && (tok.charAt(4) | 32) == 'n';
    }

    public static boolean isAllKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 'a'
                && (tok.charAt(1) | 32) == 'l'
                && (tok.charAt(2) | 32) == 'l';
    }

    public static boolean isAlterKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'a'
                && (tok.charAt(1) | 32) == 'l'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'r';
    }

    public static boolean isAndKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 'a'
                && (tok.charAt(1) | 32) == 'n'
                && (tok.charAt(2) | 32) == 'd';
    }

    public static boolean isArrayKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'a'
                && (tok.charAt(1) | 32) == 'r'
                && (tok.charAt(2) | 32) == 'r'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 'y';
    }

    public static boolean isAsKeyword(CharSequence tok) {
        return tok.length() == 2
                && (tok.charAt(0) | 32) == 'a'
                && (tok.charAt(1) | 32) == 's';
    }

    public static boolean isAscKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 'a'
                && (tok.charAt(1) | 32) == 's'
                && (tok.charAt(2) | 32) == 'c';
    }

    public static boolean isAtKeyword(CharSequence tok) {
        return tok.length() == 2
                && (tok.charAt(0) | 32) == 'a'
                && (tok.charAt(1) | 32) == 't';
    }

    public static boolean isAtomicKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'a'
                && (tok.charAt(1) | 32) == 't'
                && (tok.charAt(2) | 32) == 'o'
                && (tok.charAt(3) | 32) == 'm'
                && (tok.charAt(4) | 32) == 'i'
                && (tok.charAt(5) | 32) == 'c';
    }

    public static boolean isAttachKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'a'
                && (tok.charAt(1) | 32) == 't'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 'c'
                && (tok.charAt(5) | 32) == 'h';
    }

    public static boolean isBatchKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'b'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'c'
                && (tok.charAt(4) | 32) == 'h';
    }

    public static boolean isBetweenKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'b'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'w'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 'e'
                && (tok.charAt(6) | 32) == 'n';
    }

    public static boolean isByKeyword(CharSequence tok) {
        return tok.length() == 2
                && (tok.charAt(0) | 32) == 'b'
                && (tok.charAt(1) | 32) == 'y';
    }

    public static boolean isBypassKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'b'
                && (tok.charAt(1) | 32) == 'y'
                && (tok.charAt(2) | 32) == 'p'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 's'
                && (tok.charAt(5) | 32) == 's';
    }

    public static boolean isCacheKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'c'
                && (tok.charAt(3) | 32) == 'h'
                && (tok.charAt(4) | 32) == 'e';
    }

    public static boolean isCalendarKeyword(CharSequence tok) {
        return tok.length() == 8
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'n'
                && (tok.charAt(5) | 32) == 'd'
                && (tok.charAt(6) | 32) == 'a'
                && (tok.charAt(7) | 32) == 'r';
    }

    public static boolean isCancelKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'c'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 'l';
    }

    public static boolean isCancelledKeyword(CharSequence tok) {
        return tok.length() == 9
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'c'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 'l'
                && (tok.charAt(6) | 32) == 'l'
                && (tok.charAt(7) | 32) == 'e'
                && (tok.charAt(8) | 32) == 'd';
    }


    public static boolean isCapacityKeyword(CharSequence tok) {
        return tok.length() == 8
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'p'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 'c'
                && (tok.charAt(5) | 32) == 'i'
                && (tok.charAt(6) | 32) == 't'
                && (tok.charAt(7) | 32) == 'y';
    }

    public static boolean isCaseKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 's'
                && (tok.charAt(3) | 32) == 'e';
    }

    public static boolean isCastKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 's'
                && (tok.charAt(3) | 32) == 't';
    }

    public static boolean isCenturyKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 't'
                && (tok.charAt(4) | 32) == 'u'
                && (tok.charAt(5) | 32) == 'r'
                && (tok.charAt(6) | 32) == 'y';
    }

    public static boolean isColonColon(CharSequence tok) {
        return tok.length() == 2
                && tok.charAt(0) == ':'
                && tok.charAt(1) == ':';
    }

    public static boolean isColumnKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'u'
                && (tok.charAt(4) | 32) == 'm'
                && (tok.charAt(5) | 32) == 'n';
    }

    public static boolean isColumnsKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'u'
                && (tok.charAt(4) | 32) == 'm'
                && (tok.charAt(5) | 32) == 'n'
                && (tok.charAt(6) | 32) == 's';
    }

    public static boolean isConcatKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        // Reference equal in case it's already replaced token name
        if (tok == CONCAT_FUNC_NAME) return true;

        return (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'c'
                && (tok.charAt(4) | 32) == 'a'
                && (tok.charAt(5) | 32) == 't';
    }

    public static boolean isConcatOperator(CharSequence tok) {
        return tok.length() == 2
                && tok.charAt(0) == '|'
                && tok.charAt(1) == '|';
    }

    public static boolean isConvertKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'v'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 'r'
                && (tok.charAt(6) | 32) == 't';
    }

    public static boolean isCopyKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'p'
                && (tok.charAt(3) | 32) == 'y';
    }

    public static boolean isCountKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'u'
                && (tok.charAt(3) | 32) == 'n'
                && (tok.charAt(4) | 32) == 't';
    }

    public static boolean isCreateKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'r'
                && (tok.charAt(2) | 32) == 'e'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 't'
                && (tok.charAt(5) | 32) == 'e';
    }

    public static boolean isCsvKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 's'
                && (tok.charAt(2) | 32) == 'v';
    }

    public static boolean isCurrentKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'c'
                && (tok.charAt(1) | 32) == 'u'
                && (tok.charAt(2) | 32) == 'r'
                && (tok.charAt(3) | 32) == 'r'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 'n'
                && (tok.charAt(6) | 32) == 't'
                ;
    }

    public static boolean isDatabaseKeyword(CharSequence tok) {
        return tok.length() == 8
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 'b'
                && (tok.charAt(5) | 32) == 'a'
                && (tok.charAt(6) | 32) == 's'
                && (tok.charAt(7) | 32) == 'e';
    }

    public static boolean isDateStyleKeyword(CharSequence tok) {
        return tok.length() == 9
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 's'
                && (tok.charAt(5) | 32) == 't'
                && (tok.charAt(6) | 32) == 'y'
                && (tok.charAt(7) | 32) == 'l'
                && (tok.charAt(8) | 32) == 'e';
    }

    public static boolean isDayKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'y';
    }

    public static boolean isDaysKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'y'
                && (tok.charAt(3) | 32) == 's';
    }

    public static boolean isDecadeKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'c'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 'd'
                && (tok.charAt(5) | 32) == 'e';
    }

    public static boolean isDecimalKeyword(CharSequence tok) {
        return tok.length() == 7
                && isDecimalKeywordInternal(tok);
    }

    public static boolean isDeclareKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'c'
                && (tok.charAt(3) | 32) == 'l'
                && (tok.charAt(4) | 32) == 'a'
                && (tok.charAt(5) | 32) == 'r'
                && (tok.charAt(6) | 32) == 'e';
    }

    public static boolean isDedupKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'd'
                && (tok.charAt(3) | 32) == 'u'
                && (tok.charAt(4) | 32) == 'p';
    }

    public static boolean isDeduplicateKeyword(CharSequence tok) {
        return tok.length() == 11
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'd'
                && (tok.charAt(3) | 32) == 'u'
                && (tok.charAt(4) | 32) == 'p'
                && (tok.charAt(5) | 32) == 'l'
                && (tok.charAt(6) | 32) == 'i'
                && (tok.charAt(7) | 32) == 'c'
                && (tok.charAt(8) | 32) == 'a'
                && (tok.charAt(9) | 32) == 't'
                && (tok.charAt(10) | 32) == 'e';
    }

    public static boolean isDeferredKeyword(CharSequence tok) {
        return tok.length() == 8
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'f'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'r'
                && (tok.charAt(5) | 32) == 'r'
                && (tok.charAt(6) | 32) == 'e'
                && (tok.charAt(7) | 32) == 'd';
    }

    public static boolean isDelayKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 'y';
    }

    public static boolean isDelimiterKeyword(CharSequence tok) {
        return tok.length() == 9
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'i'
                && (tok.charAt(4) | 32) == 'm'
                && (tok.charAt(5) | 32) == 'i'
                && (tok.charAt(6) | 32) == 't'
                && (tok.charAt(7) | 32) == 'e'
                && (tok.charAt(8) | 32) == 'r';
    }

    public static boolean isDescKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 's'
                && (tok.charAt(3) | 32) == 'c';
    }

    public static boolean isDetachKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 'c'
                && (tok.charAt(5) | 32) == 'h';
    }

    public static boolean isDisableKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 's'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 'b'
                && (tok.charAt(5) | 32) == 'l'
                && (tok.charAt(6) | 32) == 'e';
    }

    public static boolean isDistinctKeyword(CharSequence tok) {
        return tok.length() == 8
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 's'
                && (tok.charAt(3) | 32) == 't'
                && (tok.charAt(4) | 32) == 'i'
                && (tok.charAt(5) | 32) == 'n'
                && (tok.charAt(6) | 32) == 'c'
                && (tok.charAt(7) | 32) == 't';
    }

    public static boolean isDoubleKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'u'
                && (tok.charAt(3) | 32) == 'b'
                && (tok.charAt(4) | 32) == 'l'
                && (tok.charAt(5) | 32) == 'e';
    }

    public static boolean isDowKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'w';
    }

    public static boolean isDoyKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'y';
    }

    public static boolean isDropKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'r'
                && (tok.charAt(2) | 32) == 'o'
                && (tok.charAt(3) | 32) == 'p';
    }

    public static boolean isEmptyAlias(CharSequence tok) {
        return tok.length() == 2
                && ((tok.charAt(0) == '\'' && tok.charAt(1) == '\'') || (tok.charAt(0) == '"' && tok.charAt(1) == '"'));
    }

    public static boolean isEnableKeyword(@NotNull CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'e'
                && (tok.charAt(1) | 32) == 'n'
                && (tok.charAt(2) | 32) == 'a'
                && (tok.charAt(3) | 32) == 'b'
                && (tok.charAt(4) | 32) == 'l'
                && (tok.charAt(5) | 32) == 'e';
    }

    public static boolean isEndKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 'e'
                && (tok.charAt(1) | 32) == 'n'
                && (tok.charAt(2) | 32) == 'd';
    }

    public static boolean isEpochKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'e'
                && (tok.charAt(1) | 32) == 'p'
                && (tok.charAt(2) | 32) == 'o'
                && (tok.charAt(3) | 32) == 'c'
                && (tok.charAt(4) | 32) == 'h';
    }

    public static boolean isEveryKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'e'
                && (tok.charAt(1) | 32) == 'v'
                && (tok.charAt(2) | 32) == 'e'
                && (tok.charAt(3) | 32) == 'r'
                && (tok.charAt(4) | 32) == 'y';
    }

    public static boolean isExceptKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'e'
                && (tok.charAt(1) | 32) == 'x'
                && (tok.charAt(2) | 32) == 'c'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'p'
                && (tok.charAt(5) | 32) == 't';
    }

    public static boolean isExcludeKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'e'
                && (tok.charAt(1) | 32) == 'x'
                && (tok.charAt(2) | 32) == 'c'
                && (tok.charAt(3) | 32) == 'l'
                && (tok.charAt(4) | 32) == 'u'
                && (tok.charAt(5) | 32) == 'd'
                && (tok.charAt(6) | 32) == 'e';
    }

    public static boolean isExclusiveKeyword(CharSequence tok) {
        return tok.length() == 9
                && (tok.charAt(0) | 32) == 'e'
                && (tok.charAt(1) | 32) == 'x'
                && (tok.charAt(2) | 32) == 'c'
                && (tok.charAt(3) | 32) == 'l'
                && (tok.charAt(4) | 32) == 'u'
                && (tok.charAt(5) | 32) == 's'
                && (tok.charAt(6) | 32) == 'i'
                && (tok.charAt(7) | 32) == 'v'
                && (tok.charAt(8) | 32) == 'e';
    }

    public static boolean isExistsKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'e'
                && (tok.charAt(1) | 32) == 'x'
                && (tok.charAt(2) | 32) == 'i'
                && (tok.charAt(3) | 32) == 's'
                && (tok.charAt(4) | 32) == 't'
                && (tok.charAt(5) | 32) == 's';
    }

    public static boolean isExplainKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'e'
                && (tok.charAt(1) | 32) == 'x'
                && (tok.charAt(2) | 32) == 'p'
                && (tok.charAt(3) | 32) == 'l'
                && (tok.charAt(4) | 32) == 'a'
                && (tok.charAt(5) | 32) == 'i'
                && (tok.charAt(6) | 32) == 'n';
    }

    public static boolean isExtractKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'e'
                && (tok.charAt(1) | 32) == 'x'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'r'
                && (tok.charAt(4) | 32) == 'a'
                && (tok.charAt(5) | 32) == 'c'
                && (tok.charAt(6) | 32) == 't';
    }

    public static boolean isFailedKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'f'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'i'
                && (tok.charAt(3) | 32) == 'l'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 'd';
    }

    public static boolean isFalseKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'f'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 's'
                && (tok.charAt(4) | 32) == 'e';
    }

    public static boolean isFalseKeyword(Utf8Sequence tok) {
        return tok.size() == 5
                && (tok.byteAt(0) | 32) == 'f'
                && (tok.byteAt(1) | 32) == 'a'
                && (tok.byteAt(2) | 32) == 'l'
                && (tok.byteAt(3) | 32) == 's'
                && (tok.byteAt(4) | 32) == 'e';
    }

    public static boolean isFillKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'f'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'l';
    }

    public static boolean isFinishedKeyword(CharSequence tok) {
        return tok.length() == 8
                && (tok.charAt(0) | 32) == 'f'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'i'
                && (tok.charAt(4) | 32) == 's'
                && (tok.charAt(5) | 32) == 'h'
                && (tok.charAt(6) | 32) == 'e'
                && (tok.charAt(7) | 32) == 'd';
    }

    public static boolean isFirstKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'f'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'r'
                && (tok.charAt(3) | 32) == 's'
                && (tok.charAt(4) | 32) == 't';
    }

    public static boolean isFloat4Keyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'f'
                && (tok.charAt(1) | 32) == 'l'
                && (tok.charAt(2) | 32) == 'o'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 't'
                && (tok.charAt(5)) == '4';
    }

    public static boolean isFloat8Keyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'f'
                && (tok.charAt(1) | 32) == 'l'
                && (tok.charAt(2) | 32) == 'o'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 't'
                && (tok.charAt(5)) == '8';
    }

    // only for Python drivers, which use 'float' keyword to represent double in Java
    // for example, 'NaN'::float   'Infinity'::float
    public static boolean isFloatKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'f'
                && (tok.charAt(1) | 32) == 'l'
                && (tok.charAt(2) | 32) == 'o'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 't';
    }

    public static boolean isFollowingKeyword(CharSequence tok) {
        return tok.length() == 9
                && (tok.charAt(0) | 32) == 'f'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'l'
                && (tok.charAt(4) | 32) == 'o'
                && (tok.charAt(5) | 32) == 'w'
                && (tok.charAt(6) | 32) == 'i'
                && (tok.charAt(7) | 32) == 'n'
                && (tok.charAt(8) | 32) == 'g';
    }

    public static boolean isForceKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'f'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'r'
                && (tok.charAt(3) | 32) == 'c'
                && (tok.charAt(4) | 32) == 'e';
    }

    public static boolean isFormatKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'f'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'r'
                && (tok.charAt(3) | 32) == 'm'
                && (tok.charAt(4) | 32) == 'a'
                && (tok.charAt(5) | 32) == 't';
    }

    public static boolean isFromKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'f'
                && (tok.charAt(1) | 32) == 'r'
                && (tok.charAt(2) | 32) == 'o'
                && (tok.charAt(3) | 32) == 'm';
    }

    public static boolean isFullKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'f'
                && (tok.charAt(1) | 32) == 'u'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'l';
    }

    public static boolean isGeoHashKeyword(CharSequence tok) {
        return tok.length() == 7
                && isGeoHashKeywordInternal(tok);
    }

    public static boolean isGroupKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'g'
                && (tok.charAt(1) | 32) == 'r'
                && (tok.charAt(2) | 32) == 'o'
                && (tok.charAt(3) | 32) == 'u'
                && (tok.charAt(4) | 32) == 'p';
    }

    public static boolean isGroupsKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'g'
                && (tok.charAt(1) | 32) == 'r'
                && (tok.charAt(2) | 32) == 'o'
                && (tok.charAt(3) | 32) == 'u'
                && (tok.charAt(4) | 32) == 'p'
                && (tok.charAt(5) | 32) == 's';
    }

    public static boolean isHeaderKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'h'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'a'
                && (tok.charAt(3) | 32) == 'd'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 'r';
    }

    public static boolean isHourKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'h'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'u'
                && (tok.charAt(3) | 32) == 'r';
    }

    public static boolean isHoursKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'h'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'u'
                && (tok.charAt(3) | 32) == 'r'
                && (tok.charAt(4) | 32) == 's';
    }

    public static boolean isIfKeyword(CharSequence tok) {
        return tok.length() == 2
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 'f';
    }

    public static boolean isIgnoreWord(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 'g'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'o'
                && (tok.charAt(4) | 32) == 'r'
                && (tok.charAt(5) | 32) == 'e';
    }

    public static boolean isImmediateKeyword(CharSequence tok) {
        return tok.length() == 9
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 'm'
                && (tok.charAt(2) | 32) == 'm'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'd'
                && (tok.charAt(5) | 32) == 'i'
                && (tok.charAt(6) | 32) == 'a'
                && (tok.charAt(7) | 32) == 't'
                && (tok.charAt(8) | 32) == 'e';
    }

    public static boolean isInKeyword(CharSequence tok) {
        return tok.length() == 2
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 'n';
    }

    public static boolean isIncrementalKeyword(CharSequence tok) {
        return tok.length() == 11
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 'n'
                && (tok.charAt(2) | 32) == 'c'
                && (tok.charAt(3) | 32) == 'r'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 'm'
                && (tok.charAt(6) | 32) == 'e'
                && (tok.charAt(7) | 32) == 'n'
                && (tok.charAt(8) | 32) == 't'
                && (tok.charAt(9) | 32) == 'a'
                && (tok.charAt(10) | 32) == 'l';
    }

    public static boolean isIndexKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 'n'
                && (tok.charAt(2) | 32) == 'd'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'x';
    }

    public static boolean isInsertKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 'n'
                && (tok.charAt(2) | 32) == 's'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'r'
                && (tok.charAt(5) | 32) == 't';
    }

    public static boolean isInt2Keyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 'n'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == '2';
    }

    public static boolean isInt4Keyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 'n'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == '4';
    }

    public static boolean isInt8Keyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 'n'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == '8';
    }

    public static boolean isIntersectKeyword(CharSequence tok) {
        return tok.length() == 9
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 'n'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'r'
                && (tok.charAt(5) | 32) == 's'
                && (tok.charAt(6) | 32) == 'e'
                && (tok.charAt(7) | 32) == 'c'
                && (tok.charAt(8) | 32) == 't';
    }

    public static boolean isIntervalKeyword(CharSequence tok) {
        return tok.length() == 8
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 'n'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'r'
                && (tok.charAt(5) | 32) == 'v'
                && (tok.charAt(6) | 32) == 'a'
                && (tok.charAt(7) | 32) == 'l';
    }

    public static boolean isIntoKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 'n'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'o';
    }

    public static boolean isIsKeyword(CharSequence tok) {
        return tok.length() == 2
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 's';
    }

    public static boolean isIsoDowKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 's'
                && (tok.charAt(2) | 32) == 'o'
                && (tok.charAt(3) | 32) == 'd'
                && (tok.charAt(4) | 32) == 'o'
                && (tok.charAt(5) | 32) == 'w';
    }

    public static boolean isIsoYearKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 's'
                && (tok.charAt(2) | 32) == 'o'
                && (tok.charAt(3) | 32) == 'y'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 'a'
                && (tok.charAt(6) | 32) == 'r';
    }

    public static boolean isIsolationKeyword(CharSequence tok) {
        return tok.length() == 9
                && (tok.charAt(0) | 32) == 'i'
                && (tok.charAt(1) | 32) == 's'
                && (tok.charAt(2) | 32) == 'o'
                && (tok.charAt(3) | 32) == 'l'
                && (tok.charAt(4) | 32) == 'a'
                && (tok.charAt(5) | 32) == 't'
                && (tok.charAt(6) | 32) == 'i'
                && (tok.charAt(7) | 32) == 'o'
                && (tok.charAt(8) | 32) == 'n';
    }

    public static boolean isJsonExtract(CharSequence tok) {
        return tok.length() == 12
                && (tok.charAt(0) | 32) == 'j'
                && (tok.charAt(1) | 32) == 's'
                && (tok.charAt(2) | 32) == 'o'
                && (tok.charAt(3) | 32) == 'n'
                && tok.charAt(4) == '_'
                && (tok.charAt(5) | 32) == 'e'
                && (tok.charAt(6) | 32) == 'x'
                && (tok.charAt(7) | 32) == 't'
                && (tok.charAt(8) | 32) == 'r'
                && (tok.charAt(9) | 32) == 'a'
                && (tok.charAt(10) | 32) == 'c'
                && (tok.charAt(11) | 32) == 't';
    }

    public static boolean isJsonKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'j'
                && (tok.charAt(1) | 32) == 's'
                && (tok.charAt(2) | 32) == 'o'
                && (tok.charAt(3) | 32) == 'n';
    }

    public static boolean isKeepKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'k'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'e'
                && (tok.charAt(3) | 32) == 'p';
    }

    public static boolean isKeysKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'k'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'y'
                && (tok.charAt(3) | 32) == 's';
    }

    public static boolean isKeyword(CharSequence text) {
        if (text != null) {
            return KEYWORDS.contains(text);
        }
        return false;
    }

    public static boolean isLastKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'l'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 's'
                && (tok.charAt(3) | 32) == 't';
    }

    public static boolean isLatestKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'l'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 's'
                && (tok.charAt(5) | 32) == 't';
    }

    public static boolean isLeftKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'l'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'f'
                && (tok.charAt(3) | 32) == 't';
    }

    public static boolean isLengthKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'l'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'g'
                && (tok.charAt(4) | 32) == 't'
                && (tok.charAt(5) | 32) == 'h';
    }

    public static boolean isLevelKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'l'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'v'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'l';
    }

    public static boolean isLikeKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'l'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'k'
                && (tok.charAt(3) | 32) == 'e';
    }

    public static boolean isLimitKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'l'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'm'
                && (tok.charAt(3) | 32) == 'i'
                && (tok.charAt(4) | 32) == 't';
    }

    public static boolean isLinearKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'l'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'a'
                && (tok.charAt(5) | 32) == 'r';
    }

    public static boolean isListKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'l'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 's'
                && (tok.charAt(3) | 32) == 't';
    }

    public static boolean isLockKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'l'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'c'
                && (tok.charAt(3) | 32) == 'k';
    }

    public static boolean isManualKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'm'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'u'
                && (tok.charAt(4) | 32) == 'a'
                && (tok.charAt(5) | 32) == 'l';
    }

    public static boolean isMapsKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'm'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'p'
                && (tok.charAt(3) | 32) == 's';
    }

    public static boolean isMaterializedKeyword(CharSequence tok) {
        return tok.length() == 12
                && (tok.charAt(0) | 32) == 'm'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'r'
                && (tok.charAt(5) | 32) == 'i'
                && (tok.charAt(6) | 32) == 'a'
                && (tok.charAt(7) | 32) == 'l'
                && (tok.charAt(8) | 32) == 'i'
                && (tok.charAt(9) | 32) == 'z'
                && (tok.charAt(10) | 32) == 'e'
                && (tok.charAt(11) | 32) == 'd';
    }

    public static boolean isMaxIdentifierLength(CharSequence tok) {
        return tok.length() == 21
                && (tok.charAt(0) | 32) == 'm'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'x'
                && tok.charAt(3) == '_'
                && (tok.charAt(4) | 32) == 'i'
                && (tok.charAt(5) | 32) == 'd'
                && (tok.charAt(6) | 32) == 'e'
                && (tok.charAt(7) | 32) == 'n'
                && (tok.charAt(8) | 32) == 't'
                && (tok.charAt(9) | 32) == 'i'
                && (tok.charAt(10) | 32) == 'f'
                && (tok.charAt(11) | 32) == 'i'
                && (tok.charAt(12) | 32) == 'e'
                && (tok.charAt(13) | 32) == 'r'
                && tok.charAt(14) == '_'
                && (tok.charAt(15) | 32) == 'l'
                && (tok.charAt(16) | 32) == 'e'
                && (tok.charAt(17) | 32) == 'n'
                && (tok.charAt(18) | 32) == 'g'
                && (tok.charAt(19) | 32) == 't'
                && (tok.charAt(20) | 32) == 'h';
    }

    public static boolean isMaxUncommittedRowsKeyword(CharSequence tok) {
        return tok.length() == 18
                && (tok.charAt(0) | 32) == 'm'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'x'
                && (tok.charAt(3) | 32) == 'u'
                && (tok.charAt(4) | 32) == 'n'
                && (tok.charAt(5) | 32) == 'c'
                && (tok.charAt(6) | 32) == 'o'
                && (tok.charAt(7) | 32) == 'm'
                && (tok.charAt(8) | 32) == 'm'
                && (tok.charAt(9) | 32) == 'i'
                && (tok.charAt(10) | 32) == 't'
                && (tok.charAt(11) | 32) == 't'
                && (tok.charAt(12) | 32) == 'e'
                && (tok.charAt(13) | 32) == 'd'
                && (tok.charAt(14) | 32) == 'r'
                && (tok.charAt(15) | 32) == 'o'
                && (tok.charAt(16) | 32) == 'w'
                && (tok.charAt(17) | 32) == 's';
    }

    public static boolean isMicrosecondKeyword(CharSequence tok) {
        return tok.length() == 11
                && (tok.charAt(0) | 32) == 'm'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'c'
                && (tok.charAt(3) | 32) == 'r'
                && (tok.charAt(4) | 32) == 'o'
                && (tok.charAt(5) | 32) == 's'
                && (tok.charAt(6) | 32) == 'e'
                && (tok.charAt(7) | 32) == 'c'
                && (tok.charAt(8) | 32) == 'o'
                && (tok.charAt(9) | 32) == 'n'
                && (tok.charAt(10) | 32) == 'd';
    }

    public static boolean isMicrosecondsKeyword(CharSequence tok) {
        return tok.length() == 12
                && (tok.charAt(0) | 32) == 'm'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'c'
                && (tok.charAt(3) | 32) == 'r'
                && (tok.charAt(4) | 32) == 'o'
                && (tok.charAt(5) | 32) == 's'
                && (tok.charAt(6) | 32) == 'e'
                && (tok.charAt(7) | 32) == 'c'
                && (tok.charAt(8) | 32) == 'o'
                && (tok.charAt(9) | 32) == 'n'
                && (tok.charAt(10) | 32) == 'd'
                && (tok.charAt(11) | 32) == 's';
    }

    public static boolean isMillenniumKeyword(CharSequence tok) {
        return tok.length() == 10
                && (tok.charAt(0) | 32) == 'm'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'l'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 'n'
                && (tok.charAt(6) | 32) == 'n'
                && (tok.charAt(7) | 32) == 'i'
                && (tok.charAt(8) | 32) == 'u'
                && (tok.charAt(9) | 32) == 'm';
    }

    public static boolean isMillisecondKeyword(CharSequence tok) {
        return tok.length() == 11
                && (tok.charAt(0) | 32) == 'm'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'l'
                && (tok.charAt(4) | 32) == 'i'
                && (tok.charAt(5) | 32) == 's'
                && (tok.charAt(6) | 32) == 'e'
                && (tok.charAt(7) | 32) == 'c'
                && (tok.charAt(8) | 32) == 'o'
                && (tok.charAt(9) | 32) == 'n'
                && (tok.charAt(10) | 32) == 'd';
    }

    public static boolean isMillisecondsKeyword(CharSequence tok) {
        return tok.length() == 12
                && (tok.charAt(0) | 32) == 'm'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'l'
                && (tok.charAt(4) | 32) == 'i'
                && (tok.charAt(5) | 32) == 's'
                && (tok.charAt(6) | 32) == 'e'
                && (tok.charAt(7) | 32) == 'c'
                && (tok.charAt(8) | 32) == 'o'
                && (tok.charAt(9) | 32) == 'n'
                && (tok.charAt(10) | 32) == 'd'
                && (tok.charAt(11) | 32) == 's';
    }

    public static boolean isMinuteKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'm'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'u'
                && (tok.charAt(4) | 32) == 't'
                && (tok.charAt(5) | 32) == 'e';
    }

    public static boolean isMinutesKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'm'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'u'
                && (tok.charAt(4) | 32) == 't'
                && (tok.charAt(5) | 32) == 'e'
                && (tok.charAt(6) | 32) == 's';
    }

    public static boolean isMonthKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'm'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 't'
                && (tok.charAt(4) | 32) == 'h';
    }

    public static boolean isNanKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 'n'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'n';
    }

    public static boolean isNanosecondKeyword(CharSequence tok) {
        return tok.length() == 10
                && (tok.charAt(0) | 32) == 'n'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'o'
                && (tok.charAt(4) | 32) == 's'
                && (tok.charAt(5) | 32) == 'e'
                && (tok.charAt(6) | 32) == 'c'
                && (tok.charAt(7) | 32) == 'o'
                && (tok.charAt(8) | 32) == 'n'
                && (tok.charAt(9) | 32) == 'd';
    }

    public static boolean isNanosecondsKeyword(CharSequence tok) {
        return tok.length() == 11
                && (tok.charAt(0) | 32) == 'n'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'o'
                && (tok.charAt(4) | 32) == 's'
                && (tok.charAt(5) | 32) == 'e'
                && (tok.charAt(6) | 32) == 'c'
                && (tok.charAt(7) | 32) == 'o'
                && (tok.charAt(8) | 32) == 'n'
                && (tok.charAt(9) | 32) == 'd'
                && (tok.charAt(10) | 32) == 's';
    }

    public static boolean isNativeKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'n'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'i'
                && (tok.charAt(4) | 32) == 'v'
                && (tok.charAt(5) | 32) == 'e';
    }

    public static boolean isNoCacheKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'n'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'c'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 'c'
                && (tok.charAt(5) | 32) == 'h'
                && (tok.charAt(6) | 32) == 'e';
    }

    public static boolean isNoKeyword(CharSequence tok) {
        return tok.length() == 2
                && (tok.charAt(0) | 32) == 'n'
                && (tok.charAt(1) | 32) == 'o';
    }

    public static boolean isNoneKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'n'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'e';
    }

    public static boolean isNotJoinKeyword(CharSequence tok) {
        return tok.length() != 4
                || (tok.charAt(0) | 32) != 'j'
                || (tok.charAt(1) | 32) != 'o'
                || (tok.charAt(2) | 32) != 'i'
                || (tok.charAt(3) | 32) != 'n';
    }

    public static boolean isNotKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 'n'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 't';
    }

    public static boolean isNullKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'n'
                && (tok.charAt(1) | 32) == 'u'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'l';
    }

    public static boolean isNullKeyword(Utf8Sequence tok) {
        return tok.size() == 4
                && (tok.byteAt(0) | 32) == 'n'
                && (tok.byteAt(1) | 32) == 'u'
                && (tok.byteAt(2) | 32) == 'l'
                && (tok.byteAt(3) | 32) == 'l';
    }

    public static boolean isNullsWord(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'n'
                && (tok.charAt(1) | 32) == 'u'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'l'
                && (tok.charAt(4) | 32) == 's';
    }

    public static boolean isNumericKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'n'
                && (tok.charAt(1) | 32) == 'u'
                && (tok.charAt(2) | 32) == 'm'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'r'
                && (tok.charAt(5) | 32) == 'i'
                && (tok.charAt(6) | 32) == 'c';
    }

    public static boolean isO3MaxLagKeyword(CharSequence tok) {
        return tok.length() == 8
                && (tok.charAt(0) | 32) == 'o'
                && (tok.charAt(1) | 32) == '3'
                && (tok.charAt(2) | 32) == 'm'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 'x'
                && (tok.charAt(5) | 32) == 'l'
                && (tok.charAt(6) | 32) == 'a'
                && (tok.charAt(7) | 32) == 'g';
    }

    public static boolean isObservationKeyword(CharSequence tok) {
        return tok.length() == 11
                && (tok.charAt(0) | 32) == 'o'
                && (tok.charAt(1) | 32) == 'b'
                && (tok.charAt(2) | 32) == 's'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'r'
                && (tok.charAt(5) | 32) == 'v'
                && (tok.charAt(6) | 32) == 'a'
                && (tok.charAt(7) | 32) == 't'
                && (tok.charAt(8) | 32) == 'i'
                && (tok.charAt(9) | 32) == 'o'
                && (tok.charAt(10) | 32) == 'n';
    }

    public static boolean isOffsetKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'o'
                && (tok.charAt(1) | 32) == 'f'
                && (tok.charAt(2) | 32) == 'f'
                && (tok.charAt(3) | 32) == 's'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 't';
    }

    public static boolean isOnKeyword(CharSequence tok) {
        return tok.length() == 2
                && (tok.charAt(0) | 32) == 'o'
                && (tok.charAt(1) | 32) == 'n';
    }

    public static boolean isOnlyKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'o'
                && (tok.charAt(1) | 32) == 'n'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'y';
    }

    public static boolean isOrKeyword(CharSequence tok) {
        return tok.length() == 2
                && (tok.charAt(0) | 32) == 'o'
                && (tok.charAt(1) | 32) == 'r';
    }

    public static boolean isOrderKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'o'
                && (tok.charAt(1) | 32) == 'r'
                && (tok.charAt(2) | 32) == 'd'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'r';
    }

    public static boolean isOthersKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'o'
                && (tok.charAt(1) | 32) == 't'
                && (tok.charAt(2) | 32) == 'h'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'r'
                && (tok.charAt(5) | 32) == 's';
    }

    public static boolean isOuterKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'o'
                && (tok.charAt(1) | 32) == 'u'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'r';
    }

    public static boolean isOverKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'o'
                && (tok.charAt(1) | 32) == 'v'
                && (tok.charAt(2) | 32) == 'e'
                && (tok.charAt(3) | 32) == 'r';
    }

    public static boolean isParamKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'p'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'r'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 'm';
    }

    public static boolean isParametersKeyword(CharSequence tok) {
        if (tok.length() != 10) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isParquetKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'p'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'r'
                && (tok.charAt(3) | 32) == 'q'
                && (tok.charAt(4) | 32) == 'u'
                && (tok.charAt(5) | 32) == 'e'
                && (tok.charAt(6) | 32) == 't';
    }

    public static boolean isPartitionKeyword(CharSequence tok) {
        return tok.length() == 9
                && (tok.charAt(0) | 32) == 'p'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'r'
                && (tok.charAt(3) | 32) == 't'
                && (tok.charAt(4) | 32) == 'i'
                && (tok.charAt(5) | 32) == 't'
                && (tok.charAt(6) | 32) == 'i'
                && (tok.charAt(7) | 32) == 'o'
                && (tok.charAt(8) | 32) == 'n';
    }

    public static boolean isPartitionsKeyword(CharSequence tok) {
        return tok.length() == 10
                && (tok.charAt(0) | 32) == 'p'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'r'
                && (tok.charAt(3) | 32) == 't'
                && (tok.charAt(4) | 32) == 'i'
                && (tok.charAt(5) | 32) == 't'
                && (tok.charAt(6) | 32) == 'i'
                && (tok.charAt(7) | 32) == 'o'
                && (tok.charAt(8) | 32) == 'n'
                && (tok.charAt(9) | 32) == 's';
    }

    public static boolean isPeriodKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'p'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'r'
                && (tok.charAt(3) | 32) == 'i'
                && (tok.charAt(4) | 32) == 'o'
                && (tok.charAt(5) | 32) == 'd';
    }

    public static boolean isPrecedingKeyword(CharSequence tok) {
        return tok.length() == 9
                && (tok.charAt(0) | 32) == 'p'
                && (tok.charAt(1) | 32) == 'r'
                && (tok.charAt(2) | 32) == 'e'
                && (tok.charAt(3) | 32) == 'c'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 'd'
                && (tok.charAt(6) | 32) == 'i'
                && (tok.charAt(7) | 32) == 'n'
                && (tok.charAt(8) | 32) == 'g';
    }

    public static boolean isPrecisionKeyword(CharSequence tok) {
        return tok.length() == 9
                && (tok.charAt(0) | 32) == 'p'
                && (tok.charAt(1) | 32) == 'r'
                && (tok.charAt(2) | 32) == 'e'
                && (tok.charAt(3) | 32) == 'c'
                && (tok.charAt(4) | 32) == 'i'
                && (tok.charAt(5) | 32) == 's'
                && (tok.charAt(6) | 32) == 'i'
                && (tok.charAt(7) | 32) == 'o'
                && (tok.charAt(8) | 32) == 'n';
    }

    public static boolean isPrevKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'p'
                && (tok.charAt(1) | 32) == 'r'
                && (tok.charAt(2) | 32) == 'e'
                && (tok.charAt(3) | 32) == 'v';
    }

    public static boolean isPublicKeyword(CharSequence tok, int len) {
        return isPublicKeyword(tok, 0, len);
    }

    public static boolean isPublicKeyword(CharSequence tok, int lo, int hi) {
        int len = hi - lo;

        return len == 6
                && (tok.charAt(lo) | 32) == 'p'
                && (tok.charAt(lo + 1) | 32) == 'u'
                && (tok.charAt(lo + 2) | 32) == 'b'
                && (tok.charAt(lo + 3) | 32) == 'l'
                && (tok.charAt(lo + 4) | 32) == 'i'
                && (tok.charAt(lo + 5) | 32) == 'c';
    }

    public static boolean isQuarterKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'q'
                && (tok.charAt(1) | 32) == 'u'
                && (tok.charAt(2) | 32) == 'a'
                && (tok.charAt(3) | 32) == 'r'
                && (tok.charAt(4) | 32) == 't'
                && (tok.charAt(5) | 32) == 'e'
                && (tok.charAt(6) | 32) == 'r';
    }

    public static boolean isQueryKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'q'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i) | 32) == 'y';
    }

    public static boolean isQuote(CharSequence tok) {
        return tok.length() == 1
                && tok.charAt(0) == '\'';
    }

    public static boolean isRangeKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'r'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'g'
                && (tok.charAt(4) | 32) == 'e';
    }

    public static boolean isRefreshKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'r'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'f'
                && (tok.charAt(3) | 32) == 'r'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 's'
                && (tok.charAt(6) | 32) == 'h';
    }

    public static boolean isRenameKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'r'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 'm'
                && (tok.charAt(5) | 32) == 'e';
    }

    public static boolean isRespectWord(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 'r'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 's'
                && (tok.charAt(3) | 32) == 'p'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 'c'
                && (tok.charAt(6) | 32) == 't';
    }

    public static boolean isResumeKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'r'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 's'
                && (tok.charAt(3) | 32) == 'u'
                && (tok.charAt(4) | 32) == 'm'
                && (tok.charAt(5) | 32) == 'e';
    }

    public static boolean isRightKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'r'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'g'
                && (tok.charAt(3) | 32) == 'h'
                && (tok.charAt(4) | 32) == 't';
    }

    public static boolean isRowKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 'r'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'w';
    }

    public static boolean isRowsKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'r'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'w'
                && (tok.charAt(3) | 32) == 's';
    }

    public static boolean isSampleKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 's'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'm'
                && (tok.charAt(3) | 32) == 'p'
                && (tok.charAt(4) | 32) == 'l'
                && (tok.charAt(5) | 32) == 'e';
    }

    public static boolean isSearchPath(CharSequence tok) {
        return tok.length() == 11
                && (tok.charAt(0) | 32) == 's'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'a'
                && (tok.charAt(3) | 32) == 'r'
                && (tok.charAt(4) | 32) == 'c'
                && (tok.charAt(5) | 32) == 'h'
                && (tok.charAt(6)) == '_'
                && (tok.charAt(7) | 32) == 'p'
                && (tok.charAt(8) | 32) == 'a'
                && (tok.charAt(9) | 32) == 't'
                && (tok.charAt(10) | 32) == 'h';
    }

    public static boolean isSecondKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 's'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'c'
                && (tok.charAt(3) | 32) == 'o'
                && (tok.charAt(4) | 32) == 'n'
                && (tok.charAt(5) | 32) == 'd';
    }

    public static boolean isSecondsKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 's'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'c'
                && (tok.charAt(3) | 32) == 'o'
                && (tok.charAt(4) | 32) == 'n'
                && (tok.charAt(5) | 32) == 'd'
                && (tok.charAt(6) | 32) == 's';
    }

    public static boolean isSelectKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 's'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'c'
                && (tok.charAt(5) | 32) == 't';
    }

    public static boolean isSemicolon(CharSequence token) {
        return token.length() == 1
                && token.charAt(0) == ';';
    }

    public static boolean isServerVersionKeyword(CharSequence tok) {
        return tok.length() == 14
                && (tok.charAt(0) | 32) == 's'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'r'
                && (tok.charAt(3) | 32) == 'v'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 'r'
                && (tok.charAt(6)) == '_'
                && (tok.charAt(7) | 32) == 'v'
                && (tok.charAt(8) | 32) == 'e'
                && (tok.charAt(9) | 32) == 'r'
                && (tok.charAt(10) | 32) == 's'
                && (tok.charAt(11) | 32) == 'i'
                && (tok.charAt(12) | 32) == 'o'
                && (tok.charAt(13) | 32) == 'n';
    }

    public static boolean isServerVersionNumKeyword(CharSequence tok) {
        return tok.length() == 18
                && (tok.charAt(0) | 32) == 's'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'r'
                && (tok.charAt(3) | 32) == 'v'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 'r'
                && (tok.charAt(6)) == '_'
                && (tok.charAt(7) | 32) == 'v'
                && (tok.charAt(8) | 32) == 'e'
                && (tok.charAt(9) | 32) == 'r'
                && (tok.charAt(10) | 32) == 's'
                && (tok.charAt(11) | 32) == 'i'
                && (tok.charAt(12) | 32) == 'o'
                && (tok.charAt(13) | 32) == 'n'
                && (tok.charAt(14)) == '_'
                && (tok.charAt(15) | 32) == 'n'
                && (tok.charAt(16) | 32) == 'u'
                && (tok.charAt(17) | 32) == 'm';
    }

    public static boolean isSetKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 's'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 't';
    }

    public static boolean isShowKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'h'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'w';
    }

    public static boolean isSquashKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 's'
                && (tok.charAt(1) | 32) == 'q'
                && (tok.charAt(2) | 32) == 'u'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 's'
                && (tok.charAt(5) | 32) == 'h';
    }

    public static boolean isStandardConformingStrings(CharSequence tok) {
        return tok.length() == 27
                && (tok.charAt(0) | 32) == 's'
                && (tok.charAt(1) | 32) == 't'
                && (tok.charAt(2) | 32) == 'a'
                && (tok.charAt(3) | 32) == 'n'
                && (tok.charAt(4) | 32) == 'd'
                && (tok.charAt(5) | 32) == 'a'
                && (tok.charAt(6) | 32) == 'r'
                && (tok.charAt(7) | 32) == 'd'
                && (tok.charAt(8)) == '_'
                && (tok.charAt(9) | 32) == 'c'
                && (tok.charAt(10) | 32) == 'o'
                && (tok.charAt(11) | 32) == 'n'
                && (tok.charAt(12) | 32) == 'f'
                && (tok.charAt(13) | 32) == 'o'
                && (tok.charAt(14) | 32) == 'r'
                && (tok.charAt(15) | 32) == 'm'
                && (tok.charAt(16) | 32) == 'i'
                && (tok.charAt(17) | 32) == 'n'
                && (tok.charAt(18) | 32) == 'g'
                && (tok.charAt(19)) == '_'
                && (tok.charAt(20) | 32) == 's'
                && (tok.charAt(21) | 32) == 't'
                && (tok.charAt(22) | 32) == 'r'
                && (tok.charAt(23) | 32) == 'i'
                && (tok.charAt(24) | 32) == 'n'
                && (tok.charAt(25) | 32) == 'g'
                && (tok.charAt(26) | 32) == 's';
    }

    public static boolean isStartKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 's'
                && (tok.charAt(1) | 32) == 't'
                && (tok.charAt(2) | 32) == 'a'
                && (tok.charAt(3) | 32) == 'r'
                && (tok.charAt(4) | 32) == 't';
    }

    public static boolean isSumKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 's'
                && (tok.charAt(1) | 32) == 'u'
                && (tok.charAt(2) | 32) == 'm';
    }

    public static boolean isSuspendKeyword(CharSequence tok) {
        return tok.length() == 7
                && (tok.charAt(0) | 32) == 's'
                && (tok.charAt(1) | 32) == 'u'
                && (tok.charAt(2) | 32) == 's'
                && (tok.charAt(3) | 32) == 'p'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 'n'
                && (tok.charAt(6) | 32) == 'd';
    }

    public static boolean isSymbolKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 's'
                && (tok.charAt(1) | 32) == 'y'
                && (tok.charAt(2) | 32) == 'm'
                && (tok.charAt(3) | 32) == 'b'
                && (tok.charAt(4) | 32) == 'o'
                && (tok.charAt(5) | 32) == 'l';
    }

    public static boolean isTableKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 't'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'b'
                && (tok.charAt(3) | 32) == 'l'
                && (tok.charAt(4) | 32) == 'e';
    }

    public static boolean isTablesKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 't'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'b'
                && (tok.charAt(3) | 32) == 'l'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 's';
    }

    public static boolean isTextKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 't'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'x'
                && (tok.charAt(3) | 32) == 't';
    }

    public static boolean isTiesKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 't'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'e'
                && (tok.charAt(3) | 32) == 's';
    }

    public static boolean isTimeKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 't'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'm'
                && (tok.charAt(3) | 32) == 'e';
    }

    public static boolean isTimestampKeyword(CharSequence tok) {
        return tok.length() == 9
                && (tok.charAt(0) | 32) == 't'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'm'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 's'
                && (tok.charAt(5) | 32) == 't'
                && (tok.charAt(6) | 32) == 'a'
                && (tok.charAt(7) | 32) == 'm'
                && (tok.charAt(8) | 32) == 'p';
    }

    public static boolean isTimestampNsKeyword(CharSequence tok) {
        return tok.length() == 12
                && (tok.charAt(0) | 32) == 't'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'm'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 's'
                && (tok.charAt(5) | 32) == 't'
                && (tok.charAt(6) | 32) == 'a'
                && (tok.charAt(7) | 32) == 'm'
                && (tok.charAt(8) | 32) == 'p'
                && tok.charAt(9) == '_'
                && (tok.charAt(10) | 32) == 'n'
                && (tok.charAt(11) | 32) == 's';
    }

    public static boolean isToKeyword(CharSequence tok) {
        return tok.length() == 2
                && (tok.charAt(0) | 32) == 't'
                && (tok.charAt(1) | 32) == 'o';
    }

    public static boolean isToleranceKeyword(CharSequence tok) {
        return tok.length() == 9
                && (tok.charAt(0) | 32) == 't'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'r'
                && (tok.charAt(5) | 32) == 'a'
                && (tok.charAt(6) | 32) == 'n'
                && (tok.charAt(7) | 32) == 'c'
                && (tok.charAt(8) | 32) == 'e';
    }

    public static boolean isTransactionIsolation(CharSequence tok) {
        return tok.length() == 21
                && (tok.charAt(0) | 32) == 't'
                && (tok.charAt(1) | 32) == 'r'
                && (tok.charAt(2) | 32) == 'a'
                && (tok.charAt(3) | 32) == 'n'
                && (tok.charAt(4) | 32) == 's'
                && (tok.charAt(5) | 32) == 'a'
                && (tok.charAt(6) | 32) == 'c'
                && (tok.charAt(7) | 32) == 't'
                && (tok.charAt(8) | 32) == 'i'
                && (tok.charAt(9) | 32) == 'o'
                && (tok.charAt(10) | 32) == 'n'
                && (tok.charAt(11)) == '_'
                && (tok.charAt(12) | 32) == 'i'
                && (tok.charAt(13) | 32) == 's'
                && (tok.charAt(14) | 32) == 'o'
                && (tok.charAt(15) | 32) == 'l'
                && (tok.charAt(16) | 32) == 'a'
                && (tok.charAt(17) | 32) == 't'
                && (tok.charAt(18) | 32) == 'i'
                && (tok.charAt(19) | 32) == 'o'
                && (tok.charAt(20) | 32) == 'n';
    }

    public static boolean isTransactionKeyword(CharSequence tok) {
        return tok.length() == 11
                && (tok.charAt(0) | 32) == 't'
                && (tok.charAt(1) | 32) == 'r'
                && (tok.charAt(2) | 32) == 'a'
                && (tok.charAt(3) | 32) == 'n'
                && (tok.charAt(4) | 32) == 's'
                && (tok.charAt(5) | 32) == 'a'
                && (tok.charAt(6) | 32) == 'c'
                && (tok.charAt(7) | 32) == 't'
                && (tok.charAt(8) | 32) == 'i'
                && (tok.charAt(9) | 32) == 'o'
                && (tok.charAt(10) | 32) == 'n';
    }

    public static boolean isTrueKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 't'
                && (tok.charAt(1) | 32) == 'r'
                && (tok.charAt(2) | 32) == 'u'
                && (tok.charAt(3) | 32) == 'e';
    }

    public static boolean isTrueKeyword(Utf8Sequence tok) {
        return tok.size() == 4
                && (tok.byteAt(0) | 32) == 't'
                && (tok.byteAt(1) | 32) == 'r'
                && (tok.byteAt(2) | 32) == 'u'
                && (tok.byteAt(3) | 32) == 'e';
    }

    public static boolean isTtlKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 't'
                && (tok.charAt(1) | 32) == 't'
                && (tok.charAt(2) | 32) == 'l';
    }

    public static boolean isTxnKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 't'
                && (tok.charAt(1) | 32) == 'x'
                && (tok.charAt(2) | 32) == 'n';
    }

    public static boolean isTypeKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 't'
                && (tok.charAt(1) | 32) == 'y'
                && (tok.charAt(2) | 32) == 'p'
                && (tok.charAt(3) | 32) == 'e';
    }

    public static boolean isUTC(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0)) == '\''
                && (tok.charAt(1) | 32) == 'u'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'c'
                && (tok.charAt(4)) == '\'';
    }

    public static boolean isUnboundedKeyword(CharSequence tok) {
        return tok.length() == 9
                && (tok.charAt(0) | 32) == 'u'
                && (tok.charAt(1) | 32) == 'n'
                && (tok.charAt(2) | 32) == 'b'
                && (tok.charAt(3) | 32) == 'o'
                && (tok.charAt(4) | 32) == 'u'
                && (tok.charAt(5) | 32) == 'n'
                && (tok.charAt(6) | 32) == 'd'
                && (tok.charAt(7) | 32) == 'e'
                && (tok.charAt(8) | 32) == 'd';
    }

    public static boolean isUnionKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'u'
                && (tok.charAt(1) | 32) == 'n'
                && (tok.charAt(2) | 32) == 'i'
                && (tok.charAt(3) | 32) == 'o'
                && (tok.charAt(4) | 32) == 'n';
    }

    public static boolean isUpdateKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'u'
                && (tok.charAt(1) | 32) == 'p'
                && (tok.charAt(2) | 32) == 'd'
                && (tok.charAt(3) | 32) == 'a'
                && (tok.charAt(4) | 32) == 't'
                && (tok.charAt(5) | 32) == 'e';
    }

    public static boolean isUpsertKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'u'
                && (tok.charAt(1) | 32) == 'p'
                && (tok.charAt(2) | 32) == 's'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 'r'
                && (tok.charAt(5) | 32) == 't';
    }

    public static boolean isValuesKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'v'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'u'
                && (tok.charAt(4) | 32) == 'e'
                && (tok.charAt(5) | 32) == 's';
    }

    public static boolean isViewKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'v'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 'e'
                && (tok.charAt(3) | 32) == 'w';
    }

    public static boolean isVolumeKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'v'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'u'
                && (tok.charAt(4) | 32) == 'm'
                && (tok.charAt(5) | 32) == 'e';
    }

    public static boolean isWalKeyword(CharSequence tok) {
        return tok.length() == 3
                && (tok.charAt(0) | 32) == 'w'
                && (tok.charAt(1) | 32) == 'a'
                && (tok.charAt(2) | 32) == 'l';
    }

    public static boolean isWeekKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'w'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'e'
                && (tok.charAt(3) | 32) == 'k';
    }

    public static boolean isWhereKeyword(CharSequence tok) {
        return tok.length() == 5
                && (tok.charAt(0) | 32) == 'w'
                && (tok.charAt(1) | 32) == 'h'
                && (tok.charAt(2) | 32) == 'e'
                && (tok.charAt(3) | 32) == 'r'
                && (tok.charAt(4) | 32) == 'e';
    }

    public static boolean isWithKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'w'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'h';
    }

    public static boolean isWithinKeyword(CharSequence tok) {
        return tok != null
                && tok.length() == 6
                && (tok.charAt(0) | 32) == 'w'
                && (tok.charAt(1) | 32) == 'i'
                && (tok.charAt(2) | 32) == 't'
                && (tok.charAt(3) | 32) == 'h'
                && (tok.charAt(4) | 32) == 'i'
                && (tok.charAt(5) | 32) == 'n';
    }

    public static boolean isYearKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'y'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'a'
                && (tok.charAt(3) | 32) == 'r';
    }

    public static boolean isZeroOffset(CharSequence tok) {
        return
                tok.length() == 7
                        && tok.charAt(0) == '\''
                        && tok.charAt(1) == '0'
                        && tok.charAt(2) == '0'
                        && tok.charAt(3) == ':'
                        && tok.charAt(4) == '0'
                        && tok.charAt(5) == '0'
                        && tok.charAt(6) == '\'';
    }

    public static boolean isZoneKeyword(CharSequence tok) {
        return tok.length() == 4
                && (tok.charAt(0) | 32) == 'z'
                && (tok.charAt(1) | 32) == 'o'
                && (tok.charAt(2) | 32) == 'n'
                && (tok.charAt(3) | 32) == 'e';
    }

    public static boolean startsWithDecimalKeyword(CharSequence tok) {
        return isDecimalKeywordInternal(tok);
    }

    public static boolean startsWithGeoHashKeyword(CharSequence tok) {
        return (tok.length() >= 7)
                && isGeoHashKeywordInternal(tok);
    }

    public static boolean validateExtractPart(CharSequence token) {
        return TIMESTAMP_PART_SET.contains(token);
    }

    public static void validateLiteral(int pos, CharSequence tok) throws SqlException {
        switch (tok.charAt(0)) {
            case '(':
            case ')':
            case ',':
            case '`':
            case '\'':
            case ';':
                throw SqlException.position(pos).put("literal expected");
            default:
                break;
        }
    }

    private static boolean isDecimalKeywordInternal(CharSequence tok) {
        return (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'c'
                && (tok.charAt(3) | 32) == 'i'
                && (tok.charAt(4) | 32) == 'm'
                && (tok.charAt(5) | 32) == 'a'
                && (tok.charAt(6) | 32) == 'l';
    }

    private static boolean isGeoHashKeywordInternal(CharSequence tok) {
        return (tok.charAt(0) | 32) == 'g'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'o'
                && (tok.charAt(3) | 32) == 'h'
                && (tok.charAt(4) | 32) == 'a'
                && (tok.charAt(5) | 32) == 's'
                && (tok.charAt(6) | 32) == 'h';
    }

    static void assertNameIsQuotedOrNotAKeyword(CharSequence keyword, int position) throws SqlException {
        final boolean quoted = Chars.isQuoted(keyword);
        if (!quoted && SqlKeywords.isKeyword(keyword)) {
            throw SqlException.$(position, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"").put(keyword).put('"');
        }
    }

    static {
        TIMESTAMP_PART_SET.add("nanoseconds");
        TIMESTAMP_PART_SET.add("microseconds");
        TIMESTAMP_PART_SET.add("milliseconds");
        TIMESTAMP_PART_SET.add("second");
        TIMESTAMP_PART_SET.add("minute");
        TIMESTAMP_PART_SET.add("hour");
        TIMESTAMP_PART_SET.add("day");
        TIMESTAMP_PART_SET.add("doy");
        TIMESTAMP_PART_SET.add("dow");
        TIMESTAMP_PART_SET.add("week");
        TIMESTAMP_PART_SET.add("month");
        TIMESTAMP_PART_SET.add("quarter");
        TIMESTAMP_PART_SET.add("year");
        TIMESTAMP_PART_SET.add("isoyear");
        TIMESTAMP_PART_SET.add("isodow");
        TIMESTAMP_PART_SET.add("decade");
        TIMESTAMP_PART_SET.add("century");
        TIMESTAMP_PART_SET.add("millennium");
        TIMESTAMP_PART_SET.add("epoch");

        KEYWORDS.add("add");
        KEYWORDS.add("align");
        KEYWORDS.add("all");
        KEYWORDS.add("alter");
        KEYWORDS.add("and");
        KEYWORDS.add("asc");
        KEYWORDS.add("as");
        KEYWORDS.add("attach");
        KEYWORDS.add("batch");
        KEYWORDS.add("between");
        KEYWORDS.add("bypass");
        KEYWORDS.add("cancel");
        KEYWORDS.add("case");
        KEYWORDS.add("cast");
        KEYWORDS.add("column");
        KEYWORDS.add("create");
        KEYWORDS.add("desc");
        KEYWORDS.add("detach");
        KEYWORDS.add("disable");
        KEYWORDS.add("distinct");
        KEYWORDS.add("drop");
        KEYWORDS.add("enable");
        KEYWORDS.add("end");
        KEYWORDS.add("except");
        KEYWORDS.add("exists");
        KEYWORDS.add("explain");
        KEYWORDS.add("false");
        KEYWORDS.add("from");
        KEYWORDS.add("in");
        KEYWORDS.add("insert");
        KEYWORDS.add("intersect");
        KEYWORDS.add("into");
        KEYWORDS.add("like");
        KEYWORDS.add("limit");
        KEYWORDS.add("lock");
        KEYWORDS.add("nan");
        KEYWORDS.add("join");
        KEYWORDS.add("not");
        KEYWORDS.add("null");
        KEYWORDS.add("on");
        KEYWORDS.add("order");
        KEYWORDS.add("or");
        KEYWORDS.add("outer");
        KEYWORDS.add("over");
        KEYWORDS.add("partition");
        KEYWORDS.add("rename");
        KEYWORDS.add("resume");
        KEYWORDS.add("sample");
        KEYWORDS.add("select");
        KEYWORDS.add("set");
        KEYWORDS.add("squash");
        KEYWORDS.add("table");
        KEYWORDS.add("to");
        KEYWORDS.add("true");
        KEYWORDS.add("union");
        KEYWORDS.add("update");
        KEYWORDS.add("upsert");
        KEYWORDS.add("values");
        KEYWORDS.add("where");
        KEYWORDS.add("within");
        KEYWORDS.add("with");
    }
}
