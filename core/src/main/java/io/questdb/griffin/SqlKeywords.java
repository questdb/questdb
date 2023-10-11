/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import org.jetbrains.annotations.NotNull;

public class SqlKeywords {
    public static final int CASE_KEYWORD_LENGTH = 4;
    public static final String CONCAT_FUNC_NAME = "concat";
    public static final int GEOHASH_KEYWORD_LENGTH = 7;
    protected static final LowerCaseCharSequenceHashSet KEYWORDS = new LowerCaseCharSequenceHashSet();
    private static final LowerCaseCharSequenceHashSet TIMESTAMP_PART_SET = new LowerCaseCharSequenceHashSet();

    public static boolean isAddKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i) | 32) == 'd';
    }

    public static boolean isAlignKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'g'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isAllKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i) | 32) == 'l';
    }

    public static boolean isAlterKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'r';
    }

    public static boolean isAndKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i) | 32) == 'd';
    }

    public static boolean isAsKeyword(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isAscKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i) | 32) == 'c';
    }

    public static boolean isAtKeyword(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isAttachKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i) | 32) == 'h';
    }

    public static boolean isBatchKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'b'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i) | 32) == 'h';
    }

    public static boolean isBetweenKeyword(CharSequence tok) {
        if (tok.length() != 7) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'b'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'w'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isByKeyword(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'b'
                && (tok.charAt(i) | 32) == 'y';
    }

    public static boolean isBypassKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'b'
                && (tok.charAt(i++) | 32) == 'y'
                && (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isCacheKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'h'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isCalendarKeyword(CharSequence tok) {
        if (tok.length() != 8) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 'r';
    }

    public static boolean isCancelKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'l';
    }

    public static boolean isCapacityKeyword(CharSequence tok) {
        if (tok.length() != 8) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i) | 32) == 'y';
    }

    public static boolean isCaseKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isCastKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isCenturyKeyword(CharSequence tok) {
        if (tok.length() != 7) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i) | 32) == 'y';
    }

    public static boolean isColonColon(CharSequence tok) {
        return tok.length() == 2 && tok.charAt(0) == ':' && tok.charAt(1) == ':';
    }

    public static boolean isColumnKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isColumnsKeyword(CharSequence tok) {
        if (tok.length() != 7) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isConcatKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        // Reference equal in case it's already replaced token name
        if (tok == CONCAT_FUNC_NAME) return true;

        int i = 0;
        return (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isConcatOperator(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        int i = 0;
        return tok.charAt(i++) == '|'
                && tok.charAt(i) == '|';
    }

    public static boolean isCopyKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i) | 32) == 'y';
    }

    public static boolean isCountKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isCreateKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isCurrentKeyword(CharSequence tok) {
        if (tok.length() != 7) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i) | 32) == 't'
                ;
    }

    public static boolean isDatabaseKeyword(CharSequence tok) {
        if (tok.length() != 8) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'b'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isDateKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isDateStyleKeyword(CharSequence tok) {
        if (tok.length() != 9) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'y'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isDayKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 'y';
    }

    public static boolean isDecadeKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isDedupKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i) | 32) == 'p';
    }

    public static boolean isDeduplicateKeyword(CharSequence tok) {
        if (tok.length() != 11) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isDelimiterKeyword(CharSequence tok) {
        if (tok.length() != 9) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'r';
    }

    public static boolean isDescKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i) | 32) == 'c';
    }

    public static boolean isDetachKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i) | 32) == 'h';
    }

    public static boolean isDisableKeyword(CharSequence tok) {
        if (tok.length() != 7) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'b'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isDistinctKeyword(CharSequence tok) {
        if (tok.length() != 8) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isDowKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'w';
    }

    public static boolean isDoyKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'y';
    }

    public static boolean isDropKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'p';
    }

    public static boolean isEmptyAlias(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        return (tok.charAt(0) == '\'' && tok.charAt(1) == '\'') || (tok.charAt(0) == '"' && tok.charAt(1) == '"');
    }

    public static boolean isEnableKeyword(@NotNull CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'b'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isEndKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i) | 32) == 'd';
    }

    public static boolean isEpochKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i) | 32) == 'h';
    }

    public static boolean isExceptKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'x'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isExcludeKeyword(CharSequence tok) {
        if (tok.length() != 7) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'x'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isExclusiveKeyword(CharSequence tok) {
        if (tok.length() != 9) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'x'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'v'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isExistsKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'x'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isExplainKeyword(CharSequence tok) {
        if (tok.length() != 7) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'x'
                && (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isExtractKeyword(CharSequence tok) {
        if (tok.length() != 7) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'x'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isFalseKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'f'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isFillKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'f'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i) | 32) == 'l';
    }

    public static boolean isFirstKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'f'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isFloat4Keyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'f'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i)) == '4';
    }

    public static boolean isFloat8Keyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'f'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i)) == '8';
    }

    // only for Python drivers, which use 'float' keyword to represent double in Java
    // for example, 'NaN'::float   'Infinity'::float
    public static boolean isFloatKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'f'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isFollowingKeyword(CharSequence tok) {
        if (tok.length() != 9) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'f'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'w'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i) | 32) == 'g';
    }

    public static boolean isFormatKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'f'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isFromKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'f'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'm';
    }

    public static boolean isFullKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'f'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i) | 32) == 'l';
    }

    public static boolean isGeoHashKeyword(CharSequence tok) {
        if (tok.length() != 7) {
            return false;
        }

        int i = 0;
        return isGeoHashKeyword(tok, i);
    }

    public static boolean isGroupKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'g'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i) | 32) == 'p';
    }

    public static boolean isHeaderKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'h'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'r';
    }

    public static boolean isHourKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'h'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i) | 32) == 'r';
    }

    public static boolean isIfKeyword(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i) | 32) == 'f';
    }

    public static boolean isInKeyword(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isIndexKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'x';
    }

    public static boolean isInsertKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isIntersectKeyword(CharSequence tok) {
        if (tok.length() != 9) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isIntoKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i) | 32) == 'o';
    }

    public static boolean isIsKeyword(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isIsoDowKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'w';
    }

    public static boolean isIsoYearKeyword(CharSequence tok) {
        if (tok.length() != 7) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'y'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 'r';
    }

    public static boolean isIsolationKeyword(CharSequence tok) {
        if (tok.length() != 9) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isJsonKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'j'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isKeepKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'k'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'p';
    }

    public static boolean isKeysKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'k'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'y'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isKeyword(CharSequence text) {
        if (text != null) {
            return KEYWORDS.contains(text);
        }
        return false;
    }

    public static boolean isLastKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isLatestKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isLeftKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'f'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isLevelKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'v'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'l';
    }

    public static boolean isLikeKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'k'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isLimitKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isLinearKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 'r';
    }

    public static boolean isListKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isLockKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i) | 32) == 'k';
    }

    public static boolean isMapsKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isMaxIdentifierLength(CharSequence tok) {
        if (tok.length() != 21) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'x'
                && tok.charAt(i++) == '_'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'f'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'r'
                && tok.charAt(i++) == '_'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'g'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i) | 32) == 'h';
    }

    public static boolean isMaxUncommittedRowsKeyword(CharSequence tok) {
        if (tok.length() != 18) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'x'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'w'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isMicrosecondsKeyword(CharSequence tok) {
        if (tok.length() != 12) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isMillenniumKeyword(CharSequence tok) {
        if (tok.length() != 10) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i) | 32) == 'm';
    }

    public static boolean isMillisecondsKeyword(CharSequence tok) {
        if (tok.length() != 12) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isMinuteKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isMonthKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i) | 32) == 'h';
    }

    public static boolean isNanKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isNoCacheKeyword(CharSequence tok) {
        if (tok.length() != 7) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'h'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isNoKeyword(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i) | 32) == 'o';
    }

    public static boolean isNoneKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isNotJoinKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return true;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) != 'j'
                || (tok.charAt(i++) | 32) != 'o'
                || (tok.charAt(i++) | 32) != 'i'
                || (tok.charAt(i) | 32) != 'n';
    }

    public static boolean isNotKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isNullKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i) | 32) == 'l';
    }

    public static boolean isO3MaxLagKeyword(CharSequence tok) {
        if (tok.length() != 8) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == '3'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'x'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 'g';
    }

    public static boolean isObservationKeyword(CharSequence tok) {
        if (tok.length() != 11) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'b'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'v'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isOffsetKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'f'
                && (tok.charAt(i++) | 32) == 'f'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isOnKeyword(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isOnlyKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i) | 32) == 'y';
    }

    public static boolean isOrKeyword(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'r';
    }

    public static boolean isOrderKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'r';
    }

    public static boolean isOuterKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'r';
    }

    public static boolean isOverKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'v'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'r';
    }

    public static boolean isParamKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 'm';
    }

    public static boolean isPartitionKeyword(CharSequence tok) {
        if (tok.length() != 9) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isPartitionsKeyword(CharSequence tok) {
        if (tok.length() != 10) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isPrecedingKeyword(CharSequence tok) {
        if (tok.length() != 9) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i) | 32) == 'g';
    }

    public static boolean isPrecisionKeyword(CharSequence tok) {
        if (tok.length() != 9) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isPrevKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'v';
    }

    public static boolean isQuarterKeyword(CharSequence tok) {
        if (tok.length() != 7) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'q'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'r';
    }

    public static boolean isQuote(CharSequence tok) {
        return tok.length() == 1 && tok.charAt(0) == '\'';
    }

    public static boolean isRangeKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'g'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isRenameKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isResumeKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isRightKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'g'
                && (tok.charAt(i++) | 32) == 'h'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isRowKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'w';
    }

    public static boolean isOthersKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'h'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isRowsKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'w'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isSampleKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isSearchPath(CharSequence tok) {
        if (tok.length() != 11) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'h'
                && (tok.charAt(i++)) == '_'
                && (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i) | 32) == 'h';
    }

    public static boolean isSecondKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i) | 32) == 'd';
    }

    public static boolean isSelectKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isSemicolon(CharSequence token) {
        return Chars.equals(token, ';');
    }

    public static boolean isSetKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isSquashKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'q'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i) | 32) == 'h';
    }

    public static boolean isStandardConformingStrings(CharSequence tok) {
        if (tok.length() != 27) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++)) == '_'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'f'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'g'
                && (tok.charAt(i++)) == '_'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'g'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isSumKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i) | 32) == 'm';
    }

    public static boolean isSymbolKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'y'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'b'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'l';
    }

    public static boolean isTableKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'b'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isTablesKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'b'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isTextKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'x'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isTiesKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isTimeKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isTimestampKeyword(CharSequence tok) {
        if (tok.length() != 9) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i) | 32) == 'p';
    }

    public static boolean isToKeyword(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i) | 32) == 'o';
    }

    public static boolean isTransactionIsolation(CharSequence tok) {
        if (tok.length() != 21) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++)) == '_'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isTransactionKeyword(CharSequence tok) {
        if (tok.length() != 11) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isTrueKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isTxnKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'x'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isTypeKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'y'
                && (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isUnboundedKeyword(CharSequence tok) {
        if (tok.length() != 9) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'b'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'd';
    }

    public static boolean isUnionKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isUpdateKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isUpsertKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'p'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i) | 32) == 't';
    }

    public static boolean isValuesKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'v'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isVolumeKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'v'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isWalKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'w'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 'l';
    }

    public static boolean isWeekKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'w'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'k';
    }

    public static boolean isWhereKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'w'
                && (tok.charAt(i++) | 32) == 'h'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean isWithKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'w'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i) | 32) == 'h';
    }

    public static boolean isWithinKeyword(CharSequence tok) {
        if (tok == null || tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'w'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'h'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i) | 32) == 'n';
    }

    public static boolean isYearKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'y'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 'r';
    }

    public static boolean isZoneKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'z'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i) | 32) == 'e';
    }

    public static boolean startsWithGeoHashKeyword(CharSequence tok) {
        if (tok.length() < 7) {
            return false;
        }

        int i = 0;
        return isGeoHashKeyword(tok, i);
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

    private static boolean isGeoHashKeyword(CharSequence tok, int i) {
        return (tok.charAt(i++) | 32) == 'g'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'h'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i) | 32) == 'h';
    }

    static void assertTableNameIsQuotedOrNotAKeyword(CharSequence keyword, int position) throws SqlException {
        final boolean quoted = Chars.isQuoted(keyword);
        if (!quoted && SqlKeywords.isKeyword(keyword)) {
            throw SqlException.$(position, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"").put(keyword).put('"');
        }
    }

    static {
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
