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

public class SqlKeywords {

    public static boolean isAsKeyword(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 's';
    }

    public static boolean isInKeyword(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i) | 32) == 'n';
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

    public static boolean isSystemKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 'y'
                && (tok.charAt(i++) | 32) == 's'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'm'
                ;
    }

    public static boolean isUnlockKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i) | 32) == 'k';
    }

    public static boolean isSemicolon(CharSequence tok) {
        return tok.length() == 1 && (tok.charAt(0) | 32) == ';';
    }

    public static boolean isOnKeyword(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'o'
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

    public static boolean isAndKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i) | 32) == 'd';
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

    public static boolean isNotKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i) | 32) == 't';
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

    public static boolean isJoinKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'j'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i) | 32) == 'n';
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

    public static boolean isWriterKeyword(CharSequence tok) {
        if (tok.length() != 6) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'w'
                && (tok.charAt(i++) | 32) == 'r'
                && (tok.charAt(i++) | 32) == 'i'
                && (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'r';
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
                && (tok.charAt(i) | 32) == 'r'
                ;
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

    public static boolean isToKeyboard(CharSequence tok) {
        if (tok.length() != 2) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i) | 32) == 'o';
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

    public static boolean isHourKeyword(CharSequence tok) {
        if (tok.length() != 4) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++)) == 'h'
                && (tok.charAt(i++)) == 'o'
                && (tok.charAt(i++)) == 'u'
                && (tok.charAt(i)) == 'r';
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
        // @formatter:off
        return (tok.charAt(i++) | 32) == 't'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'b'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 's';
        // @formatter:off
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

    public static boolean isAddKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i++) | 32) == 'd'
                && (tok.charAt(i) | 32) == 'd';
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

    public static boolean isNanKeyword(CharSequence tok) {
        if (tok.length() != 3) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i++) | 32) == 'a'
                && (tok.charAt(i) | 32) == 'n';
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
        // @formatter:off
        return (tok.charAt(i++) | 32) == 'c'
                && (tok.charAt(i++) | 32) == 'o'
                && (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'u'
                && (tok.charAt(i++) | 32) == 'm'
                && (tok.charAt(i++) | 32) == 'n'
                && (tok.charAt(i) | 32) == 's';
        // @formatter:on
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

    public static boolean isStandardConformingStringsKeyword(CharSequence tok) {
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

    public static boolean isLevelKeyword(CharSequence tok) {
        if (tok.length() != 5) {
            return false;
        }

        int i = 0;
        return (tok.charAt(i++) | 32) == 'l'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i++) | 32) == 'v'
                && (tok.charAt(i++) | 32) == 'e'
                && (tok.charAt(i) | 32) == 'l'
                ;
    }

    public static boolean isInvalidColumnName(CharSequence seq) {
        for (int i = 0, l = seq.length(); i < l; i++) {
            char c = seq.charAt(i);
            switch (c) {
                case ' ':
                case '?':
                case '.':
                case ',':
                case '\'':
                case '\"':
                case '\\':
                case '/':
                case '\0':
                case ':':
                case ')':
                case '(':
                case '+':
                case '-':
                case '*':
                case '%':
                case '~':
                case 0xfeff: // UTF-8 BOM (Byte Order Mark) can appear at the beginning of a character stream
                    return true;
                default:

            }
        }
        return false;
    }
}
