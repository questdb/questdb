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

import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Chars;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class SqlHints {
    public static final String ASOF_JOIN_AVOID_BINARY_SEARCH_HINT = "avoid_asof_binary_search";
    public static final char HINTS_PARAMS_DELIMITER = ' ';
    public static final String LT_JOIN_AVOID_BINARY_SEARCH_HINT = "avoid_lt_binary_search";

    public static boolean hasAvoidAsOfJoinBinarySearchHint(@NotNull QueryModel queryModel, @Nullable CharSequence tableNameA, @Nullable CharSequence tableNameB) {
        return hasHintWithParams(queryModel, ASOF_JOIN_AVOID_BINARY_SEARCH_HINT, tableNameA, tableNameB);
    }

    public static boolean hasAvoidLtJoinBinarySearchHint(@NotNull QueryModel queryModel, @Nullable CharSequence tableNameA, @Nullable CharSequence tableNameB) {
        return hasHintWithParams(queryModel, LT_JOIN_AVOID_BINARY_SEARCH_HINT, tableNameA, tableNameB);
    }

    private static boolean hasHintWithParams(
            @NotNull QueryModel queryModel,
            @NotNull CharSequence hintName,
            @Nullable CharSequence tableNameA,
            @Nullable CharSequence tableNameB
    ) {
        LowerCaseCharSequenceObjHashMap<CharSequence> hints = queryModel.getHints();
        CharSequence params = hints.get(hintName);
        return Chars.containsWordIgnoreCase(params, tableNameA, HINTS_PARAMS_DELIMITER) &&
                Chars.containsWordIgnoreCase(params, tableNameB, HINTS_PARAMS_DELIMITER);
    }
}
