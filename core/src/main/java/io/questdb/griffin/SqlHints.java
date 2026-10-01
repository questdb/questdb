/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
    public static final String ASOF_DENSE_HINT = "asof_dense";
    public static final String ASOF_INDEX_HINT = "asof_index";
    public static final String ASOF_LINEAR_HINT = "asof_linear";
    public static final String ASOF_MEMOIZED_DRIVEBY_HINT = "asof_memoized_driveby";
    public static final String ASOF_MEMOIZED_HINT = "asof_memoized";
    public static final String ENABLE_PRE_TOUCH_HINT = "enable_pre_touch";
    public static final String FORCE_USE_COVERING_HINT = "force_use_covering";
    public static final char HINTS_PARAMS_DELIMITER = ' ';
    public static final String MARKOUT_HORIZON_HINT = "markout_horizon";
    public static final String NO_COVERING_HINT = "no_covering";
    public static final String NO_INDEX_HINT = "no_index";
    public static final String NO_SYMBOL_PATTERN_INDEX_HINT = "no_symbol_pattern_index";

    static boolean hasHintWithParams(
            @Nullable LowerCaseCharSequenceObjHashMap<CharSequence> hints,
            @NotNull CharSequence hintName,
            @Nullable CharSequence tableNameA,
            @Nullable CharSequence tableNameB
    ) {
        final CharSequence params = hints == null ? null : hints.get(hintName);
        return Chars.containsWordIgnoreCase(params, tableNameA, HINTS_PARAMS_DELIMITER) &&
                Chars.containsWordIgnoreCase(params, tableNameB, HINTS_PARAMS_DELIMITER);
    }

    private static boolean hasHintWithParams(
            @NotNull QueryModel queryModel,
            @NotNull CharSequence hintName,
            @Nullable CharSequence tableNameA,
            @Nullable CharSequence tableNameB
    ) {
        return hasHintWithParams(queryModel.getHints(), hintName, tableNameA, tableNameB);
    }
}
