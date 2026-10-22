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

package io.questdb.test.griffin.engine.functions.groupby;

/**
 * Window-function formulations of {@code SELECT sym, arg_max(value, key) FROM tab GROUP BY sym ORDER BY sym} and its
 * arg_min twin. They never run a GROUP BY merge, so they check the parallel merge independently. They agree with
 * arg_max/arg_min only when every sym has a unique extreme non-null key.
 */
final class ArgMinMaxTestUtils {
    static final String ARG_MAX_ORACLE = """
            SELECT sym, value arg_max
            FROM (
                SELECT sym, value, row_number() OVER (PARTITION BY sym ORDER BY key DESC) rn
                FROM tab
                WHERE key IS NOT NULL
            )
            WHERE rn = 1
            ORDER BY sym
            """;
    static final String ARG_MIN_ORACLE = """
            SELECT sym, value arg_min
            FROM (
                SELECT sym, value, row_number() OVER (PARTITION BY sym ORDER BY key) rn
                FROM tab
                WHERE key IS NOT NULL
            )
            WHERE rn = 1
            ORDER BY sym
            """;

    private ArgMinMaxTestUtils() {
    }
}
