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

package io.questdb.test.griffin.fuzz;

import io.questdb.std.Rnd;

/**
 * Wraps table and column identifiers with a small chance of returning a
 * garbage name instead of the real one. This exercises the engine's
 * "table does not exist" / "Invalid column" paths, which surface as
 * {@link io.questdb.griffin.SqlException} and are swallowed by the oracle.
 * <p>
 * The probability is low on purpose: we want most queries to still run
 * cleanly so the fuzzer keeps exercising the main code paths.
 */
public final class FuzzNames {
    // 1 in this many identifier emissions is replaced with a nonexistent name.
    private static final int GARBLE_ONE_IN_N = 100;

    private FuzzNames() {
    }

    public static String column(Rnd rnd, String real) {
        return rnd.nextInt(GARBLE_ONE_IN_N) == 0 ? garbage(rnd, "bork_col") : real;
    }

    public static String table(Rnd rnd, String real) {
        return rnd.nextInt(GARBLE_ONE_IN_N) == 0 ? garbage(rnd, "bork_tbl") : real;
    }

    private static String garbage(Rnd rnd, String prefix) {
        return prefix + '_' + Integer.toHexString(rnd.nextInt() & 0xffff);
    }
}
