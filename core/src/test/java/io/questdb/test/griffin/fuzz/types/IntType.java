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

package io.questdb.test.griffin.fuzz.types;

import io.questdb.std.Rnd;

public final class IntType implements FuzzColumnType {
    public static final IntType INSTANCE = new IntType();

    private IntType() {
    }

    @Override
    public String getDdl() {
        return "INT";
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.NUMERIC;
    }

    @Override
    public String getRndCall() {
        // mild null-rate so NULL handling gets exercised
        return "rnd_int(-1000000, 1000000, 8)";
    }

    @Override
    public String randomLiteral(Rnd rnd) {
        if (rnd.nextInt(32) == 0) {
            return "null";
        }
        return Integer.toString(rnd.nextInt(2_000_000) - 1_000_000);
    }
}
