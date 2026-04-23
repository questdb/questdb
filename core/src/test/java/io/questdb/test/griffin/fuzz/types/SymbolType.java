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

public final class SymbolType implements FuzzColumnType {
    public static final SymbolType INSTANCE = new SymbolType();

    private SymbolType() {
    }

    @Override
    public String getDdl() {
        return "SYMBOL";
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.STRING_LIKE;
    }

    @Override
    public String getRndCall() {
        // 8 distinct symbols of length 3..5, null rate 1:8
        return "rnd_symbol(8, 3, 5, 8)";
    }

    @Override
    public String randomLiteral(Rnd rnd) {
        if (rnd.nextInt(32) == 0) {
            return "null";
        }
        return "'" + rnd.nextString(1 + rnd.nextInt(4)) + "'";
    }
}
