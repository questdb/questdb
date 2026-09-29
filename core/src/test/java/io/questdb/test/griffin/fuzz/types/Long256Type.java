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

public final class Long256Type implements FuzzColumnType {
    public static final Long256Type INSTANCE = new Long256Type();

    private static final String HEX = "0123456789abcdef";

    private Long256Type() {
    }

    @Override
    public String getDdl() {
        return "LONG256";
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.LONG256;
    }

    @Override
    public String getRndCall() {
        // rnd_long256() never returns NULL; mix one in so IS NULL / equality
        // against null sees a matching row.
        return "CASE WHEN rnd_boolean() AND rnd_boolean() AND rnd_boolean()"
                + " THEN null::LONG256 ELSE rnd_long256() END";
    }

    @Override
    public String randomLiteral(Rnd rnd) {
        if (rnd.nextInt(32) == 0) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(66);
        sb.append("'0x");
        // 64 hex chars = 256 bits
        for (int i = 0; i < 64; i++) {
            sb.append(HEX.charAt(rnd.nextInt(16)));
        }
        sb.append('\'');
        return sb.toString();
    }
}
