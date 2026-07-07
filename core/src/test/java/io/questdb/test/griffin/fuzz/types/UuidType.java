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

public final class UuidType implements FuzzColumnType {
    public static final UuidType INSTANCE = new UuidType();

    private static final String HEX = "0123456789abcdef";

    private UuidType() {
    }

    @Override
    public String getDdl() {
        return "UUID";
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.IDENTIFIER;
    }

    @Override
    public String getRndCall() {
        return "rnd_uuid4()";
    }

    @Override
    public String randomLiteral(Rnd rnd) {
        if (rnd.nextInt(32) == 0) {
            return "null";
        }
        // 8-4-4-4-12
        StringBuilder sb = new StringBuilder(38);
        sb.append('\'');
        appendHex(sb, rnd, 8);
        sb.append('-');
        appendHex(sb, rnd, 4);
        sb.append('-');
        appendHex(sb, rnd, 4);
        sb.append('-');
        appendHex(sb, rnd, 4);
        sb.append('-');
        appendHex(sb, rnd, 12);
        sb.append('\'');
        return sb.toString();
    }

    private static void appendHex(StringBuilder sb, Rnd rnd, int count) {
        for (int i = 0; i < count; i++) {
            sb.append(HEX.charAt(rnd.nextInt(16)));
        }
    }
}
