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

public final class TimestampType implements FuzzColumnType {
    public static final TimestampType INSTANCE = new TimestampType();

    private TimestampType() {
    }

    @Override
    public String getDdl() {
        return "TIMESTAMP";
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.TEMPORAL;
    }

    @Override
    public String getRndCall() {
        // 2024-01-01 .. 2024-04-01 in microseconds since epoch
        return "rnd_timestamp(to_timestamp('2024-01-01', 'yyyy-MM-dd'), to_timestamp('2024-04-01', 'yyyy-MM-dd'), 8)";
    }

    @Override
    public String randomLiteral(Rnd rnd) {
        if (rnd.nextInt(32) == 0) {
            return "null";
        }
        int month = 1 + rnd.nextInt(3);
        int day = 1 + rnd.nextInt(27);
        int hour = rnd.nextInt(24);
        int minute = rnd.nextInt(60);
        return String.format(
                java.util.Locale.ROOT,
                "'2024-%02d-%02dT%02d:%02d:00.000000Z'",
                month, day, hour, minute
        ) + "::TIMESTAMP";
    }
}
