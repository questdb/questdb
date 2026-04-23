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

public final class DateType implements FuzzColumnType {
    public static final DateType INSTANCE = new DateType();

    private DateType() {
    }

    @Override
    public String getDdl() {
        return "DATE";
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.TEMPORAL;
    }

    @Override
    public String getRndCall() {
        // epoch 2020-01-01 .. 2026-01-01 (millis)
        return "rnd_date(1577836800000L, 1767225600000L, 8)";
    }

    @Override
    public String randomLiteral(Rnd rnd) {
        if (rnd.nextInt(32) == 0) {
            return "null";
        }
        // Pick a date in 2024 (day granularity is enough)
        int dayOfYear = 1 + rnd.nextInt(365);
        int month = Math.min(12, 1 + (dayOfYear - 1) / 31);
        int day = 1 + (dayOfYear - 1) % 28;
        return String.format(java.util.Locale.ROOT, "'2024-%02d-%02d'", month, day) + "::DATE";
    }
}
