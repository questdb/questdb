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
import io.questdb.test.griffin.fuzz.expr.FuzzConstant;

public final class FloatType implements FuzzColumnType {
    public static final FloatType INSTANCE = new FloatType();

    private FloatType() {
    }

    @Override
    public FuzzConstant generateConstant(Rnd rnd) {
        if (rnd.nextInt(32) == 0) {
            return FuzzConstant.nonBindable("null");
        }
        if (rnd.nextInt(4) == 0) {
            // 1:4 -> a bare integer value, cast so the literal stays FLOAT-typed. The range is
            // kept below 2^24 so it is exactly representable in a float (no INT->FLOAT rounding).
            String v = Integer.toString(rnd.nextInt(2_000_000) - 1_000_000);
            return new FuzzConstant(v + "::FLOAT", "FLOAT", v);
        }
        String v = String.format(java.util.Locale.ROOT, "%.6f", rnd.nextFloat());
        return new FuzzConstant(v + "::FLOAT", "FLOAT", v);
    }

    @Override
    public String getDdl() {
        return "FLOAT";
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.NUMERIC;
    }

    @Override
    public String getRndCall() {
        return "rnd_float(8)";
    }

    @Override
    public String randomLiteral(Rnd rnd) {
        return generateConstant(rnd).literal();
    }
}
