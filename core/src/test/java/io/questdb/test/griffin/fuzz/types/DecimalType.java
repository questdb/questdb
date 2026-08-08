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

/**
 * Parameterised DECIMAL(p, s). Precision is drawn from the set that covers
 * all six internal DECIMAL widths (8/16/32/64/128/256) so the fuzzer touches
 * each of them roughly evenly.
 */
public final class DecimalType implements FuzzColumnType {
    // Representative precisions for each internal DECIMAL width.
    private static final int[] PRECISIONS = {2, 4, 9, 18, 38, 76};

    private final int precision;
    private final int scale;

    public DecimalType(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }

    public static DecimalType random(Rnd rnd) {
        int p = PRECISIONS[rnd.nextInt(PRECISIONS.length)];
        int s = rnd.nextInt(Math.min(p, 6) + 1);
        return new DecimalType(p, s);
    }

    @Override
    public String getDdl() {
        return "DECIMAL(" + precision + ", " + scale + ")";
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.DECIMAL;
    }

    @Override
    public String getRndCall() {
        // null rate 1:8
        return "rnd_decimal(" + precision + ", " + scale + ", 8)";
    }

    @Override
    public String randomLiteral(Rnd rnd) {
        if (rnd.nextInt(32) == 0) {
            return "null";
        }
        // Respect the DDL's precision/scale budget: the integer part gets at
        // most (precision - scale) digits, and we append `scale` fractional
        // digits so the cast doesn't overflow at runtime.
        int integerDigits = Math.min(precision - scale, 6);
        long magnitude = 1;
        for (int i = 0; i < integerDigits; i++) {
            magnitude *= 10L;
        }
        long intPart = magnitude == 0 ? 0 : rnd.nextLong(magnitude);
        boolean negative = rnd.nextBoolean();
        StringBuilder sb = new StringBuilder();
        if (negative) {
            sb.append('-');
        }
        sb.append(intPart);
        if (scale > 0) {
            sb.append('.');
            for (int i = 0; i < scale; i++) {
                sb.append(rnd.nextInt(10));
            }
        }
        sb.append("::").append(getDdl());
        return sb.toString();
    }
}
