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
 * Parameterised {@code DOUBLE[]} or {@code DOUBLE[][]}. Dimensionality is
 * fixed per column so the DDL and the generator stay in sync.
 */
public final class DoubleArrayType implements FuzzColumnType {

    private final int dims;

    public DoubleArrayType(int dims) {
        this.dims = dims;
    }

    public static DoubleArrayType random(Rnd rnd) {
        return new DoubleArrayType(1 + rnd.nextInt(2));
    }

    @Override
    public String getDdl() {
        StringBuilder sb = new StringBuilder("DOUBLE");
        for (int i = 0; i < dims; i++) {
            sb.append("[]");
        }
        return sb.toString();
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.ARRAY;
    }

    @Override
    public String getRndCall() {
        return "rnd_double_array(" + dims + ")";
    }

    @Override
    public String randomLiteral(Rnd rnd) {
        // Fix the length at every dimension up front so nested arrays are
        // regular (non-ragged); QuestDB rejects ragged multi-dim arrays.
        int[] dimLens = new int[dims];
        for (int i = 0; i < dims; i++) {
            dimLens[i] = 1 + rnd.nextInt(3);
        }
        StringBuilder sb = new StringBuilder();
        buildArrayLiteral(sb, rnd, dimLens, 0);
        return sb.toString();
    }

    private static void buildArrayLiteral(StringBuilder sb, Rnd rnd, int[] dimLens, int depth) {
        int len = dimLens[depth];
        sb.append("ARRAY[");
        for (int i = 0; i < len; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            if (depth == dimLens.length - 1) {
                sb.append(String.format(java.util.Locale.ROOT, "%.4f", rnd.nextDouble()));
            } else {
                buildArrayLiteral(sb, rnd, dimLens, depth + 1);
            }
        }
        sb.append(']');
    }
}
