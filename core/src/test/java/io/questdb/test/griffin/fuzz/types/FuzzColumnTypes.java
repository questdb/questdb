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

import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

/**
 * Registry of all {@link FuzzColumnType}s the fuzzer can emit. New types go
 * here in one place: add the class under {@code types/} and wire it into
 * {@link #SINGLETONS} or the parameterised-dispatch code in
 * {@link #pickRandom(Rnd)}.
 */
public final class FuzzColumnTypes {
    private static final ObjList<FuzzColumnType> SINGLETONS = new ObjList<>();

    static {
        SINGLETONS.add(BooleanType.INSTANCE);
        SINGLETONS.add(ByteType.INSTANCE);
        SINGLETONS.add(ShortType.INSTANCE);
        SINGLETONS.add(CharType.INSTANCE);
        SINGLETONS.add(IntType.INSTANCE);
        SINGLETONS.add(LongType.INSTANCE);
        SINGLETONS.add(FloatType.INSTANCE);
        SINGLETONS.add(DoubleType.INSTANCE);
        SINGLETONS.add(DateType.INSTANCE);
        SINGLETONS.add(TimestampType.INSTANCE);
        SINGLETONS.add(StringType.INSTANCE);
        SINGLETONS.add(VarcharType.INSTANCE);
        SINGLETONS.add(SymbolType.INSTANCE);
        SINGLETONS.add(Long256Type.INSTANCE);
        SINGLETONS.add(UuidType.INSTANCE);
        SINGLETONS.add(IPv4Type.INSTANCE);
    }

    private FuzzColumnTypes() {
    }

    /**
     * Pick a random column type. Parameterised families (DECIMAL, DOUBLE[])
     * produce a fresh instance with random parameters so one generated table
     * can carry several distinct DECIMAL or array columns.
     */
    public static FuzzColumnType pickRandom(Rnd rnd) {
        int bucket = rnd.nextInt(100);
        if (bucket < 10) {
            return DecimalType.random(rnd);
        }
        if (bucket < 20) {
            return DoubleArrayType.random(rnd);
        }
        return SINGLETONS.get(rnd.nextInt(SINGLETONS.size()));
    }

    /**
     * Pick a random column type whose {@link ColumnKind} matches. Used by
     * cast generation to land on a specific kind regardless of the inner
     * expression's type. Returns {@code null} if no registered singleton
     * matches (callers should fall back to a column reference instead).
     */
    public static FuzzColumnType pickOfKind(Rnd rnd, ColumnKind kind) {
        if (kind == ColumnKind.DECIMAL) {
            return DecimalType.random(rnd);
        }
        if (kind == ColumnKind.ARRAY) {
            return DoubleArrayType.random(rnd);
        }
        ObjList<FuzzColumnType> match = new ObjList<>();
        for (int i = 0, n = SINGLETONS.size(); i < n; i++) {
            FuzzColumnType t = SINGLETONS.get(i);
            if (t.getKind() == kind) {
                match.add(t);
            }
        }
        if (match.size() == 0) {
            return null;
        }
        return match.get(rnd.nextInt(match.size()));
    }
}
