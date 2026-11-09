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

/**
 * The type of the fixed LONG join-key column every fuzz table carries next to
 * {@code k} and {@code sym}. It draws from the same small domain
 * {@link IntKeyType} uses, so an equi-join over it matches rows and NULL keys
 * appear on both sides. A lone LONG key is the narrowest shape the fused hash
 * join GROUP BY stages through its key sinks rather than reading as an INT, so
 * this column is what sends fuzz queries down the map-backed build.
 * <p>
 * {@code FuzzTableFactory} adds the column by name and does not deal this type
 * off the deck, so no other column carries it.
 */
public final class LongKeyType implements FuzzColumnType {
    public static final LongKeyType INSTANCE = new LongKeyType();

    private LongKeyType() {
    }

    @Override
    public FuzzConstant generateConstant(Rnd rnd) {
        if (rnd.nextInt(16) == 0) {
            return FuzzConstant.nonBindable("null");
        }
        String v = Integer.toString(rnd.nextInt(IntKeyType.DOMAIN_SIZE));
        return new FuzzConstant(v, "LONG", v);
    }

    @Override
    public String getDdl() {
        return "LONG";
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.NUMERIC;
    }

    @Override
    public String getRndCall() {
        // One row in eight on average is NULL, like IntKeyType.
        return "rnd_long(0, " + (IntKeyType.DOMAIN_SIZE - 1) + ", 8)";
    }

    @Override
    public String randomLiteral(Rnd rnd) {
        return generateConstant(rnd).literal();
    }
}
