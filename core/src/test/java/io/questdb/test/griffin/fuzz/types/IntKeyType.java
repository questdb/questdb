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
 * The type of the fixed INT join-key column every fuzz table carries next to
 * {@code sym}. It is an INT like {@link IntType}, but its values come from a
 * small domain plus NULL: a random INT over {@link IntType}'s two-million-value
 * range almost never equals a value in another table, so an equi-join over it
 * would return no pairs. With {@link #DOMAIN_SIZE} values two tables of 60 to
 * 150 rows join into about 170 to 1_100 pairs, and NULL keys appear on both
 * sides.
 * <p>
 * {@code FuzzTableFactory} adds the column by name and does not deal this type
 * off the deck, so no other column carries it.
 */
public final class IntKeyType implements FuzzColumnType {
    public static final int DOMAIN_SIZE = 16;
    public static final IntKeyType INSTANCE = new IntKeyType();

    private IntKeyType() {
    }

    @Override
    public FuzzConstant generateConstant(Rnd rnd) {
        if (rnd.nextInt(16) == 0) {
            return FuzzConstant.nonBindable("null");
        }
        String v = Integer.toString(rnd.nextInt(DOMAIN_SIZE));
        return new FuzzConstant(v, "INT", v);
    }

    @Override
    public String getDdl() {
        return "INT";
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.NUMERIC;
    }

    @Override
    public String getRndCall() {
        // One row in eight on average is NULL, like IntType.
        return "rnd_int(0, " + (DOMAIN_SIZE - 1) + ", 8)";
    }

    @Override
    public String randomLiteral(Rnd rnd) {
        return generateConstant(rnd).literal();
    }
}
