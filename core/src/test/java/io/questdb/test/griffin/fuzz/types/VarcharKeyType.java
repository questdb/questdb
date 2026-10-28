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
 * The type of the fixed VARCHAR join-key column every fuzz table carries next to
 * {@code k} and {@code sym}. Its values are {@link SymbolType#DOMAIN}, so the column
 * joins itself across tables and also joins {@code sym}: a SYMBOL against a VARCHAR
 * reconciles to VARCHAR and both sides stage their text, which is the encoding the
 * fused hash join GROUP BY uses wherever it cannot translate symbol keys.
 * <p>
 * {@code FuzzTableFactory} adds the column by name and does not deal this type off the
 * deck, so no other column carries it.
 */
public final class VarcharKeyType implements FuzzColumnType {
    public static final VarcharKeyType INSTANCE = new VarcharKeyType();

    private VarcharKeyType() {
    }

    @Override
    public FuzzConstant generateConstant(Rnd rnd) {
        if (rnd.nextInt(16) == 0) {
            return FuzzConstant.nonBindable("null");
        }
        String v = SymbolType.DOMAIN[rnd.nextInt(SymbolType.DOMAIN.length)];
        return new FuzzConstant("'" + v + "'::VARCHAR", "VARCHAR", v);
    }

    @Override
    public String getDdl() {
        return "VARCHAR";
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.STRING_LIKE;
    }

    @Override
    public String getRndCall() {
        // Draws from SymbolType.DOMAIN with NULL mixed in, so the texts match sym's.
        StringBuilder sb = new StringBuilder("rnd_varchar(");
        for (String v : SymbolType.DOMAIN) {
            sb.append('\'').append(v).append("', ");
        }
        sb.append("null)");
        return sb.toString();
    }

    @Override
    public String randomLiteral(Rnd rnd) {
        return generateConstant(rnd).literal();
    }
}
