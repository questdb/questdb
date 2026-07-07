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

public final class SymbolType implements FuzzColumnType {
    // A fixed, known symbol domain (plus NULL in the data). Keeping it small and
    // known lets the query generator emit equality/IN predicates that actually
    // match stored rows -- without this, a random literal almost never equals a
    // random stored symbol, so WHERE sym = ..., ON (sym) joins, and posting
    // covering reads would all be effectively empty.
    public static final String[] DOMAIN = {"s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7"};
    public static final SymbolType INSTANCE = new SymbolType();

    private SymbolType() {
    }

    @Override
    public FuzzConstant generateConstant(Rnd rnd) {
        if (rnd.nextInt(16) == 0) {
            return FuzzConstant.nonBindable("null");
        }
        String v = DOMAIN[rnd.nextInt(DOMAIN.length)];
        return new FuzzConstant("'" + v + "'", "SYMBOL", v);
    }

    @Override
    public String getDdl() {
        return "SYMBOL";
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.STRING_LIKE;
    }

    @Override
    public String getRndCall() {
        // Draws from DOMAIN with NULL mixed in (null rate ~1:9).
        StringBuilder sb = new StringBuilder("rnd_symbol(");
        for (String v : DOMAIN) {
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
