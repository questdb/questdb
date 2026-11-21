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

package io.questdb.test.griffin.fuzz.expr;

import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

/**
 * Threaded through {@link FuzzExpr#appendSql(io.questdb.std.str.StringSink, BindContext)}
 * to drive bind-variable substitution. When a {@code BindContext} is non-null and
 * a {@link ConstantExpr} is bindable, each call to {@link #shouldBind()} flips an
 * independent coin: a "yes" allocates a fresh {@code :bN} placeholder name (named
 * bind variables avoid the {@code ?}-vs-column-ref ambiguity in projection slots)
 * and registers the value; a "no" falls through to the literal text. Two parallel
 * lists track the placeholder names and string values; the runner later replays
 * them through {@code BindVariableService.setStr(name, value)}.
 * <p>
 * A query's LIMIT and an expression of its SELECT list that reuses the LIMIT's value
 * read the positional {@code $1} instead, see {@link #bindLimit(String)}, which the
 * runner leaves undefined at compile, as a PostgreSQL Parse without parameter types
 * does, and binds once the query compiled.
 */
public final class BindContext {
    public static final String BIND_NAME_PREFIX = "b";
    public static final String LIMIT_BIND_VARIABLE = "$1";
    private final ObjList<String> bindNames = new ObjList<>();
    private final ObjList<String> bindValues = new ObjList<>();
    // Per-constant bind probability, in percent. 50 means each bindable
    // constant has a 50% chance of becoming a bind variable; the rest stay
    // as literals so a query mixes both forms.
    private final int probabilityPct;
    private final Rnd rnd;
    // The value of the positional $1 that the LIMIT and the SELECT list share, null without one.
    private String limitValue;

    public BindContext(Rnd rnd, int probabilityPct) {
        this.rnd = rnd;
        this.probabilityPct = probabilityPct;
    }

    public String addBinding(String value) {
        String name = BIND_NAME_PREFIX + bindValues.size();
        bindNames.add(name);
        bindValues.add(value);
        return name;
    }

    /**
     * Registers the value of the query's LIMIT, which the LIMIT and an expression of the SELECT
     * list then read as the positional bind variable this returns. Draws nothing.
     */
    public String bindLimit(String value) {
        limitValue = value;
        return LIMIT_BIND_VARIABLE;
    }

    public ObjList<String> getBindNames() {
        return bindNames;
    }

    public ObjList<String> getBindValues() {
        return bindValues;
    }

    public String getLimitValue() {
        return limitValue;
    }

    public boolean shouldBind() {
        return rnd.nextInt(100) < probabilityPct;
    }
}
