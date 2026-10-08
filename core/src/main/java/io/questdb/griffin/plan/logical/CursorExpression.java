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

package io.questdb.griffin.plan.logical;

import io.questdb.cairo.ColumnType;
import io.questdb.std.ObjectFactory;

/**
 * A sub-query used as a function argument: the statement's {@link Subquery}, whose plan's output schema types the
 * consumer. Every copy of the expression shares the sub-query. The generator generates the sub-query once per statement
 * and evaluates it at most once per execution, so its value is stable within an execution by construction
 * ({@link BoundExpression#STABLE_WITHIN_EXECUTION}).
 */
public final class CursorExpression extends BoundExpression {
    public static final ObjectFactory<CursorExpression> FACTORY = CursorExpression::new;
    private Subquery subquery;

    @Override
    public void clear() {
        super.clear();
        subquery = null;
    }

    public LogicalPlan getPlan() {
        return subquery.getRoot();
    }

    public Subquery getSubquery() {
        return subquery;
    }

    public boolean isBoolean() {
        return getDataType() == ColumnType.BOOLEAN;
    }

    public CursorExpression of(Subquery subquery, int position) {
        return of(subquery, ColumnType.CURSOR, STABLE_WITHIN_EXECUTION, position);
    }

    public CursorExpression ofBoolean(CursorExpression cursor, int functionFlags) {
        return of(cursor.subquery, ColumnType.BOOLEAN, functionFlags, cursor.getPosition());
    }

    private CursorExpression of(Subquery subquery, int dataType, int functionFlags, int position) {
        configure(dataType, position, functionFlags);
        this.subquery = subquery;
        return this;
    }
}
