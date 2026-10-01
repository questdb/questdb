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
 * A sub-query used as a function argument. The optimized plan stays with the
 * expression so that each additional executable consumer can generate its own cursor.
 */
public final class CursorExpression extends BoundExpression {
    public static final ObjectFactory<CursorExpression> FACTORY = CursorExpression::new;
    private LogicalPlan plan;
    private int subqueryIndex = -1;

    @Override
    public void clear() {
        super.clear();
        plan = null;
        subqueryIndex = -1;
    }

    public LogicalPlan getPlan() {
        return plan;
    }

    public int getSubqueryIndex() {
        return subqueryIndex;
    }

    public boolean isBoolean() {
        return getDataType() == ColumnType.BOOLEAN;
    }

    public CursorExpression of(LogicalPlan plan, int subqueryIndex, int functionFlags, int position) {
        return of(plan, subqueryIndex, ColumnType.CURSOR, functionFlags, position);
    }

    public CursorExpression ofBoolean(CursorExpression cursor, int functionFlags) {
        return of(cursor.plan, cursor.subqueryIndex, ColumnType.BOOLEAN, functionFlags, cursor.getPosition());
    }

    private CursorExpression of(LogicalPlan plan, int subqueryIndex, int dataType, int functionFlags, int position) {
        configure(dataType, position, functionFlags);
        this.plan = plan;
        this.subqueryIndex = subqueryIndex;
        return this;
    }
}
