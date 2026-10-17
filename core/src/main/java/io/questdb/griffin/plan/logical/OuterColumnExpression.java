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

import io.questdb.std.ObjectFactory;

/**
 * A column of the inputs before a dependent join step, read by the step's input (a LATERAL body).
 * Its value is fixed for each outer row; see {@link JoinInput#isDependent()}.
 */
public final class OuterColumnExpression extends BoundExpression {
    public static final ObjectFactory<OuterColumnExpression> FACTORY = OuterColumnExpression::new;
    private int columnId = -1;

    @Override
    public void clear() {
        super.clear();
        columnId = -1;
    }

    /**
     * The id of the column in the output of the inputs before the dependent step.
     */
    public int getColumnId() {
        return columnId;
    }

    public OuterColumnExpression of(int columnId, int dataType, int position) {
        if (columnId < 0) {
            throw new IllegalArgumentException("negative logical column ID");
        }
        configure(dataType, position);
        this.columnId = columnId;
        return this;
    }
}
