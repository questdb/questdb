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

public final class ColumnExpression extends BoundExpression {
    public static final ObjectFactory<ColumnExpression> FACTORY = ColumnExpression::new;
    private int columnId = -1;
    private boolean isCast;
    private boolean isDirectReference = true;

    @Override
    public void clear() {
        super.clear();
        columnId = -1;
        isCast = false;
        isDirectReference = true;
    }

    public int getColumnId() {
        return columnId;
    }

    public boolean isCast() {
        return isCast;
    }

    public boolean isDirectReference() {
        return isDirectReference;
    }

    public ColumnExpression of(int columnId, int dataType, int position) {
        return of(columnId, dataType, position, true);
    }

    public ColumnExpression of(int columnId, int dataType, int position, boolean isDirectReference) {
        return of(columnId, dataType, position, isDirectReference, false);
    }

    public ColumnExpression of(int columnId, int dataType, int position, boolean isDirectReference, boolean isCast) {
        if (columnId < 0) {
            throw new IllegalArgumentException("negative logical column ID");
        }
        configure(dataType, position);
        this.columnId = columnId;
        this.isCast = isCast;
        this.isDirectReference = isDirectReference;
        return this;
    }
}
