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
import io.questdb.std.Mutable;

/**
 * A compilation-owned expression description. Populate before publication and do
 * not change it until pool reset; rewrites acquire a replacement expression.
 */
public abstract class BoundExpression implements Mutable {
    public static final int CONSTANT = 1;
    public static final int NON_DETERMINISTIC = 4;
    public static final int RUNTIME_CONSTANT = 2;
    public static final int STABLE_WITHIN_EXECUTION = 8;
    private int dataType = ColumnType.UNDEFINED;
    private int functionFlags;
    private int position = -1;

    @Override
    public void clear() {
        dataType = ColumnType.UNDEFINED;
        functionFlags = 0;
        position = -1;
    }

    public int getDataType() {
        return dataType;
    }

    public int getFunctionFlags() {
        return functionFlags;
    }

    public int getPosition() {
        return position;
    }

    protected void configure(int dataType, int position) {
        this.dataType = dataType;
        this.position = position;
        this.functionFlags = STABLE_WITHIN_EXECUTION;
    }

    protected void configure(int dataType, int position, int functionFlags) {
        configure(dataType, position);
        this.functionFlags = functionFlags;
    }
}

