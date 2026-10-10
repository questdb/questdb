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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.functions.UntypedFunction;
import io.questdb.std.Mutable;

/**
 * Stands in, while binding, for a call whose factory declares its result type, or for a constant that has no
 * value; it carries only that type and is never evaluated.
 */
public final class StaticTypeFunction extends UntypedFunction implements Mutable {
    private boolean isParallel;
    private int type;

    @Override
    public void clear() {
        type = ColumnType.UNDEFINED;
        isParallel = true;
    }

    @Override
    public int getType() {
        return type;
    }

    public StaticTypeFunction of(int type, boolean isParallel) {
        this.type = type;
        this.isParallel = isParallel;
        return this;
    }

    @Override
    public boolean supportsParallelism() {
        return isParallel;
    }
}
