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

package io.questdb.griffin.engine.functions.columns;

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.memoization.MemoizerFunction;

/**
 * Stands for functions that represent a physical table column, such as {@link IntColumn}.
 * Should not be implemented by types that can't be used for columns, e.g. {@link IntervalColumn}.
 */
public interface ColumnFunction extends Function {

    /**
     * Returns the underlying {@link ColumnFunction} of {@code f}, peeling any
     * {@link MemoizerFunction} wrappers the SQL code generator inserts when an
     * alias is referenced more than once. Returns {@code null} if {@code f} is
     * not a column reference.
     */
    static ColumnFunction unwrap(Function f) {
        while (f instanceof MemoizerFunction mf) {
            f = mf.getArg();
        }
        return f instanceof ColumnFunction cf ? cf : null;
    }

    /**
     * Returns index of the column in the table metadata.
     */
    int getColumnIndex();

    @Override
    default boolean isEquivalentTo(Function obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ColumnFunction that) {
            return getColumnIndex() == that.getColumnIndex();
        }
        return false;
    }

    @Override
    default void toPlan(PlanSink sink) {
        sink.putColumnName(getColumnIndex());
    }
}
