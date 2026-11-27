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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.sql.Record;
import org.jetbrains.annotations.Nullable;

/**
 * A hash join factory that runs a RIGHT or FULL join with join keys. The record of its cursor tells
 * whether the current row NULL-extends the left side of the join, see {@link OuterJoinNullCheck}.
 */
public interface OuterJoinRecordSource {

    /**
     * Returns the record of the join's cursor, a {@link RightOuterJoinRecord} for a RIGHT join and a
     * {@link FullOuterJoinRecord} for a FULL join, or null while the factory has not opened a cursor yet.
     * The factory creates its cursor in its first getCursor() call and keeps it, with the same record,
     * until it closes.
     *
     * @return the record of the join's cursor, or null before the first getCursor() call
     */
    @Nullable
    Record getOuterJoinRecord();
}
