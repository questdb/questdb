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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.join.HashJoinGroupByRecord;

/**
 * Aggregates one joined row into its group, on behalf of the fused reducers.
 * <p>
 * The key sink and the aggregate updater are classes that each query generates for itself, so a
 * shared copy of this step would profile every query's classes; see {@link HiddenClassCopies}.
 * {@link #newInstance()} therefore hands every fused factory a hidden copy of
 * {@link HashJoinGroupByRowUpdaterTemplate} of its own, so that each factory's copy profiles only
 * that factory's classes.
 */
abstract class HashJoinGroupByRowUpdater {
    private static final HiddenClassCopies<HashJoinGroupByRowUpdater> COPIES = new HiddenClassCopies<>(
            HashJoinGroupByRowUpdater.class,
            HashJoinGroupByRowUpdaterTemplate.class,
            HashJoinGroupByRowUpdaterTemplate::new
    );

    /**
     * Returns a row updater whose class no other factory shares, or the shared template when the
     * JVM cannot define hidden classes.
     */
    static HashJoinGroupByRowUpdater newInstance() {
        return COPIES.newInstance();
    }

    abstract void update(
            AsyncHashJoinGroupByAtom.Slot slot,
            GroupByMapFragment fragment,
            Map map,
            RecordSink sink,
            GroupByFunctionsUpdater updater,
            HashJoinGroupByRecord record,
            Function filter,
            long rowId
    );
}
