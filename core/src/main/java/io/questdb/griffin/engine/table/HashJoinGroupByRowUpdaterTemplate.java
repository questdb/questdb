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
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.join.HashJoinGroupByRecord;

/**
 * The bytecode that {@link HashJoinGroupByRowUpdater#newInstance()} defines a hidden class from,
 * once per fused factory. It must stay a top-level class without lambdas, nested classes or
 * static state, since each hidden copy shares nothing with this class but its bytecode.
 */
final class HashJoinGroupByRowUpdaterTemplate extends HashJoinGroupByRowUpdater {

    @Override
    void update(
            AsyncHashJoinGroupByAtom.Slot slot,
            GroupByMapFragment fragment,
            Map map,
            RecordSink sink,
            GroupByFunctionsUpdater updater,
            HashJoinGroupByRecord record,
            Function filter,
            long rowId
    ) {
        if (filter == null || filter.getBool(record)) {
            final MapValue value;
            if (slot.value != null) {
                value = slot.value;
            } else {
                MapKey key = map.withKey();
                sink.copy(record, key);
                if (fragment.isNotSharded()) {
                    value = key.createValue();
                } else {
                    key.commit();
                    final long hashCode = key.hash();
                    final Map shard = fragment.getShardMap(hashCode);
                    if (shard != map) {
                        MapKey shardKey = shard.withKey();
                        shardKey.copyFrom(key);
                        value = shardKey.createValue(hashCode);
                    } else {
                        value = key.createValue(hashCode);
                    }
                }
            }
            if (value.isNew()) {
                updater.updateNew(value, record, rowId);
                if (slot.value != null) {
                    slot.value.setNew(false);
                }
            } else {
                updater.updateExisting(value, record, rowId);
            }
        }
    }
}
