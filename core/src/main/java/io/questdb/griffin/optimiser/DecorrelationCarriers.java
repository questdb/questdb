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

package io.questdb.griffin.optimiser;

import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjObjHashMap;
import io.questdb.std.ObjectPool;

/**
 * Carrier columns remain valid only until the next optimiser pass renames columns.
 */
public final class DecorrelationCarriers implements Mutable {
    private final ObjectPool<IntList> columnIds = new ObjectPool<>(IntList::new, 8);
    private final IntList none = new IntList();
    private final ObjObjHashMap<JoinInput, IntList> steps = new ObjObjHashMap<>();

    public void add(JoinInput step, int columnId) {
        final int index = steps.keyIndex(step);
        final IntList carriers;
        if (index < 0) {
            carriers = steps.valueAt(index);
        } else {
            carriers = columnIds.next();
            steps.putAt(index, step, carriers);
        }
        if (!carriers.contains(columnId)) {
            carriers.add(columnId);
        }
    }

    @Override
    public void clear() {
        steps.clear();
        columnIds.clear();
    }

    IntList of(JoinInput step) {
        final IntList carriers = steps.get(step);
        return carriers == null ? none : carriers;
    }

    void remap(JoinPlan join, IntIntHashMap remappedIds) {
        for (int i = 0, n = join.getInputs().size(); i < n; i++) {
            final IntList carriers = of(join.getInputs().getQuick(i));
            for (int k = 0, m = carriers.size(); k < m; k++) {
                final int columnId = carriers.getQuick(k);
                final int remappedId = columnId < 0 ? -1 : remappedIds.get(columnId);
                if (remappedId > -1) {
                    carriers.setQuick(k, remappedId);
                }
            }
        }
    }
}
