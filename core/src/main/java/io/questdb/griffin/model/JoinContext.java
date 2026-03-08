/*******************************************************************************
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

package io.questdb.griffin.model;

import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;


// Metadata describing join conditions
public class JoinContext implements Mutable, Plannable {
    public static final ObjectFactory<JoinContext> FACTORY = JoinContext::new;
    private static final int TYPICAL_NUMBER_OF_JOIN_COLUMNS = 4;

    public final IntList aIndexes = new IntList();
    public final ObjList<CharSequence> aNames = new ObjList<>(TYPICAL_NUMBER_OF_JOIN_COLUMNS);
    public final ObjList<ExpressionNode> aNodes = new ObjList<>(TYPICAL_NUMBER_OF_JOIN_COLUMNS);
    public final IntList bIndexes = new IntList();
    public final ObjList<CharSequence> bNames = new ObjList<>(TYPICAL_NUMBER_OF_JOIN_COLUMNS);
    public final ObjList<ExpressionNode> bNodes = new ObjList<>(TYPICAL_NUMBER_OF_JOIN_COLUMNS);
    // indexes of parent join clauses
    public final IntHashSet parents = new IntHashSet(4);
    public int inCount;
    public int slaveIndex = -1;

    @Override
    public void clear() {
        aIndexes.clear();
        aNames.clear();
        aNodes.clear();

        bIndexes.clear();
        bNames.clear();
        bNodes.clear();

        inCount = 0;
        slaveIndex = -1;
        parents.clear();
    }

    public boolean isEmpty() {
        return aNodes.size() == 0 && bNodes.size() == 0;
    }

    @Override
    public void toPlan(PlanSink sink) {
        for (int i = 0, n = aNodes.size(); i < n; i++) {
            if (i > 0) {
                sink.val(" and ");
            }
            sink.val(aNodes.get(i)).val('=').val(bNodes.get(i));
        }
    }
}
