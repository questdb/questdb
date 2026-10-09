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

import io.questdb.std.IntHashSet;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

/**
 * What one join input waits for: the inputs it must join after and the equalities that key it to them.
 */
public final class JoinDependency implements Mutable {
    public static final ObjectFactory<JoinDependency> FACTORY = JoinDependency::new;
    private final ObjList<JoinEquality> keys = new ObjList<>();
    private final IntHashSet parents = new IntHashSet(4);
    private int slave = -1;

    @Override
    public void clear() {
        keys.clear();
        parents.clear();
        slave = -1;
    }

    public ObjList<JoinEquality> getKeys() {
        return keys;
    }

    public IntHashSet getParents() {
        return parents;
    }

    public int getSlave() {
        return slave;
    }

    public JoinDependency of(int slave) {
        this.slave = slave;
        return this;
    }
}
