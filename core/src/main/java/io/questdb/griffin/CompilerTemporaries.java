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

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.JoinDependency;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.UniqueColumnNames;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.IntSortedList;
import io.questdb.std.IntStack;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.StringSink;

/**
 * The scratch collections the components of one compiler share. A component picks the members it uses in its
 * constructor; a user fills a member and consumes it before another user of the same member runs.
 */
public final class CompilerTemporaries implements Mutable {
    public final ObjList<BoundExpression> arguments = new ObjList<>(2);
    public final ObjList<BoundExpression> expressions = new ObjList<>();
    public final IntList functionPositions = new IntList();
    public final ObjList<Function> functions = new ObjList<>();
    public final IntIntHashMap idMap = new IntIntHashMap();
    public final IntHashSet ids = new IntHashSet();
    public final IntList indexes = new IntList();
    public final ObjList<IntHashSet> joinChildren = new ObjList<>();
    public final ObjectPool<IntHashSet> joinChildrenPool;
    public final ObjList<JoinDependency> joinDependencies = new ObjList<>();
    public final ObjList<JoinKind> joinTypes = new ObjList<>();
    public final LongList longs = new LongList();
    public final IntList masterKeys = new IntList();
    public final ObjList<CharSequence> names = new ObjList<>();
    public final ObjList<ExpressionNode> nodes = new ObjList<>();
    public final IntList positions = new IntList(2);
    public final IntSortedList readyInputs = new IntSortedList();
    public final OutputSchema scope = new OutputSchema();
    public final StringSink sink = new StringSink();
    public final IntList slaveKeys = new IntList();
    public final IntStack stack = new IntStack();
    public final CharSequenceHashSet tokens = new CharSequenceHashSet();
    public final UniqueColumnNames uniqueNames = new UniqueColumnNames();
    public final IntList values = new IntList();

    public CompilerTemporaries(int minRetainedJoinContexts) {
        this.joinChildrenPool = new ObjectPool<>(IntHashSet::new, 4, minRetainedJoinContexts);
    }

    /**
     * Empties the functions, the join children with their pool and the names, which the compiler releases between
     * statements.
     */
    @Override
    public void clear() {
        functions.clear();
        joinChildren.clear();
        joinChildrenPool.clear();
        names.clear();
    }
}
