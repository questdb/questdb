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
import io.questdb.std.RetainedCapacity;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.TestOnly;

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
    // the room each int list had when its statement began, which tells whether the statement grew it
    private final IntList intListCapacities = new IntList();
    private final ObjList<RetainedCapacity> intListRetainedCapacities = new ObjList<>();
    private final ObjList<IntList> intLists = new ObjList<>();
    private final RetainedCapacity longsRetainedCapacity = new RetainedCapacity(longs.capacity());
    // the room the long list had when its statement began
    private int longsCapacity = longs.capacity();

    public CompilerTemporaries(int minRetainedJoinNodes) {
        this.joinChildrenPool = new ObjectPool<>(IntHashSet::new, 4, minRetainedJoinNodes);
        retain(functionPositions);
        retain(indexes);
        retain(masterKeys);
        retain(positions);
        retain(slaveKeys);
        retain(values);
    }

    /**
     * Empties the functions, the join children with their pool and the names, which the compiler releases between
     * statements, and the int and long lists, which keep the room their recent statements needed, see
     * {@link RetainedCapacity}.
     * A statement that grew a list needed about its new room; one that did not needed no more than the room the list
     * had, and at least the values the list holds at the end.
     */
    @Override
    public void clear() {
        functions.clear();
        joinChildren.clear();
        joinChildrenPool.clear();
        names.clear();
        for (int i = 0, n = intLists.size(); i < n; i++) {
            final IntList list = intLists.getQuick(i);
            list.clearAndTrim(intListRetainedCapacities.getQuick(i).endUse(used(list.capacity(), intListCapacities.getQuick(i), list.size())));
            intListCapacities.setQuick(i, list.capacity());
        }
        longs.clearAndTrim(longsRetainedCapacity.endUse(used(longs.capacity(), longsCapacity, longs.size())));
        longsCapacity = longs.capacity();
    }

    /**
     * Returns the largest room, in values, that an int or long list keeps.
     */
    @TestOnly
    int getListCapacity() {
        int capacity = longs.capacity();
        for (int i = 0, n = intLists.size(); i < n; i++) {
            capacity = Math.max(capacity, intLists.getQuick(i).capacity());
        }
        return capacity;
    }

    // The room a statement needed from a list: the room it grew the list to, or, when it did not grow the list, no
    // more than the room the list had and at least the values the list holds at the end.
    private static int used(int capacity, int startCapacity, int size) {
        return capacity > startCapacity ? capacity : size;
    }

    private void retain(IntList list) {
        intLists.add(list);
        intListCapacities.add(list.capacity());
        intListRetainedCapacities.add(new RetainedCapacity(list.capacity()));
    }
}
