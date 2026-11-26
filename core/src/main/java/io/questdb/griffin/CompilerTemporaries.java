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
import org.jetbrains.annotations.TestOnly;

/**
 * The scratch collections the components of one compiler share. A component picks the members it uses in its
 * constructor; a user fills a member and consumes it before another user of the same member runs.
 */
public final class CompilerTemporaries implements Mutable {
    /**
     * The most values an int list keeps room for between statements. A statement of tens of joins needs far fewer,
     * while a chain of hundreds of outer joins collects ordering edges that grow with the square of its inputs.
     */
    public static final int MAX_RETAINED_CAPACITY = 4096;
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

    public CompilerTemporaries(int minRetainedJoinNodes) {
        this.joinChildrenPool = new ObjectPool<>(IntHashSet::new, 4, minRetainedJoinNodes);
    }

    /**
     * Empties the functions, the join children with their pool and the names, which the compiler releases between
     * statements, and the int lists, which keep room for at most {@link #MAX_RETAINED_CAPACITY} values.
     */
    @Override
    public void clear() {
        functions.clear();
        joinChildren.clear();
        joinChildrenPool.clear();
        names.clear();
        functionPositions.clearAndTrim(MAX_RETAINED_CAPACITY);
        indexes.clearAndTrim(MAX_RETAINED_CAPACITY);
        masterKeys.clearAndTrim(MAX_RETAINED_CAPACITY);
        positions.clearAndTrim(MAX_RETAINED_CAPACITY);
        slaveKeys.clearAndTrim(MAX_RETAINED_CAPACITY);
        values.clearAndTrim(MAX_RETAINED_CAPACITY);
    }

    /**
     * Returns the largest capacity, in values, of the int lists.
     */
    @TestOnly
    public int getIntListCapacity() {
        int capacity = Math.max(functionPositions.capacity(), indexes.capacity());
        capacity = Math.max(capacity, Math.max(masterKeys.capacity(), positions.capacity()));
        return Math.max(capacity, Math.max(slaveKeys.capacity(), values.capacity()));
    }
}
