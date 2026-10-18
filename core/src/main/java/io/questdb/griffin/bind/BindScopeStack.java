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

package io.questdb.griffin.bind;

import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.RetainedCapacity;
import org.jetbrains.annotations.TestOnly;

/**
 * One {@link BindScope} per nesting depth of the compiler's statements: the statement binds at depth 0 and a
 * sub-query one depth deeper than the query that contains it. Queries bind one at a time at each depth, a sub-query
 * only inside the query that contains it, so the scope of a depth serves every sub-query of that depth. Optimising
 * and generating a sub-query enter the depth it bound at. Clearing the stack keeps the scopes of the depths its
 * recent uses reached, see {@link RetainedCapacity}, and of at least the first {@link #MIN_RETAINED_DEPTH}
 * depths, and drops deeper ones.
 */
public final class BindScopeStack implements Mutable {
    static final int MIN_RETAINED_DEPTH = 32;
    private final RetainedCapacity retainedScopeCapacity = new RetainedCapacity(MIN_RETAINED_DEPTH);
    private final ObjList<BindScope> scopes = new ObjList<>();
    private BindScope current;
    private int depth;
    private int usedScopeCount;

    public BindScopeStack() {
        current = scopeAt(0);
    }

    @Override
    public void clear() {
        depth = 0;
        for (int i = 0, n = usedScopeCount; i < n; i++) {
            scopes.getQuick(i).clear();
        }
        final int retainedScopeCount = retainedScopeCapacity.endUse(usedScopeCount);
        if (scopes.size() > retainedScopeCount) {
            scopes.remove(retainedScopeCount, scopes.size() - 1);
        }
        current = scopes.getQuick(0);
        usedScopeCount = 1;
    }

    public int depth() {
        return depth;
    }

    /**
     * Makes the scope of the depth current and returns the depth that was, for the caller to enter again.
     */
    public int enter(int depth) {
        final int previous = this.depth;
        this.depth = depth;
        current = scopeAt(depth);
        return previous;
    }

    public void pop() {
        current = scopes.getQuick(--depth);
    }

    /**
     * Makes the cleared scope of a sub-query of the query binding now current until {@link #pop()}.
     */
    public BindScope push() {
        current = scopeAt(++depth);
        current.clear();
        return current;
    }

    @TestOnly
    public int scopeCount() {
        return scopes.size();
    }

    private BindScope scopeAt(int depth) {
        while (scopes.size() <= depth) {
            scopes.add(new BindScope());
        }
        usedScopeCount = Math.max(usedScopeCount, depth + 1);
        return scopes.getQuick(depth);
    }

    /**
     * The scope of the query binding now.
     */
    BindScope current() {
        return current;
    }
}
