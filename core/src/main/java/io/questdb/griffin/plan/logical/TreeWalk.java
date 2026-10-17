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

/**
 * What a walk over an expression or plan tree does after a {@link ExpressionVisitor} or {@link PlanVisitor} visits a
 * node, which a walk visits before its children.
 */
public final class TreeWalk {
    /**
     * Visits the node's children, then its later siblings.
     */
    public static final int CONTINUE = 0;
    /**
     * Skips the node's children and visits its later siblings.
     */
    public static final int SKIP_CHILDREN = 1;
    /**
     * Ends the walk.
     */
    public static final int STOP = 2;

    private TreeWalk() {
    }
}
