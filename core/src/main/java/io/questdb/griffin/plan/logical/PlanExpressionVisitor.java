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
 * Visits what a plan node reads: every expression it evaluates and every column id a key, ordering or column list
 * of it names. The node keeps what the visitor returns, so one visitor both reads and rewrites; a reading visitor
 * returns its argument. The columns a node defines are not reads, and a node never visits its inputs.
 */
public interface PlanExpressionVisitor {

    /**
     * Visits a column id the node reads; {@code position} is the text position of the reference, or -1 when the
     * node keeps none. Returns the id the node keeps.
     */
    int visitColumnId(int columnId, int position);

    /**
     * Visits an expression the node evaluates and returns the expression the node keeps.
     */
    BoundExpression visitExpression(BoundExpression expression);
}
