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

import io.questdb.std.Mutable;

/**
 * A pooled plan object whose facts a {@link PlanFactRecorder} records: a plan node, or a part of one that the plan node
 * pools hold.
 */
public interface PlanFactSource extends Mutable {

    /**
     * Hands the visitor every fact of the object but the inputs of a node and the expressions and column ids it reads,
     * which {@link #visitReads} hands out; the facts of a part the object owns, such as a join input of a join, come
     * with its own. Clearing the object resets every fact.
     */
    void visitFacts(PlanFactRecorder visitor);

    /**
     * Hands the visitor every expression and column id the object reads, not those of a node's inputs, and keeps what
     * the visitor returns.
     */
    void visitReads(PlanExpressionVisitor visitor);
}
