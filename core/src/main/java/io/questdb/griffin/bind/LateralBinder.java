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

import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Binds a LATERAL body. A name the body does not define resolves against the preceding inputs of the
 * join, innermost lateral first, and binds as an outer column reference; {@link Decorrelation}
 * rewrites the dependent step into ordinary joins.
 */
final class LateralBinder implements Mutable {
    private final SqlBinder binder;
    private final BindContext ctx;
    private final ObjectPool<OutputSchema> scopes;

    LateralBinder(BindContext ctx, SqlBinder binder, int maxRetainedScopes) {
        this.ctx = ctx;
        this.binder = binder;
        this.scopes = new ObjectPool<>(OutputSchema::new, 2, maxRetainedScopes);
    }

    @Override
    public void clear() {
        scopes.clear();
    }

    /**
     * Binds the body of the dependent join step at {@code index}, whose outer scope is the output of
     * the inputs before it.
     */
    LogicalPlan bindLateral(QueryModel occurrence, JoinPlan join, int index, SqlExecutionContext executionContext) throws SqlException {
        final OutputSchema scope = scopes.next();
        for (int i = 0; i < index; i++) {
            final JoinInput input = join.getInputs().getQuick(i);
            scope.addColumnsFrom(input.getSourceOutput(), input.getBindingAlias());
        }
        final ObjList<OutputSchema> outerScopes = ctx.scope().outerScopes;
        outerScopes.add(scope);
        try {
            return binder.bindSource(occurrence, executionContext);
        } finally {
            outerScopes.remove(outerScopes.size() - 1);
        }
    }
}
