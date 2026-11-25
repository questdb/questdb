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

import io.questdb.griffin.engine.functions.lt.AbstractScalarCursorFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * The scalar sub-queries of one filter compile: each QUERY node that is the operand of a comparison
 * {@link FunctionParser} compiled as an {@link AbstractScalarCursorFunction} (the sub-query runs once
 * per execution), with that function. {@link SqlCodeGenerator} installs it on the parser for the
 * parse that produces the Java filter only, so a later parse of the same nodes (per-worker filters,
 * the {@code read_parquet()} pushdown filter) cannot replace a link, and it is dropped with the
 * compile: no expression node keeps a compiled function.
 */
final class ScalarSubQueryLinks {
    private final ObjList<AbstractScalarCursorFunction> functions = new ObjList<>();
    private final ObjList<ExpressionNode> nodes = new ObjList<>();

    /**
     * @return the function the latest parse compiled {@code node}'s comparison into, or null
     */
    @Nullable
    AbstractScalarCursorFunction find(ExpressionNode node) {
        for (int i = nodes.size() - 1; i > -1; i--) {
            if (nodes.getQuick(i) == node) {
                return functions.getQuick(i);
            }
        }
        return null;
    }

    boolean isEmpty() {
        return nodes.size() == 0;
    }

    void link(ExpressionNode node, AbstractScalarCursorFunction function) {
        nodes.add(node);
        functions.add(function);
    }
}
