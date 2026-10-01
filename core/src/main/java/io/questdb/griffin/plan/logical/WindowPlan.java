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

import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

/**
 * Input columns followed by window results; every expression reads the input.
 */
public final class WindowPlan extends UnaryPlan {
    public static final ObjectFactory<WindowPlan> FACTORY = WindowPlan::new;
    private final IntList functionColumnIds = new IntList();
    private final ObjList<FunctionExpression> functions = new ObjList<>();
    private final ObjList<WindowSpec> specs = new ObjList<>();
    private boolean isSelectOrdered;

    @Override
    public void clear() {
        super.clear();
        functionColumnIds.clear();
        functions.clear();
        specs.clear();
        isSelectOrdered = false;
    }

    public IntList getFunctionColumnIds() {
        return functionColumnIds;
    }

    public ObjList<FunctionExpression> getFunctions() {
        return functions;
    }

    public ObjList<WindowSpec> getSpecs() {
        return specs;
    }

    @Override
    public Type getType() {
        return Type.WINDOW;
    }

    /**
     * True when the ORDER BY of the SELECT that computes these windows sorts their output.
     */
    public boolean isSelectOrdered() {
        return isSelectOrdered;
    }

    public void markSelectOrdered() {
        isSelectOrdered = true;
    }

    public WindowPlan of(LogicalPlan input, int position) {
        configure(input, position);
        return this;
    }
}
