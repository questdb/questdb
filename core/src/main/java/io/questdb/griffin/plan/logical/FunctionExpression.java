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

import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

/**
 * Published calls retain descriptions only; executable roots have a separate owner.
 */
public final class FunctionExpression extends BoundExpression {
    public static final ObjectFactory<FunctionExpression> FACTORY = FunctionExpression::new;
    private final IntList argumentPositions = new IntList();
    private final ObjList<BoundExpression> arguments = new ObjList<>();
    private boolean isProjectedOffset;
    private boolean isSetOperation;
    private FunctionFactoryDescriptor overload;

    public BoundExpression argumentAt(int index) {
        return arguments.getQuick(index);
    }

    @Override
    public void clear() {
        super.clear();
        argumentPositions.clear();
        arguments.clear();
        overload = null;
        isSetOperation = false;
        isProjectedOffset = false;
    }

    public int getArgumentCount() {
        return arguments.size();
    }

    public int getArgumentPosition(int index) {
        return argumentPositions.getQuick(index);
    }

    public ObjList<BoundExpression> getArguments() {
        return arguments;
    }

    public String getName() {
        return overload.getName();
    }

    public FunctionFactoryDescriptor getOverload() {
        return overload;
    }

    public String getSignature() {
        return overload.getFactory().getSignature();
    }

    public boolean isAggregate() {
        return overload.getFactory().isGroupBy();
    }

    public boolean isAnd() {
        return overload.isAnd();
    }

    public boolean isOr() {
        return overload.isOr();
    }

    /**
     * A projected timestamp offset pushed below its projection. Interval extraction inverts it by
     * shifting the intervals of the predicate over its input.
     */
    public boolean isProjectedOffset() {
        return isProjectedOffset;
    }

    /**
     * A call parsed as a set operation, such as an IN without parentheses.
     */
    public boolean isSetOperation() {
        return isSetOperation;
    }

    public boolean isWindow() {
        return overload.getFactory().isWindow();
    }

    public void markSetOperation() {
        isSetOperation = true;
    }

    public FunctionExpression of(FunctionExpression expression, int position) {
        of(expression.overload, expression.arguments, expression.argumentPositions,
                expression.getDataType(), expression.getFunctionFlags(), position);
        isSetOperation = expression.isSetOperation;
        isProjectedOffset = expression.isProjectedOffset;
        return this;
    }

    public FunctionExpression of(FunctionExpression expression, ObjList<BoundExpression> arguments) {
        of(expression.overload, arguments, expression.argumentPositions,
                expression.getDataType(), expression.getFunctionFlags(), expression.getPosition());
        isSetOperation = expression.isSetOperation;
        isProjectedOffset = expression.isProjectedOffset;
        return this;
    }

    public FunctionExpression of(
            FunctionFactoryDescriptor overload,
            ObjList<BoundExpression> arguments,
            IntList argumentPositions,
            int dataType,
            int functionFlags,
            int position
    ) {
        configure(dataType, position, functionFlags);
        this.overload = overload;
        this.arguments.addAll(arguments);
        this.argumentPositions.addAll(argumentPositions);
        return this;
    }

    public FunctionExpression ofProjectedOffset(FunctionExpression expression) {
        of(expression, expression.getPosition());
        isProjectedOffset = true;
        return this;
    }
}
