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

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import org.jetbrains.annotations.TestOnly;

/**
 * Published calls retain descriptions only; executable roots have a separate owner.
 */
public final class FunctionExpression extends BoundExpression {
    public static final ObjectFactory<FunctionExpression> FACTORY = FunctionExpression::new;
    private final IntList argumentPositions = new IntList();
    private final ObjList<BoundExpression> arguments;
    private RequirementFact administrativeFact;
    private boolean isOrderIdentity;
    private boolean isPolicyPredicate;
    private boolean isProjectedOffset;
    private boolean isSetOperation;
    private int orderArgumentIndex = -1;
    private FunctionFactoryDescriptor overload;

    public FunctionExpression() {
        arguments = new ObjList<>();
    }

    @TestOnly
    public FunctionExpression(ObjList<BoundExpression> arguments) {
        this.arguments = arguments;
    }

    public BoundExpression argumentAt(int index) {
        return arguments.getQuick(index);
    }

    @Override
    public void clear() {
        super.clear();
        argumentPositions.clear();
        arguments.clear();
        overload = null;
        administrativeFact = null;
        isPolicyPredicate = false;
        isSetOperation = false;
        isProjectedOffset = false;
        orderArgumentIndex = -1;
        isOrderIdentity = false;
    }

    /**
     * The fact of the call when its factory needs the Enterprise security context of the caller, or null, see
     * {@link io.questdb.griffin.SqlExecutionRequirements#REQUIRES_ENTERPRISE_SECURITY_CONTEXT}. Binding admits such a
     * call in a sub-query nested in a view's PIVOT IN subquery, and code generation rejects it where it builds the
     * call while it generates that subquery, see {@link RequirementFact#firstAdministrative}. A copy of the call keeps
     * the fact.
     */
    public RequirementFact getAdministrativeFact() {
        return administrativeFact;
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

    /**
     * The value the aggregate reports over a group of no rows, as its factory declares it.
     */
    public FunctionFactory.EmptyGroupValue getEmptyGroupValue() {
        return overload.getFactory().getEmptyGroupValue();
    }

    /**
     * The row of a group whose argument the aggregate reports over rows ascending by the argument, as its factory
     * declares it.
     */
    public FunctionFactory.EndpointRow getEndpointRow() {
        return overload.getFactory().getEndpointRow();
    }

    public String getName() {
        return overload.getName();
    }


    /**
     * The argument whose order the values of the call keep, -1 for none: they never decrease as it grows while every
     * other argument stays the same, or the call is that argument itself, see {@link #isOrderIdentity}, as the factory
     * declares it, see {@link io.questdb.griffin.engine.functions.MonotonicTimestampFunctionFactory}.
     */
    public int getOrderArgumentIndex() {
        return orderArgumentIndex;
    }

    public FunctionFactoryDescriptor getOverload() {
        return overload;
    }

    /**
     * How the values of the aggregate over consecutive parts of a group merge, as its factory declares it.
     */
    public FunctionFactory.PartialMerge getPartialMerge() {
        return overload.getFactory().getPartialMerge();
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

    public boolean isEquality() {
        return overload.isEquality();
    }

    public boolean isNot() {
        return overload.isNot();
    }

    public boolean isOr() {
        return overload.isOr();
    }

    /**
     * True when the function built for the call is its order argument itself, see {@link #getOrderArgumentIndex}, as
     * the factory declares it.
     */
    public boolean isOrderIdentity() {
        return isOrderIdentity;
    }

    /**
     * A call an EXPIRE ROWS policy states rather than the query. DDL validated the policy, which compares its operands
     * as its text says, so what a query's own predicate may not compare still reads here.
     */
    public boolean isPolicyPredicate() {
        return isPolicyPredicate;
    }

    /**
     * A projected timestamp offset pushed below its projection. Interval extraction inverts it by
     * shifting the intervals of the predicate over its input.
     */
    public boolean isProjectedOffset() {
        return isProjectedOffset;
    }

    /**
     * Whether the call counts every row of its group, as its overload declares it, see
     * {@link FunctionFactoryDescriptor#isRowCount()}.
     */
    public boolean isRowCount() {
        return overload.isRowCount();
    }

    /**
     * An IN parsed without parentheses, whose right operand is a single value or interval rather than a list.
     */
    public boolean isSetOperation() {
        return isSetOperation;
    }

    public boolean isWindow() {
        return overload.getFactory().isWindow();
    }

    public void markPolicyPredicate() {
        isPolicyPredicate = true;
    }

    public FunctionExpression markProjectedOffset() {
        isProjectedOffset = true;
        return this;
    }

    public void markSetOperation() {
        isSetOperation = true;
    }

    public FunctionExpression of(FunctionExpression expression, int position) {
        of(expression.overload, expression.arguments, expression.argumentPositions,
                expression.getDataType(), expression.getFunctionFlags(), position);
        copyFacts(expression);
        return this;
    }

    public FunctionExpression of(FunctionExpression expression, ObjList<BoundExpression> arguments) {
        return of(expression, arguments, expression.getFunctionFlags());
    }

    public FunctionExpression of(FunctionExpression expression, ObjList<BoundExpression> arguments, int functionFlags) {
        of(expression.overload, arguments, expression.argumentPositions,
                expression.getDataType(), functionFlags, expression.getPosition());
        copyFacts(expression);
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

    /**
     * Records that the call needs the Enterprise security context of the caller, see {@link #getAdministrativeFact()};
     * null records that it does not.
     */
    public FunctionExpression setAdministrativeFact(RequirementFact fact) {
        administrativeFact = fact;
        return this;
    }

    public void setOrderArgument(int orderArgumentIndex, boolean isOrderIdentity) {
        assert !isOrderIdentity || orderArgumentIndex >= 0;
        this.orderArgumentIndex = orderArgumentIndex;
        this.isOrderIdentity = isOrderIdentity;
    }

    private void copyFacts(FunctionExpression expression) {
        administrativeFact = expression.administrativeFact;
        isPolicyPredicate = expression.isPolicyPredicate;
        isSetOperation = expression.isSetOperation;
        isProjectedOffset = expression.isProjectedOffset;
        orderArgumentIndex = expression.orderArgumentIndex;
        isOrderIdentity = expression.isOrderIdentity;
    }
}
