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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjectFactory;
import io.questdb.std.str.StringSink;

/**
 * A projection column whose expression failed to bind with an error the column raises only when something
 * evaluates it or reads its type: code generation raises it in output order, and a pruned column never does.
 * Its type is unknown. A WHERE or ON conjunct that failed to bind is a BOOLEAN one ({@link #ofConjunct}),
 * which generation raises when it builds the filter, and so is a conjunct that is not BOOLEAN
 * ({@link #ofNonBoolean}). The message is a copy because the failed exception may
 * be a reused flyweight; close failures suppressed by that exception stay reachable from the raised one.
 */
public final class DeferredErrorExpression extends BoundExpression {
    public static final ObjectFactory<DeferredErrorExpression> FACTORY = DeferredErrorExpression::new;
    private final StringSink message = new StringSink();
    private SqlException cause;
    private int comparedColumnId = -1;
    private boolean isAggregate;
    private int joinInput = -1;
    private int nonBooleanType = -1;

    /**
     * Adds the close failures of a later error swallowed while this one was pending.
     */
    public void addSuppressed(Throwable swallowed) {
        if (swallowed != cause) {
            copySuppressed(cause, swallowed);
        }
    }

    @Override
    public void clear() {
        super.clear();
        message.clear();
        cause = null;
        comparedColumnId = -1;
        joinInput = -1;
        nonBooleanType = -1;
        isAggregate = false;
    }

    /**
     * The column a comparison conjunct compares with a constant or a call, or -1. Interval extraction raises the
     * error when the column is the designated timestamp of the scan, in extraction order.
     */
    public int getComparedColumnId() {
        return comparedColumnId;
    }

    /**
     * The index of the join input whose columns alone the conjunct reads, or -1. Filter pushdown moves the
     * conjunct to that input as it moves any other single-source conjunct.
     */
    public int getJoinInput() {
        return joinInput;
    }

    /**
     * True when the failed expression is an aggregate call, whose grouping column pruning may drop.
     */
    public boolean isAggregate() {
        return isAggregate;
    }

    /**
     * True when the conjunct bound to a value that is not BOOLEAN. Alone in its filter it raises "boolean
     * expression expected"; the conjunction that holds it raises its type mismatch instead
     * ({@link #raiseTypeMismatch}).
     */
    public boolean isNonBoolean() {
        return nonBooleanType != -1;
    }

    public DeferredErrorExpression markAggregate() {
        isAggregate = true;
        return this;
    }

    public DeferredErrorExpression of(SqlException cause) {
        configure(ColumnType.UNDEFINED, cause.getPosition(), 0);
        message.clear();
        message.put(cause.getFlyweightMessage());
        this.cause = cause;
        comparedColumnId = -1;
        joinInput = -1;
        nonBooleanType = -1;
        isAggregate = false;
        return this;
    }

    /**
     * A conjunct that failed to bind. It never evaluates, so it is deterministic and stable; passes keep it in
     * place by {@code LogicalPlans.hasDeferredConjunct}.
     */
    public DeferredErrorExpression ofConjunct(SqlException cause, int comparedColumnId, int joinInput) {
        of(cause);
        configure(ColumnType.BOOLEAN, cause.getPosition());
        this.comparedColumnId = comparedColumnId;
        this.joinInput = joinInput;
        return this;
    }

    /**
     * A conjunct that bound to a value of the given type, which is not BOOLEAN.
     */
    public DeferredErrorExpression ofNonBoolean(int type, int position, int joinInput) {
        ofConjunct(SqlException.$(position, "boolean expression expected"), -1, joinInput);
        nonBooleanType = type;
        return this;
    }

    public SqlException raise() {
        final SqlException raised = SqlException.$(getPosition(), message);
        if (raised != cause) {
            copySuppressed(raised, cause);
        }
        return raised;
    }

    public SqlException raiseTypeMismatch() {
        return SqlException.$(getPosition(), "expression type mismatch, expected: BOOLEAN, actual: ").put(ColumnType.nameOf(nonBooleanType));
    }

    private static void copySuppressed(Throwable target, Throwable source) {
        final Throwable[] suppressed = source.getSuppressed();
        for (int i = 0; i < suppressed.length; i++) {
            target.addSuppressed(suppressed[i]);
        }
    }
}
