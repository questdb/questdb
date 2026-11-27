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

import io.questdb.std.Chars;
import org.jetbrains.annotations.Nullable;

/**
 * A call or SHOW statement that binding admits and a later check of the same compile may refuse: its name as the SQL
 * writes it, the position where its requirement surfaces in the compiled SQL, whether it is a SHOW statement, and the
 * requirement it carries, see {@link Kind}. Binding records the fact on what the check reads: an administrative call
 * on its {@link FunctionExpression} and on the {@link ConstantExpression} a fold makes of it, the administrative and
 * non-deterministic facts of a table function or SHOW statement on its {@link FunctionSourcePlan}. A fact is
 * immutable, so every copy of what holds it shares it. Like the name it copies, binding allocates it where it records
 * it, which only administrative calls and, in a context that refuses them, non-deterministic calls make it do.
 */
public final class RequirementFact {
    private final boolean isStatement;
    private final Kind kind;
    private final String name;
    private final int position;

    private RequirementFact(Kind kind, CharSequence name, int position, boolean isStatement) {
        this.kind = kind;
        this.name = Chars.toString(name);
        this.position = position;
        this.isStatement = isStatement;
    }

    /**
     * A call or SHOW statement that needs the Enterprise security context of the caller, see
     * {@link io.questdb.griffin.SqlExecutionRequirements#REQUIRES_ENTERPRISE_SECURITY_CONTEXT}.
     *
     * @param name        the name of the call as the SQL writes it, or the SHOW statement
     * @param position    where the requirement surfaces in the compiled SQL
     * @param isStatement whether name is a SHOW statement rather than a function
     */
    public static RequirementFact administrative(CharSequence name, int position, boolean isStatement) {
        return new RequirementFact(Kind.ADMINISTRATIVE, name, position, isStatement);
    }

    /**
     * The administrative fact of the first call code generation builds when it builds the expression, in the order
     * binding and master's FunctionParser build calls, which {@link io.questdb.griffin.PostOrderTreeTraversalAlgo}
     * sets: the arguments of a call, the last one first, before the call. A constant contributes the fact of the
     * first call the fold that made it evaluated, see {@link ConstantExpression#getAdministrativeFact()}. Returns null
     * when the expression holds no such call; the walk does not enter a sub-query, which generates on its own.
     */
    public static @Nullable RequirementFact firstAdministrative(BoundExpression expression) {
        if (expression instanceof ConstantExpression constant) {
            return constant.getAdministrativeFact();
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = call.getArgumentCount() - 1; i > -1; i--) {
                final RequirementFact fact = firstAdministrative(call.argumentAt(i));
                if (fact != null) {
                    return fact;
                }
            }
            return call.getAdministrativeFact();
        }
        return null;
    }

    /**
     * A non-deterministic function created for a context that refuses them, a materialized or live view.
     *
     * @param name     the name of the function as the SQL writes it
     * @param position the position of the call in the compiled SQL
     */
    public static RequirementFact nonDeterministic(CharSequence name, int position) {
        return new RequirementFact(Kind.NON_DETERMINISTIC, name, position, false);
    }

    public Kind getKind() {
        return kind;
    }

    /**
     * The name of the call as the SQL writes it, or the SHOW statement.
     */
    public String getName() {
        return name;
    }

    /**
     * Where the requirement surfaces in the compiled SQL: the call or statement itself, or the name of the outermost
     * view binding expanded around it, see {@link io.questdb.griffin.FunctionResolver#getRequirementPosition}.
     */
    public int getPosition() {
        return position;
    }

    public boolean isAdministrative() {
        return kind == Kind.ADMINISTRATIVE;
    }

    /**
     * Whether {@link #getName()} names a SHOW statement rather than a function.
     */
    public boolean isStatement() {
        return isStatement;
    }

    /**
     * The requirement a fact carries.
     */
    public enum Kind {
        /**
         * The call or SHOW statement needs the Enterprise security context of the caller: a view's PIVOT IN subquery,
         * which the view compiler runs detached from any caller, refuses it, and so do materialized and live views.
         */
        ADMINISTRATIVE,
        /**
         * The function returns another value on another run: materialized and live views refuse it.
         */
        NON_DETERMINISTIC
    }
}
