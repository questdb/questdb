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

package io.questdb.test.griffin.fuzz.expr;

import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.griffin.fuzz.FuzzColumn;
import io.questdb.test.griffin.fuzz.types.ColumnKind;
import io.questdb.test.griffin.fuzz.types.FuzzColumnType;
import io.questdb.test.griffin.fuzz.types.FuzzColumnTypes;

/**
 * Builds typed {@link FuzzExpr} trees over a fixed column list. The
 * entry points are {@link #generateAnyKind()} for projection slots where
 * any result is fine, and {@link #generateOfKind(ColumnKind)} for slots
 * that need a specific kind (e.g. a numeric aggregate argument).
 * <p>
 * The grammar is deliberately narrow to keep noise down:
 * <ul>
 *     <li>column ref;</li>
 *     <li>typed constant;</li>
 *     <li>arithmetic {@code a op b} where both sides are numeric;</li>
 *     <li>curated single-arg function call (abs, length, upper, ...);</li>
 *     <li>cast {@code inner::TYPE} to a target kind, provided a numeric
 *     expression can be cast to it at all -- see
 *     {@link #castTargetFromNumeric(ColumnKind)}.</li>
 * </ul>
 */
public final class ExpressionGenerator {

    private static final String[] ARITH_OPS = {"+", "-", "*"};
    private static final FunctionSpec[] FUNCTIONS = {
            new FunctionSpec("abs", ColumnKind.NUMERIC, ColumnKind.NUMERIC),
            new FunctionSpec("length", ColumnKind.STRING_LIKE, ColumnKind.NUMERIC),
            new FunctionSpec("upper", ColumnKind.STRING_LIKE, ColumnKind.STRING_LIKE),
            new FunctionSpec("lower", ColumnKind.STRING_LIKE, ColumnKind.STRING_LIKE),
    };

    private final ObjList<FuzzColumn> columns;
    private final int maxDepth;
    private final String qualifier;
    private final Rnd rnd;

    public ExpressionGenerator(Rnd rnd, ObjList<FuzzColumn> columns, String qualifier, int maxDepth) {
        this.rnd = rnd;
        this.columns = columns;
        this.qualifier = qualifier;
        this.maxDepth = maxDepth;
    }

    /**
     * Any-kind expression suitable for a projection slot that doesn't
     * constrain the result type. Biased towards plain column refs so the
     * corpus still exercises bare-column projections often.
     */
    public FuzzExpr generateAnyKind() {
        if (rnd.nextInt(3) == 0) {
            return generateLeafAny();
        }
        ColumnKind kind = ColumnKind.values()[rnd.nextInt(ColumnKind.values().length)];
        return generateOfKind(kind, 0);
    }

    public FuzzExpr generateOfKind(ColumnKind kind) {
        return generateOfKind(kind, 0);
    }

    /**
     * Draws the identifier kind for a slot the caller is about to fill with
     * {@link #generateOfKind(ColumnKind)}: one this generator's column list
     * carries, or -- when it carries none of the three -- any of them, via
     * {@link ColumnKind#randomIdentifier}. A kind carried by several columns is
     * that much likelier to come up, the way the leaf picked uniformly among
     * all identifier columns while UUID, IPv4 and LONG256 shared one kind.
     * <p>
     * A picker that drew one of the three blind sent the slot to a kind the
     * table has no column of, and {@link #generateLeafOfKind} answers that with
     * a literal constant -- a GROUP BY key that collapses every row into one
     * bucket. Of the tables {@code FuzzTableFactory} deals, 41% carry exactly
     * one identifier type, 32% carry two and 18% carry none, so a blind draw
     * missed most of the time: over 249_818 such tables, 26.2% of identifier
     * key-slot draws reached a real column, against 48.9% once the draw follows
     * the table. The shared kind gave 45.7% on the same corpus; the table-aware
     * draw comes out slightly above it because UUID no longer spends draws on
     * the {@code (<numeric>)::UUID} cast that never compiled -- see
     * {@link #castTargetFromNumeric}.
     * <p>
     * The draw belongs HERE, in the kind picker, rather than in
     * {@link #generateLeafOfKind}. A leaf that substituted another identifier
     * kind for the one it was handed would break the promise
     * {@code generateOfKind(kind)} makes to its caller, and
     * {@code PredicateGenerator.appendComparison} rests on that promise: it
     * draws both operands of a comparison from a single kind, and the three
     * identifier types are mutually incomparable, so a substituting leaf would
     * put {@code uuidCol < ipv4Literal} back into the corpus -- the very noise
     * that splitting the kinds removed. Settling the kind before generation
     * starts leaves every comparison type-coherent by construction.
     */
    public ColumnKind pickIdentifierKind() {
        int carried = 0;
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (columns.getQuick(i).getType().getKind().isIdentifier()) {
                carried++;
            }
        }
        if (carried == 0) {
            return ColumnKind.randomIdentifier(rnd);
        }
        int pick = rnd.nextInt(carried);
        for (int i = 0, n = columns.size(); i < n; i++) {
            ColumnKind kind = columns.getQuick(i).getType().getKind();
            if (kind.isIdentifier() && pick-- == 0) {
                return kind;
            }
        }
        throw new AssertionError("the counting pass found identifier columns the picking pass did not");
    }

    private FuzzExpr generateLeafAny() {
        if (columns.size() > 0 && rnd.nextInt(5) != 0) {
            FuzzColumn col = columns.getQuick(rnd.nextInt(columns.size()));
            return new ColumnRefExpr(rnd, col, qualifier);
        }
        // fall back to a numeric constant
        return new ConstantExpr(ColumnKind.NUMERIC, Integer.toString(rnd.nextInt(2_000) - 1_000));
    }

    private FuzzExpr generateLeafOfKind(ColumnKind target) {
        // Prefer an existing column of the right kind if we have one.
        ObjList<FuzzColumn> matching = columnsOfKind(target);
        if (matching.size() > 0 && rnd.nextInt(3) != 0) {
            FuzzColumn col = matching.getQuick(rnd.nextInt(matching.size()));
            return new ColumnRefExpr(rnd, col, qualifier);
        }
        // Otherwise pick a constant whose type matches the kind.
        FuzzColumnType type = FuzzColumnTypes.pickOfKind(rnd, target);
        if (type != null) {
            return new ConstantExpr(target, type.generateConstant(rnd));
        }
        // Last resort: any column ref. Slot consumers tolerate kind noise.
        return generateLeafAny();
    }

    private FuzzExpr generateOfKind(ColumnKind target, int depth) {
        if (depth >= maxDepth || rnd.nextInt(3) == 0) {
            return generateLeafOfKind(target);
        }
        int choice = rnd.nextInt(4);
        if (choice == 0 && target == ColumnKind.NUMERIC) {
            String op = ARITH_OPS[rnd.nextInt(ARITH_OPS.length)];
            return new ArithmeticExpr(
                    generateOfKind(ColumnKind.NUMERIC, depth + 1),
                    op,
                    generateOfKind(ColumnKind.NUMERIC, depth + 1)
            );
        }
        if (choice == 1) {
            FunctionSpec fn = pickFunctionProducing(target);
            if (fn != null) {
                return new FunctionCallExpr(fn.name, fn.resultKind, generateOfKind(fn.argKind, depth + 1));
            }
        }
        if (choice == 2) {
            FuzzColumnType castTarget = castTargetFromNumeric(target);
            if (castTarget != null) {
                return new CastExpr(generateOfKind(ColumnKind.NUMERIC, depth + 1), castTarget);
            }
        }
        return generateLeafOfKind(target);
    }

    /**
     * Type for the {@code (<numeric>)::TYPE} arm of {@link #generateOfKind},
     * or null when no cast reaches {@code target} from a numeric expression
     * and the arm has to fall through to a leaf instead.
     * <p>
     * UUID is the one such kind. QuestDB registers exactly two casts producing
     * a UUID -- {@code CastStrToUuidFunctionFactory} and
     * {@code CastVarcharToUuidFunctionFactory} -- so every
     * {@code (<numeric>)::UUID} this arm could emit dies with "there is no
     * matching function `cast` with the argument types: (INT, UUID)", and the
     * oracle throws the whole generated query away as an expected skip.
     * Measured over 2000 predicates on a schema carrying one column of each
     * type: 478 failed to compile and 126 of those -- 26% of every failure --
     * were this cast. It costs no coverage to drop: the shape never compiled,
     * and {@link #generateLeafOfKind} still fills a UUID slot with a column
     * reference or a UUID literal.
     * <p>
     * The exclusion sits here rather than in
     * {@link FuzzColumnTypes#pickOfKind}, which {@link #generateLeafOfKind}
     * also calls -- to pick the type whose {@code generateConstant} spells the
     * UUID literal. Excluding UUID there would take the literal away too and
     * leave a UUID slot filled by an any-kind column reference.
     */
    private FuzzColumnType castTargetFromNumeric(ColumnKind target) {
        if (target == ColumnKind.UUID) {
            return null;
        }
        return FuzzColumnTypes.pickOfKind(rnd, target);
    }

    private ObjList<FuzzColumn> columnsOfKind(ColumnKind kind) {
        ObjList<FuzzColumn> out = new ObjList<>();
        for (int i = 0, n = columns.size(); i < n; i++) {
            FuzzColumn c = columns.getQuick(i);
            if (c.getType().getKind() == kind) {
                out.add(c);
            }
        }
        return out;
    }

    private FunctionSpec pickFunctionProducing(ColumnKind target) {
        ObjList<FunctionSpec> matches = new ObjList<>();
        for (FunctionSpec f : FUNCTIONS) {
            if (f.resultKind == target) {
                matches.add(f);
            }
        }
        if (matches.size() == 0) {
            return null;
        }
        return matches.get(rnd.nextInt(matches.size()));
    }

    private static final class FunctionSpec {
        final ColumnKind argKind;
        final String name;
        final ColumnKind resultKind;

        FunctionSpec(String name, ColumnKind argKind, ColumnKind resultKind) {
            this.name = name;
            this.argKind = argKind;
            this.resultKind = resultKind;
        }
    }
}
