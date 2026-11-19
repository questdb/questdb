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

package io.questdb.test.griffin;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.ArgSwappingFunctionFactory;
import io.questdb.griffin.engine.functions.NegatingFunctionFactory;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.griffin.engine.functions.constants.CharTypeConstant;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

/**
 * Pins every call whose overload depends on how a score tie treats untyped NULL arguments. The test walks every
 * function name with several overloads, every arity they accept, in and out of a window context, and every placement
 * of untyped NULL arguments before a typed one, with columns, constants, typed NULL constants, an untyped bind variable
 * and an untyped empty array at the typed positions. It resolves each call with the production resolver and with two
 * copies of {@code OverloadResolver.resolve}: one breaks a score tie on the first argument whose conversion distances
 * differ, the rule before master's NULL CHAR ordering fix was merged, the other on the typed arguments first and on the
 * untyped NULL arguments after them, the current rule. The copy of the current rule must agree with production on
 * every call, which proves the copies faithful, and the calls on which the two rules disagree are pinned.
 * {@code testComparisonAliasesBindMirrorOverload} walks every pair of those arguments, untyped NULL included, through
 * every comparison and requires each negating or argument-swapping alias to bind the overload its base binds.
 * <p>
 * Only a call with an untyped NULL argument before a typed one can tell the rules apart: when every NULL argument
 * follows every typed one, the first difference among all the arguments is the first typed difference, or else the
 * first NULL difference, under both rules. An untyped NULL converts to every type at distance 0 except STRING and
 * SYMBOL, at distance -1, so the rules can disagree only between overloads that take STRING or SYMBOL at a NULL position
 * and overloads that do not.
 */
public class OverloadNullTieBreakTest extends AbstractCairoTest {
    private static final int MATCH_EXACT_MATCH = 3;
    private static final int MATCH_FUZZY_MATCH = 1;
    private static final int MATCH_NO_MATCH = 0;
    private static final int MATCH_PARTIAL_MATCH = 2;
    private static final int MAX_COMBINATIONS = 20_000;
    private static final int MAX_VAR_ARGS = 2;
    private static final int TRAIT_CHAR_TYPE = 2;
    private static final int TRAIT_CONSTANT = 1;
    private static final int TRAIT_MULTI_CHAR = 4;
    private static final int TRAIT_NULL_CHAR_ORDERED = 16;
    private static final int TRAIT_UNTYPED_EMPTY_ARRAY = 8;
    private final IntList argPositions = new IntList();
    private final IntList argTraits = new IntList();
    private final IntList argTypes = new IntList();
    private final ObjList<Function> args = new ObjList<>();
    private final ObjList<Arg> castTargets = new ObjList<>();
    private final IntList distancesA = new IntList();
    private final IntList distancesB = new IntList();
    private final ObjList<String> failures = new ObjList<>();
    private final TreeSet<String> ruleDifferences = new TreeSet<>();
    private final ObjList<Arg> typedArgs = new ObjList<>();
    private int compared;
    private FunctionResolver resolver;

    @Test
    public void testComparisonAliasesBindMirrorOverload() throws Exception {
        // a != b and a <> b must bind the negation of the overload a = b binds, and a >= b, b > a and b <= a the
        // overload a < b binds, with every argument converted to the same type. Otherwise the alias compares the
        // values at another type than its base and can return another result: STRING >= BYTE compared as LONG while
        // STRING < BYTE compared as INT, and IPv4 != VARCHAR parsed the VARCHAR as IPv4 while IPv4 = VARCHAR compared
        // the text.
        assertMemoryLeak(() -> {
            resolver = new FunctionResolver(configuration, engine.getFunctionFactoryCache());
            final ArrayConstant emptyArray = ArrayConstant.emptyUntyped(1);
            final TreeSet<String> aliasDifferences = new TreeSet<>();
            final TreeSet<String> commutedDifferences = new TreeSet<>();
            try {
                buildArgs(emptyArray);
                final ObjList<Arg> options = new ObjList<>();
                options.add(new Arg("NULL", NullConstant.NULL, ColumnType.NULL, true));
                options.addAll(typedArgs);
                for (int i = 0, n = options.size(); i < n; i++) {
                    for (int j = 0; j < n; j++) {
                        final Arg a = options.getQuick(i);
                        final Arg b = options.getQuick(j);
                        checkMirror(aliasDifferences, "=", a, b, "!=", false);
                        checkMirror(aliasDifferences, "=", a, b, "<>", false);
                        checkMirror(aliasDifferences, "<", a, b, ">=", false);
                        checkMirror(aliasDifferences, "<", a, b, ">", true);
                        checkMirror(aliasDifferences, "<", a, b, "<=", true);
                        checkMirror(aliasDifferences, ">", a, b, "<=", false);
                        checkMirror(aliasDifferences, ">", a, b, "<", true);
                        checkMirror(aliasDifferences, ">", a, b, ">=", true);
                        if (j > i) {
                            checkMirror(commutedDifferences, "=", a, b, "=", true);
                        }
                        compared++;
                    }
                }
            } finally {
                Misc.free(emptyArray);
            }
            Assert.assertTrue("compared too few calls: " + compared, compared > 4_000);
            Assert.assertEquals("", join(aliasDifferences));
            // b = a binds the overload a = b binds, with the arguments swapped, except for an untyped NULL beside an
            // argument that one overload takes exactly and another converts. A NULL that comes first makes every
            // overload a partial match and leaves the choice to the score and the function list, a NULL that comes
            // after an exactly matched argument keeps that overload partial while the converting one stays fuzzy.
            // NULL = DATE and DATE = NULL, and NULL = INTERVAL and INTERVAL = NULL, return the same result through
            // either overload. NULL = $1 and $1 = NULL do not when $1 has no type yet: the second binds the cursor
            // overload and fails to compile.
            Assert.assertEquals(
                    """
                            =($1, NULL) binds EqNullCursorFunctionFactory(CURSOR, NULL constant), =(NULL, $1) binds EqDoubleFunctionFactory(DOUBLE, DOUBLE)
                            =(DATE, NULL) binds EqDateFunctionFactory(DATE, DATE), =(NULL, DATE) binds EqSymTimestampFunctionFactory(SYMBOL, TIMESTAMP)
                            =(INTERVAL, NULL) binds EqIntervalFunctionFactory(INTERVAL, INTERVAL), =(NULL, INTERVAL) binds EqStrFunctionFactory(STRING, STRING)
                            =(NULL::DATE, NULL) binds EqDateFunctionFactory(DATE, DATE), =(NULL, NULL::DATE) binds EqSymTimestampFunctionFactory(SYMBOL, TIMESTAMP)
                            =(NULL::INTERVAL, NULL) binds EqIntervalFunctionFactory(INTERVAL, INTERVAL), =(NULL, NULL::INTERVAL) binds EqStrFunctionFactory(STRING, STRING)
                            """,
                    join(commutedDifferences)
            );
        });
    }

    @Test
    public void testNullArgumentTieBreak() throws Exception {
        assertMemoryLeak(() -> {
            resolver = new FunctionResolver(configuration, engine.getFunctionFactoryCache());
            final ArrayConstant emptyArray = ArrayConstant.emptyUntyped(1);
            try {
                buildArgs(emptyArray);
                final List<String> names = new ArrayList<>();
                engine.getFunctionFactoryCache().getFactories().forEach((name, overloads) -> {
                    if (overloads.size() > 1) {
                        names.add(name.toString());
                    }
                });
                names.sort(String::compareTo);
                for (int i = 0, n = names.size(); i < n; i++) {
                    checkName(names.get(i));
                }
            } finally {
                Misc.free(emptyArray);
            }
            final StringBuilder sb = new StringBuilder();
            for (int i = 0, n = failures.size(); i < n; i++) {
                sb.append(failures.getQuick(i)).append('\n');
            }
            Assert.assertEquals("", sb.toString());
            Assert.assertTrue("compared too few calls: " + compared, compared > 100_000);
            final StringBuilder differences = new StringBuilder();
            for (String difference : ruleDifferences) {
                differences.append(difference).append('\n');
            }
            // No production call depends on the rule. The comparisons that did, != and <> against DATE, INTERVAL and
            // SHORT and <=, > and >= against CHAR, are negating or argument-swapping aliases of = and <, which now
            // keep the list order of the overload they derive from, as testComparisonAliasesBindMirrorOverload
            // requires, so both rules bind every alias to the mirror of the overload its base binds.
            Assert.assertEquals("", differences.toString());
        });
    }

    @Test
    public void testTieBreakOfUnlistedOverloads() throws Exception {
        // Overloads that no function list orders tie the same way in either order.
        assertMemoryLeak(() -> {
            final Function charColumn = FunctionResolver.createColumn(0, 0, ColumnType.CHAR, false);
            final Function intColumn = FunctionResolver.createColumn(0, 1, ColumnType.INT, false);
            // (SS) converts NULL at -1 and CHAR at 1, (AA) both at 0: the typed argument decides, whichever side the
            // NULL stands on, so the call binds CHAR as PostgreSQL types an unknown literal from the other operand
            assertTieBreak("tb(AA)", "tb(SS)", "tb(AA)", NullConstant.NULL, charColumn);
            assertTieBreak("tb(AA)", "tb(SS)", "tb(AA)", charColumn, NullConstant.NULL);
            // no argument is typed: the first NULL argument decides, STRING at -1 before INT at 0
            assertTieBreak("tb(SI)", "tb(SI)", "tb(IS)", NullConstant.NULL, NullConstant.NULL);
            // the distances match: the NULL argument prefers the parameter type that can hold NULL
            assertTieBreak("tb(II)", "tb(EI)", "tb(II)", NullConstant.NULL, intColumn);
        });
    }

    private static boolean accepts(int sigTypeWithFlags, Arg arg) {
        if (arg.type == ColumnType.UNDEFINED) {
            return true;
        }
        if (FunctionFactoryDescriptor.isConstant(sigTypeWithFlags) && !arg.isConstant) {
            return false;
        }
        final short sigTag = FunctionFactoryDescriptor.toTypeTag(sigTypeWithFlags);
        final short argTag = ColumnType.tagOf(arg.type);
        if (FunctionFactoryDescriptor.isArray(sigTypeWithFlags) || argTag == ColumnType.ARRAY) {
            return FunctionFactoryDescriptor.isArray(sigTypeWithFlags) && argTag == ColumnType.ARRAY;
        }
        return sigTag == argTag
                || (sigTag == ColumnType.GEOHASH && ColumnType.isGeoHash(arg.type))
                || (sigTag == ColumnType.DECIMAL && ColumnType.isDecimal(arg.type))
                || (sigTag != ColumnType.VAR_ARG && ColumnType.overloadDistance(argTag, sigTag) != ColumnType.OVERLOAD_NONE);
    }

    // Whether two overloads of the arity can convert an untyped NULL argument of the placement at different distances.
    // When they cannot, both rules break every tie of the placement alike.
    private static boolean canNullDistancesDiffer(ObjList<FunctionFactoryDescriptor> overloads, int arity, int mask) {
        for (int p = 0; p < arity; p++) {
            if ((mask & (1 << p)) == 0) {
                continue;
            }
            int seenDistance = Integer.MIN_VALUE;
            for (int i = 0, n = overloads.size(); i < n; i++) {
                final FunctionFactoryDescriptor descriptor = overloads.getQuick(i);
                final int fixed = fixedArgCount(descriptor);
                if (fixed != arity && (!isVarArg(descriptor) || arity < fixed)) {
                    continue;
                }
                // a variadic argument carries no distance
                if (p >= fixed) {
                    continue;
                }
                final int sigTypeWithFlags = descriptor.getArgTypeWithFlags(p);
                if (FunctionFactoryDescriptor.isArray(sigTypeWithFlags)) {
                    continue;
                }
                final int distance = ColumnType.overloadDistance(ColumnType.NULL, FunctionFactoryDescriptor.toTypeTag(sigTypeWithFlags));
                if (distance == ColumnType.OVERLOAD_NONE) {
                    continue;
                }
                if (seenDistance != Integer.MIN_VALUE && seenDistance != distance) {
                    return true;
                }
                seenDistance = distance;
            }
        }
        return false;
    }

    private static String describe(FunctionFactoryDescriptor descriptor) {
        if (descriptor == null) {
            return "none";
        }
        return descriptor.getFactory().getSignature() + ' ' + unwrap(descriptor.getFactory()).getClass().getSimpleName();
    }

    // The factory that computes the call and the parameter type each argument converts to, in the order of the call
    // or, with isSwapped, in the reverse order. A negating or swapping alias computes with the factory it wraps.
    private static String describeBinding(FunctionFactoryDescriptor descriptor, boolean isSwapped) {
        if (descriptor == null) {
            return "none";
        }
        final String first = describeParameter(descriptor.getArgTypeWithFlags(isSwapped ? 1 : 0));
        final String second = describeParameter(descriptor.getArgTypeWithFlags(isSwapped ? 0 : 1));
        return unwrap(descriptor.getFactory()).getClass().getSimpleName() + '(' + first + ", " + second + ')';
    }

    private static String describeParameter(int sigTypeWithFlags) {
        return ColumnType.nameOf(FunctionFactoryDescriptor.toTypeTag(sigTypeWithFlags))
                + (FunctionFactoryDescriptor.isArray(sigTypeWithFlags) ? "[]" : "")
                + (FunctionFactoryDescriptor.isConstant(sigTypeWithFlags) ? " constant" : "");
    }

    private static int firstDistanceDifference(IntList distances, IntList candidateDistances, IntList argTypes, int n, boolean isNull) {
        for (int i = 0; i < n; i++) {
            if ((ColumnType.tagOf(argTypes.getQuick(i)) == ColumnType.NULL) == isNull) {
                final int difference = distances.getQuick(i) - candidateDistances.getQuick(i);
                if (difference != 0) {
                    return difference;
                }
            }
        }
        return 0;
    }

    private static int fixedArgCount(FunctionFactoryDescriptor descriptor) {
        return isVarArg(descriptor) ? descriptor.getSigArgCount() - 1 : descriptor.getSigArgCount();
    }

    private static boolean isNullCharOrderedAgainstNullOrChar(Function arg, Function other) {
        if (ColumnType.tagOf(arg.getType()) != ColumnType.CHAR || arg.getChar(null) != Numbers.CHAR_NULL) {
            return false;
        }
        final int otherType = other.getType();
        return otherType == ColumnType.NULL || otherType == ColumnType.CHAR;
    }

    // The tie break of OverloadResolver. With isTypedFirst the typed arguments decide before the untyped NULL ones,
    // as they do now; without it the first argument whose distances differ decides, as it did before.
    private static boolean isPreferredOnTie(
            FunctionFactoryDescriptor descriptor,
            IntList distances,
            FunctionFactoryDescriptor candidate,
            IntList candidateDistances,
            IntList argTypes,
            boolean isTypedFirst
    ) {
        if (descriptor.isListed() || candidate.isListed()) {
            return false;
        }
        final int n = Math.min(distances.size(), candidateDistances.size());
        if (isTypedFirst) {
            final int typedDifference = firstDistanceDifference(distances, candidateDistances, argTypes, n, false);
            if (typedDifference != 0) {
                return typedDifference < 0;
            }
            final int nullDifference = firstDistanceDifference(distances, candidateDistances, argTypes, n, true);
            if (nullDifference != 0) {
                return nullDifference < 0;
            }
        } else {
            for (int i = 0; i < n; i++) {
                final int distance = distances.getQuick(i);
                final int candidateDistance = candidateDistances.getQuick(i);
                if (distance != candidateDistance) {
                    return distance < candidateDistance;
                }
            }
        }
        for (int i = 0; i < n; i++) {
            if (ColumnType.tagOf(argTypes.getQuick(i)) == ColumnType.NULL) {
                final boolean hasNoNull = ColumnType.isNoNullSentinelFixedType(
                        FunctionFactoryDescriptor.toTypeTag(descriptor.getArgTypeWithFlags(i)));
                final boolean candidateHasNoNull = ColumnType.isNoNullSentinelFixedType(
                        FunctionFactoryDescriptor.toTypeTag(candidate.getArgTypeWithFlags(i)));
                if (hasNoNull != candidateHasNoNull) {
                    return candidateHasNoNull;
                }
            }
        }
        return false;
    }

    private static boolean isVarArg(FunctionFactoryDescriptor descriptor) {
        final int sigArgCount = descriptor.getSigArgCount();
        return sigArgCount > 0
                && FunctionFactoryDescriptor.toTypeTag(descriptor.getArgTypeWithFlags(sigArgCount - 1)) == ColumnType.VAR_ARG;
    }

    private static String join(TreeSet<String> lines) {
        final StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(line).append('\n');
        }
        return sb.toString();
    }

    private static int mergeWithExactMatch(int match) {
        return match == MATCH_NO_MATCH ? MATCH_EXACT_MATCH
                : match == MATCH_FUZZY_MATCH ? MATCH_PARTIAL_MATCH
                  : match;
    }

    private static String render(String name, ObjList<Arg> call, boolean isWindowContext) {
        final StringBuilder sb = new StringBuilder(name).append('(');
        for (int i = 0, n = call.size(); i < n; i++) {
            sb.append(i > 0 ? ", " : "").append(call.getQuick(i).label);
        }
        sb.append(')');
        if (isWindowContext) {
            sb.append(" OVER ()");
        }
        return sb.toString();
    }

    // A copy of OverloadResolver.resolve that breaks ties by either rule.
    private static FunctionFactoryDescriptor resolve(
            ObjList<FunctionFactoryDescriptor> overloads,
            IntList argTypes,
            IntList argTraits,
            boolean isCast,
            boolean isWindowContext,
            IntList distancesA,
            IntList distancesB,
            boolean isTypedFirst
    ) {
        final int argCount = argTypes.size();
        boolean hasLong256Argument = false;
        for (int i = 0; i < argCount; i++) {
            hasLong256Argument |= ColumnType.tagOf(argTypes.getQuick(i)) == ColumnType.LONG256;
        }
        IntList distances = distancesA;
        IntList candidateDistances = distancesB;
        FunctionFactoryDescriptor candidate = null;
        boolean candidateSigVarArg = true;
        int candidateSigArgTypeScore = -1;
        int bestMatch = MATCH_NO_MATCH;

        for (int i = 0, n = overloads.size(); i < n; i++) {
            final FunctionFactoryDescriptor descriptor = overloads.getQuick(i);
            final FunctionFactory factory = descriptor.getFactory();
            final boolean sigVarArg = isVarArg(descriptor);
            final int sigArgCount = fixedArgCount(descriptor);

            if (argCount == 0 && sigArgCount == 0) {
                if (factory.isWindow() == isWindowContext || n == 1) {
                    return descriptor;
                }
                continue;
            }

            if (sigVarArg && argCount != sigArgCount) {
                if (argCount < sigArgCount) {
                    continue;
                }
                boolean variadicTypeAreAllUndefinedVariables = true;
                for (int argIdx = sigArgCount; argIdx < argCount; argIdx++) {
                    if (argTypes.getQuick(argIdx) != ColumnType.UNDEFINED) {
                        variadicTypeAreAllUndefinedVariables = false;
                        break;
                    }
                }
                if (variadicTypeAreAllUndefinedVariables && !factory.variadicTypeSupportUndefinedBindVariables(argCount)) {
                    continue;
                }
            }

            if (sigArgCount == argCount || (sigVarArg && argCount >= sigArgCount)) {
                int match = sigArgCount == 0 ? MATCH_EXACT_MATCH : MATCH_NO_MATCH;
                int sigArgTypeScore = 0;
                distances.setAll(sigArgCount, 0);
                for (int argIdx = 0; argIdx < sigArgCount; argIdx++) {
                    final int argTraitBits = argTraits.getQuick(argIdx);
                    final boolean isArgConstant = (argTraitBits & TRAIT_CONSTANT) != 0;
                    final int sigArgTypeWithFlags = descriptor.getArgTypeWithFlags(argIdx);

                    if (FunctionFactoryDescriptor.isConstant(sigArgTypeWithFlags) && !isArgConstant) {
                        match = MATCH_NO_MATCH;
                        break;
                    }

                    final int argType = argTypes.getQuick(argIdx);
                    final short argTypeTag = ColumnType.tagOf(argType);
                    final short sigArgTypeTag = FunctionFactoryDescriptor.toTypeTag(sigArgTypeWithFlags);

                    final boolean sigIsArray = FunctionFactoryDescriptor.isArray(sigArgTypeWithFlags);
                    final boolean argIsArray = argTypeTag == ColumnType.ARRAY;
                    final boolean argIsStringArray = argTypeTag == ColumnType.ARRAY_STRING;
                    final boolean sigIsStringArray = sigArgTypeTag == ColumnType.ARRAY_STRING;
                    if (sigIsArray != argIsArray || sigIsStringArray != argIsStringArray) {
                        if (argType != ColumnType.UNDEFINED) {
                            match = MATCH_NO_MATCH;
                            break;
                        }
                    }
                    if (argIsStringArray) {
                        match = mergeWithExactMatch(match);
                        continue;
                    }
                    if (argIsArray) {
                        if (sigArgTypeTag == ColumnType.decodeArrayElementType(argType)
                                || (sigArgTypeTag == ColumnType.DOUBLE && (argTraitBits & TRAIT_UNTYPED_EMPTY_ARRAY) != 0)) {
                            match = mergeWithExactMatch(match);
                            continue;
                        }
                        match = MATCH_NO_MATCH;
                        break;
                    }

                    if (sigArgTypeTag == argTypeTag ||
                            (argTypeTag == ColumnType.CHAR &&
                                    sigArgTypeTag == ColumnType.STRING &&
                                    factory.supportImplicitCastCharToStr() &&
                                    isArgConstant &&
                                    (argTraitBits & (TRAIT_CHAR_TYPE | TRAIT_NULL_CHAR_ORDERED)) == 0) ||
                            (sigArgTypeTag == ColumnType.GEOHASH && ColumnType.isGeoHash(argType)) ||
                            (sigArgTypeTag == ColumnType.DECIMAL && ColumnType.isDecimal(argType))) {
                        match = mergeWithExactMatch(match);
                        continue;
                    }

                    boolean overloadPossible = false;
                    if (argIdx != 1 || !isCast) {
                        int overloadDistance = ColumnType.overloadDistance(argTypeTag, sigArgTypeTag);

                        if (argTypeTag == ColumnType.STRING && sigArgTypeTag == ColumnType.CHAR) {
                            if (isArgConstant) {
                                if ((argTraitBits & TRAIT_MULTI_CHAR) != 0) {
                                    overloadDistance = ColumnType.OVERLOAD_NONE;
                                }
                            } else {
                                overloadDistance = 2 * overloadDistance;
                            }
                        } else if (argTypeTag == ColumnType.CHAR && sigArgTypeTag == ColumnType.STRING && !factory.supportImplicitCastCharToStr()) {
                            overloadDistance = ColumnType.OVERLOAD_NONE;
                        } else if (sigArgTypeTag == ColumnType.LONG256 && argTypeTag != ColumnType.NULL && !hasLong256Argument) {
                            overloadDistance = ColumnType.OVERLOAD_NONE;
                        }

                        sigArgTypeScore += overloadDistance;
                        distances.setQuick(argIdx, overloadDistance);
                        overloadPossible = overloadDistance != ColumnType.OVERLOAD_NONE;
                        overloadPossible |= argType == ColumnType.UNDEFINED;
                    }
                    if (overloadPossible) {
                        switch (match) {
                            case MATCH_NO_MATCH:
                                match = argTypeTag == ColumnType.NULL ? MATCH_PARTIAL_MATCH : MATCH_FUZZY_MATCH;
                                break;
                            case MATCH_EXACT_MATCH:
                                match = MATCH_PARTIAL_MATCH;
                                break;
                            default:
                                break;
                        }
                    } else {
                        match = MATCH_NO_MATCH;
                        break;
                    }
                }

                if (match == MATCH_NO_MATCH) {
                    continue;
                }

                if (isWindowContext != factory.isWindow()) {
                    match = MATCH_FUZZY_MATCH;
                    sigArgTypeScore += 20;
                } else if (factory.isWindow()) {
                    sigArgTypeScore -= 20;
                }

                if (match == MATCH_EXACT_MATCH || match >= bestMatch) {
                    if (match == bestMatch && sigVarArg && !candidateSigVarArg) {
                        continue;
                    }

                    if (match != MATCH_EXACT_MATCH) {
                        if (candidateSigArgTypeScore > sigArgTypeScore || bestMatch < match
                                || (candidateSigArgTypeScore == sigArgTypeScore
                                && isPreferredOnTie(descriptor, distances, candidate, candidateDistances, argTypes, isTypedFirst))) {
                            candidate = descriptor;
                            candidateSigVarArg = sigVarArg;
                            candidateSigArgTypeScore = sigArgTypeScore;
                            final IntList swap = candidateDistances;
                            candidateDistances = distances;
                            distances = swap;
                        }
                        bestMatch = match;
                    } else {
                        candidate = descriptor;
                        candidateSigVarArg = sigVarArg;
                        bestMatch = match;
                        if (isWindowContext == factory.isWindow()) {
                            break;
                        }
                    }
                }
            }
        }
        return candidate;
    }

    private static FunctionFactory unwrap(FunctionFactory factory) {
        while (true) {
            if (factory instanceof NegatingFunctionFactory negating) {
                factory = negating.getDelegate();
            } else if (factory instanceof ArgSwappingFunctionFactory swapping) {
                factory = swapping.getDelegate();
            } else {
                return factory;
            }
        }
    }

    // Keeps one argument of each kind the resolver tells apart: type, constness and trait.
    private void addArg(ObjList<Arg> target, HashSet<String> keys, String label, Function function) {
        final int type = function.getType();
        final boolean isConstant = function.isConstant();
        final String key = type + ":" + isConstant
                + ':' + (function == CharTypeConstant.INSTANCE)
                + ':' + (isConstant && ColumnType.tagOf(type) == ColumnType.CHAR && function.getChar(null) == Numbers.CHAR_NULL)
                + ':' + (isConstant && ColumnType.tagOf(type) == ColumnType.STRING && function.getStrLen(null) > 1)
                + ':' + (function instanceof ArrayConstant && ColumnType.decodeArrayElementType(type) == ColumnType.UNDEFINED);
        if (keys.add(key)) {
            target.add(new Arg(label, function, type, isConstant));
        }
    }

    // Resolves tb(a, b) over the two overloads listed in both orders and asserts both bind the expected one.
    private void assertTieBreak(String expected, String signatureA, String signatureB, Function a, Function b) throws SqlException {
        final ObjList<Function> callArgs = new ObjList<>();
        final IntList callPositions = new IntList();
        for (int order = 0; order < 2; order++) {
            final List<FunctionFactory> factories = new ArrayList<>();
            factories.add(new TieBreakFactory(order == 0 ? signatureA : signatureB));
            factories.add(new TieBreakFactory(order == 0 ? signatureB : signatureA));
            final FunctionResolver tieResolver = new FunctionResolver(configuration, new FunctionFactoryCache(configuration, factories));
            callArgs.clear();
            callArgs.add(a);
            callArgs.add(b);
            callPositions.clear();
            callPositions.add(0);
            callPositions.add(0);
            final FunctionFactoryDescriptor selected = tieResolver.selectOverload("tb", 0, callArgs, callPositions, sqlExecutionContext);
            Assert.assertEquals(signatureA + ", " + signatureB + " in order " + order, expected, selected.getFactory().getSignature());
        }
    }

    private void buildArgs(ArrayConstant emptyArray) throws SqlException {
        final int[] columnTypes = {
                ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT, ColumnType.CHAR, ColumnType.INT, ColumnType.LONG,
                ColumnType.DATE, ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO, ColumnType.FLOAT, ColumnType.DOUBLE,
                ColumnType.STRING, ColumnType.SYMBOL, ColumnType.LONG256, ColumnType.LONG128,
                ColumnType.getGeoHashTypeWithBits(5), ColumnType.getGeoHashTypeWithBits(10),
                ColumnType.getGeoHashTypeWithBits(20), ColumnType.getGeoHashTypeWithBits(40), ColumnType.BINARY,
                ColumnType.UUID, ColumnType.IPv4, ColumnType.VARCHAR, ColumnType.getDecimalType(2, 1),
                ColumnType.getDecimalType(4, 1), ColumnType.getDecimalType(9, 2), ColumnType.getDecimalType(18, 2),
                ColumnType.getDecimalType(38, 2), ColumnType.getDecimalType(76, 2), ColumnType.INTERVAL_TIMESTAMP_MICRO,
                ColumnType.encodeArrayType(ColumnType.DOUBLE, 1), ColumnType.encodeArrayType(ColumnType.DOUBLE, 2)
        };
        final HashSet<String> keys = new HashSet<>();
        for (int type : columnTypes) {
            addArg(typedArgs, keys, ColumnType.nameOf(type), FunctionResolver.createColumn(0, 0, type, false));
        }
        final String[] literals = {"'a'", "'abc'", "1", "3_000_000_000", "1.5", "true", "#sp", "1.5m"};
        for (String literal : literals) {
            addArg(typedArgs, keys, literal, resolver.createConstant(0, literal, sqlExecutionContext));
        }
        for (int type : columnTypes) {
            addArg(typedArgs, keys, "NULL::" + ColumnType.nameOf(type), Constants.getNullConstant(type));
        }
        addArg(typedArgs, keys, "ARRAY[]", emptyArray);
        bindVariableService.clear();
        addArg(typedArgs, keys, "$1", resolver.createIndexedBindVariable(0, 0, sqlExecutionContext));
        final HashSet<String> castKeys = new HashSet<>();
        for (int type : columnTypes) {
            final Function typeConstant = ColumnType.tagOf(type) == ColumnType.INTERVAL || ColumnType.tagOf(type) == ColumnType.LONG128
                    ? null : Constants.getTypeConstant(type);
            if (typeConstant != null) {
                addArg(castTargets, castKeys, ColumnType.nameOf(type), typeConstant);
            }
        }
    }

    private void checkCall(String name, ObjList<FunctionFactoryDescriptor> overloads, ObjList<Arg> call, boolean isWindowContext) {
        final int argCount = call.size();
        final boolean isOrdering = argCount == 2 && FunctionResolver.isOrderingOperator(name);
        args.clear();
        argPositions.clear();
        argTypes.clear();
        argTraits.clear();
        for (int i = 0; i < argCount; i++) {
            final Arg arg = call.getQuick(i);
            args.add(arg.function);
            argPositions.add(0);
            argTypes.add(arg.type);
        }
        for (int i = 0; i < argCount; i++) {
            final Function arg = args.getQuick(i);
            final int type = argTypes.getQuick(i);
            final boolean isConstant = arg.isConstant();
            argTraits.add((isConstant ? TRAIT_CONSTANT : 0)
                    | (arg == CharTypeConstant.INSTANCE ? TRAIT_CHAR_TYPE : 0)
                    | (isOrdering && isConstant && isNullCharOrderedAgainstNullOrChar(arg, args.getQuick(1 - i)) ? TRAIT_NULL_CHAR_ORDERED : 0)
                    | (arg instanceof ArrayConstant && ColumnType.decodeArrayElementType(type) == ColumnType.UNDEFINED ? TRAIT_UNTYPED_EMPTY_ARRAY : 0)
                    | (isConstant && ColumnType.tagOf(type) == ColumnType.STRING && arg.getStrLen(null) > 1 ? TRAIT_MULTI_CHAR : 0));
        }
        final boolean isCast = name.equalsIgnoreCase("cast");
        final FunctionFactoryDescriptor before = resolve(overloads, argTypes, argTraits, isCast, isWindowContext, distancesA, distancesB, false);
        final FunctionFactoryDescriptor now = resolve(overloads, argTypes, argTraits, isCast, isWindowContext, distancesA, distancesB, true);
        FunctionFactoryDescriptor actual;
        try {
            actual = resolver.selectOverload(name, 0, args, argPositions, sqlExecutionContext);
        } catch (SqlException e) {
            actual = null;
        }
        compared++;
        if (actual != now) {
            failures.add(render(name, call, isWindowContext) + ": resolver " + describe(actual) + ", copy " + describe(now));
        }
        if (before != now) {
            ruleDifferences.add(render(name, call, isWindowContext) + ": before " + describe(before) + ", now " + describe(now));
        }
    }

    // Records the alias call when it binds another overload than base(a, b), or converts an argument to another type.
    // With isSwapped the alias takes the arguments in the reverse order.
    private void checkMirror(TreeSet<String> differences, String base, Arg a, Arg b, String alias, boolean isSwapped) {
        final FunctionFactoryDescriptor baseOverload = select(base, a, b);
        final Arg x = isSwapped ? b : a;
        final Arg y = isSwapped ? a : b;
        final FunctionFactoryDescriptor aliasOverload = select(alias, x, y);
        if (!describeBinding(aliasOverload, isSwapped).equals(describeBinding(baseOverload, false))) {
            differences.add(alias + '(' + x.label + ", " + y.label + ") binds " + describeBinding(aliasOverload, false)
                    + ", " + base + '(' + a.label + ", " + b.label + ") binds " + describeBinding(baseOverload, false));
        }
    }

    private void checkName(String name) throws SqlException {
        final ObjList<FunctionFactoryDescriptor> overloads = engine.getFunctionFactoryCache().getOverloadList(name);
        boolean hasWindow = false;
        final TreeSet<Integer> arities = new TreeSet<>();
        for (int i = 0, n = overloads.size(); i < n; i++) {
            final FunctionFactoryDescriptor descriptor = overloads.getQuick(i);
            hasWindow |= descriptor.getFactory().isWindow();
            final int fixed = fixedArgCount(descriptor);
            if (isVarArg(descriptor)) {
                for (int k = fixed; k <= fixed + MAX_VAR_ARGS; k++) {
                    arities.add(k);
                }
            } else {
                arities.add(fixed);
            }
        }
        for (int arity : arities) {
            if (arity < 2) {
                continue;
            }
            final ObjList<ObjList<Arg>> options = new ObjList<>();
            for (int p = 0; p < arity; p++) {
                options.add(optionsAt(name, overloads, arity, p));
            }
            for (int mask = 1, maskEnd = 1 << arity; mask < maskEnd - 1; mask++) {
                final int firstNull = Integer.numberOfTrailingZeros(mask);
                final int lastTyped = 31 - Integer.numberOfLeadingZeros(~mask & (maskEnd - 1));
                if (firstNull < lastTyped) {
                    checkPlacement(name, overloads, options, arity, mask, false);
                    if (hasWindow) {
                        checkPlacement(name, overloads, options, arity, mask, true);
                    }
                }
            }
        }
    }

    private void checkPlacement(
            String name,
            ObjList<FunctionFactoryDescriptor> overloads,
            ObjList<ObjList<Arg>> options,
            int arity,
            int mask,
            boolean isWindowContext
    ) throws SqlException {
        final ObjList<Arg> nullOnly = new ObjList<>();
        nullOnly.add(new Arg("NULL", NullConstant.NULL, ColumnType.NULL, true));
        final ObjList<ObjList<Arg>> positions = new ObjList<>();
        long product = 1;
        for (int p = 0; p < arity; p++) {
            final ObjList<Arg> at = (mask & (1 << p)) != 0 ? nullOnly : options.getQuick(p);
            positions.add(at);
            product *= at.size();
        }
        if (product == 0) {
            return;
        }
        if (product > MAX_COMBINATIONS && canNullDistancesDiffer(overloads, arity, mask)) {
            failures.add(name + " arity " + arity + " placement " + mask + ": " + product + " calls are too many to walk");
            return;
        }
        if (isWindowContext) {
            sqlExecutionContext.configureWindowContext(null, null, new ArrayColumnTypes(), true,
                    RecordCursorFactory.SCAN_DIRECTION_FORWARD, 0, WindowExpression.FRAMING_RANGE,
                    Long.MIN_VALUE, (char) 0, 0, 0, 0, (char) 0, 0, 0, WindowExpression.EXCLUDE_NO_OTHERS, 0,
                    1, ColumnType.TIMESTAMP_MICRO, false, 0);
        }
        try {
            final ObjList<Arg> call = new ObjList<>();
            if (product <= MAX_COMBINATIONS) {
                enumerate(name, overloads, positions, 0, call, isWindowContext);
            } else {
                // the rules agree on every call of the placement, a sample still checks the copy against the resolver
                final long seed0 = name.hashCode() * 31L + mask;
                final long seed1 = arity;
                LOG.info().$("sampling [name=").$(name).$(", arity=").$(arity).$(", mask=").$(mask)
                        .$(", calls=").$(product).$(", seed0=").$(seed0).$(", seed1=").$(seed1).I$();
                final Rnd rnd = new Rnd(seed0, seed1);
                for (int s = 0; s < MAX_COMBINATIONS; s++) {
                    call.clear();
                    for (int p = 0; p < arity; p++) {
                        final ObjList<Arg> at = positions.getQuick(p);
                        call.add(at.getQuick(rnd.nextInt(at.size())));
                    }
                    checkCall(name, overloads, call, isWindowContext);
                }
            }
        } finally {
            if (isWindowContext) {
                sqlExecutionContext.clearWindowContext();
            }
        }
    }

    private void enumerate(
            String name,
            ObjList<FunctionFactoryDescriptor> overloads,
            ObjList<ObjList<Arg>> positions,
            int p,
            ObjList<Arg> call,
            boolean isWindowContext
    ) {
        if (p == positions.size()) {
            checkCall(name, overloads, call, isWindowContext);
            return;
        }
        final ObjList<Arg> at = positions.getQuick(p);
        for (int i = 0, n = at.size(); i < n; i++) {
            call.add(at.getQuick(i));
            enumerate(name, overloads, positions, p + 1, call, isWindowContext);
            call.setPos(p);
        }
    }

    // The typed arguments some overload of the arity accepts at the position.
    private ObjList<Arg> optionsAt(String name, ObjList<FunctionFactoryDescriptor> overloads, int arity, int p) {
        final ObjList<Arg> source = name.equalsIgnoreCase("cast") && p == 1 ? castTargets : typedArgs;
        final ObjList<Arg> options = new ObjList<>();
        for (int i = 0, n = source.size(); i < n; i++) {
            final Arg arg = source.getQuick(i);
            for (int j = 0, m = overloads.size(); j < m; j++) {
                final FunctionFactoryDescriptor descriptor = overloads.getQuick(j);
                final int fixed = fixedArgCount(descriptor);
                if (fixed != arity && (!isVarArg(descriptor) || arity < fixed)) {
                    continue;
                }
                if (p >= fixed || accepts(descriptor.getArgTypeWithFlags(p), arg)) {
                    options.add(arg);
                    break;
                }
            }
        }
        return options;
    }

    // The overload the production resolver binds name(x, y) to, or null when none accepts the arguments.
    private FunctionFactoryDescriptor select(String name, Arg x, Arg y) {
        args.clear();
        args.add(x.function);
        args.add(y.function);
        argPositions.clear();
        argPositions.add(0);
        argPositions.add(0);
        try {
            return resolver.selectOverload(name, 0, args, argPositions, sqlExecutionContext);
        } catch (SqlException e) {
            return null;
        }
    }

    private record Arg(String label, Function function, int type, boolean isConstant) {
    }

    private static class TieBreakFactory implements FunctionFactory {
        private final String signature;

        private TieBreakFactory(String signature) {
            this.signature = signature;
        }

        @Override
        public String getSignature() {
            return signature;
        }

        @Override
        public Function newInstance(
                int position,
                ObjList<Function> args,
                IntList argPositions,
                CairoConfiguration configuration,
                SqlExecutionContext sqlExecutionContext
        ) {
            throw new UnsupportedOperationException();
        }
    }
}
