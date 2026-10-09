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

import io.questdb.cairo.ColumnType;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * Selects the overload a call binds to from argument type descriptors alone; it never builds or evaluates a function.
 */
final class OverloadResolver {
    /**
     * The argument is the CHAR type constant of {@code cast(x as char)}.
     */
    static final int ARG_CHAR_TYPE = 2;
    /**
     * The argument is a compile-time constant.
     */
    static final int ARG_CONSTANT = 1;
    /**
     * The argument is a constant STRING longer than one character.
     */
    static final int ARG_MULTI_CHAR = 4;
    private static final int MATCH_EXACT_MATCH = 3;
    private static final int MATCH_FUZZY_MATCH = 1;
    // order of values matters here, partial match must have greater value than fuzzy match
    private static final int MATCH_NO_MATCH = 0;
    private static final int MATCH_PARTIAL_MATCH = 2;

    private OverloadResolver() {
    }

    private static int mergeWithExactMatch(int match) {
        return match == MATCH_NO_MATCH ? MATCH_EXACT_MATCH
                : match == MATCH_FUZZY_MATCH ? MATCH_PARTIAL_MATCH
                  : match;
    }

    /**
     * Returns the overload the call binds to, or null when no signature accepts the arguments.
     *
     * @param argTypes  the argument types, UNDEFINED for an untyped bind variable
     * @param argTraits the {@code ARG_*} bits of each argument
     */
    static FunctionFactoryDescriptor resolve(
            ObjList<FunctionFactoryDescriptor> overloads,
            IntList argTypes,
            IntList argTraits,
            boolean isCast,
            boolean isWindowContext
    ) {
        final int argCount = argTypes.size();
        FunctionFactoryDescriptor candidate = null;
        boolean candidateSigVarArg = true;
        int candidateSigArgTypeScore = -1;
        int bestMatch = MATCH_NO_MATCH;

        for (int i = 0, n = overloads.size(); i < n; i++) {
            final FunctionFactoryDescriptor descriptor = overloads.getQuick(i);
            final FunctionFactory factory = descriptor.getFactory();
            int sigArgCount = descriptor.getSigArgCount();
            final boolean sigVarArg = sigArgCount > 0
                    && FunctionFactoryDescriptor.toTypeTag(descriptor.getArgTypeWithFlags(sigArgCount - 1)) == ColumnType.VAR_ARG;
            if (sigVarArg) {
                sigArgCount--;
            }

            // this is no-arg function, match right away
            if (argCount == 0 && sigArgCount == 0) {
                if (factory.isWindow() == isWindowContext || n == 1) {
                    return descriptor;
                }
                continue;
            }

            // Resolves ambiguity between variadic functions (e.g. InSymbolFunctionFactory "in(KV)") and array
            // functions (e.g. InSymbolVarcharArrayFunctionFactory "in(KØ[])") when bind variables have undefined
            // types: for "symbol_col IN ($1)" the variadic version would otherwise match exactly, while the single
            // undefined argument should match "in(KØ[])". The factory decides whether all-undefined variadic
            // arguments still select it.
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
                for (int argIdx = 0; argIdx < sigArgCount; argIdx++) {
                    final int argTraitBits = argTraits.getQuick(argIdx);
                    final boolean isArgConstant = (argTraitBits & ARG_CONSTANT) != 0;
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
                    if (argIsStringArray) { // given the above checks, implies that sigIsStringArray is also true
                        match = mergeWithExactMatch(match);
                        continue;
                    }
                    if (argIsArray) { // given the above checks, implies that sigIsArray is also true
                        if (sigArgTypeTag == ColumnType.decodeArrayElementType(argType)) {
                            match = mergeWithExactMatch(match);
                            continue;
                        }
                        match = MATCH_NO_MATCH;
                        break;
                    }

                    if (sigArgTypeTag == argTypeTag ||
                            (argTypeTag == ColumnType.CHAR &&              // 'a' could also be a string literal, so it should count as proper match
                                    sigArgTypeTag == ColumnType.STRING &&  // for both string and char, otherwise ? > 'a' matches char function even though
                                    factory.supportImplicitCastCharToStr() &&
                                    isArgConstant && // bind variable parameter might be a string and throw error during execution.
                                    (argTraitBits & ARG_CHAR_TYPE) == 0) ||   // Ignore type constant to keep cast(X as char) working
                            (sigArgTypeTag == ColumnType.GEOHASH && ColumnType.isGeoHash(argType)) ||
                            (sigArgTypeTag == ColumnType.DECIMAL && ColumnType.isDecimal(argType))) {
                        match = mergeWithExactMatch(match);
                        continue;
                    }

                    boolean overloadPossible = false;
                    // The output of cast(), always its 2nd argument, must be the exact type the user specified,
                    // so it never takes an overload; every other argument may be implicitly cast.
                    if (argIdx != 1 || !isCast) {
                        int overloadDistance = ColumnType.overloadDistance(argTypeTag, sigArgTypeTag); // NULL to any is 0

                        if (argTypeTag == ColumnType.STRING && sigArgTypeTag == ColumnType.CHAR) {
                            if (isArgConstant) {
                                // string longer than 1 char can't be cast to char implicitly
                                if ((argTraitBits & ARG_MULTI_CHAR) != 0) {
                                    overloadDistance = ColumnType.OVERLOAD_NONE;
                                }
                            } else {
                                // prefer CHAR -> STRING to STRING -> CHAR conversion
                                overloadDistance = 2 * overloadDistance;
                            }
                        } else if (argTypeTag == ColumnType.CHAR && sigArgTypeTag == ColumnType.STRING && !factory.supportImplicitCastCharToStr()) {
                            overloadDistance = ColumnType.OVERLOAD_NONE;
                        }

                        sigArgTypeScore += overloadDistance;
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
                } else if (factory.isWindow()) { // make windowFunction high priority when isWindowContext
                    sigArgTypeScore -= 20;
                }

                if (match == MATCH_EXACT_MATCH || match >= bestMatch) {
                    // when match is the same, prefer non-var-arg functions
                    if (match == bestMatch && sigVarArg && !candidateSigVarArg) {
                        continue;
                    }

                    if (match != MATCH_EXACT_MATCH) {
                        if (candidateSigArgTypeScore > sigArgTypeScore || bestMatch < match) {
                            candidate = descriptor;
                            candidateSigVarArg = sigVarArg;
                            candidateSigArgTypeScore = sigArgTypeScore;
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

}
