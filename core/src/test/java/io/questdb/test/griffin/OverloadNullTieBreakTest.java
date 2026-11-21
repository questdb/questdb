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
 * {@code testTieGroupsAreEntirelyListedOrUnlisted} walks every call of every (name, arity) group, untyped NULL at any
 * position, and pins the groups in which the tie break decides between overloads of a group that mixes listed and
 * unlisted overloads.
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
    private static final long MAX_TIE_CALLS = 1_000_000;
    private static final int MAX_VAR_ARGS = 2;
    // The groups in which overloads tie that mix listed and unlisted overloads, each with the reason it stays mixed.
    // Every listed overload precedes every unlisted one, so the list order binds the listed overload of a tie between
    // the two kinds: NULL = NULL binds STRING rather than SYMBOL, and abs(NULL) INT rather than DECIMAL. An untyped
    // NULL converts to DECIMAL as to the other numeric types, and an untyped bind variable to DOUBLE[] as to DOUBLE,
    // so either ties an unlisted overload of such a type with a listed one.
    private static final String[][] MIXED_TIE_GROUPS = {
            {"!=/2: unlisted EqDoubleArrayFunctionFactory, EqDecimalFunctionFactory, EqSymFunctionFactory; listed and unlisted overloads tie",
                    "negating alias of =, which keeps the status of the = overload it negates, see =/2"},
            {"%/2: unlisted RemDecimalFunctionFactory; listed and unlisted overloads tie",
                    "the DECIMAL overload has no list entry; NULL % NULL ties it with the listed INT overload"},
            {"*/2: unlisted DoubleArrayMultiplyFunctionFactory, DoubleArrayMultiplyScalarFunctionFactory, MulDecimalFunctionFactory; listed and unlisted overloads tie",
                    "the DOUBLE[] and DECIMAL overloads have no list entry; NULL * NULL ties DECIMAL with the listed numeric overloads"},
            {"+/2: unlisted DoubleArrayAddFunctionFactory, DoubleArrayAddScalarFunctionFactory, AddDecimalFunctionFactory; listed and unlisted overloads tie",
                    "the DOUBLE[] and DECIMAL overloads have no list entry; NULL + NULL ties DECIMAL with the listed numeric overloads"},
            {"-/1: unlisted DoubleNegArrayFunctionFactory, NegDecimalFunctionFactory, NegNullFunctionFactory; listed and unlisted overloads tie",
                    "the DOUBLE[], DECIMAL and NULL negations have no list entry; -$1 ties DOUBLE[] with the listed DOUBLE negation"},
            {"-/2: unlisted DoubleArraySubtractFunctionFactory, DoubleArraySubtractScalarFunctionFactory, DoubleScalarSubtractArrayFunctionFactory, SubDecimalFunctionFactory; listed and unlisted overloads tie",
                    "the DOUBLE[] and DECIMAL overloads have no list entry; NULL - $1 ties DOUBLE[] with the listed DOUBLE overload"},
            {"//2: unlisted DoubleArrayDivFunctionFactory, DoubleArrayDivScalarFunctionFactory, DoubleScalarDivArrayFunctionFactory, DivDecimalFunctionFactory; listed and unlisted overloads tie",
                    "the DOUBLE[] and DECIMAL overloads have no list entry; NULL / NULL ties DECIMAL with the listed numeric overloads"},
            {"</2: unlisted LtDecimalFunctionFactory; only overloads of one kind tie",
                    "the DECIMAL ordering has no list entry but ties no listed one, as NULL < NULL binds the listed STRING ordering"},
            {"<=/2: unlisted LtDecimalFunctionFactory; only overloads of one kind tie",
                    "argument-swapping and negating alias of <, see </2"},
            {"<>/2: unlisted EqDoubleArrayFunctionFactory, EqDecimalFunctionFactory, EqSymFunctionFactory; listed and unlisted overloads tie",
                    "negating alias of =, see =/2"},
            {"=/2: unlisted EqDoubleArrayFunctionFactory, EqDecimalFunctionFactory, EqSymFunctionFactory; listed and unlisted overloads tie",
                    "the DOUBLE[], DECIMAL and SYMBOL = SYMBOL equalities have no list entry; NULL = NULL ties SYMBOL with the listed STRING equality"},
            {">/2: unlisted LtDecimalFunctionFactory; only overloads of one kind tie",
                    "argument-swapping alias of <, see </2"},
            {">=/2: unlisted LtDecimalFunctionFactory; only overloads of one kind tie",
                    "negating alias of <, see </2"},
            {"[]/2: unlisted DoubleArrayAccessFunctionFactory, DoubleArraySliceFunctionFactory; only overloads of one kind tie",
                    "the DOUBLE[] element access and slice have no list entry and tie only with each other, as [](DOUBLE[], NULL)"},
            {"abs/1: unlisted AbsDecimalFunctionFactory; listed and unlisted overloads tie",
                    "the DECIMAL overload has no list entry; abs(NULL) ties it with the listed numeric overloads"},
            {"approx_percentile/2: unlisted MultiApproxPercentileDoubleGroupByDefaultFunctionFactory, MultiApproxPercentileLongGroupByDefaultFunctionFactory; listed and unlisted overloads tie",
                    "the multi-percentile DOUBLE[] overloads have no list entry; a $1 percentile ties them with the listed scalar ones"},
            {"approx_percentile/3: unlisted MultiApproxPercentileDoubleGroupByFunctionFactory, MultiApproxPercentileLongGroupByFunctionFactory; listed and unlisted overloads tie",
                    "the multi-percentile DOUBLE[] overloads have no list entry; a $1 percentile ties them with the listed scalar ones"},
            {"avg/1: unlisted AvgDecimalGroupByFunctionFactory, AvgIntGroupByFunctionFactory, AvgLongGroupByFunctionFactory, AvgShortGroupByFunctionFactory, AvgDecimalWindowFunctionFactory, AvgDoubleWindowFunctionFactory; listed and unlisted overloads tie",
                    "the DECIMAL, INT, LONG and SHORT group by overloads and the window overloads have no list entry; avg(NULL) ties them with the listed DOUBLE one"},
            {"cast/2: unlisted CastDoubleArrayToDoubleArrayFunctionFactory, CastDoubleArrayToStrFunctionFactory, CastDoubleArrayToVarcharFunctionFactory, CastDoubleToDoubleArray, CastStrToDoubleArrayFunctionFactory, CastByteToDecimalFunctionFactory, CastDecimalToByteFunctionFactory, CastDecimalToDecimalFunctionFactory, CastDecimalToDoubleFunctionFactory, CastDecimalToFloatFunctionFactory, CastDecimalToIntFunctionFactory, CastDecimalToLongFunctionFactory, CastDecimalToShortFunctionFactory, CastDecimalToStrFunctionFactory, CastDecimalToVarcharFunctionFactory, CastDoubleToDecimalFunctionFactory, CastFloatToDecimalFunctionFactory, CastGeoHashToStrFunctionFactory, CastGeoHashToVarcharFunctionFactory, CastIntToDecimalFunctionFactory, CastLongToDecimalFunctionFactory, CastShortToDecimalFunctionFactory, CastStrToDecimalFunctionFactory, CastTimestampToTimestampFunctionFactory, CastVarcharToDecimalFunctionFactory; listed and unlisted overloads tie",
                    "the DOUBLE[], DECIMAL, GEOHASH-to-text and TIMESTAMP-to-TIMESTAMP casts have no list entry; cast($1 AS STRING) ties DOUBLE[] with the listed DOUBLE cast"},
            {"ceil/1: unlisted CeilDecimalZeroScaleFunctionFactory; listed and unlisted overloads tie",
                    "the DECIMAL overload has no list entry; ceil(NULL) ties it with the listed DOUBLE one"},
            {"ceiling/1: unlisted CeilingDecimalZeroScaleFunctionFactory; listed and unlisted overloads tie",
                    "the DECIMAL overload has no list entry; ceiling(NULL) ties it with the listed DOUBLE one"},
            {"count/1: unlisted CountDecimalGroupByFunctionFactory, CountUuidGroupByFunctionFactory, CountDecimalWindowFunctionFactory; only overloads of one kind tie",
                    "the DECIMAL and UUID group by overloads and the DECIMAL window overload have no list entry but tie no listed one, as count(NULL) binds the listed STRING overload"},
            {"first/1: unlisted FirstArrayGroupByFunctionFactory, FirstDecimalGroupByFunctionFactory; listed and unlisted overloads tie",
                    "the DOUBLE[] and DECIMAL overloads have no list entry; first($1) ties DOUBLE[] with the listed DOUBLE one"},
            {"first_not_null/1: unlisted FirstNotNullArrayGroupByFunctionFactory, FirstNotNullDecimalGroupByFunctionFactory; listed and unlisted overloads tie",
                    "the DOUBLE[] and DECIMAL overloads have no list entry; first_not_null($1) ties DOUBLE[] with the listed DOUBLE one"},
            {"floor/1: unlisted FloorDecimalZeroScaleFunctionFactory; listed and unlisted overloads tie",
                    "the DECIMAL overload has no list entry; floor(NULL) ties it with the listed DOUBLE one"},
            {"last/1: unlisted LastArrayGroupByFunctionFactory, LastDecimalGroupByFunctionFactory; listed and unlisted overloads tie",
                    "the DOUBLE[] and DECIMAL overloads have no list entry; last($1) ties DOUBLE[] with the listed DOUBLE one"},
            {"last_not_null/1: unlisted LastNotNullArrayGroupByFunctionFactory, LastNotNullDecimalGroupByFunctionFactory; listed and unlisted overloads tie",
                    "the DOUBLE[] and DECIMAL overloads have no list entry; last_not_null($1) ties DOUBLE[] with the listed DOUBLE one"},
            {"max/1: unlisted MaxDecimalGroupByFunctionFactory, MaxShortGroupByFunctionFactory, MaxDateWindowFunctionFactory, MaxDecimalWindowFunctionFactory, MaxDoubleWindowFunctionFactory, MaxLongWindowFunctionFactory, MaxTimestampWindowFunctionFactory; only overloads of one kind tie",
                    "the DECIMAL and SHORT group by overloads and the window overloads have no list entry; only the unlisted window overloads tie with each other, as max(NULL) OVER ()"},
            {"min/1: unlisted MinDecimalGroupByFunctionFactory, MinShortGroupByFunctionFactory, MinDateWindowFunctionFactory, MinDecimalWindowFunctionFactory, MinDoubleWindowFunctionFactory, MinLongWindowFunctionFactory, MinTimestampWindowFunctionFactory; only overloads of one kind tie",
                    "the DECIMAL and SHORT group by overloads and the window overloads have no list entry; only the unlisted window overloads tie with each other, as min(NULL) OVER ()"},
            {"nullif/2: unlisted NullIfDecimalFunctionFactory; only overloads of one kind tie",
                    "the DECIMAL overload has no list entry but ties no listed one, as nullif(NULL, NULL) binds the listed STRING overload"},
            {"percentile_cont/2: unlisted MultiPercentileContDoubleGroupByFunctionFactory, MultiPercentileContDoubleWindowFunctionFactory, PercentileContDoubleWindowFunctionFactory; listed and unlisted overloads tie",
                    "the multi-percentile DOUBLE[] overloads and the window overloads have no list entry; a $1 percentile ties them with the listed scalar one"},
            {"percentile_disc/2: unlisted MultiPercentileDiscDoubleGroupByFunctionFactory, MultiPercentileDiscDoubleWindowFunctionFactory, PercentileDiscLongGroupByFunctionFactory, PercentileDiscLongWindowFunctionFactory; listed and unlisted overloads tie",
                    "the multi-percentile DOUBLE[] and LONG overloads and the window overloads have no list entry; percentile_disc(NULL, NULL) ties LONG with the listed DOUBLE one"},
            {"quantile_cont/2: unlisted MultiQuantileContDoubleGroupByFunctionFactory, MultiQuantileContDoubleWindowFunctionFactory, QuantileContDoubleWindowFunctionFactory; listed and unlisted overloads tie",
                    "the multi-quantile DOUBLE[] overloads and the window overloads have no list entry; a $1 quantile ties them with the listed scalar one"},
            {"quantile_disc/2: unlisted MultiQuantileDiscDoubleGroupByFunctionFactory, MultiQuantileDiscDoubleWindowFunctionFactory, QuantileDiscLongGroupByFunctionFactory, QuantileDiscLongWindowFunctionFactory; listed and unlisted overloads tie",
                    "the multi-quantile DOUBLE[] and LONG overloads and the window overloads have no list entry; quantile_disc(NULL, NULL) ties LONG with the listed DOUBLE one"},
            {"round/1: unlisted RoundDecimalZeroScaleFunctionFactory; listed and unlisted overloads tie",
                    "the DECIMAL overload has no list entry; round(NULL) ties it with the listed DOUBLE one"},
            {"round/2: unlisted DoubleArrayRoundFunctionFactory, RoundDecimalFunctionFactory; listed and unlisted overloads tie",
                    "the DOUBLE[] and DECIMAL overloads have no list entry; round(NULL, NULL) ties DECIMAL with the listed DOUBLE one"},
            {"round_down/2: unlisted RoundDownDecimalFunctionFactory; listed and unlisted overloads tie",
                    "the DECIMAL overload has no list entry; round_down(NULL, NULL) ties it with the listed DOUBLE one"},
            {"round_half_even/2: unlisted RoundHalfEvenDecimalFunctionFactory; listed and unlisted overloads tie",
                    "the DECIMAL overload has no list entry; round_half_even(NULL, NULL) ties it with the listed DOUBLE one"},
            {"round_up/2: unlisted RoundUpDecimalFunctionFactory; listed and unlisted overloads tie",
                    "the DECIMAL overload has no list entry; round_up(NULL, NULL) ties it with the listed DOUBLE one"},
            {"sign/1: unlisted SignDecimalFunctionFactory; listed and unlisted overloads tie",
                    "the DECIMAL overload has no list entry; sign(NULL) ties it with the listed numeric ones"},
            {"sleep/1: unlisted TestSleepFunctionFactory; listed and unlisted overloads tie",
                    "a factory of the test sources, which production does not load"},
            {"sum/1: unlisted SumDecimalGroupByFunctionFactory, SumShortGroupByFunctionFactory, SumDecimalWindowFunctionFactory, SumDoubleWindowFunctionFactory; listed and unlisted overloads tie",
                    "the DECIMAL and SHORT group by overloads and the window overloads have no list entry; sum(NULL) ties them with the listed ones"},
    };
    // argOutcome() codes besides a conversion distance
    private static final int OUTCOME_EXACT = Integer.MIN_VALUE + 1;
    private static final int OUTCOME_NONE = Integer.MIN_VALUE;
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

    @Test
    public void testTieGroupsAreEntirelyListedOrUnlisted() throws Exception {
        // OverloadResolver breaks a score tie by the function list order when either overload is listed, and by the
        // conversion distances when neither is. A (name, arity) group that mixes listed and unlisted overloads which
        // can tie lets a new overload, or a change to the list, switch the rule that decides a call. The test walks
        // every call of every group with each argument kind the resolver tells apart at each position (every column
        // type, constants, typed and untyped NULL, an untyped bind variable and an untyped empty array), in and out of
        // a window context, and requires each group in which two overloads tie for some call, both with the best match
        // other than an exact one and the best score, to be entirely listed or entirely unlisted. It resolves each
        // call with the production resolver and the copy too, which must agree.
        assertMemoryLeak(() -> {
            resolver = new FunctionResolver(configuration, engine.getFunctionFactoryCache());
            final ArrayConstant emptyArray = ArrayConstant.emptyUntyped(1);
            final TreeSet<String> mixedGroups = new TreeSet<>();
            final TreeSet<String> examples = new TreeSet<>();
            int tieGroupCount = 0;
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
                    tieGroupCount += checkTieGroups(names.get(i), mixedGroups, examples);
                }
            } finally {
                Misc.free(emptyArray);
            }
            Assert.assertEquals("", join(failures));
            Assert.assertTrue("compared too few calls: " + compared, compared > 80_000);
            Assert.assertTrue("found too few tie groups: " + tieGroupCount, tieGroupCount > 70);
            final StringBuilder expected = new StringBuilder();
            for (String[] group : MIXED_TIE_GROUPS) {
                Assert.assertFalse(group[0] + " has no reason", group[1].isEmpty());
                expected.append(group[0]).append('\n');
            }
            Assert.assertEquals("first ties:\n" + join(examples), expected.toString(), join(mixedGroups));
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

    // How the overload takes the argument at the fixed signature position argIdx, as OverloadResolver.resolve reads
    // it: OUTCOME_NONE when it cannot, OUTCOME_EXACT for an exact match, otherwise the conversion distance, which the
    // score adds.
    private static int argOutcome(
            FunctionFactoryDescriptor descriptor,
            int argIdx,
            int argType,
            int argTraitBits,
            boolean isCast,
            boolean hasLong256Argument
    ) {
        final FunctionFactory factory = descriptor.getFactory();
        final boolean isArgConstant = (argTraitBits & TRAIT_CONSTANT) != 0;
        final int sigArgTypeWithFlags = descriptor.getArgTypeWithFlags(argIdx);

        if (FunctionFactoryDescriptor.isConstant(sigArgTypeWithFlags) && !isArgConstant) {
            return OUTCOME_NONE;
        }

        final short argTypeTag = ColumnType.tagOf(argType);
        final short sigArgTypeTag = FunctionFactoryDescriptor.toTypeTag(sigArgTypeWithFlags);

        final boolean sigIsArray = FunctionFactoryDescriptor.isArray(sigArgTypeWithFlags);
        final boolean argIsArray = argTypeTag == ColumnType.ARRAY;
        final boolean argIsStringArray = argTypeTag == ColumnType.ARRAY_STRING;
        final boolean sigIsStringArray = sigArgTypeTag == ColumnType.ARRAY_STRING;
        if (sigIsArray != argIsArray || sigIsStringArray != argIsStringArray) {
            if (argType != ColumnType.UNDEFINED) {
                return OUTCOME_NONE;
            }
        }
        if (argIsStringArray) {
            return OUTCOME_EXACT;
        }
        if (argIsArray) {
            if (sigArgTypeTag == ColumnType.decodeArrayElementType(argType)
                    || (sigArgTypeTag == ColumnType.DOUBLE && (argTraitBits & TRAIT_UNTYPED_EMPTY_ARRAY) != 0)) {
                return OUTCOME_EXACT;
            }
            return OUTCOME_NONE;
        }

        if (sigArgTypeTag == argTypeTag ||
                (argTypeTag == ColumnType.CHAR &&
                        sigArgTypeTag == ColumnType.STRING &&
                        factory.supportImplicitCastCharToStr() &&
                        isArgConstant &&
                        (argTraitBits & (TRAIT_CHAR_TYPE | TRAIT_NULL_CHAR_ORDERED)) == 0) ||
                (sigArgTypeTag == ColumnType.GEOHASH && ColumnType.isGeoHash(argType)) ||
                (sigArgTypeTag == ColumnType.DECIMAL && ColumnType.isDecimal(argType))) {
            return OUTCOME_EXACT;
        }

        if (argIdx == 1 && isCast) {
            return OUTCOME_NONE;
        }
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
        return overloadDistance != ColumnType.OVERLOAD_NONE || argType == ColumnType.UNDEFINED ? overloadDistance : OUTCOME_NONE;
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

    // The match and score OverloadResolver.resolve gives the overload for the call, packed by pack(), with
    // MATCH_NO_MATCH when the overload cannot take the call. Fills distances with the conversion distance of each fixed
    // argument.
    private static long evaluate(
            FunctionFactoryDescriptor descriptor,
            IntList argTypes,
            IntList argTraits,
            boolean isCast,
            boolean isWindowContext,
            boolean hasLong256Argument,
            IntList distances
    ) {
        final FunctionFactory factory = descriptor.getFactory();
        final int argCount = argTypes.size();
        final boolean sigVarArg = isVarArg(descriptor);
        final int sigArgCount = fixedArgCount(descriptor);

        if (sigVarArg && argCount != sigArgCount) {
            if (argCount < sigArgCount) {
                return pack(MATCH_NO_MATCH, 0);
            }
            boolean variadicTypeAreAllUndefinedVariables = true;
            for (int argIdx = sigArgCount; argIdx < argCount; argIdx++) {
                if (argTypes.getQuick(argIdx) != ColumnType.UNDEFINED) {
                    variadicTypeAreAllUndefinedVariables = false;
                    break;
                }
            }
            if (variadicTypeAreAllUndefinedVariables && !factory.variadicTypeSupportUndefinedBindVariables(argCount)) {
                return pack(MATCH_NO_MATCH, 0);
            }
        }

        if (sigArgCount != argCount && !(sigVarArg && argCount >= sigArgCount)) {
            return pack(MATCH_NO_MATCH, 0);
        }
        int match = sigArgCount == 0 ? MATCH_EXACT_MATCH : MATCH_NO_MATCH;
        int sigArgTypeScore = 0;
        distances.setAll(sigArgCount, 0);
        for (int argIdx = 0; argIdx < sigArgCount; argIdx++) {
            final int outcome = argOutcome(descriptor, argIdx, argTypes.getQuick(argIdx), argTraits.getQuick(argIdx), isCast, hasLong256Argument);
            if (outcome == OUTCOME_NONE) {
                return pack(MATCH_NO_MATCH, 0);
            }
            if (outcome == OUTCOME_EXACT) {
                match = mergeWithExactMatch(match);
                continue;
            }
            sigArgTypeScore += outcome;
            distances.setQuick(argIdx, outcome);
            switch (match) {
                case MATCH_NO_MATCH:
                    match = ColumnType.tagOf(argTypes.getQuick(argIdx)) == ColumnType.NULL ? MATCH_PARTIAL_MATCH : MATCH_FUZZY_MATCH;
                    break;
                case MATCH_EXACT_MATCH:
                    match = MATCH_PARTIAL_MATCH;
                    break;
                default:
                    break;
            }
        }

        if (isWindowContext != factory.isWindow()) {
            match = MATCH_FUZZY_MATCH;
            sigArgTypeScore += 20;
        } else if (factory.isWindow()) {
            sigArgTypeScore -= 20;
        }
        return pack(match, sigArgTypeScore);
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

    private static boolean hasLong256Argument(IntList argTypes) {
        for (int i = 0, n = argTypes.size(); i < n; i++) {
            if (ColumnType.tagOf(argTypes.getQuick(i)) == ColumnType.LONG256) {
                return true;
            }
        }
        return false;
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

    private static String join(ObjList<String> lines) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0, n = lines.size(); i < n; i++) {
            sb.append(lines.getQuick(i)).append('\n');
        }
        return sb.toString();
    }

    private static String join(TreeSet<String> lines) {
        final StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(line).append('\n');
        }
        return sb.toString();
    }

    // What decides how the overloads of a group take the argument at position p: the traits it lends the other
    // arguments, and its outcome at each overload under each value of the traits the other arguments lend it.
    private static String kindKey(ObjList<FunctionFactoryDescriptor> group, int p, Arg arg, boolean isCast, boolean isOrdering) {
        final int type = arg.type;
        final short tag = ColumnType.tagOf(type);
        final boolean isNullChar = arg.isConstant && tag == ColumnType.CHAR && arg.function.getChar(null) == Numbers.CHAR_NULL;
        final StringBuilder key = new StringBuilder()
                .append(tag == ColumnType.LONG256 ? 'L' : '-')
                .append(tag == ColumnType.NULL ? 'N' : '-')
                .append(type == ColumnType.UNDEFINED ? 'U' : '-')
                .append(isNullChar ? 'C' : '-')
                .append(type == ColumnType.NULL || type == ColumnType.CHAR ? 'O' : '-');
        for (int i = 0, n = group.size(); i < n; i++) {
            final FunctionFactoryDescriptor descriptor = group.getQuick(i);
            key.append('|');
            if (p >= fixedArgCount(descriptor)) {
                key.append('V');
                continue;
            }
            for (int long256 = 0; long256 < 2; long256++) {
                for (int other = 0, otherEnd = isOrdering ? 2 : 1; other < otherEnd; other++) {
                    final int traits = traitsOf(arg, isOrdering && isNullChar && other == 1);
                    key.append(argOutcome(descriptor, p, type, traits, isCast, long256 == 1)).append(',');
                }
            }
        }
        return key.toString();
    }

    private static int matchOf(long evaluation) {
        return (int) (evaluation >> 32);
    }

    private static int mergeWithExactMatch(int match) {
        return match == MATCH_NO_MATCH ? MATCH_EXACT_MATCH
                : match == MATCH_FUZZY_MATCH ? MATCH_PARTIAL_MATCH
                  : match;
    }

    private static long pack(int match, int score) {
        return ((long) match << 32) | (score & 0xFFFF_FFFFL);
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
        final boolean hasLong256Argument = hasLong256Argument(argTypes);
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

            final long evaluation = evaluate(descriptor, argTypes, argTraits, isCast, isWindowContext, hasLong256Argument, distances);
            final int match = matchOf(evaluation);
            if (match == MATCH_NO_MATCH) {
                continue;
            }
            final int sigArgTypeScore = scoreOf(evaluation);

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
        return candidate;
    }

    private static int scoreOf(long evaluation) {
        return (int) evaluation;
    }

    // The traits OverloadResolver reads off the argument; isNullCharOrdered when the argument is a NULL CHAR constant
    // an ordering operator compares with NULL or CHAR.
    private static int traitsOf(Arg arg, boolean isNullCharOrdered) {
        final Function function = arg.function;
        final boolean isConstant = function.isConstant();
        return (isConstant ? TRAIT_CONSTANT : 0)
                | (function == CharTypeConstant.INSTANCE ? TRAIT_CHAR_TYPE : 0)
                | (isNullCharOrdered ? TRAIT_NULL_CHAR_ORDERED : 0)
                | (function instanceof ArrayConstant && ColumnType.decodeArrayElementType(arg.type) == ColumnType.UNDEFINED ? TRAIT_UNTYPED_EMPTY_ARRAY : 0)
                | (isConstant && ColumnType.tagOf(arg.type) == ColumnType.STRING && function.getStrLen(null) > 1 ? TRAIT_MULTI_CHAR : 0);
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

    // The argument kinds that the overloads of a group take differently at position p: the first argument of each
    // kind, with an untyped NULL among them, and only the type constants at the target of cast().
    private ObjList<Arg> argumentKinds(ObjList<FunctionFactoryDescriptor> group, int p, boolean isCast, boolean isOrdering) {
        final ObjList<Arg> source = new ObjList<>();
        if (isCast && p == 1) {
            source.addAll(castTargets);
        } else {
            source.add(new Arg("NULL", NullConstant.NULL, ColumnType.NULL, true));
            source.addAll(typedArgs);
        }
        final HashSet<String> keys = new HashSet<>();
        final ObjList<Arg> kinds = new ObjList<>();
        for (int i = 0, n = source.size(); i < n; i++) {
            final Arg arg = source.getQuick(i);
            if (keys.add(kindKey(group, p, arg, isCast, isOrdering))) {
                kinds.add(arg);
            }
        }
        return kinds;
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
        prepareCall(name, call);
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

    // Resolves the call with the copy and with the production resolver, which must agree, and records each pair of
    // overloads of the group that tie for the call: both have the best match, other than an exact one, and the best
    // score, so the tie break decides between them when OverloadResolver.resolve meets the later of the two.
    private void checkTieCall(String name, ObjList<FunctionFactoryDescriptor> overloads, TieGroup group, ObjList<Arg> call, boolean isWindowContext) {
        prepareCall(name, call);
        final boolean isCast = name.equalsIgnoreCase("cast");
        final boolean hasLong256Argument = hasLong256Argument(argTypes);
        final ObjList<FunctionFactoryDescriptor> members = group.overloads;
        final int n = members.size();
        // the best evaluation: the highest match, then the lowest score; an exact match ends the walk, so it ties with
        // no other overload
        long best = pack(MATCH_NO_MATCH, 0);
        for (int i = 0; i < n; i++) {
            final long evaluation = evaluate(members.getQuick(i), argTypes, argTraits, isCast, isWindowContext, hasLong256Argument, distancesA);
            group.evaluations[i] = evaluation;
            final int match = matchOf(evaluation);
            if (match > matchOf(best) || (match == matchOf(best) && scoreOf(evaluation) < scoreOf(best))) {
                best = evaluation;
            }
        }
        if (matchOf(best) != MATCH_NO_MATCH && matchOf(best) != MATCH_EXACT_MATCH) {
            for (int i = 0; i < n; i++) {
                if (group.evaluations[i] != best) {
                    continue;
                }
                for (int j = i + 1; j < n; j++) {
                    // a later variadic overload never displaces an earlier fixed one of the same match
                    if (group.evaluations[j] == best && (!isVarArg(members.getQuick(j)) || isVarArg(members.getQuick(i)))) {
                        group.recordTie(members.getQuick(i), members.getQuick(j), call, isWindowContext);
                    }
                }
            }
        }
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
    }

    // Walks every group of the name that has two or more overloads and records each mixed one in which overloads
    // tie, with its first tie among the examples. Returns the number of groups of the name in which overloads tie.
    private int checkTieGroups(String name, TreeSet<String> mixedGroups, TreeSet<String> examples) throws SqlException {
        final ObjList<FunctionFactoryDescriptor> overloads = engine.getFunctionFactoryCache().getOverloadList(name);
        final TreeSet<Integer> arities = new TreeSet<>();
        for (int i = 0, n = overloads.size(); i < n; i++) {
            final FunctionFactoryDescriptor descriptor = overloads.getQuick(i);
            final int fixed = fixedArgCount(descriptor);
            if (isVarArg(descriptor)) {
                for (int k = fixed; k <= fixed + MAX_VAR_ARGS; k++) {
                    arities.add(k);
                }
            } else {
                arities.add(fixed);
            }
        }
        final boolean isCast = name.equalsIgnoreCase("cast");
        int tieGroupCount = 0;
        for (int arity : arities) {
            // a call without arguments binds the first overload without one
            if (arity == 0) {
                continue;
            }
            final TieGroup group = new TieGroup();
            boolean hasWindow = false;
            for (int i = 0, n = overloads.size(); i < n; i++) {
                final FunctionFactoryDescriptor descriptor = overloads.getQuick(i);
                final int fixed = fixedArgCount(descriptor);
                if (fixed == arity || (isVarArg(descriptor) && arity >= fixed)) {
                    group.overloads.add(descriptor);
                    hasWindow |= descriptor.getFactory().isWindow();
                }
            }
            if (group.overloads.size() < 2) {
                continue;
            }
            group.evaluations = new long[group.overloads.size()];
            final boolean isOrdering = arity == 2 && FunctionResolver.isOrderingOperator(name);
            final ObjList<ObjList<Arg>> positions = new ObjList<>();
            long product = 1;
            for (int p = 0; p < arity; p++) {
                final ObjList<Arg> kinds = argumentKinds(group.overloads, p, isCast, isOrdering);
                positions.add(kinds);
                product *= kinds.size();
            }
            if (product > MAX_TIE_CALLS) {
                failures.add(name + " arity " + arity + ": " + product + " calls are too many to walk");
                continue;
            }
            final ObjList<Arg> call = new ObjList<>();
            enumerateTies(name, overloads, group, positions, 0, call, false);
            if (hasWindow) {
                sqlExecutionContext.configureWindowContext(null, null, new ArrayColumnTypes(), true,
                        RecordCursorFactory.SCAN_DIRECTION_FORWARD, 0, WindowExpression.FRAMING_RANGE,
                        Long.MIN_VALUE, (char) 0, 0, 0, 0, (char) 0, 0, 0, WindowExpression.EXCLUDE_NO_OTHERS, 0,
                        1, ColumnType.TIMESTAMP_MICRO, false, 0);
                try {
                    enumerateTies(name, overloads, group, positions, 0, call, true);
                } finally {
                    sqlExecutionContext.clearWindowContext();
                }
            }
            if (group.firstTie != null) {
                tieGroupCount++;
                if (group.isMixed()) {
                    mixedGroups.add(group.summary(name, arity));
                    examples.add(group.example(name, arity));
                }
            }
        }
        return tieGroupCount;
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

    private void enumerateTies(
            String name,
            ObjList<FunctionFactoryDescriptor> overloads,
            TieGroup group,
            ObjList<ObjList<Arg>> positions,
            int p,
            ObjList<Arg> call,
            boolean isWindowContext
    ) {
        if (p == positions.size()) {
            checkTieCall(name, overloads, group, call, isWindowContext);
            return;
        }
        final ObjList<Arg> at = positions.getQuick(p);
        for (int i = 0, n = at.size(); i < n; i++) {
            call.add(at.getQuick(i));
            enumerateTies(name, overloads, group, positions, p + 1, call, isWindowContext);
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

    // Fills args, argPositions, argTypes and argTraits for the call, as FunctionResolver.selectOverload does.
    private void prepareCall(String name, ObjList<Arg> call) {
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

    // The overloads of one (name, arity) group, in the order OverloadResolver meets them, and the first ties among them.
    private static final class TieGroup {
        private final ObjList<FunctionFactoryDescriptor> overloads = new ObjList<>();
        private long[] evaluations;
        private String firstMixedTie;
        private String firstTie;

        private static String describePair(FunctionFactoryDescriptor a, FunctionFactoryDescriptor b, ObjList<Arg> call, boolean isWindowContext) {
            return describe(a) + " and " + describe(b) + " on " + render(a.getName(), call, isWindowContext);
        }

        // The first tie, between a listed and an unlisted overload when there is one.
        private String example(String name, int arity) {
            return name + '/' + arity + ": " + (firstMixedTie != null ? firstMixedTie : firstTie);
        }

        private boolean isMixed() {
            boolean hasListed = false;
            boolean hasUnlisted = false;
            for (int i = 0, n = overloads.size(); i < n; i++) {
                final boolean isListed = overloads.getQuick(i).isListed();
                hasListed |= isListed;
                hasUnlisted |= !isListed;
            }
            return hasListed && hasUnlisted;
        }

        private void recordTie(FunctionFactoryDescriptor a, FunctionFactoryDescriptor b, ObjList<Arg> call, boolean isWindowContext) {
            if (firstTie == null) {
                firstTie = describePair(a, b, call, isWindowContext);
            }
            if (firstMixedTie == null && a.isListed() != b.isListed()) {
                firstMixedTie = describePair(a, b, call, isWindowContext);
            }
        }

        // The group, the factories of its unlisted overloads, and whether a listed overload ties with an unlisted one.
        private String summary(String name, int arity) {
            final StringBuilder sb = new StringBuilder(name).append('/').append(arity).append(": unlisted ");
            final HashSet<String> factories = new HashSet<>();
            for (int i = 0, n = overloads.size(); i < n; i++) {
                final FunctionFactoryDescriptor descriptor = overloads.getQuick(i);
                final String factory = unwrap(descriptor.getFactory()).getClass().getSimpleName();
                if (!descriptor.isListed() && factories.add(factory)) {
                    sb.append(factories.size() > 1 ? ", " : "").append(factory);
                }
            }
            return sb.append(firstMixedTie != null ? "; listed and unlisted overloads tie" : "; only overloads of one kind tie").toString();
        }
    }
}
