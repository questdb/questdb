/*******************************************************************************
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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.vect.VectorAggregateFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * Every aggregate must be deliberately classified as order-sensitive or not, because a wrong
 * answer from {@link GroupByFunction#isOrderSensitive()} or
 * {@link VectorAggregateFunction#isOrderSensitive()} is silent: a consumer with no
 * order-sensitive aggregate tells its base it no longer needs designated-timestamp order, and
 * a covering scan then emits one frame per index key. A misflagged order-sensitive aggregate
 * over that base returns a plausible wrong value rather than an error.
 * <p>
 * Both methods are abstract, so a NEW aggregate cannot compile without stating an answer -- the
 * compiler, not this test, is what stops an author from saying nothing. What the compiler cannot
 * see is the 40-odd concrete classes that inherit a declaration from a concrete ancestor (the
 * {@code First*}/{@code Last*} Decimal, GeoHash and IPv4 family, including the anonymous classes
 * in {@code LastGeoHashGroupByFunctionFactory}). Adding a subclass there compiles silently with
 * whatever the parent said. THIS TEST is what covers them: it enumerates every concrete
 * implementation of both interfaces off the compiled class tree and fails on any class that is
 * in neither list below, so an inherited answer nobody re-examined fails here rather than in
 * production.
 * <p>
 * The two interfaces are disjoint -- they share no supertype carrying the method -- so both
 * hierarchies are enumerated separately.
 * <p>
 * <b>What the check can and cannot see.</b> Every {@code isOrderSensitive()} body in the tree is
 * a constant return, so the test reads the effective VALUE off an instance allocated without a
 * constructor and asserts it matches the list the class is in. It does not check that the value
 * is RIGHT -- that judgement lives in the list javadocs below. The behavioural half of the
 * contract lives in {@code io.questdb.test.cairo.covering.CoveringIndexOrderSensitiveTest},
 * which compares the per-key arm against a full scan for the aggregates that actually reach the
 * covering path.
 */
public class GroupByFunctionOrderSensitivityTest {

    /**
     * Packages this test is responsible for. A class under one of these that will not load is
     * an aggregate nobody classified, so the scan refuses to quietly shrink and fails. A class
     * anywhere else that will not load -- say because a future change makes it unable to
     * resolve a superclass -- is logged and skipped: it is not an aggregate, and turning this
     * aggregate test red for it would invite someone to widen
     * {@link #TOLERATED_UNLOADABLE}, which is the allowlist erosion this test exists to stop.
     * <p>
     * functions.test is on the list because it really does hold aggregates -- the three
     * TestSum* functions below -- not because the scan should be lenient about it.
     */
    private static final String[] AGGREGATE_PACKAGES = {
            "io.questdb.griffin.engine.functions.groupby.",
            "io.questdb.griffin.engine.functions.test.",
            "io.questdb.griffin.engine.groupby.vect."
    };
    private static final Log LOG = LogFactory.getLog(GroupByFunctionOrderSensitivityTest.class);

    /**
     * Class files that are on the class tree but cannot be loaded reflectively, and are not
     * aggregates. Anything else under {@link #AGGREGATE_PACKAGES} that fails to load fails the
     * test rather than being skipped silently, because a skipped aggregate is an unclassified
     * aggregate.
     */
    private static final Set<String> TOLERATED_UNLOADABLE = new TreeSet<>(Arrays.asList(
            "io.questdb.mp.continuation.Fiber$PinnableContinuation",
            "module-info"
    ));

    /**
     * Order-sensitive: the result depends on the order rows arrive in, so the aggregate is
     * only correct over a base that delivers designated-timestamp order.
     * <p>
     * first()/last() and their not-null, geohash and decimal variants compare a stored row
     * id whose high bits are the page-frame sequence index ({@code rowId >>> 44}); per-key
     * frames make that sequence key-major, so "first" would become "whichever key was
     * scanned first". twap(), sparkline() and array_agg() buffer per-frame batches and sort
     * them by frame id for the same reason. haversine_dist_deg() accumulates distance
     * between consecutive rows; isOrdered() is itself an order predicate; string_agg() and
     * string_distinct_agg() render in arrival / first-occurrence order.
     * <p>
     * arg_min()/arg_max() are here by DECISION rather than by discovered property. They break
     * ties with a strict {@code >} against the stored key, so among equal keys the first row
     * seen wins. At a parallel group-by site that is already unspecified -- merge() applies
     * the same strict comparison across workers in a nondeterministic order -- but at a serial
     * site there is no merge(), computeNext() makes first-seen deterministic, and per-key
     * frames change which row is seen first. Three reasons to flag them anyway:
     * <ul>
     *   <li>safe beats fast is this change's established principle; the guard is deliberately
     *       over-conservative elsewhere for exactly this reason.</li>
     *   <li>the cost is narrow: per-key is lost for arg_min()/arg_max() queries only, and only
     *       when they group by something the optimisation would otherwise have accelerated.</li>
     *   <li>the alternative is an internal optimisation silently changing which value a user
     *       gets back for a tied key. "Unspecified" in the docs is not the same as "changed by
     *       a patch that claims to be an optimisation".</li>
     * </ul>
     * <p>
     * mode() over LONG/DOUBLE/STRING/VARCHAR/SYMBOL is here for the same reason as
     * arg_min()/arg_max(). The per-value counts are order-invariant, but the winner is picked by
     * scanning the open-addressed count map in SLOT order and taking the first strict
     * {@code >}, and a key's slot under linear probing -- and its position after a rehash, which
     * reinserts in slot-scan order -- depends on insertion order. So among values tied on count,
     * which one wins is a function of the order rows arrived in. That is already unspecified at a
     * parallel site (these functions supportsParallelism(), and merge() combines in
     * nondeterministic worker order), which is exactly the argument that was previously used to
     * leave them in the insensitive list. It is not a good enough argument: at a SERIAL site
     * there is no merge(), the slot-order tie-break is deterministic today, and per-key frames
     * would change it. mode() over BOOLEAN stays insensitive -- it keeps no map at all, just a
     * {@code +1/-1} counter, so there is nothing order-dependent to break.
     * <p>
     * approx_percentile() over DOUBLE (packed and unpacked) is here because it accumulates into
     * an HdrHistogram {@code DoubleHistogram} with {@code setAutoResize(true)}. Recording a value
     * outside the currently covered range shifts the whole covered range and rescales the
     * double-to-integer conversion ratio, which re-buckets every value ALREADY recorded. The
     * approximation therefore depends on the sequence values arrived in. This is latent today --
     * both report supportsParallelism() false, so nothing reorders their input yet -- and is
     * flagged so it stays correct when something does. The LONG variants are genuinely
     * insensitive and stay in the other list: their integer histogram only ever appends buckets
     * on resize, leaving the value-to-bucket mapping of existing values untouched.
     */
    private static final Set<String> ORDER_SENSITIVE = new TreeSet<>(Arrays.asList(
            "io.questdb.griffin.engine.functions.groupby.ApproxPercentileDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ApproxPercentileDoublePackedGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxCharDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxCharLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxCharTimestampGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxDoubleDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxDoubleLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxDoubleTimestampGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxLongDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxLongTimestampGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxTimestampDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxTimestampLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxTimestampUuidGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxUuidTimestampGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxVarcharDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxVarcharIntGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxVarcharLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMaxVarcharTimestampGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMinCharDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMinCharLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMinCharTimestampGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMinDoubleDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMinDoubleLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMinDoubleTimestampGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMinLongDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMinLongTimestampGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMinTimestampDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMinTimestampLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMinTimestampUuidGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArgMinUuidTimestampGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArrayAggDoubleArrayGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ArrayAggDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstArrayGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstBooleanGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstByteGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstCharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstDateGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstDecimalGroupByFunctionFactory$Decimal128Func",
            "io.questdb.griffin.engine.functions.groupby.FirstDecimalGroupByFunctionFactory$Decimal16Func",
            "io.questdb.griffin.engine.functions.groupby.FirstDecimalGroupByFunctionFactory$Decimal256Func",
            "io.questdb.griffin.engine.functions.groupby.FirstDecimalGroupByFunctionFactory$Decimal32Func",
            "io.questdb.griffin.engine.functions.groupby.FirstDecimalGroupByFunctionFactory$Decimal64Func",
            "io.questdb.griffin.engine.functions.groupby.FirstDecimalGroupByFunctionFactory$Decimal8Func",
            "io.questdb.griffin.engine.functions.groupby.FirstDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstFloatGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstGeoHashGroupByFunctionByte",
            "io.questdb.griffin.engine.functions.groupby.FirstGeoHashGroupByFunctionInt",
            "io.questdb.griffin.engine.functions.groupby.FirstGeoHashGroupByFunctionLong",
            "io.questdb.griffin.engine.functions.groupby.FirstGeoHashGroupByFunctionShort",
            "io.questdb.griffin.engine.functions.groupby.FirstIPv4GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstIntGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullArrayGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullCharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullDateGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullDecimalGroupByFunctionFactory$Decimal128Func",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullDecimalGroupByFunctionFactory$Decimal16Func",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullDecimalGroupByFunctionFactory$Decimal256Func",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullDecimalGroupByFunctionFactory$Decimal32Func",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullDecimalGroupByFunctionFactory$Decimal64Func",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullDecimalGroupByFunctionFactory$Decimal8Func",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullFloatGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullGeoHashGroupByFunctionFactory$FirstNotNullGeoHashGroupByFunctionByte",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullGeoHashGroupByFunctionFactory$FirstNotNullGeoHashGroupByFunctionInt",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullGeoHashGroupByFunctionFactory$FirstNotNullGeoHashGroupByFunctionLong",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullGeoHashGroupByFunctionFactory$FirstNotNullGeoHashGroupByFunctionShort",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullIPv4GroupByFunctionFactory$Func",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullIntGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullStrGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullSymbolGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullTimestampGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullUuidGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstNotNullVarcharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstShortGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstStrGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstSymbolGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstTimestampGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstUuidGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.FirstVarcharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.HaversineDistDegreeGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.IsIPv4OrderedGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.IsLongOrderedGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastArrayGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastBooleanGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastByteGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastCharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastDateGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory$Decimal128Func",
            "io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory$Decimal16Func",
            "io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory$Decimal256Func",
            "io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory$Decimal32Func",
            "io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory$Decimal64Func",
            "io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory$Decimal8Func",
            "io.questdb.griffin.engine.functions.groupby.LastDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastFloatGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastGeoHashGroupByFunctionFactory$1",
            "io.questdb.griffin.engine.functions.groupby.LastGeoHashGroupByFunctionFactory$2",
            "io.questdb.griffin.engine.functions.groupby.LastGeoHashGroupByFunctionFactory$3",
            "io.questdb.griffin.engine.functions.groupby.LastGeoHashGroupByFunctionFactory$4",
            "io.questdb.griffin.engine.functions.groupby.LastIPv4GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastIntGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullArrayGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullCharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullDateGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullDecimalGroupByFunctionFactory$Decimal128Func",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullDecimalGroupByFunctionFactory$Decimal16Func",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullDecimalGroupByFunctionFactory$Decimal256Func",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullDecimalGroupByFunctionFactory$Decimal32Func",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullDecimalGroupByFunctionFactory$Decimal64Func",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullDecimalGroupByFunctionFactory$Decimal8Func",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullFloatGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullGeoHashGroupByFunctionFactory$LastNotNullGeoHashGroupByFunctionByte",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullGeoHashGroupByFunctionFactory$LastNotNullGeoHashGroupByFunctionInt",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullGeoHashGroupByFunctionFactory$LastNotNullGeoHashGroupByFunctionLong",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullGeoHashGroupByFunctionFactory$LastNotNullGeoHashGroupByFunctionShort",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullIPv4GroupByFunctionFactory$Func",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullIntGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullStrGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullSymbolGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullTimestampGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullUuidGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastNotNullVarcharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastShortGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastStrGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastSymbolGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastTimestampGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastUuidGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.LastVarcharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ModeDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ModeLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ModeStringGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ModeSymbolGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ModeVarcharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.SparklineGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.StringAggGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.StringAggVarcharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.StringDistinctAggGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.StringDistinctAggSymbolGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.StringDistinctAggVarcharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.TwapGroupByFunction"
    ));

    /**
     * Classified, but neither always-true nor always-false: this is a decorator that wraps
     * another GroupByFunction for SAMPLE BY FILL(LINEAR), so it must delegate the flag. It
     * is required to declare the override exactly like the order-sensitive set.
     */
    private static final Set<String> ORDER_SENSITIVE_DELEGATING = new TreeSet<>(Arrays.asList(
            "io.questdb.griffin.engine.functions.groupby.InterpolationGroupByFunction"
    ));

    /**
     * Order-invariant: the result is a commutative, associative combine of the values (sum,
     * count, bitwise, boolean, min/max, set cardinality, sketches) or a pure ratio of such
     * sums, and nothing in computeNext/merge reads the row id or the frame index.
     * <p>
     * Deliberately left here after inspection, with the reason:
     * <ul>
     *   <li>vwap() looks like twap() but is notional/volume -- a pure ratio of two sums, so
     *       it is order-invariant despite the name similarity.</li>
     *   <li>mode() over BOOLEAN keeps no map -- it sums {@code +1} for true and {@code -1} for
     *       false and reads the sign -- so it has no slot order to be sensitive to. The other
     *       mode() overloads DO, and are in {@code ORDER_SENSITIVE}.</li>
     *   <li>approx_percentile() over LONG accumulates into a fixed-lowest-value integer
     *       histogram; its auto-resize only appends buckets and never re-buckets what is
     *       already recorded. The DOUBLE overloads rescale on resize and are in
     *       {@code ORDER_SENSITIVE}.</li>
     *   <li>ksum()/nsum() and the floating-point sums are order-dependent only in the last
     *       bits of rounding, which is not a semantic guarantee and already varies with
     *       worker count.</li>
     * </ul>
     */
    private static final Set<String> ORDER_INSENSITIVE = new TreeSet<>(Arrays.asList(
            "io.questdb.griffin.engine.functions.groupby.ApproxCountDistinctIntGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ApproxCountDistinctIPv4GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ApproxCountDistinctLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ApproxPercentileLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ApproxPercentileLongPackedGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgDecimal128GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgDecimal128Rescale256GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgDecimal16GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgDecimal16Rescale256GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgDecimal256GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgDecimal256Rescale256GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgDecimal32GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgDecimal32Rescale256GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgDecimal64GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgDecimal64Rescale256GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgDecimal8GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgDecimal8Rescale256GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgIntGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.AvgShortGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.BitAndByteGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.BitAndIntGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.BitAndLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.BitAndShortGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.BitOrByteGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.BitOrIntGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.BitOrLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.BitOrShortGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.BitXorByteGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.BitXorIntGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.BitXorLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.BitXorShortGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.BoolAndGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.BoolOrGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CorrGroupByFunctionFactory$CorrGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountDecimalGroupByFunctionFactory$Decimal128Func",
            "io.questdb.griffin.engine.functions.groupby.CountDecimalGroupByFunctionFactory$Decimal16Func",
            "io.questdb.griffin.engine.functions.groupby.CountDecimalGroupByFunctionFactory$Decimal256Func",
            "io.questdb.griffin.engine.functions.groupby.CountDecimalGroupByFunctionFactory$Decimal32Func",
            "io.questdb.griffin.engine.functions.groupby.CountDecimalGroupByFunctionFactory$Decimal64Func",
            "io.questdb.griffin.engine.functions.groupby.CountDecimalGroupByFunctionFactory$Decimal8Func",
            "io.questdb.griffin.engine.functions.groupby.CountDistinctIntGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountDistinctIPv4GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountDistinctLong256GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountDistinctLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountDistinctStringGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountDistinctSymbolGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountDistinctUuidGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountDistinctVarcharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountFloatGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountGeoHashGroupByFunctionByte",
            "io.questdb.griffin.engine.functions.groupby.CountGeoHashGroupByFunctionInt",
            "io.questdb.griffin.engine.functions.groupby.CountGeoHashGroupByFunctionLong",
            "io.questdb.griffin.engine.functions.groupby.CountGeoHashGroupByFunctionShort",
            "io.questdb.griffin.engine.functions.groupby.CountIntGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountIPv4GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountLong256GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountStrGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountSymbolGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountUuidGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CountVarcharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CovarPopGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.CovarSampleGroupByFunctionFactory$CovarSampleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.DoubleArrayElemAvgGroupByFunctionFactory$DoubleArrayElemAvgGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.DoubleArrayElemMaxGroupByFunctionFactory$DoubleArrayElemMaxGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.DoubleArrayElemMinGroupByFunctionFactory$DoubleArrayElemMinGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.DoubleArrayElemSumGroupByFunctionFactory$DoubleArrayElemSumGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.GeomeanDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.KSumDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.KurtosisPopGroupByFunctionFactory$KurtosisPopGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.KurtosisSampleGroupByFunctionFactory$KurtosisSampleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MaxCharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MaxDateGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MaxDecimalGroupByFunctionFactory$Decimal128Func",
            "io.questdb.griffin.engine.functions.groupby.MaxDecimalGroupByFunctionFactory$Decimal16Func",
            "io.questdb.griffin.engine.functions.groupby.MaxDecimalGroupByFunctionFactory$Decimal256Func",
            "io.questdb.griffin.engine.functions.groupby.MaxDecimalGroupByFunctionFactory$Decimal32Func",
            "io.questdb.griffin.engine.functions.groupby.MaxDecimalGroupByFunctionFactory$Decimal64Func",
            "io.questdb.griffin.engine.functions.groupby.MaxDecimalGroupByFunctionFactory$Decimal8Func",
            "io.questdb.griffin.engine.functions.groupby.MaxDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MaxFloatGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MaxIntGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MaxIPv4GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MaxLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MaxShortGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MaxStrGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MaxTimestampGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MaxVarcharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MinCharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MinDateGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MinDecimalGroupByFunctionFactory$Decimal128Func",
            "io.questdb.griffin.engine.functions.groupby.MinDecimalGroupByFunctionFactory$Decimal16Func",
            "io.questdb.griffin.engine.functions.groupby.MinDecimalGroupByFunctionFactory$Decimal256Func",
            "io.questdb.griffin.engine.functions.groupby.MinDecimalGroupByFunctionFactory$Decimal32Func",
            "io.questdb.griffin.engine.functions.groupby.MinDecimalGroupByFunctionFactory$Decimal64Func",
            "io.questdb.griffin.engine.functions.groupby.MinDecimalGroupByFunctionFactory$Decimal8Func",
            "io.questdb.griffin.engine.functions.groupby.MinDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MinFloatGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MinIntGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MinIPv4GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MinLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MinShortGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MinStrGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MinTimestampGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.MinVarcharGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.ModeBooleanGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.NSumDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.RegressionInterceptFunctionFactory$RegressionInterceptFunction",
            "io.questdb.griffin.engine.functions.groupby.RegressionR2FunctionFactory$RegressionR2Function",
            "io.questdb.griffin.engine.functions.groupby.RegressionSlopeFunctionFactory$RegressionSlopeFunction",
            "io.questdb.griffin.engine.functions.groupby.SkewnessPopGroupByFunctionFactory$SkewnessPopGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.SkewnessSampleGroupByFunctionFactory$SkewnessSampleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.StdDevPopGroupByFunctionFactory$StdDevPopGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.StdDevSampleGroupByFunctionFactory$StdDevSampleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.SumDecimal128GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.SumDecimal16GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.SumDecimal256GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.SumDecimal32GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.SumDecimal64GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.SumDecimal8GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.SumDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.SumFloatGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.SumIntGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.SumLong256GroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.SumLongGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.SumShortGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.VarPopGroupByFunctionFactory$VarPopGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.VarSampleGroupByFunctionFactory$VarSampleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.VwapDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.WeightedAvgDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.WeightedStdDevFrequencyGroupByFunctionFactory$WeightedStdDevFrequencyGroupByFunction",
            "io.questdb.griffin.engine.functions.groupby.WeightedStdDevReliabilityGroupByFunctionFactory$WeightedStdDevReliabilityGroupByFunction",
            "io.questdb.griffin.engine.functions.test.TestSumDoubleGroupByFunction",
            "io.questdb.griffin.engine.functions.test.TestSumStringGroupByFunction",
            "io.questdb.griffin.engine.functions.test.TestSumTDoubleGroupByFunction"
    ));

    /**
     * The vectorized group-by site has its own aggregate hierarchy. Every implementation
     * there today (count/sum/avg/min/max/ksum/nsum) is order-invariant, so this set is
     * empty on purpose. Its value is that it stops being empty silently: a future vector
     * first()/last() lands in VECT_UNCLASSIFIED below and fails this test.
     */
    private static final Set<String> VECT_ORDER_SENSITIVE = new TreeSet<>(Arrays.asList(
    ));

    private static final Set<String> VECT_ORDER_INSENSITIVE = new TreeSet<>(Arrays.asList(
            "io.questdb.griffin.engine.groupby.vect.AvgDoubleVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.AvgIntVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.AvgLongVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.AvgShortVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.CountDoubleVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.CountIntVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.CountLongVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.CountVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.KSumDoubleVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.MaxDateVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.MaxDoubleVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.MaxIntVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.MaxLongVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.MaxShortVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.MaxTimestampVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.MinDateVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.MinDoubleVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.MinIntVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.MinLongVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.MinShortVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.MinTimestampVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.NSumDoubleVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.SumDateVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.SumDoubleVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.SumIntVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.SumLong256VectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.SumLongVectorAggregateFunction",
            "io.questdb.griffin.engine.groupby.vect.SumShortVectorAggregateFunction"
    ));

    @Test
    public void testEveryGroupByFunctionIsClassified() {
        assertClassification(
                GroupByFunction.class,
                "ORDER_SENSITIVE",
                union(ORDER_SENSITIVE, ORDER_SENSITIVE_DELEGATING),
                "ORDER_INSENSITIVE",
                ORDER_INSENSITIVE,
                ORDER_SENSITIVE_DELEGATING
        );
    }

    @Test
    public void testEveryVectorAggregateFunctionIsClassified() {
        assertClassification(
                VectorAggregateFunction.class,
                "VECT_ORDER_SENSITIVE",
                VECT_ORDER_SENSITIVE,
                "VECT_ORDER_INSENSITIVE",
                VECT_ORDER_INSENSITIVE,
                Collections.emptySet()
        );
    }

    private static void assertClassification(
            Class<?> iface,
            String sensitiveListName,
            Set<String> sensitive,
            String insensitiveListName,
            Set<String> insensitive,
            Set<String> valueCheckExempt
    ) {
        final Set<String> overlap = new TreeSet<>(sensitive);
        overlap.retainAll(insensitive);
        Assert.assertTrue(
                "a class cannot be in both " + sensitiveListName + " and " + insensitiveListName + ": " + overlap,
                overlap.isEmpty()
        );

        final List<Class<?>> found = enumerateConcreteImplementations(iface);
        final Set<String> actual = new TreeSet<>();
        for (int i = 0, n = found.size(); i < n; i++) {
            actual.add(found.get(i).getName());
        }

        final Set<String> classified = union(sensitive, insensitive);
        final Set<String> unclassified = new TreeSet<>(actual);
        unclassified.removeAll(classified);
        final Set<String> stale = new TreeSet<>(classified);
        stale.removeAll(actual);
        if (!unclassified.isEmpty() || !stale.isEmpty()) {
            final StringBuilder sb = new StringBuilder();
            sb.append("order-sensitivity classification is out of date for ")
                    .append(iface.getSimpleName())
                    .append('.');
            if (!unclassified.isEmpty()) {
                sb.append("\n  UNCLASSIFIED -- read computeNext()/merge() and decide whether the RESULT")
                        .append("\n  depends on the order rows arrive in, then add each to ")
                        .append(sensitiveListName).append(" or ").append(insensitiveListName)
                        .append(":");
                for (String name : unclassified) {
                    sb.append("\n    ").append(name);
                }
            }
            if (!stale.isEmpty()) {
                sb.append("\n  STALE -- listed but no longer a concrete implementation; remove:");
                for (String name : stale) {
                    sb.append("\n    ").append(name);
                }
            }
            Assert.fail(sb.toString());
        }

        final Set<String> saysFalse = new TreeSet<>();
        final Set<String> saysTrue = new TreeSet<>();
        for (int i = 0, n = found.size(); i < n; i++) {
            final Class<?> c = found.get(i);
            if (valueCheckExempt.contains(c.getName())) {
                continue;
            }
            final boolean value = readOrderSensitive(c);
            if (sensitive.contains(c.getName()) && !value) {
                saysFalse.add(c.getName());
            } else if (insensitive.contains(c.getName()) && value) {
                saysTrue.add(c.getName());
            }
        }
        if (!saysFalse.isEmpty() || !saysTrue.isEmpty()) {
            final StringBuilder sb = new StringBuilder();
            sb.append("isOrderSensitive() values do not match the classification for ")
                    .append(iface.getSimpleName())
                    .append(". Either the code or the list is wrong -- decide which, and if it")
                    .append("\n  is the list, move the class AND write down why in the list javadoc.");
            if (!saysFalse.isEmpty()) {
                sb.append("\n  LISTED IN ").append(sensitiveListName)
                        .append(" BUT isOrderSensitive() RETURNS false:");
                for (String name : saysFalse) {
                    sb.append("\n    ").append(name);
                }
            }
            if (!saysTrue.isEmpty()) {
                sb.append("\n  LISTED IN ").append(insensitiveListName)
                        .append(" BUT isOrderSensitive() RETURNS true:");
                for (String name : saysTrue) {
                    sb.append("\n    ").append(name);
                }
            }
            Assert.fail(sb.toString());
        }
    }

    /**
     * The value {@code isOrderSensitive()} returns for this class, read off an instance allocated
     * WITHOUT running a constructor -- these aggregates all take constructor arguments, and some
     * allocate native memory, so constructing them here is neither possible nor desirable.
     * <p>
     * This is sound only because every {@code isOrderSensitive()} body in the tree is a constant
     * return, which is what the mandatory-declaration convention produces. An implementation that
     * computed the answer from instance state would read it off a zeroed object and could answer
     * wrongly here; the one implementation that delegates rather than returning a constant,
     * {@code InterpolationGroupByFunction}, is passed in as exempt for exactly this reason. If a
     * second delegating implementation ever appears, exempt it too and say why.
     */
    private static boolean readOrderSensitive(Class<?> c) {
        try {
            final Object instance = Unsafe.getUnsafe().allocateInstance(c);
            return (Boolean) c.getMethod("isOrderSensitive").invoke(instance);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("could not read isOrderSensitive() from " + c.getName(), e);
        }
    }

    private static List<Class<?>> enumerateConcreteImplementations(Class<?> iface) {
        final Path root = mainClassRoot();
        final List<Class<?>> result = new ArrayList<>();
        final List<String> unloadable = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(root)) {
            paths.filter(p -> p.getFileName().toString().endsWith(".class")).forEach(p -> {
                final String relative = root.relativize(p).toString();
                final String binaryName = relative
                        .substring(0, relative.length() - ".class".length())
                        .replace(File.separatorChar, '.');
                if (TOLERATED_UNLOADABLE.contains(binaryName)) {
                    return;
                }
                final Class<?> c;
                try {
                    c = Class.forName(binaryName, false, GroupByFunction.class.getClassLoader());
                } catch (Throwable t) {
                    if (inAggregatePackage(binaryName)) {
                        unloadable.add(binaryName + " (" + t + ')');
                    } else {
                        LOG.info().$("skipping unloadable non-aggregate class [class=").$(binaryName)
                                .$(", error=").$(t.toString()).I$();
                    }
                    return;
                }
                if (c.isInterface() || Modifier.isAbstract(c.getModifiers())) {
                    return;
                }
                if (iface.isAssignableFrom(c)) {
                    result.add(c);
                }
            });
        } catch (IOException e) {
            throw new AssertionError("could not walk the compiled class tree at " + root, e);
        }
        // An AGGREGATE skipped because it would not load is an aggregate nobody classified, so
        // refuse to let the scan quietly shrink. Classes outside those packages were logged.
        Assert.assertTrue(
                "aggregate classes could not be loaded, so the scan is incomplete: " + unloadable,
                unloadable.isEmpty()
        );
        return result;
    }

    private static boolean inAggregatePackage(String binaryName) {
        for (int i = 0, n = AGGREGATE_PACKAGES.length; i < n; i++) {
            if (binaryName.startsWith(AGGREGATE_PACKAGES[i])) {
                return true;
            }
        }
        return false;
    }

    /**
     * Root of core's compiled MAIN classes, derived from a class that lives in it rather than
     * from the working directory or the raw classpath string.
     */
    private static Path mainClassRoot() {
        final URL location = GroupByFunction.class.getProtectionDomain().getCodeSource().getLocation();
        final Path root;
        try {
            root = Paths.get(location.toURI());
        } catch (URISyntaxException e) {
            throw new AssertionError("could not resolve the class tree location " + location, e);
        }
        Assert.assertTrue(
                "expected core's main classes as a directory, got " + root
                        + "; this scan cannot enumerate a jar",
                Files.isDirectory(root)
        );
        return root;
    }

    private static Set<String> union(Set<String> a, Set<String> b) {
        final Set<String> result = new TreeSet<>(a);
        result.addAll(b);
        return result;
    }
}
