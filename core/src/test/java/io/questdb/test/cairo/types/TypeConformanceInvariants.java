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

package io.questdb.test.cairo.types;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.FixedSizeTypeDriver;
import io.questdb.cairo.RelationKind;
import io.questdb.cairo.RelationRules;
import io.questdb.cairo.TypeDriver;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Checks a type registered later, which has no recording of its own. The
 * {@code TypeConformance*Test} classes call it for every type whose
 * {@link TypeConformanceTypes.Entry#isLater()} is true; it needs no per-type test code.
 * <p>
 * Paths: such a type runs on the paths its resource line lists
 * ({@link TypeConformanceTypes#LATER_TYPES_RESOURCE}). A pattern is {@code <path>} or
 * {@code <path>@<mode>}, where {@code *} matches any text, and {@code -} lists none. A tag
 * registered later without a resource line fails every kit class.
 * <p>
 * The invariants:
 * <ol>
 * <li>every value reads back as written ({@link #assertReadsBackAsWritten}), bit for bit, except
 * that for a float tier every NaN is the same value;</li>
 * <li>the NULL row and the sentinel-pattern row behave as the NULL policy says
 * ({@link #assertNullPolicy}): SENTINEL, both read the same; NONE, the NULL row reads as false or
 * 0; another type's sentinel pattern of the same width reads as a value except under SENTINEL
 * ({@link #assertOtherSentinel}). The kit knows these two policies, the ones the existing types
 * have; a type with a NULL policy of its own adds its rules here;</li>
 * <li>rows compare and sort by the order the type's arithmetic tier implies
 * ({@link #assertOrdered}, {@link #compare}), for an integer or float tier, and the rows that read
 * as NULL sort together where the existing types put NULL: lowest, and highest for a float tier;</li>
 * <li>the mixing cases give the results the resource states: its
 * {@code mix|<name>|<sql>|<expected>} lines ({@link #mixingCases()}), which the SQL class runs;</li>
 * <li>the relation paths follow the type's declared relations: a cast, CASE branch, ALTER COLUMN
 * TYPE target or dedup key the rules admit runs, and gives what the rules and the tier imply
 * ({@link #castRule}, {@link #widened}); the SQL and storage classes state each path's checks;</li>
 * <li>a path that reaches a guarded site the type declares it is refused at fails there with the
 * site's refusal ({@link #assertDeclaredRefusal}), under the memory-leak check of the test that
 * runs the path, so the refusing factory's cleanup is checked too. The add-a-type tool's
 * decisions ({@link #PLACES_FILE}) give each guarded site's refusal.</li>
 * </ol>
 * Every failure message names the type, the value row, the path and the mode.
 */
public final class TypeConformanceInvariants {
    public static final String POLICY_NONE = "NONE";
    public static final String POLICY_SENTINEL = "SENTINEL";
    /**
     * The decisions the add-a-type tool keeps per place, relative to the repository root. A
     * guarded site's row is decided {@code refused}, with the site's label as its reason, or
     * {@code <label>: <refusal>} for a site that keeps the error it raised before the guard.
     */
    public static final String PLACES_FILE = "utils/type-probe/places.tsv";
    private static final String FAMILY_ARM_REFUSAL = "no family arm for <type> at ";
    private static final String KEPT_REFUSAL_SEPARATOR = ": ";
    private static final String MIX_PREFIX = "mix|";
    // the paths a type no table can hold runs: the CREATE refusal once, and its bind values
    private static final Set<String> NOT_PERSISTED_PATHS = Set.of("sql.filter_eq", "sql.bind_value");
    private static final String TYPE_PLACEHOLDER = "<type>";
    // the guarded sites a type can declare it is refused at, by label, with each one's refusal
    private static Map<String, String> declarableSites;

    private TypeConformanceInvariants() {
    }

    /**
     * Invariant 2 for the NULL row and the sentinel-pattern row.
     *
     * @param nullText     how the NULL row reads (printed), null when writing it failed
     * @param nullBits     the NULL row's stored bits, null when writing it failed
     * @param nullError    the error writing the NULL row raised, null when it succeeded
     * @param sentinelText how the sentinel-pattern row reads (printed)
     * @param sentinelBits the sentinel-pattern row's stored bits
     * @param writtenBits  the bits written for the sentinel-pattern row
     */
    public static void assertNullPolicy(
            TypeConformanceTypes.Entry type,
            String path,
            String mode,
            @Nullable String nullText,
            long @Nullable [] nullBits,
            @Nullable String nullError,
            String sentinelText,
            long[] sentinelBits,
            long[] writtenBits
    ) {
        final String policy = policyOf(type);
        switch (policy) {
            case POLICY_SENTINEL -> {
                assertNoError(type, "null", path, mode, nullError);
                Assert.assertEquals(context(type, "null", path, mode) + ": SENTINEL, the NULL row must read as the sentinel-pattern row", sentinelText, nullText);
            }
            case POLICY_NONE -> {
                assertNoError(type, "null", path, mode, nullError);
                Assert.assertNotNull(context(type, "null", path, mode) + ": no stored bits", nullBits);
                Assert.assertArrayEquals(context(type, "null", path, mode) + ": NONE, the NULL row must read as false or 0", new long[4], nullBits);
            }
            default -> Assert.fail("type=" + type.label + ": unknown NULL policy " + policy);
        }
    }

    /**
     * Invariant 2 for another type's sentinel pattern written as a value ({@code sentinel_<TAG>}
     * rows): except under SENTINEL, it must read differently from the NULL row. Invariant 1
     * already requires it to read back with its bits.
     *
     * @param nullText how the NULL row reads (printed), null when writing it failed
     * @param text     how the row reads (printed)
     */
    public static void assertOtherSentinel(TypeConformanceTypes.Entry type, String row, String path, String mode, @Nullable String nullText, String text) {
        if (!POLICY_SENTINEL.equals(policyOf(type)) && nullText != null && nullText.equals(text)) {
            Assert.fail(context(type, row, path, mode) + ": " + policyOf(type) + ", another type's sentinel is a value, but reads as the NULL row: " + text);
        }
    }

    /**
     * Invariant 1: every non-NULL row reads back with the bits it was written with.
     */
    public static void assertReadsBackAsWritten(TypeConformanceTypes.Entry type, String row, String path, String mode, long[] written, long[] read) {
        if (type.isFloat() && isNaN(type, written) && isNaN(type, read)) {
            // every NaN is the same float value; a path may carry any NaN pattern
            return;
        }
        if (!Arrays.equals(written, read)) {
            Assert.fail(context(type, row, path, mode) + ": reads back " + hex(read) + ", written " + hex(written));
        }
    }

    /**
     * Invariant 3 for an interpolated value: it lies between its two neighbours, both ends
     * included, by the order the declared arithmetic tier implies. Without a declared tier
     * nothing is checked.
     */
    public static void assertBetween(TypeConformanceTypes.Entry type, String row, String path, String mode, long[] low, long[] value, long[] high) {
        if (type.laterTier == null) {
            return;
        }
        final boolean isAscending = compare(type, low, high) <= 0;
        final long[] first = isAscending ? low : high;
        final long[] last = isAscending ? high : low;
        if (compare(type, first, value) > 0 || compare(type, value, last) > 0) {
            Assert.fail(context(type, row, path, mode) + ": " + hex(value) + " lies outside " + hex(first) + " .. " + hex(last)
                    + " by tier " + type.laterTier);
        }
    }

    /**
     * Invariant 6, the declared refusal: when the type declares it is refused at one of the guarded
     * sites the path reaches, the path must have failed at setup with that site's refusal, naming
     * the type. The caller runs the path under {@code assertMemoryLeak}, which checks that the
     * refusing factory released what it held. Returns false, checking nothing, when the type
     * declares none of the sites, so the path's other invariants apply.
     *
     * @param error what the path failed with, null when it ran
     * @param sites the guarded sites the path reaches, labelled as {@link #PLACES_FILE} labels them
     */
    public static boolean assertDeclaredRefusal(TypeConformanceTypes.Entry type, String row, String path, String mode, @Nullable CharSequence error, String... sites) {
        if (!isDeclaredRefused(type, sites)) {
            return false;
        }
        final StringBuilder expected = new StringBuilder();
        for (String site : sites) {
            if (!type.laterRefusedSites.contains(site)) {
                continue;
            }
            final String refusal = refusalOf(type, site);
            if (error != null && error.toString().contains(refusal)) {
                return true;
            }
            expected.append(expected.length() == 0 ? "" : " or ").append(refusal);
        }
        if (error == null) {
            Assert.fail(context(type, row, path, mode) + ": declared refused (" + expected + "), but the path ran");
        }
        Assert.fail(context(type, row, path, mode) + ": declared refused (" + expected + "), but failed with: " + error);
        return true;
    }

    /**
     * Invariant 3: the rows that read as NULL sort together where every existing type with a NULL
     * puts it: lowest (first ascending, last descending), except that a float tier sorts it
     * highest, as FLOAT and DOUBLE sort NaN. The other rows come in the order the type's
     * arithmetic tier implies: signed or unsigned integers, or floats where every NaN is one value
     * above +Infinity and -0.0 equals 0.0. A WIDE or NONE tier implies no order, so the order of
     * the other rows is not checked.
     *
     * @param nullLabels the rows that read as NULL, from the rows as written and the NULL policy
     */
    public static void assertOrdered(
            TypeConformanceTypes.Entry type,
            String path,
            String mode,
            ObjList<String> labels,
            ObjList<long[]> bits,
            Set<String> nullLabels,
            boolean ascending
    ) {
        int nulls = 0;
        for (int i = 0, n = labels.size(); i < n; i++) {
            if (nullLabels.contains(labels.getQuick(i))) {
                nulls++;
            }
        }
        final boolean isNullFirst = ascending != type.isFloat();
        for (int i = 0, n = labels.size(); i < n; i++) {
            final boolean isNullPlace = isNullFirst ? i < nulls : i >= n - nulls;
            if (nullLabels.contains(labels.getQuick(i)) != isNullPlace) {
                Assert.fail(context(type, labels.getQuick(i), path, mode) + ": " + (ascending ? "ascending" : "descending")
                        + " order must put the rows that read as NULL " + (isNullFirst ? "first" : "last")
                        + " (NULL sorts lowest, and highest for a float tier, as the existing types sort it), but reads "
                        + labels + ", NULL rows " + nullLabels);
            }
        }
        if (type.laterTier == null) {
            return;
        }
        String previousLabel = null;
        long[] previous = null;
        for (int i = 0, n = labels.size(); i < n; i++) {
            final String label = labels.getQuick(i);
            if (nullLabels.contains(label)) {
                continue;
            }
            final long[] current = bits.getQuick(i);
            if (previous != null) {
                final int cmp = compare(type, previous, current);
                if (ascending ? cmp > 0 : cmp < 0) {
                    Assert.fail(context(type, label, path, mode) + ": " + (ascending ? "ascending" : "descending")
                            + " order by tier " + type.laterTier + " breaks between " + previousLabel + " and " + label);
                }
            }
            previousLabel = label;
            previous = current;
        }
    }

    /**
     * The rule of the declared relations that admits an explicit cast between two types: W
     * (built-in widening), C (widening cast) or N (narrowing); null when none does.
     */
    @Nullable
    public static String castRule(int fromType, int toType) {
        final short from = ColumnType.tagOf(fromType);
        final short to = ColumnType.tagOf(toType);
        if (contains(RelationRules.builtInWidening(from), to)) {
            return "W";
        }
        if (contains(RelationRules.wideningCast(from), to)) {
            return "C";
        }
        if (contains(RelationRules.narrowing(from), to)) {
            return "N";
        }
        return null;
    }

    /**
     * Compares two values of a type with a declared tier by the order the tier implies (invariant
     * 3): signed or unsigned integers, most significant long first, or floats where every NaN is
     * one value above +Infinity and -0.0 equals 0.0.
     */
    public static int compare(TypeConformanceTypes.Entry type, long[] a, long[] b) {
        final String tier = type.laterTier;
        assert tier != null;
        if (tier.startsWith("F")) {
            final double x = "F32".equals(tier) ? Float.intBitsToFloat((int) a[0]) : Double.longBitsToDouble(a[0]);
            final double y = "F32".equals(tier) ? Float.intBitsToFloat((int) b[0]) : Double.longBitsToDouble(b[0]);
            if (Double.isNaN(x) || Double.isNaN(y)) {
                return Boolean.compare(Double.isNaN(x), Double.isNaN(y));
            }
            return Double.compare(x == 0 ? 0.0 : x, y == 0 ? 0.0 : y);
        }
        // integers, most significant long first; the rows hold the value's width only
        final int bitsWide = Integer.parseInt(tier.substring(1));
        for (int i = 3; i >= 0; i--) {
            long x = a[i];
            long y = b[i];
            if (i * 64 < bitsWide && bitsWide - i * 64 < 64 && tier.startsWith("I")) {
                // sign-extend the top long of a signed value narrower than 64 bits in that long
                final int shift = 64 - (bitsWide - i * 64);
                x = x << shift >> shift;
                y = y << shift >> shift;
            }
            if (x != y) {
                final boolean isTopLong = (i + 1) * 64 >= bitsWide;
                return tier.startsWith("I") && isTopLong ? Long.compare(x, y) : Long.compareUnsigned(x, y);
            }
        }
        return 0;
    }

    /**
     * The NULL row's write error in the setup steps of a type registered later, or null when writing
     * it succeeded. A later type's value rows are raw bits except the NULL row, so the error of a
     * literal INSERT ({@link TypeConformanceValues#writeRows}) is that row's. Only a NULL policy that
     * refuses NULL could let it fail, and the kit knows none, so every {@code error: } line fails
     * the path, naming the steps.
     */
    @Nullable
    public static String nullRowWriteError(TypeConformanceTypes.Entry type, String path, String mode, CharSequence steps) {
        String nullError = null;
        boolean isOtherError = false;
        for (String line : steps.toString().split("\n")) {
            if (line.startsWith("error: insert ")) {
                if (nullError == null) {
                    nullError = line;
                }
            } else if (line.startsWith("error: ")) {
                isOtherError = true;
            }
        }
        if (isOtherError || nullError != null) {
            Assert.fail(context(type, "-", path, mode) + ": " + steps);
        }
        return nullError;
    }

    public static String context(TypeConformanceTypes.Entry type, String row, String path, String mode) {
        return "type=" + type.label + " row=" + row + " path=" + path + " mode=" + mode;
    }

    /**
     * The guarded sites a type can declare it is refused at: the labels of the rows of
     * {@link #PLACES_FILE} decided {@code refused}.
     */
    public static synchronized Set<String> declarableSites() {
        if (declarableSites == null) {
            declarableSites = readDeclarableSites();
        }
        return declarableSites.keySet();
    }

    /**
     * Whether two values of a target type are the same value: equal bits, or for a float kind both
     * NaN.
     */
    public static boolean isSameValue(RelationKind kind, int width, long[] a, long[] b) {
        if (Arrays.equals(a, b)) {
            return true;
        }
        if (kind != RelationKind.FLOAT) {
            return false;
        }
        return width == 4
                ? Float.isNaN(Float.intBitsToFloat((int) a[0])) && Float.isNaN(Float.intBitsToFloat((int) b[0]))
                : Double.isNaN(Double.longBitsToDouble(a[0])) && Double.isNaN(Double.longBitsToDouble(b[0]));
    }

    /**
     * Whether a kit path and mode runs for the type: always for an existing type; for a type
     * registered later, when a pattern of its resource line matches. A later tag without a
     * resource line fails here, so no kit class passes it silently.
     */
    /**
     * Whether the type declares it is refused at one of the guarded sites a path reaches; always
     * false for an existing type.
     */
    public static boolean isDeclaredRefused(TypeConformanceTypes.Entry type, String... sites) {
        for (String site : sites) {
            if (type.laterRefusedSites.contains(site)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isEnabled(TypeConformanceTypes.Entry type, String path, String mode) {
        if (!type.isLater()) {
            // a type no table can hold (INTERVAL, VARCHAR_SLICE) fails every path at CREATE TABLE:
            // it runs one such path, for the refusal, and its bind values
            return ColumnType.isPersisted(ColumnType.tagOf(type.columnType)) || NOT_PERSISTED_PATHS.contains(path);
        }
        if (type.laterPaths == null) {
            Assert.fail("type=" + type.label + " path=" + path + " mode=" + mode + ": registered later, with no recording, but "
                    + TypeConformanceTypes.LATER_TYPES_RESOURCE + " declares neither its NULL policy nor its paths");
        }
        for (String pattern : type.laterPaths.split("\\s+")) {
            if (pattern.isEmpty() || "-".equals(pattern)) {
                continue;
            }
            final int at = pattern.indexOf('@');
            final String pathPattern = at > -1 ? pattern.substring(0, at) : pattern;
            final String modePattern = at > -1 ? pattern.substring(at + 1) : "*";
            if (glob(pathPattern).matcher(path).matches() && glob(modePattern).matcher(mode).matches()) {
                return true;
            }
        }
        return false;
    }

    public static RelationKind kindOf(int columnType) {
        return ColumnType.getTypeDriver(columnType).getRelationKind();
    }

    /**
     * The mixing cases (invariant 4): {@code mix|<name>|<sql>|<expected>} lines of the resource,
     * where {@code \n} and {@code \t} in the expected text stand for newline and tab. Each entry is
     * {name, sql, expected}.
     */
    public static ObjList<String[]> mixingCases() {
        final ObjList<String[]> cases = new ObjList<>();
        try (InputStream in = TypeConformanceInvariants.class.getResourceAsStream(TypeConformanceTypes.LATER_TYPES_RESOURCE)) {
            if (in == null) {
                return cases;
            }
            final BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.startsWith(MIX_PREFIX)) {
                    continue;
                }
                final String[] parts = line.split("\\|", -1);
                if (parts.length != 4) {
                    throw new IllegalStateException("bad line in " + TypeConformanceTypes.LATER_TYPES_RESOURCE + ": " + line);
                }
                cases.add(new String[]{parts[1].trim(), parts[2].trim(), parts[3].trim().replace("\\n", "\n").replace("\\t", "\t")});
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return cases;
    }

    /**
     * The type's NULL policy: SENTINEL or NONE for an existing type, from its type driver; for a
     * type registered later, as its resource line declares it.
     */
    public static String policyOf(TypeConformanceTypes.Entry type) {
        if (type.laterPolicy != null) {
            return type.laterPolicy;
        }
        if (type.isLater()) {
            Assert.fail("type=" + type.label + ": registered later, with no recording, but "
                    + TypeConformanceTypes.LATER_TYPES_RESOURCE + " declares no NULL policy for it");
        }
        return switch (ColumnType.getTypeDriver(type.columnType).getNullPolicy()) {
            case SENTINEL -> POLICY_SENTINEL;
            case NONE -> POLICY_NONE;
        };
    }

    /**
     * The refusal a guarded site raises for the type: {@code no family arm for <type> at <site>},
     * or the error a site that keeps one names in {@link #PLACES_FILE}, with the type's name in
     * place of {@code <type>}.
     */
    public static String refusalOf(TypeConformanceTypes.Entry type, String site) {
        declarableSites();
        final String message = declarableSites.get(site);
        if (message == null) {
            throw new IllegalStateException(site + " is not a guarded site of " + PLACES_FILE);
        }
        return message.replace(TYPE_PLACEHOLDER, ColumnType.nameOf(type.columnType));
    }

    /**
     * The value a widening (rule W) gives for a row of a type with a declared tier: an integer
     * tier's value, sign- or zero-extended, into an integer or temporal target of
     * {@code targetWidth} bytes, or converted into a float target; a float tier's value into a
     * float target. Null where the tier and the target give no expectation.
     */
    public static long @Nullable [] widened(TypeConformanceTypes.Entry type, long[] bits, RelationKind targetKind, int targetWidth) {
        final String tier = type.laterTier;
        if (tier == null || targetWidth <= 0 || targetWidth > 8) {
            return null;
        }
        final long[] out = new long[4];
        if (tier.startsWith("F")) {
            if (targetKind != RelationKind.FLOAT) {
                return null;
            }
            final double value = "F32".equals(tier) ? Float.intBitsToFloat((int) bits[0]) : Double.longBitsToDouble(bits[0]);
            out[0] = targetWidth == 4 ? Float.floatToRawIntBits((float) value) & 0xFFFF_FFFFL : Double.doubleToRawLongBits(value);
            return out;
        }
        final int bitsWide = Integer.parseInt(tier.substring(1));
        final boolean isSigned = tier.startsWith("I");
        if (bitsWide > 64 || (!isSigned && bitsWide == 64 && targetKind != RelationKind.INT && targetKind != RelationKind.TEMPORAL)) {
            return null;
        }
        long value = bits[0];
        if (bitsWide < 64) {
            value = isSigned ? value << (64 - bitsWide) >> (64 - bitsWide) : value & ((1L << bitsWide) - 1);
        }
        switch (targetKind) {
            case INT, TEMPORAL -> out[0] = targetWidth == 8 ? value : value & ((1L << (targetWidth * 8)) - 1);
            case FLOAT -> out[0] = targetWidth == 4
                    ? Float.floatToRawIntBits((float) value) & 0xFFFF_FFFFL
                    : Double.doubleToRawLongBits((double) value);
            default -> {
                return null;
            }
        }
        return out;
    }

    // the fixed width of a type in bytes, -1 for a var-size type
    public static int widthOf(int columnType) {
        final TypeDriver driver = ColumnType.getTypeDriver(columnType);
        return driver instanceof FixedSizeTypeDriver fixed ? fixed.getMovement().size() : -1;
    }

    private static void assertNoError(TypeConformanceTypes.Entry type, String row, String path, String mode, @Nullable String error) {
        if (error != null) {
            Assert.fail(context(type, row, path, mode) + ": " + policyOf(type) + ", writing NULL must succeed, but failed: " + error);
        }
    }

    private static boolean contains(short[] row, short tag) {
        for (short t : row) {
            if (t == tag) {
                return true;
            }
        }
        return false;
    }

    private static Map<String, String> readDeclarableSites() {
        final File file = findPlacesFile();
        final List<String> lines;
        try {
            lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("cannot read " + file, e);
        }
        if (lines.isEmpty()) {
            throw new IllegalStateException(file + " is empty");
        }
        final List<String> header = Arrays.asList(lines.get(0).split("\t", -1));
        final int decisionColumn = header.indexOf("decision");
        final int reasonColumn = header.indexOf("reason");
        if (decisionColumn < 0 || reasonColumn < 0) {
            throw new IllegalStateException(file + ": the header must name the decision and reason columns: " + lines.get(0));
        }
        final Map<String, String> sites = new HashMap<>();
        for (int i = 1, n = lines.size(); i < n; i++) {
            final String[] cells = lines.get(i).split("\t", -1);
            if (cells.length != header.size()) {
                throw new IllegalStateException(file + ":" + (i + 1) + ": " + cells.length + " cells, the header has " + header.size());
            }
            if (!"refused".equals(cells[decisionColumn])) {
                continue;
            }
            final String reason = cells[reasonColumn];
            final int separator = reason.indexOf(KEPT_REFUSAL_SEPARATOR);
            final String label = separator < 0 ? reason : reason.substring(0, separator);
            sites.putIfAbsent(label, separator < 0 ? FAMILY_ARM_REFUSAL + label : reason.substring(separator + KEPT_REFUSAL_SEPARATOR.length()));
        }
        return Collections.unmodifiableMap(sites);
    }

    private static boolean isNaN(TypeConformanceTypes.Entry type, long @Nullable [] bits) {
        if (bits == null) {
            return false;
        }
        return "F32".equals(type.laterTier) ? Float.isNaN(Float.intBitsToFloat((int) bits[0])) : Double.isNaN(Double.longBitsToDouble(bits[0]));
    }

    // the repository root of the test checkout: the first directory up from the working one that holds the decisions
    private static File findPlacesFile() {
        for (File dir = new File(System.getProperty("user.dir")).getAbsoluteFile(); dir != null; dir = dir.getParentFile()) {
            final File file = new File(dir, PLACES_FILE);
            if (file.isFile()) {
                return file;
            }
        }
        throw new IllegalStateException("no " + PLACES_FILE + " above " + System.getProperty("user.dir"));
    }

    private static Pattern glob(String pattern) {
        final StringBuilder regex = new StringBuilder();
        for (int i = 0, n = pattern.length(); i < n; i++) {
            final char c = pattern.charAt(i);
            if (c == '*') {
                regex.append(".*");
            } else {
                regex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return Pattern.compile(regex.toString());
    }

    private static String hex(long @Nullable [] bits) {
        if (bits == null) {
            return "null";
        }
        final StringBuilder sb = new StringBuilder("0x");
        for (int i = bits.length - 1; i >= 0; i--) {
            final String h = Long.toHexString(bits[i]);
            for (int pad = h.length(); pad < 16; pad++) {
                sb.append('0');
            }
            sb.append(h);
        }
        return sb.toString();
    }
}
