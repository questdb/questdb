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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Checks a type registered after the S12 recording, which has no recording of its own (F28,
 * {@code contracts/conformance-kit.md} section 4). The {@code TypeConformance*Test} classes
 * call it for every type whose {@link TypeConformanceTypes.Entry#isLater()} is true; it needs
 * no per-type test code.
 * <p>
 * Paths: a later type runs on the paths its resource line lists
 * ({@link TypeConformanceTypes#LATER_TYPES_RESOURCE}); tracer types list F31's paths of the
 * converted layers (F26's at the final tip), design-proof types the paths E3 implements. A
 * pattern is {@code <path>} or {@code <path>@<mode>}, where {@code *} matches any text, and
 * {@code -} lists none. A later tag without a resource line fails every kit class.
 * <p>
 * The invariants:
 * <ol>
 * <li>every value reads back as written ({@link #assertReadsBackAsWritten}), bit for bit, except
 * that for a float tier every NaN is the same value;</li>
 * <li>the NULL row and the sentinel-pattern row behave as the NULL policy says
 * ({@link #assertNullPolicy}): SENTINEL, both read the same; NONE, the NULL row reads as false
 * or 0; BITMAP, the two stay distinct; NOT_NULL, writing NULL fails with a clear error and the
 * sentinel-pattern row reads back as a value; another type's sentinel pattern of the same
 * width reads as a value except under SENTINEL ({@link #assertOtherSentinel});</li>
 * <li>rows compare and sort by the order the type's arithmetic tier implies
 * ({@link #assertOrdered}), when the resource declares the tier; from S14b the definition
 * answers it;</li>
 * <li>design-proof mixing cases give the results the R9 note states: the resource's
 * {@code mix|<name>|<sql>|<expected>} lines ({@link #mixingCases()}), which the SQL class runs;</li>
 * <li>the relation paths follow the type's declared relations (F89): a cast, CASE branch,
 * ALTER COLUMN TYPE target or dedup key the rules admit runs, and gives what the rules and the
 * declared tier imply ({@link #castRule}, {@link #widened}); the SQL and storage classes state
 * each path's checks.</li>
 * </ol>
 * Every failure message names the type, the value row, the path and the mode.
 */
public final class TypeConformanceInvariants {
    public static final String POLICY_BITMAP = "BITMAP";
    public static final String POLICY_NONE = "NONE";
    public static final String POLICY_NOT_NULL = "NOT_NULL";
    public static final String POLICY_SENTINEL = "SENTINEL";
    private static final String MIX_PREFIX = "mix|";

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
            case POLICY_BITMAP -> {
                assertNoError(type, "null", path, mode, nullError);
                Assert.assertNotEquals(context(type, "null", path, mode) + ": BITMAP, the NULL row and the sentinel-pattern row must stay distinct", sentinelText, nullText);
                Assert.assertArrayEquals(context(type, "sentinel", path, mode) + ": BITMAP, the sentinel pattern is a value", writtenBits, sentinelBits);
            }
            case POLICY_NOT_NULL -> {
                if (nullError == null || nullError.isEmpty()) {
                    Assert.fail(context(type, "null", path, mode) + ": NOT_NULL, writing NULL must fail with an error");
                }
                if (!nullError.toLowerCase().contains("null")) {
                    Assert.fail(context(type, "null", path, mode) + ": NOT_NULL, the error must say NULL: " + nullError);
                }
                Assert.assertArrayEquals(context(type, "sentinel", path, mode) + ": NOT_NULL, the sentinel pattern is a value", writtenBits, sentinelBits);
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
            // every NaN is the same float value (PA-13); a path may carry any NaN pattern
            return;
        }
        if (!Arrays.equals(written, read)) {
            Assert.fail(context(type, row, path, mode) + ": reads back " + hex(read) + ", written " + hex(written));
        }
    }

    /**
     * Invariant 3: rows other than the NULL row come in the order the declared arithmetic tier
     * implies: signed or unsigned integers, or floats where every NaN is one value above
     * +Infinity and -0.0 equals 0.0 (PA-13). Without a declared tier the order is not checked;
     * the definition answers the tier from S14b.
     */
    public static void assertOrdered(TypeConformanceTypes.Entry type, String path, String mode, ObjList<String> labels, ObjList<long[]> bits, boolean ascending) {
        if (type.laterTier == null) {
            return;
        }
        String previousLabel = null;
        long[] previous = null;
        for (int i = 0, n = labels.size(); i < n; i++) {
            final String label = labels.getQuick(i);
            if ("null".equals(label)) {
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
     * The NULL row's write error in the setup steps of a type registered later, or null when writing
     * it succeeded. A later type's value rows are raw bits except the NULL row, so the error of a
     * literal INSERT ({@link TypeConformanceValues#writeRows}) is that row's. Only NOT_NULL may
     * refuse it; any other {@code error: } line fails the path under every policy, naming the steps.
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
        if (isOtherError || (nullError != null && !POLICY_NOT_NULL.equals(policyOf(type)))) {
            Assert.fail(context(type, "-", path, mode) + ": " + steps);
        }
        return nullError;
    }

    public static String context(TypeConformanceTypes.Entry type, String row, String path, String mode) {
        return "type=" + type.label + " row=" + row + " path=" + path + " mode=" + mode;
    }

    /**
     * Whether two values of a target type are the same value: equal bits, or for a float kind
     * both NaN (PA-13).
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
    public static boolean isEnabled(TypeConformanceTypes.Entry type, String path, String mode) {
        if (!type.isLater()) {
            return true;
        }
        if (type.laterPaths == null) {
            Assert.fail("type=" + type.label + " path=" + path + " mode=" + mode + ": registered after the S12 recording, but "
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
     * The design-proof mixing cases (invariant 4): {@code mix|<name>|<sql>|<expected>} lines of
     * the resource, where {@code \n} and {@code \t} in the expected text stand for newline and
     * tab. Each entry is {name, sql, expected}.
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
     * The type's NULL policy: SENTINEL or NONE for an existing type, from its definition; for a
     * type registered later, as its resource line declares it.
     */
    public static String policyOf(TypeConformanceTypes.Entry type) {
        if (type.laterPolicy != null) {
            return type.laterPolicy;
        }
        if (type.isLater()) {
            Assert.fail("type=" + type.label + ": registered after the S12 recording, but "
                    + TypeConformanceTypes.LATER_TYPES_RESOURCE + " declares no NULL policy for it");
        }
        return switch (ColumnType.getTypeDriver(type.columnType).getNullPolicy()) {
            case SENTINEL -> POLICY_SENTINEL;
            case NONE -> POLICY_NONE;
        };
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
        return driver instanceof FixedSizeTypeDriver fixed ? fixed.getWidth() : -1;
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

    private static int compare(TypeConformanceTypes.Entry type, long[] a, long[] b) {
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

    private static boolean isNaN(TypeConformanceTypes.Entry type, long @Nullable [] bits) {
        if (bits == null) {
            return false;
        }
        return "F32".equals(type.laterTier) ? Float.isNaN(Float.intBitsToFloat((int) bits[0])) : Double.isNaN(Double.longBitsToDouble(bits[0]));
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
