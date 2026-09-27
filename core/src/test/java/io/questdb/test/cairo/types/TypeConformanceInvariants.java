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
 * <li>every value reads back as written ({@link #assertReadsBackAsWritten});</li>
 * <li>the NULL row and the sentinel-pattern row behave as the NULL policy says
 * ({@link #assertNullPolicy}): SENTINEL, both read the same; NONE, the NULL row reads as false
 * or 0; BITMAP, the two stay distinct; NOT_NULL, writing NULL fails with a clear error and the
 * sentinel-pattern row reads back as a value; another type's sentinel pattern of the same
 * width reads as a value except under SENTINEL ({@link #assertOtherSentinel});</li>
 * <li>rows compare and sort by the order the type's arithmetic tier implies: from S14b, when
 * the definition answers the tier;</li>
 * <li>design-proof mixing cases give the results the R9 note states: the resource's
 * {@code mix|<name>|<sql>|<expected>} lines ({@link #mixingCases()}), which the SQL class runs.</li>
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
        if (!Arrays.equals(written, read)) {
            Assert.fail(context(type, row, path, mode) + ": reads back " + hex(read) + ", written " + hex(written));
        }
    }

    public static String context(TypeConformanceTypes.Entry type, String row, String path, String mode) {
        return "type=" + type.label + " row=" + row + " path=" + path + " mode=" + mode;
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
        return io.questdb.cairo.ColumnType.getTypeDriver(type.columnType).hasNullSentinel() ? POLICY_SENTINEL : POLICY_NONE;
    }

    private static void assertNoError(TypeConformanceTypes.Entry type, String row, String path, String mode, @Nullable String error) {
        if (error != null) {
            Assert.fail(context(type, row, path, mode) + ": " + policyOf(type) + ", writing NULL must succeed, but failed: " + error);
        }
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
