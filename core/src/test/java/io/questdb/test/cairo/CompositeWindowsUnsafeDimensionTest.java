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

package io.questdb.test.cairo;

import io.questdb.cairo.TableUtils;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

/**
 * A composite cell directory name is rendered from ARBITRARY user data -- a SYMBOL value or a
 * TRUNCATE prefix -- so it must not be able to contain a character that is illegal in a filename on
 * a supported platform.
 * <p>
 * {@code putPathSafe} escapes {@code / \ . %} and C0/DEL, which covers POSIX. Windows additionally
 * forbids {@code * ? : | " < >} in a filename, and none of those were escaped: a symbol value of
 * {@code "a?b"} would render a directory name Windows cannot create.
 * <p>
 * This is easy to miss from OSS CI, which is Linux-only -- the enterprise pipeline is the sole
 * Windows signal -- so it is pinned here as a pure unit test on the escaper rather than left to be
 * discovered by a platform run.
 * <p>
 * Escaping preserves injectivity for the same reason the {@code %NULL} and {@code %EMPTY} tokens do:
 * {@code %} is itself escaped to {@code %25}, so a value containing a literal {@code "%2a"} renders
 * {@code "%252a"} and stays distinct from the escaped form of {@code "*"}.
 */
public class CompositeWindowsUnsafeDimensionTest {

    /**
     * The characters POSIX needs, as a CONTROL: if these ever stop being escaped the test below could
     * pass while the escaper had regressed entirely.
     */
    @Test
    public void testPosixUnsafeCharactersStillEscaped() {
        assertEscaped('/', "%2f");
        assertEscaped('\\', "%5c");
        assertEscaped('.', "%2e");
        assertEscaped('%', "%25");
    }

    /**
     * The gap: every character Windows forbids in a filename must be escaped too.
     */
    @Test
    public void testWindowsUnsafeCharactersAreEscaped() {
        assertEscaped('*', "%2a");
        assertEscaped('?', "%3f");
        assertEscaped(':', "%3a");
        assertEscaped('|', "%7c");
        assertEscaped('"', "%22");
        assertEscaped('<', "%3c");
        assertEscaped('>', "%3e");
    }

    /**
     * Ordinary characters must pass through untouched -- an escaper that escaped everything would
     * satisfy the assertions above while making every directory name unreadable.
     */
    @Test
    public void testOrdinaryCharactersPassThrough() {
        final StringSink sink = new StringSink();
        TableUtils.putPathSafe(sink, "BTC-USD_1a");
        Assert.assertEquals("BTC-USD_1a", sink.toString());
    }

    /**
     * Windows RESERVED DEVICE NAMES. A directory cannot be called CON, PRN, AUX, NUL, COM1-9 or
     * LPT1-9 on Windows, whatever its characters are -- the name itself is the problem, so character
     * escaping does not help. These are entirely plausible SYMBOL values: CON and AUX are real
     * ticker and sensor names.
     * <p>
     * Case-insensitive, because the reservation is.
     */
    @Test
    public void testWindowsReservedDeviceNamesAreEscaped() {
        for (String reserved : new String[]{
                "CON", "PRN", "AUX", "NUL", "COM1", "COM9", "LPT1", "LPT9",
                "con", "Nul", "lpt3"
        }) {
            final StringSink sink = new StringSink();
            TableUtils.putPathSafe(sink, reserved);
            Assert.assertNotEquals(
                    "a reserved device name must not render to itself: " + reserved,
                    reserved,
                    sink.toString()
            );
        }
    }

    /**
     * A name that merely CONTAINS a reserved name, or extends it, is fine on Windows and must not be
     * mangled -- otherwise every ticker starting with "CON" pays for the four that matter.
     */
    @Test
    public void testNamesResemblingReservedDevicesPassThrough() {
        for (String ok : new String[]{"CONS", "CONSOLE", "COM", "COM10", "LPT", "NULL", "PRNT"}) {
            final StringSink sink = new StringSink();
            TableUtils.putPathSafe(sink, ok);
            Assert.assertEquals("not a reserved device name: " + ok, ok, sink.toString());
        }
    }

    /**
     * Windows silently strips a TRAILING space from a filename, so two values differing only by one
     * would collide on a single directory -- the exact non-injectivity the %NULL and %EMPTY tokens
     * exist to prevent, arriving through the filesystem instead of the renderer.
     */
    @Test
    public void testTrailingSpaceIsEscaped() {
        final StringSink sink = new StringSink();
        TableUtils.putPathSafe(sink, "abc ");
        Assert.assertEquals("abc%20", sink.toString());

        // An INTERIOR space is legal on Windows and must survive: mangling it would rename every
        // two-word symbol value for no reason.
        final StringSink interior = new StringSink();
        TableUtils.putPathSafe(interior, "New York");
        Assert.assertEquals("New York", interior.toString());
    }

    /**
     * Injectivity across the two new transforms. Both escape into the {@code %} space, which real
     * values can never reach because {@code %} is itself escaped -- so the value that LOOKS like an
     * escaped reserved name, and the reserved name itself, must render differently.
     */
    @Test
    public void testReservedAndTrailingSpaceEscapesStayInjective() {
        final StringSink reserved = new StringSink();
        TableUtils.putPathSafe(reserved, "CON");
        final StringSink literal = new StringSink();
        TableUtils.putPathSafe(literal, reserved.toString());
        Assert.assertNotEquals(
                "a literal value equal to the escaped form must not collide with it",
                reserved.toString(),
                literal.toString()
        );

        final StringSink spaced = new StringSink();
        TableUtils.putPathSafe(spaced, "abc ");
        final StringSink spacedLiteral = new StringSink();
        TableUtils.putPathSafe(spacedLiteral, "abc%20");
        Assert.assertNotEquals(spaced.toString(), spacedLiteral.toString());
    }

    private static void assertEscaped(char c, String expected) {
        final StringSink sink = new StringSink();
        TableUtils.putPathSafe(sink, String.valueOf(c));
        Assert.assertEquals("character '" + c + "' must be escaped in a cell directory name",
                expected, sink.toString());
    }
}
