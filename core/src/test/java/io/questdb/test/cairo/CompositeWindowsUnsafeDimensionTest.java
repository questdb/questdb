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

    private static void assertEscaped(char c, String expected) {
        final StringSink sink = new StringSink();
        TableUtils.putPathSafe(sink, String.valueOf(c));
        Assert.assertEquals("character '" + c + "' must be escaped in a cell directory name",
                expected, sink.toString());
    }
}
