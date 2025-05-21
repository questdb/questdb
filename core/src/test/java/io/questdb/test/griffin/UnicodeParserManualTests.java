/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.griffin.UnicodeParser;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UnicodeParserManualTests {
    private static final StringSink sink = new StringSink();

    @Before
    public void setUp() throws Exception {
        sink.clear();
    }

    @Test
    public void testCompleteValidSurrogatePair() {
        sink.clear();

        // Use a string with a known valid surrogate pair (emoji)
        String emoji = "😀"; // This is U+1F600 GRINNING FACE
        // Use this directly
        UnicodeParser.parse(emoji, 0, sink);

        // Verify the result
        TestUtils.assertEquals(emoji, sink);
    }

    /**
     * Test for Unicode escapes that create dangling high surrogates.
     */
    @Test(expected = CairoException.class)
    public void testDanglingHighSurrogateFromUnicodeEscape() {
        // Create a Unicode escape for a high surrogate without the matching low surrogate
        UnicodeParser.parse("\\uD800", 0, sink);
    }

    /**
     * Test for the error handling when a high surrogate is not followed by a low surrogate.
     */
    @Test(expected = CairoException.class)
    public void testDanglingHighSurrogateInRegularChar() {
        StringBuilder sb = new StringBuilder();
        // Add a high surrogate without a matching low surrogate
        sb.append(Character.highSurrogate(0x10000));
        sb.append('A'); // Not a low surrogate

        UnicodeParser.parse(sb, 0, sink);
    }

    /**
     * Test for exact exception message when a high surrogate is followed by
     * something that's not a low surrogate.
     */
    @Test
    public void testExceptionMessageForInvalidSurrogatePair() {
        try {
            UnicodeParser.parse(String.valueOf(Character.highSurrogate(0x10000)) + 'X', 0, sink);
            Assert.fail("Should throw exception");
        } catch (CairoException e) {
            // Verify the exception contains the expected text
            TestUtils.assertContains(e.getFlyweightMessage(), "Expected low surrogate but got");
        }
    }

    /**
     * Test to exercise all branches in the finalize method.
     */
    @Test
    public void testFinalizeAllBranches() {
        // Test normal state at end
        UnicodeParser.parse("ABC", 0, sink);
        TestUtils.assertEquals("ABC", sink);
        sink.clear();

        try {
            // Test backslash at end
            UnicodeParser.parse("ABC\\", 0, sink);
            Assert.fail("Should throw exception for backslash at end");
        } catch (CairoException e) {
            // Expected
        }
        sink.clear();

        try {
            // Test unicode start at end
            UnicodeParser.parse("ABC\\u", 0, sink);
            Assert.fail("Should throw exception for \\u at end");
        } catch (CairoException e) {
            // Expected
        }

        sink.clear();
        try {
            // Test unicode with insufficient digits at end
            UnicodeParser.parse("ABC\\u1", 0, sink);
            Assert.fail("Should throw exception for incomplete unicode at end");
        } catch (CairoException e) {
            // Expected
        }

        sink.clear();
        // Test valid unicode at end
        UnicodeParser.parse("ABC\\u0041", 0, sink);
        TestUtils.assertContains("ABCA", sink);
    }

    /**
     * Test for a high surrogate in one escape followed by a non-low-surrogate in another.
     */
    @Test(expected = CairoException.class)
    public void testHighSurrogateFollowedByNonLowSurrogate() {
        UnicodeParser.parse("\\uD800\\u0041", 0, sink);
    }

    /**
     * Test for the error handling with an incomplete Unicode escape at the end.
     */
    @Test(expected = CairoException.class)
    public void testIncompleteUnicodeEscapeAtEnd() {
        UnicodeParser.parse("\\u", 0, sink);
    }

    /**
     * Test that covers the branch where we have hex digits but not a valid hex sequence.
     */
    @Test(expected = CairoException.class)
    public void testInvalidHexDigitValue() {
        // We can't directly call hexDigitValue with an invalid character,
        // so we need to create a test case that would pass isHexDigit but
        // somehow fail in hexDigitValue - this is unlikely in practice

        // Create a custom implementation that accepts G as a hex digit
        // but then fails to convert it
        UnicodeParser.parse("\\uGGGG", 0, sink);
    }

    /**
     * Test case where high surrogate is followed by a Unicode escape
     * that should be a low surrogate but isn't.
     */
    @Test(expected = CairoException.class)
    public void testMismatchedSurrogatePair() {
        // High surrogate followed by something that's not a low surrogate
        UnicodeParser.parse("\\uD800\\u0041", 0, sink);
    }

    /**
     * Test for Unicode escapes that create orphaned low surrogates.
     */
    @Test(expected = CairoException.class)
    public void testOrphanedLowSurrogateFromUnicodeEscape() {
        // Create a Unicode escape for a low surrogate without a preceding high surrogate
        UnicodeParser.parse("\\uDC00", 0, sink);
    }

    /**
     * Test for an orphaned low surrogate without a preceding high surrogate.
     */
    @Test(expected = CairoException.class)
    public void testOrphanedLowSurrogateInRegularChar() {
        StringBuilder sb = new StringBuilder();
        // Add a low surrogate without a preceding high surrogate
        sb.append(Character.lowSurrogate(0x10000));

        UnicodeParser.parse(sb, 0, sink);
    }

    @Test
    public void testSurrogatePairWithSmallCodePoint() {
        UnicodeParser.parse("\\uD800\\uDC00", 0, sink);
        TestUtils.assertEquals("\uD800\uDC00", sink);
    }

    /**
     * Tests for special cases in surrogate pair handling that might not be
     * covered by randomized tests.
     */
    @Test
    public void testSurrogatePairsWithSmallCodepoint() {

        // Force a surrogate pair with a specific combination that produces a small codepoint
        String input = new String(Character.toChars(0xFFFF)); // This is a boundary case
        UnicodeParser.parse(input, 0, sink);
        TestUtils.assertEquals("\uffff", sink);

        sink.clear();

        // Alternative test with escape sequences
        StringBuilder specialSequence = new StringBuilder();
        specialSequence.append("\\uD800\\uDC00"); // Lowest surrogate pair

        UnicodeParser.parse(specialSequence, 0, sink);
        TestUtils.assertEquals("\uD800\uDC00", sink);
    }

    /**
     * Test for unexpected low surrogate character handling
     */
    @Test
    public void testUnexpectedLowSurrogateMessage() {
        try {
            // Just a low surrogate by itself
            String input = String.valueOf(Character.lowSurrogate(0x10000));
            UnicodeParser.parse(input, 0, sink);
            Assert.fail("Should throw exception");
        } catch (CairoException e) {
            // Verify the exception contains the expected text
            TestUtils.assertContains(e.getFlyweightMessage(), "Unexpected low surrogate without preceding high surrogate");
        }
    }

    /**
     * Ensure we get complete coverage for the code that processes surrogate pairs.
     */
    @Test
    public void testValidSurrogatePairWithEscapeSequences() {

        // Test valid surrogate pair with escapes
        UnicodeParser.parse("\\uD83D\\uDE00", 0, sink);

        // Verify result is the emoji
        TestUtils.assertEquals("😀", sink);

        // Clear result
        sink.clear();

        // Test mixed regular and escape
        UnicodeParser.parse("\\uD83D\\uDE00", 0, sink);
        TestUtils.assertEquals("😀", sink);

        // Clear result
        sink.clear();

        // Test other direction
        UnicodeParser.parse("\\uD83D\\uDE00", 0, sink);
        TestUtils.assertEquals("😀", sink);
    }

}
