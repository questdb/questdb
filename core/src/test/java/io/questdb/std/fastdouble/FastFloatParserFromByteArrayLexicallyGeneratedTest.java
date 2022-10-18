/*
 * @(#)FastDoubleParserLexcicallyGeneratedTest.java
 * Copyright © 2021. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.NumericException;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class FastFloatParserFromByteArrayLexicallyGeneratedTest extends AbstractLexicallyGeneratedTest {
    @Override
    protected void testAgainstJdk(String str) {
        float expected = 0.0f;
        boolean isExpectedToFail = false;
        try {
            expected = Float.parseFloat(str);
        } catch (NumberFormatException t) {
            isExpectedToFail = true;
        }

        float actual = 0;
        boolean actualFailed = false;
        try {
            actual = FastFloatParser.parseFloat(str.getBytes(StandardCharsets.ISO_8859_1), false);
            assertEquals("str=" + str, expected, actual, 0.001);
        } catch (NumericException t) {
            actualFailed = true;
        }

        assertEquals(isExpectedToFail, actualFailed);
        if (!isExpectedToFail) {
            assertEquals("str=" + str, expected, actual, 0.001);
            assertEquals("intBits of " + expected, Float.floatToIntBits(expected), Float.floatToIntBits(actual));
        }
    }
}
