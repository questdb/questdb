/*
 * @(#)FastDoubleParserLexcicallyGeneratedTest.java
 * Copyright © 2021. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.NumericException;

import static org.junit.Assert.assertEquals;

public class FastDoubleParserFromCharArrayLexicallyGeneratedTest extends AbstractLexicallyGeneratedTest {
    @Override
    protected void testAgainstJdk(String str) {
        double expected = 0.0;
        boolean isExpectedToFail = false;
        try {
            expected = Double.parseDouble(str);
        } catch (NumberFormatException t) {
            isExpectedToFail = true;
        }

        double actual = 0;
        boolean actualFailed = false;
        try {
            actual = FastDoubleParser.parseDouble(str.toCharArray());
        } catch (NumericException t) {
            actualFailed = true;
        }

        assertEquals(isExpectedToFail, actualFailed);
        if (!isExpectedToFail) {
            assertEquals("str=" + str, expected, actual, 0.001);
            assertEquals("longBits of " + expected, Double.doubleToLongBits(expected), Double.doubleToLongBits(actual));
        }
    }
}
