/*
 * @(#)FastDoubleParserLexcicallyGeneratedTest.java
 * Copyright © 2021. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.NumericException;

import static org.junit.Assert.assertEquals;

public class FastDoubleParserFromCharSequenceLexicallyGeneratedTest extends AbstractLexicallyGeneratedTest {
    @Override
    protected void testAgainstJdk(String str) {
        double expected = 0.0;
        boolean isExpectedToFail = false;
        try {
            expected = Double.parseDouble(str);
            // our double parser rejects 'f' suffix
            String ss = str.trim();
            int l = ss.length();
            if (l > 0 && (ss.charAt(l - 1) | 32) == 'f') {
                isExpectedToFail = true;
            }
        } catch (NumberFormatException t) {
            isExpectedToFail = true;
        }

        double actual = 0;
        boolean actualFailed = false;
        try {
            actual = FastDoubleParser.parseDouble(str);
        } catch (NumericException t) {
            actualFailed = true;
        }

        assertEquals(str, isExpectedToFail, actualFailed);
        if (!isExpectedToFail) {
            assertEquals("str=" + str, expected, actual, 0.001);
            assertEquals("longBits of " + expected, Double.doubleToLongBits(expected), Double.doubleToLongBits(actual));
        }
    }
}
