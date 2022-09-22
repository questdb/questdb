/*
 * @(#)FastDoubleParserLexcicallyGeneratedTest.java
 * Copyright © 2021. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;

import static org.junit.Assert.assertEquals;

public class FastDoubleParserFromMemLexicallyGeneratedTest extends AbstractLexicallyGeneratedTest {

    @Override
    protected void testAgainstJdk(String str) {
        int len = str.length();
        long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            Chars.asciiStrCpy(str, mem);
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
                actual = FastDoubleParser.parseDouble(mem, len);
            } catch (NumericException t) {
                actualFailed = true;
            }

            assertEquals(isExpectedToFail, actualFailed);
            if (!isExpectedToFail) {
                assertEquals("str=" + str, expected, actual, 0.001);
                assertEquals("longBits of " + expected,
                        Double.doubleToLongBits(expected),
                        Double.doubleToLongBits(actual)
                );
            }
        } finally {
            Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
