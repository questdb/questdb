/*
 * @(#)FastDoubleParserHandPickedTest.java
 * Copyright © 2021. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.NumericException;

public class FastFloatParserFromCharArrayHandPickedTest extends AbstractFloatHandPickedTest {
    @Override
    float parse(CharSequence str) throws NumericException {
        char[] chars = new char[str.length()];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = str.charAt(i);
        }
        return FastFloatParser.parseFloat(chars);
    }

    @Override
    protected float parse(String str, int offset, int length) throws NumericException {
        return FastFloatParser.parseFloat(str.toCharArray(), offset, length);
    }
}
