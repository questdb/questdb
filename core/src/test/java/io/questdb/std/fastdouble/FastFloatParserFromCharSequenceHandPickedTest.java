/*
 * @(#)FastDoubleParserHandPickedTest.java
 * Copyright © 2021. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.NumericException;

public class FastFloatParserFromCharSequenceHandPickedTest extends AbstractFloatHandPickedTest {

    @Override
    float parse(CharSequence str) throws NumericException {
        return FastFloatParser.parseFloat(str);
    }

    @Override
    protected float parse(String str, int offset, int length) throws NumericException {
        return FastFloatParser.parseFloat(str, offset, length);
    }
}
