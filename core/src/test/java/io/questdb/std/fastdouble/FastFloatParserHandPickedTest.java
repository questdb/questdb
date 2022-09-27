/*
 * @(#)FastDoubleParserHandPickedTest.java
 * Copyright © 2021. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.NumericException;

public class FastFloatParserHandPickedTest extends AbstractFloatHandPickedTest {

    @Override
    float parse(CharSequence str, boolean rejectOverflow) throws NumericException {
        return FastFloatParser.parseFloat(str, rejectOverflow);
    }

    @Override
    protected float parse(String str, int offset, int length, boolean rejectOverflow) throws NumericException {
        return FastFloatParser.parseFloat(str, offset, length, rejectOverflow);
    }
}
