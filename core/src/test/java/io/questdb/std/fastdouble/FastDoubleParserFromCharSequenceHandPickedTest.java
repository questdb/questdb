/*
 * @(#)FastDoubleParserHandPickedTest.java
 * Copyright © 2021. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.NumericException;

public class FastDoubleParserFromCharSequenceHandPickedTest extends AbstractDoubleHandPickedTest {
    @Override
    double parse(CharSequence str, boolean rejectOverflow) throws NumericException {
        return FastDoubleParser.parseDouble(str, rejectOverflow);
    }

    @Override
    protected double parse(String str, int offset, int length, boolean rejectOverflow) throws NumericException {
        return FastDoubleParser.parseDouble(str, offset, length, rejectOverflow);
    }
}
