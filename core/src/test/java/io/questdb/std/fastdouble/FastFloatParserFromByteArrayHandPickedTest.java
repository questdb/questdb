/*
 * @(#)FastDoubleParserHandPickedTest.java
 * Copyright © 2021. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.NumericException;

import java.nio.charset.StandardCharsets;

public class FastFloatParserFromByteArrayHandPickedTest extends AbstractFloatHandPickedTest {
    @Override
    float parse(CharSequence str, boolean rejectOverflow) throws NumericException {
        byte[] bytes = new byte[str.length()];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) str.charAt(i);
        }
        return FastFloatParser.parseFloat(bytes, rejectOverflow);
    }

    @Override
    protected float parse(String str, int offset, int length, boolean rejectOverflow) throws NumericException {
        byte[] bytes = str.getBytes(StandardCharsets.ISO_8859_1);
        return FastFloatParser.parseFloat(bytes, offset, length, rejectOverflow);
    }
}
