/*
 * @(#)FastDoubleParserNumericallyGeneratedTest.java
 * Copyright © 2021. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.NumericException;

public class FastDoubleParserFromCharArrayNumericallyGeneratedTest extends AbstractDoubleNumericallyGeneratedTest {
    @Override
    protected double parse(String str) throws NumericException {
        return FastDoubleParser.parseDouble(str.toCharArray(), false);
    }
}
