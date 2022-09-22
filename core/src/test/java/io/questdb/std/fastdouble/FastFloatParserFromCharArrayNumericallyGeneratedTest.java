/*
 * @(#)FastDoubleParserNumericallyGeneratedTest.java
 * Copyright © 2021. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.NumericException;

public class FastFloatParserFromCharArrayNumericallyGeneratedTest extends AbstractFloatNumericallyGeneratedTest {
    @Override
    protected float parse(String str) throws NumericException {
        return FastFloatParser.parseFloat(str.toCharArray());
    }
}
