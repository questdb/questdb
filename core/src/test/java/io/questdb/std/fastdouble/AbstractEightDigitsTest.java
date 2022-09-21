/*
 * @(#)FastDoubleSimdTest.java
 * Copyright © 2022. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import org.junit.Test;

public abstract class AbstractEightDigitsTest {
    @Test
    public void testIllegalEightDecDigitsLiterals() {
        testDec("1234567x", 0, -1);
        testDec("x7654321", 0, -1);
        testDec("123456/7", 0, -1);
        testDec("7/654321", 0, -1);
        testDec("12345:67", 0, -1);
        testDec("76:54321", 0, -1);
        testDec("x12345678xx", 2, -1);
    }

    @Test
    public void testIllegalEightHexDigitsLiterals() {
        testHex("1234567x", 0, -1L);
        testHex("x7654321", 0, -1L);
        testHex("x1234567xxx", 1, -1L);
    }

    @Test
    public void testLegalEightDecDigitsLiterals() {
        testDec("12345678", 0, 12345678);
        testDec("87654321", 0, 87654321);
        testDec("00000000", 0, 0);
        testDec("99999999", 0, 99999999);
        testDec("x12345678xx", 1, 12345678);
    }

    @Test
    public void testLegalEightHexDigitsLiterals() {
        testHex("12345678", 0, 0x12345678L);
        testHex("87654321", 0, 0x87654321L);
        testHex("00000000", 0, 0L);
        testHex("ffffffff", 0, 0xffffffffL);
        testHex("x12345678xx", 1, 0x12345678L);
    }

    abstract void testDec(String s, int offset, int expected);

    abstract void testHex(String s, int offset, long expected);
}
