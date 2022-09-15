/*
 * @(#)FastDoubleSimdTest.java
 * Copyright © 2022. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public abstract class AbstractEightDigitsTest {
    @TestFactory
    List<DynamicNode> dynamicTestsIllegalEightDecDigitsLiterals() {
        return Arrays.asList(
                dynamicTest("1234567x", () -> testDec("1234567x", 0, -1)),
                dynamicTest("x7654321", () -> testDec("x7654321", 0, -1)),
                dynamicTest("123456/7", () -> testDec("123456/7", 0, -1)),
                dynamicTest("7/654321", () -> testDec("7/654321", 0, -1)),
                dynamicTest("12345:67", () -> testDec("12345:67", 0, -1)),
                dynamicTest("76:54321", () -> testDec("76:54321", 0, -1)),

                dynamicTest("x12345678xx", () -> testDec("x12345678xx", 2, -1))
        );
    }

    @TestFactory
    List<DynamicNode> dynamicTestsIllegalEightHexDigitsLiterals() {
        return Arrays.asList(
                dynamicTest("1234567x", () -> testHex("1234567x", 0, -1L)),
                dynamicTest("x7654321", () -> testHex("x7654321", 0, -1L)),

                dynamicTest("x1234567xxx", () -> testHex("x1234567xxx", 1, -1L))
        );
    }

    @TestFactory
    List<DynamicNode> dynamicTestsLegalEightDecDigitsLiterals() {
        return Arrays.asList(
                dynamicTest("12345678", () -> testDec("12345678", 0, 12345678)),
                dynamicTest("87654321", () -> testDec("87654321", 0, 87654321)),
                dynamicTest("00000000", () -> testDec("00000000", 0, 0)),
                dynamicTest("99999999", () -> testDec("99999999", 0, 99999999)),

                dynamicTest("x12345678xx", () -> testDec("x12345678xx", 1, 12345678))
        );
    }

    @TestFactory
    List<DynamicNode> dynamicTestsLegalEightHexDigitsLiterals() {
        return Arrays.asList(
                dynamicTest("12345678", () -> testHex("12345678", 0, 0x12345678L)),
                dynamicTest("87654321", () -> testHex("87654321", 0, 0x87654321L)),
                dynamicTest("00000000", () -> testHex("00000000", 0, 0L)),
                dynamicTest("ffffffff", () -> testHex("ffffffff", 0, 0xffffffffL)),

                dynamicTest("x12345678xx", () -> testHex("x12345678xx", 1, 0x12345678L))
        );
    }

    abstract void testDec(String s, int offset, int expected);

    abstract void testHex(String s, int offset, long expected);
}
