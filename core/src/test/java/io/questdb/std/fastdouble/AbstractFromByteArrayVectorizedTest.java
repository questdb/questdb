/*
 * @(#)TestFastDoubleParserVectorized.java
 * Copyright © 2022. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public abstract class AbstractFromByteArrayVectorizedTest {
    @TestFactory
    List<DynamicNode> dynamicTestsTryToParseEightDigits() {
        return Arrays.asList(
                dynamicTest("00000000", () -> testTryToParseEightDigits("00000000", 0)),
                dynamicTest("00000001", () -> testTryToParseEightDigits("00000001", 1)),
                dynamicTest("00000010", () -> testTryToParseEightDigits("00000010", 10)),
                dynamicTest("00000100", () -> testTryToParseEightDigits("00000100", 100)),
                dynamicTest("00001000", () -> testTryToParseEightDigits("00001000", 1000)),
                dynamicTest("00010000", () -> testTryToParseEightDigits("00010000", 10000)),
                dynamicTest("00100000", () -> testTryToParseEightDigits("00100000", 100000)),
                dynamicTest("01000000", () -> testTryToParseEightDigits("01000000", 1000000)),
                dynamicTest("10000000", () -> testTryToParseEightDigits("10000000", 10000000)),
                dynamicTest("12345678", () -> testTryToParseEightDigits("12345678", 12345678)),
                dynamicTest("98979694", () -> testTryToParseEightDigits("98979694", 98979694)),
                dynamicTest("000.0000", () -> testTryToParseEightDigits("000.0000", -1)),
                dynamicTest("000~0000", () -> testTryToParseEightDigits("000~0000", -1))
        );
    }

    @TestFactory
    List<DynamicNode> dynamicTestsTryToParseEightHexDigits() {
        return Arrays.asList(
                dynamicTest("00000000", () -> dynamicTestsTryToParseEightHexDigits("00000000", 0x0L)),
                dynamicTest("00000001", () -> dynamicTestsTryToParseEightHexDigits("00000001", 0x1L)),
                dynamicTest("00000010", () -> dynamicTestsTryToParseEightHexDigits("00000010", 0x10L)),
                dynamicTest("00000100", () -> dynamicTestsTryToParseEightHexDigits("00000100", 0x100L)),
                dynamicTest("00001000", () -> dynamicTestsTryToParseEightHexDigits("00001000", 0x1000L)),
                dynamicTest("00010000", () -> dynamicTestsTryToParseEightHexDigits("00010000", 0x10000L)),
                dynamicTest("00100000", () -> dynamicTestsTryToParseEightHexDigits("00100000", 0x100000L)),
                dynamicTest("01000000", () -> dynamicTestsTryToParseEightHexDigits("01000000", 0x1000000L)),
                dynamicTest("10000000", () -> dynamicTestsTryToParseEightHexDigits("10000000", 0x10000000L)),
                dynamicTest("12345678", () -> dynamicTestsTryToParseEightHexDigits("12345678", 0x12345678L)),
                dynamicTest("98979694", () -> dynamicTestsTryToParseEightHexDigits("98979694", 0x98979694L)),
                dynamicTest("0000000a", () -> dynamicTestsTryToParseEightHexDigits("0000000a", 0x0000000aL)),
                dynamicTest("fedcba98", () -> dynamicTestsTryToParseEightHexDigits("fedcba98", 0xfedcba98L)),
                dynamicTest("ffffffff", () -> dynamicTestsTryToParseEightHexDigits("ffffffff", 0xffffffffL)),
                dynamicTest("000.0000", () -> dynamicTestsTryToParseEightHexDigits("000.0000", -1L)),
                dynamicTest("000~0000", () -> dynamicTestsTryToParseEightHexDigits("000~0000", -1L))
        );
    }

    protected abstract void dynamicTestsTryToParseEightHexDigits(String str, long expected);

    @TestFactory
    List<DynamicNode> dynamicTestsTryToParseSevenDigits() {
        return Arrays.asList(
                dynamicTest("0000000", () -> testTryToParseSevenDigits("0000000", 0)),
                dynamicTest("0000001", () -> testTryToParseSevenDigits("0000001", 1)),
                dynamicTest("0000010", () -> testTryToParseSevenDigits("0000010", 10)),
                dynamicTest("0000100", () -> testTryToParseSevenDigits("0000100", 100)),
                dynamicTest("0001000", () -> testTryToParseSevenDigits("0001000", 1000)),
                dynamicTest("0010000", () -> testTryToParseSevenDigits("0010000", 10000)),
                dynamicTest("0100000", () -> testTryToParseSevenDigits("0100000", 100000)),
                dynamicTest("1000000", () -> testTryToParseSevenDigits("1000000", 1000000)),
                dynamicTest("1234567", () -> testTryToParseSevenDigits("1234567", 1234567)),
                dynamicTest("9897969", () -> testTryToParseSevenDigits("9897969", 9897969)),
                dynamicTest("000.000", () -> testTryToParseSevenDigits("000.000", -1)),
                dynamicTest("000~000", () -> testTryToParseSevenDigits("000~000", -1))
        );
    }

    protected abstract void testTryToParseEightDigits(String str, long expected);

    protected abstract void testTryToParseSevenDigits(String str, long expected);

}
