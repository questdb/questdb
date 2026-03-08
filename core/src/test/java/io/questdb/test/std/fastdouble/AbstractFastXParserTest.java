/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.std.fastdouble;

import io.questdb.std.Chars;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractFastXParserTest {
    static List<TestData> createAllTestData() {
        List<TestData> list = new ArrayList<>();
        list.addAll(createTestDataForNaN());
        list.addAll(createTestDataForInfinity());
        list.addAll(createDataForLimits());
        list.addAll(createDataForBadStrings());
        list.addAll(createDataForLegalDecStrings());
        list.addAll(createDataForLegalHexStrings());
        list.addAll(createDataForClingerInputClasses());
        list.addAll(createDataForLegalCroppedStrings());
        list.addAll(createTestDataForInputClassesInMethodParseFloatValue());
        return list;
    }

    static List<TestData> createAllTestDataForDouble() {
        List<TestData> list = createAllTestData();
        list.addAll(createDataForLegalDoubleSuffixes());
        return list;
    }

    static List<TestData> createAllTestDataForFloat() {
        List<TestData> list = createAllTestData();
        list.addAll(createDataForLegalFloatSuffixes());
        return list;
    }

    static List<TestData> createDataForBadStrings() {
        return Arrays.asList(
                new TestData("empty", ""),
                new TestData("+"),
                new TestData("-"),
                new TestData("+e"),
                new TestData("-e"),
                new TestData("+e123"),
                new TestData("-e456"),
                new TestData("78 e9"),
                new TestData("-01 e23"),
                new TestData("- 1"),
                new TestData("-0 .5"),
                new TestData("-0. 5"),
                new TestData("-0.5 e"),
                new TestData("-0.5e 3"),
                new TestData("45\ne6"),
                new TestData("d"),
                new TestData(".f"),
                new TestData("7_8e90"),
                new TestData("12e3_4"),
                new TestData("00x5.6p7"),
                new TestData("89p0"),
                new TestData("cafebabe.1p2"),
                new TestData("0x123pa"),
                new TestData("0x1.2e7"),
                new TestData("0xp89")
        );
    }

    static List<TestData> createDataForClingerInputClasses() {
        return Arrays.asList(
                new TestData("Dec Double: Inside Clinger fast path \"1000000000000000000e-325\")", "1000000000000000000e-325", 1000000000000000000e-325d, 0f),
                new TestData("Dec Double: Inside Clinger fast path (max_clinger_significand, max_clinger_exponent)", "9007199254740991e22", 9007199254740991e22d, 9007199254740991e22f),
                new TestData("Dec Double: Outside Clinger fast path (max_clinger_significand, max_clinger_exponent + 1)", "9007199254740991e23", 9007199254740991e23d, Float.POSITIVE_INFINITY),
                new TestData("Dec Double: Outside Clinger fast path (max_clinger_significand + 1, max_clinger_exponent)", "9007199254740992e22", 9007199254740992e22d, 9007199254740992e22f),
                new TestData("Dec Double: Inside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent)", "1e-22", 1e-22d, 1e-22f),
                new TestData("Dec Double: Outside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent - 1)", "1e-23", 1e-23d, 1e-23f),
                new TestData("Dec Double: Outside Clinger fast path, semi-fast path, 9999999999999999999", "1e23", 1e23d, 1e23f),
                new TestData("Dec Double: Outside Clinger fast path, bail-out in semi-fast path, 1e23", "1e23", 1e23d, 1e23f),
                new TestData("Dec Double: Outside Clinger fast path, mantissa overflows in semi-fast path, 7.2057594037927933e+16", "7.2057594037927933e+16", 7.2057594037927933e+16d, 7.2057594037927933e+16f),
                new TestData("Dec Double: Outside Clinger fast path, bail-out in semi-fast path, 7.3177701707893310e+15", "7.3177701707893310e+15", 7.3177701707893310e+15d, 7.3177701707893310e+15f),
                new TestData("Hex Double: Inside Clinger fast path (max_clinger_significand)", "0x1fffffffffffffp74", 0x1fffffffffffffp74, 0x1fffffffffffffp74f),
                new TestData("Hex Double: Inside Clinger fast path (max_clinger_significand), negative", "-0x1fffffffffffffp74", -0x1fffffffffffffp74, -0x1fffffffffffffp74f),
                new TestData("Hex Double: Outside Clinger fast path (max_clinger_significand, max_clinger_exponent + 1)", "0x1fffffffffffffp74", 0x1fffffffffffffp74, 0x1fffffffffffffp74f),
                new TestData("Hex Double: Outside Clinger fast path (max_clinger_significand + 1, max_clinger_exponent)", "0x20000000000000p74", 0x20000000000000p74, 0x20000000000000p74f),
                new TestData("Hex Double: Inside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent)", "0x1p-74", 0x1p-74, 0x1p-74f),
                new TestData("Hex Double: Outside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent - 1)", "0x1p-75", 0x1p-75, 0x1p-75f),
                new TestData("-2.97851206854973E-75", -2.97851206854973E-75, -0f),
                new TestData("3.0286208942000664E-69", 3.0286208942000664E-69, 0f),
                new TestData("3.7587182468424695418288325e-309", 3.7587182468424695418288325e-309, 0f),
                new TestData("10000000000000000000000000000000000000000000e+308", Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY)
        );
    }

    static List<TestData> createDataForLegalCroppedStrings() {
        return Arrays.asList(
                new TestData("x1y", 1, 1f, 1, 1),
                new TestData("xx-0x1p2yyy", -0x1p2, -0x1p2f, 2, 6)
        );
    }

    static List<TestData> createDataForLegalDecStrings() {
        return Arrays.asList(
                new TestData("1", 1, 1f),
                new TestData("1.2", 1.2, 1.2f),
                new TestData("1.2e3", 1.2e3, 1.2e3f),
                new TestData("1.2E3", 1.2e3, 1.2e3f),
                new TestData("1.2e3", 1.2e3, 1.2e3f),
                new TestData("+1", 1, 1f),
                new TestData("+1.2", 1.2, 1.2f),
                new TestData("+1.2e3", 1.2e3, 1.2e3f),
                new TestData("+1.2E3", 1.2e3, 1.2e3f),
                new TestData("+1.2e3", 1.2e3, 1.2e3f),
                new TestData("-1", -1, -1f),
                new TestData("-1.2", -1.2, -1.2f),
                new TestData("-1.2e3", -1.2e3, -1.2e3f),
                new TestData("-1.2E3", -1.2e3, -1.2e3f),
                new TestData("-1.2e3", -1.2e3, -1.2e3f),
                new TestData("1", 1, 1f),
                new TestData("1.2", 1.2, 1.2f),
                new TestData("1.2e-3", 1.2e-3, 1.2e-3f),
                new TestData("1.2E-3", 1.2e-3, 1.2e-3f),
                new TestData("1.2e-3", 1.2e-3, 1.2e-3f),

                new TestData("1", 1, 1f),
                new TestData("1.2", 1.2, 1.2f),
                new TestData("1.2e+3", 1.2e3, 1.2e3f),
                new TestData("1.2E+3", 1.2e3, 1.2e3f),
                new TestData("1.2e+3", 1.2e3, 1.2e3f),
                new TestData("-1.2e+3", -1.2e3, -1.2e3f),
                new TestData("-1.2E-3", -1.2e-3, -1.2e-3f),
                new TestData("+1.2E+3", 1.2e3, 1.2e3f),
                new TestData(" 1.2e3", 1.2e3, 1.2e3f),
                new TestData("1.2e3 ", 1.2e3, 1.2e3f),
                new TestData("  1.2e3", 1.2e3, 1.2e3f),
                new TestData("  -1.2e3", -1.2e3, -1.2e3f),
                new TestData("1.2e3  ", 1.2e3, 1.2e3f),
                new TestData("   1.2e3   ", 1.2e3, 1.2e3f),
                new TestData("1234567890", 1234567890d, 1234567890f),
                new TestData("000000000", 0d, 0f),
                new TestData("0000.0000", 0d, 0f),
                new TestData("0000.0000", 0d, 0f)
        );
    }

    static List<TestData> createDataForLegalDoubleSuffixes() {
        return Arrays.asList(
                new TestData("FloatTypeSuffix", "1d", 1, 1f),
                new TestData("FloatTypeSuffix", "1.2d", 1.2, 1.2f),
                new TestData("FloatTypeSuffix", "1.2e-3d", 1.2e-3, 1.2e-3f),
                new TestData("FloatTypeSuffix", "1.2E-3d", 1.2e-3, 1.2e-3f),
                new TestData("FloatTypeSuffix", "1.2e-3d", 1.2e-3, 1.2e-3f),

                new TestData("FloatTypeSuffix", "1D", 1, 1f),
                new TestData("FloatTypeSuffix", "1.2D", 1.2, 1.2f),
                new TestData("FloatTypeSuffix", "1.2e-3D", 1.2e-3, 1.2e-3f),
                new TestData("FloatTypeSuffix", "1.2E-3D", 1.2e-3, 1.2e-3f),
                new TestData("FloatTypeSuffix", "1.2e-3D", 1.2e-3, 1.2e-3f),
                new TestData("parseDecFloatLiteral(): With FloatTypeSuffix 'd': 1.2e3d", "1.2e3d", 0, 6, 0, 6, 1.2e3, 1.2e3f, true),
                new TestData("parseDecFloatLiteral(): With FloatTypeSuffix 'd' + whitespace: 1.2e3d ", "1.2e3d ", 0, 7, 0, 7, 1.2e3, 1.2e3f, true),
                new TestData("parseDecFloatLiteral(): With FloatTypeSuffix 'D': 1.2D", "1.2D", 0, 4, 0, 4, 1.2, 1.2f, true)
        );
    }

    static List<TestData> createDataForLegalFloatSuffixes() {
        return Arrays.asList(
                new TestData("FloatTypeSuffix", "1f", 1, 1f),
                new TestData("FloatTypeSuffix", "1.2f", 1.2, 1.2f),
                new TestData("FloatTypeSuffix", "1.2e-3f", 1.2e-3, 1.2e-3f),
                new TestData("FloatTypeSuffix", "1.2E-3f", 1.2e-3, 1.2e-3f),
                new TestData("FloatTypeSuffix", "1.2e-3f", 1.2e-3, 1.2e-3f),
                new TestData("FloatTypeSuffix", "1F", 1, 1f),
                new TestData("FloatTypeSuffix", "1.2F", 1.2, 1.2f),
                new TestData("FloatTypeSuffix", "1.2e-3F", 1.2e-3, 1.2e-3f),
                new TestData("FloatTypeSuffix", "1.2E-3F", 1.2e-3, 1.2e-3f),
                new TestData("FloatTypeSuffix", "1.2e-3F", 1.2e-3, 1.2e-3f),

                new TestData("parseDecFloatLiteral(): With FloatTypeSuffix 'f': 1f", "1f", 0, 2, 0, 2, 1d, 1f, true),
                new TestData("parseDecFloatLiteral(): With FloatTypeSuffix 'F': -1.2e-3F", "-1.2e-3F", 0, 8, 0, 8, -1.2e-3, -1.2e-3f, true)
        );
    }

    static List<TestData> createDataForLegalHexStrings() {
        return Arrays.asList(
                new TestData("0xap2", 0xap2, 0xap2f),

                new TestData("FloatTypeSuffix", "0xap2d", 0xap2, 0xap2f),
                new TestData("FloatTypeSuffix", "0xap2D", 0xap2, 0xap2f),
                new TestData("FloatTypeSuffix", "0xap2f", 0xap2, 0xap2f),
                new TestData("FloatTypeSuffix", "0xap2F", 0xap2, 0xap2f),

                new TestData(" 0xap2", 0xap2, 0xap2f),
                new TestData(" 0xap2  ", 0xap2, 0xap2f),
                new TestData("   0xap2   ", 0xap2, 0xap2f),

                new TestData("0x0.1234ab78p0", 0x0.1234ab78p0, 0x0.1234ab78p0f),
                new TestData("-0x0.1234AB78p+7", -0x0.1234AB78p7, -0x0.1234ab78p7f),
                new TestData("0x1.0p8", 256d, 256f),
                new TestData("0x1.234567890abcdefP123", 0x1.234567890abcdefp123, 0x1.234567890abcdefp123f),
                new TestData("+0x1234567890.abcdefp-45", 0x1234567890.abcdefp-45d, 0x1234567890.abcdefp-45f),
                new TestData("0x1234567890.abcdef12p-45", 0x1234567890.abcdef12p-45, 0x1234567890.abcdef12p-45f)

        );
    }

    static List<TestData> createDataForLimits() {
        return Arrays.asList(
                new TestData("Double Dec Limit a", Double.toString(Double.MIN_VALUE), Double.MIN_VALUE, (float) Double.MIN_VALUE),
                new TestData("Double Dec Limit b", Double.toString(Double.MAX_VALUE), Double.MAX_VALUE, (float) Double.MAX_VALUE),
                new TestData("Double Dec Limit c", Double.toString(Math.nextUp(0.0)), Math.nextUp(0.0), (float) Math.nextUp(0.0)),
                new TestData("Double Dec Limit d", Double.toString(Math.nextDown(0.0)), Math.nextDown(0.0), (float) Math.nextDown(0.0)),
                new TestData("Float Dec Limit a", Float.toString(Float.MIN_VALUE), 1.4E-45, Float.MIN_VALUE),
                new TestData("Float Dec Limit b", Float.toString(Float.MAX_VALUE), 3.4028235E38, Float.MAX_VALUE),
                new TestData("Float Dec Limit c", Float.toString(Math.nextUp(0.0f)), 1.4E-45, Math.nextUp(0.0f)),
                new TestData("Float Dec Limit d", Float.toString(Math.nextDown(0.0f)), -1.4E-45, Math.nextDown(0.0f)),

                new TestData("Double Hex Limit a", Double.toHexString(Double.MIN_VALUE), Double.MIN_VALUE, (float) Double.MIN_VALUE),
                new TestData("Double Hex Limit b", Double.toHexString(Double.MAX_VALUE), Double.MAX_VALUE, (float) Double.MAX_VALUE),
                new TestData("Double Hex Limit c", Double.toHexString(Math.nextUp(0.0)), Math.nextUp(0.0), 0f),
                new TestData("Double Hex Limit d", Double.toHexString(Math.nextDown(0.0)), Math.nextDown(0.0), -0f),

                new TestData("Float Hex Limit", Float.toHexString(Float.MIN_VALUE), Float.MIN_VALUE, Float.MIN_VALUE),
                new TestData("Float Hex Limit", Float.toHexString(Float.MAX_VALUE), Float.MAX_VALUE, Float.MAX_VALUE),
                new TestData("Float Hex Limit", Float.toHexString(Math.nextUp(0.0f)), Math.nextUp(0.0f), Math.nextUp(0.0f)),
                new TestData("Float Hex Limit", Float.toHexString(Math.nextDown(0.0f)), Math.nextDown(0.0f), Math.nextDown(0.0f))
        );
    }

    static List<TestData> createTestDataForInfinity() {
        return Arrays.asList(
                new TestData("NaN", Double.NaN, Float.NaN),
                new TestData("+NaN", Double.NaN, Float.NaN),
                new TestData("-NaN", Double.NaN, Float.NaN),
                new TestData("NaNf"),
                new TestData("+NaNd"),
                new TestData("-NaNF"),
                new TestData("+-NaND"),
                new TestData("NaNInfinity"),
                new TestData("nan")
        );
    }

    static List<TestData> createTestDataForInputClassesInMethodParseFloatValue() {
        return Arrays.asList(
                new TestData("parseFloatValue(): charOffset too small", "3.14", -1, 4, -1, 4, 3d, 3f, false),
                new TestData("parseFloatValue(): charOffset too big", "3.14", 8, 4, 8, 4, 3d, 3f, false),
                new TestData("parseFloatValue(): charLength too small", "3.14", 0, -4, 0, -4, 3d, 3f, false),
                new TestData("parseFloatValue(): charLength too big", "3.14", 0, 8, 0, 8, 3d, 3f, false),
                new TestData("parseFloatValue(): Significand with leading whitespace", "   3", 0, 4, 0, 4, 3d, 3f, true),
                new TestData("parseFloatValue(): Significand with trailing whitespace", "3   ", 0, 4, 0, 4, 3d, 3f, true),
                new TestData("parseFloatValue(): Empty String", "", 0, 0, 0, 0, 0d, 0f, false),
                new TestData("parseFloatValue(): Blank String", "   ", 0, 3, 0, 3, 0d, 0f, false),
                new TestData("parseFloatValue(): Very long non-blank String", Chars.repeat("a", 66).toString(), 0, 66, 0, 66, 0d, 0f, false),
                new TestData("parseFloatValue(): Plus Sign", "+", 0, 1, 0, 1, 0d, 0f, false),
                new TestData("parseFloatValue(): Negative Sign", "-", 0, 1, 0, 1, 0d, 0f, false),
                new TestData("parseFloatValue(): Infinity", "Infinity", 0, 8, 0, 8, Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY, true),
                new TestData("parseFloatValue(): NaN", "NaN", 0, 3, 0, 3, Double.NaN, Float.NaN, true),
                new TestData("parseInfinity(): Infinit (missing last char)", "Infinit", 0, 7, 0, 7, 0d, 0f, false),
                new TestData("parseInfinity(): InfinitY (bad last char)", "InfinitY", 0, 8, 0, 8, 0d, 0f, false),
                new TestData("parseNaN(): Na (missing last char)", "Na", 0, 2, 0, 2, 0d, 0f, false),
                new TestData("parseNaN(): Nan (bad last char)", "Nan", 0, 3, 0, 3, 0d, 0f, false),
                new TestData("parseFloatValue(): Leading zero", "03", 0, 2, 0, 2, 3d, 3f, true),
                new TestData("parseFloatValue(): Leading zero x", "0x3", 0, 3, 0, 3, 0d, 0f, false),
                new TestData("parseFloatValue(): Leading zero X", "0X3", 0, 3, 0, 3, 0d, 0f, false),

                new TestData("parseDecFloatLiteral(): Decimal point only", ".", 0, 1, 0, 1, 0d, 0f, false),
                new TestData("parseDecFloatLiteral(): With decimal point", "3.", 0, 2, 0, 2, 3d, 3f, true),
                new TestData("parseDecFloatLiteral(): Without decimal point", "3", 0, 1, 0, 1, 3d, 3f, true),
                new TestData("parseDecFloatLiteral(): 7 digits after decimal point", "3.1234567", 0, 9, 0, 9, 3.1234567, 3.1234567f, true),
                new TestData("parseDecFloatLiteral(): 8 digits after decimal point", "3.12345678", 0, 10, 0, 10, 3.12345678, 3.12345678f, true),
                new TestData("parseDecFloatLiteral(): 9 digits after decimal point", "3.123456789", 0, 11, 0, 11, 3.123456789, 3.123456789f, true),
                new TestData("parseDecFloatLiteral(): 1 digit + 7 chars after decimal point", "3.1abcdefg", 0, 10, 0, 10, 0d, 0f, false),
                new TestData("parseDecFloatLiteral(): With 'e' at end", "3e", 0, 2, 0, 2, 0d, 0f, false),
                new TestData("parseDecFloatLiteral(): With 'E' at end", "3E", 0, 2, 0, 2, 0d, 0f, false),
                new TestData("parseDecFloatLiteral(): With 'e' + whitespace at end", "3e   ", 0, 5, 0, 5, 0d, 0f, false),
                new TestData("parseDecFloatLiteral(): With 'E' + whitespace  at end", "3E   ", 0, 5, 0, 5, 0d, 0f, false),
                new TestData("parseDecFloatLiteral(): With 'e+' at end", "3e+", 0, 3, 0, 3, 0d, 0f, false),
                new TestData("parseDecFloatLiteral(): With 'E-' at end", "3E-", 0, 3, 0, 3, 0d, 0f, false),
                new TestData("parseDecFloatLiteral(): With 'e+9' at end", "3e+9", 0, 4, 0, 4, 3e+9, 3e+9f, true),
                new TestData("parseDecFloatLiteral(): With 20 significand digits", "12345678901234567890", 0, 20, 0, 20, 12345678901234567890d, 12345678901234567890f, true),
                new TestData("parseDecFloatLiteral(): With 20 significand digits + non-ascii char", "12345678901234567890￡", 0, 21, 0, 21, 0d, 0f, false),
                new TestData("parseDecFloatLiteral(): With 20 significand digits with decimal point", "1234567890.1234567890", 0, 21, 0, 21, 1234567890.1234567890, 1234567890.1234567890f, true),
                new TestData("parseDecFloatLiteral(): With illegal FloatTypeSuffix 'z': 1.2e3z", "1.2e3z", 0, 6, 0, 6, 1.2e3, 1.2e3f, false),
                new TestData("parseDecFloatLiteral(): No digits+whitespace+'z'", ". z", 0, 2, 0, 2, 0d, 0f, false),

                new TestData("parseHexFloatLiteral(): With decimal point", "0x3.", 0, 4, 0, 4, 0d, 0f, false),
                new TestData("parseHexFloatLiteral(): No digits with decimal point", "0x.", 0, 3, 0, 3, 0d, 0f, false),
                new TestData("parseHexFloatLiteral(): Without decimal point", "0X3", 0, 3, 0, 3, 0d, 0f, false),
                new TestData("parseHexFloatLiteral(): 7 digits after decimal point", "0x3.1234567", 0, 11, 0, 11, 0d, 0f, false),
                new TestData("parseHexFloatLiteral(): 8 digits after decimal point", "0X3.12345678", 0, 12, 0, 12, 0d, 0f, false),
                new TestData("parseHexFloatLiteral(): 9 digits after decimal point", "0x3.123456789", 0, 13, 0, 13, 0d, 0f, false),
                new TestData("parseHexFloatLiteral(): 1 digit + 7 chars after decimal point", "0X3.1abcdefg", 0, 12, 0, 12, 0d, 0f, false),
                new TestData("parseHexFloatLiteral(): With 'p' at end", "0X3p", 0, 4, 0, 4, 0d, 0f, false),
                new TestData("parseHexFloatLiteral(): With 'P' at end", "0x3P", 0, 4, 0, 4, 0d, 0f, false),
                new TestData("parseHexFloatLiteral(): With 'p' + whitespace at end", "0X3p   ", 0, 7, 0, 7, 0d, 0f, false),
                new TestData("parseHexFloatLiteral(): With 'P' + whitespace  at end", "0x3P   ", 0, 7, 0, 7, 0d, 0f, false),
                new TestData("parseHexFloatLiteral(): With 'p+' at end", "0X3p+", 0, 5, 0, 5, 0d, 0f, false),
                new TestData("parseHexFloatLiteral(): With 'P-' at end", "0x3P-", 0, 5, 0, 5, 0d, 0f, false),
                new TestData("parseHexFloatLiteral(): With 'p+9' at end", "0X3p+9", 0, 6, 0, 6, 0X3p+9, 0X3p+9f, true),
                new TestData("parseHexFloatLiteral(): With 20 significand digits", "0x12345678901234567890p0", 0, 24, 0, 24, 0x12345678901234567890p0, 0x12345678901234567890p0f, true),
                new TestData("parseHexFloatLiteral(): With 20 significand digits + non-ascii char", "0x12345678901234567890￡p0", 0, 25, 0, 25, 0d, 0f, false),
                new TestData("parseHexFloatLiteral(): With 20 significand digits with decimal point", "0x1234567890.1234567890P0", 0, 25, 0, 25, 0x1234567890.1234567890P0, 0x1234567890.1234567890P0f, true),
                new TestData("parseHexFloatLiteral(): With illegal FloatTypeSuffix 'z': 0x1.2p3z", "0x1.2p3z", 0, 8, 0, 8, 0x1.2p3d, 0x1.2p3f, false),
                new TestData("parseHexFloatLiteral(): With FloatTypeSuffix 'd': 0x1.2p3d", "0x1.2p3d", 0, 8, 0, 8, 0x1.2p3d, 0x1.2p3f, true),
                new TestData("parseHexFloatLiteral(): With FloatTypeSuffix 'd' + whitespace: 0x1.2p3d ", "0x1.2p3d ", 0, 9, 0, 9, 0x1.2p3d, 0x1.2p3f, true),
                new TestData("parseHexFloatLiteral(): With FloatTypeSuffix 'D': 0x1.2p3D", "0x1.2p3D", 0, 8, 0, 8, 0x1.2p3d, 0x1.2p3f, true),
                new TestData("parseHexFloatLiteral(): With FloatTypeSuffix 'f': 0x1.2p3f", "0x1.2p3f", 0, 8, 0, 8, 0x1.2p3d, 0x1.2p3f, true),
                new TestData("parseHexFloatLiteral(): With FloatTypeSuffix 'F': 0x1.2p3F", "0x1.2p3F", 0, 8, 0, 8, 0x1.2p3d, 0x1.2p3f, true)
        );
    }

    static List<TestData> createTestDataForNaN() {
        return Arrays.asList(
                new TestData("Infinity", Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY),
                new TestData("+Infinity", Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY),
                new TestData("-Infinity", Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY),
                new TestData("Infinit"),
                new TestData("+Infinityf"),
                new TestData("-InfinityF"),
                new TestData("+Infinityd"),
                new TestData("+-InfinityD"),
                new TestData("+InfinityNaN"),
                new TestData("infinity")
        );
    }

}