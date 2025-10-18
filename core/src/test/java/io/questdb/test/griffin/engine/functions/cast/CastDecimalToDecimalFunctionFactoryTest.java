/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.cast.CastDecimalToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToDecimalFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastNullTypeFunctionFactory;
import io.questdb.std.Decimals;
import io.questdb.test.griffin.BaseFunctionFactoryTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CastDecimalToDecimalFunctionFactoryTest extends BaseFunctionFactoryTest {
    private FunctionParser functionParser;
    private GenericRecordMetadata metadata;

    @Before
    public void setUp5() {
        functions.add(new CastDecimalToDecimalFunctionFactory());
        functions.add(new CastLongToDecimalFunctionFactory());
        functions.add(new CastNullTypeFunctionFactory());
        functionParser = createFunctionParser();
        metadata = new GenericRecordMetadata();
    }

    @Test
    public void testConstantCast() throws SqlException {
        assertSql("cast\n123.450\n", "SELECT cast(cast(123.45 as DECIMAL(5, 2)) as DECIMAL(6, 3))");
    }

    @Test
    public void testConstantCastFunction() throws SqlException {
        Function function = parse("cast(cast(123.45 as DECIMAL(5, 2)) as DECIMAL(6, 3))");

        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(ColumnType.getDecimalType(6, 3), function.getType());
        Assert.assertEquals(123450, function.getDecimal32(null));
    }

    @Test
    public void testConstantCastFunctionArithmeticOverflow() throws SqlException {
        try {
            parse("cast(cast(12345L as DECIMAL(5, 0)) as DECIMAL(76, 75))");
            Assert.fail();
        } catch (ImplicitCastException ex) {
            TestUtils.assertContains(ex.getMessage(), "inconvertible value: 12345 [DECIMAL(5,0) -> DECIMAL(76,75)]");
        }
    }

    @Test
    public void testConstantCastFunctionNull() throws SqlException {
        Function function = parse("cast(cast(cast(NULL as LONG) as DECIMAL(5, 2)) as DECIMAL(6, 3))");

        Assert.assertTrue(function.isConstant());
        Assert.assertEquals(ColumnType.getDecimalType(6, 3), function.getType());
        Assert.assertEquals(Decimals.DECIMAL32_NULL, function.getDecimal32(null));
    }

    @Test
    public void testConstantCastFunctionOverflow() throws SqlException {
        try {
            parse("cast(cast(123.45 as DECIMAL(5, 2)) as DECIMAL(5, 3))");
            Assert.fail();
        } catch (ImplicitCastException ex) {
            TestUtils.assertContains(ex.getMessage(), "inconvertible value: 123.45 [DECIMAL(5,2) -> DECIMAL(5,3)]");
        }
    }

    @Test
    public void testConstantFolding() throws Exception {
        testConstantCast("99m", "99", "DECIMAL(4)");
        testConstantCast("99m", "99.00", "DECIMAL(4,2)");
        testConstantCast("cast(9999 as DECIMAL(4))", "9999", "DECIMAL(9)");
        testConstantCast("cast(123456 as DECIMAL(9,3))", "123456.000", "DECIMAL(9,3)");
    }

    @Test
    public void testEdgeCases() throws Exception {
        testRuntimeCast("0m", "0", "DECIMAL(1)");
        testRuntimeCast("0m", "0.000", "DECIMAL(4,3)");
        testRuntimeCast("99m", "99", "DECIMAL(2)");
        testRuntimeCast("cast(9999 as DECIMAL(4))", "9999", "DECIMAL(4)");
    }

    @Test
    public void testExplainPlans() throws Exception {
        assertSql("QUERY PLAN\n" +
                        "VirtualRecord\n" +
                        "  functions: [value::DECIMAL(4,0)]\n" +
                        "    VirtualRecord\n" +
                        "      functions: [99]\n" +
                        "        long_sequence count: 1\n",
                "EXPLAIN WITH data AS (SELECT 99m AS value) SELECT cast(value as DECIMAL(4)) FROM data");
        assertSql("QUERY PLAN\n" +
                        "VirtualRecord\n" +
                        "  functions: [value::DECIMAL(4,2)]\n" +
                        "    VirtualRecord\n" +
                        "      functions: [99]\n" +
                        "        long_sequence count: 1\n",
                "EXPLAIN WITH data AS (SELECT 99m AS value) SELECT cast(value as DECIMAL(4,2)) FROM data");
        // Constant folding
        assertSql("QUERY PLAN\n" +
                        "VirtualRecord\n" +
                        "  functions: [99.00]\n" +
                        "    long_sequence count: 1\n",
                "EXPLAIN SELECT cast(99m as DECIMAL(4,2))");
    }

    @Test
    public void testNarrowDecimal128() throws Exception {
        testRuntimeCast("cast(999999999999999999 as DECIMAL(19))", "999999999999999999", "DECIMAL(18)");
        testRuntimeCast("cast(-999999999999999999 as DECIMAL(19))", "-999999999999999999", "DECIMAL(18)");
        testRuntimeCast("cast(999999999 as DECIMAL(19))", "999999999", "DECIMAL(9)");
        testRuntimeCast("cast(-999999999 as DECIMAL(19))", "-999999999", "DECIMAL(9)");
        testRuntimeCast("cast(9999 as DECIMAL(19))", "9999", "DECIMAL(4)");
        testRuntimeCast("cast(-9999 as DECIMAL(19))", "-9999", "DECIMAL(4)");
        testRuntimeCast("cast(99 as DECIMAL(19))", "99", "DECIMAL(2)");
        testRuntimeCast("cast(-99 as DECIMAL(19))", "-99", "DECIMAL(2)");
        testRuntimeCastOverflow("cast(99999999999999999999 as DECIMAL(20))", "DECIMAL(18)", "inconvertible value: 99999999999999999999 [DECIMAL(20,0) -> DECIMAL(18,0)]");
        testRuntimeCast("cast(NULL as DECIMAL(30))", "", "DECIMAL(18)");
        testRuntimeCast("cast(NULL as DECIMAL(30))", "", "DECIMAL(9)");
        testRuntimeCast("cast(NULL as DECIMAL(30))", "", "DECIMAL(4)");
        testRuntimeCast("cast(NULL as DECIMAL(30))", "", "DECIMAL(2)");
    }

    @Test
    public void testNarrowDecimal16() throws Exception {
        testRuntimeCast("cast(99 as DECIMAL(4))", "99", "DECIMAL(2)");
        testRuntimeCast("cast(-99 as DECIMAL(4))", "-99", "DECIMAL(2)");
        testRuntimeCastOverflow("cast(999 as DECIMAL(4))", "DECIMAL(2)", "inconvertible value: 999 [DECIMAL(4,0) -> DECIMAL(2,0)]");
        testRuntimeCastOverflow("cast(-999 as DECIMAL(4))", "DECIMAL(2)", "inconvertible value: -999 [DECIMAL(4,0) -> DECIMAL(2,0)]");
        testRuntimeCast("cast(NULL as DECIMAL(4))", "", "DECIMAL(2)");
    }

    @Test
    public void testNarrowDecimal256() throws Exception {
        testRuntimeCast("cast(9223372036854775807 as DECIMAL(40))", "9223372036854775807", "DECIMAL(19)");
        testRuntimeCast("cast(-9223372036854775807 as DECIMAL(40))", "-9223372036854775807", "DECIMAL(19)");
        testRuntimeCast("cast(999999999999999999 as DECIMAL(40))", "999999999999999999", "DECIMAL(18)");
        testRuntimeCast("cast(-999999999999999999 as DECIMAL(40))", "-999999999999999999", "DECIMAL(18)");
        testRuntimeCast("cast(999999999 as DECIMAL(40))", "999999999", "DECIMAL(9)");
        testRuntimeCast("cast(-999999999 as DECIMAL(40))", "-999999999", "DECIMAL(9)");
        testRuntimeCast("cast(9999 as DECIMAL(40))", "9999", "DECIMAL(4)");
        testRuntimeCast("cast(-9999 as DECIMAL(40))", "-9999", "DECIMAL(4)");
        testRuntimeCast("cast(99 as DECIMAL(40))", "99", "DECIMAL(2)");
        testRuntimeCast("cast(-99 as DECIMAL(40))", "-99", "DECIMAL(2)");
        testRuntimeCastOverflow("cast(999999999999999999999999999999999999999 as DECIMAL(39))", "DECIMAL(18)", "inconvertible value: 999999999999999999999999999999999999999 [DECIMAL(39,0) -> DECIMAL(18,0)]");
        testRuntimeCast("cast(NULL as DECIMAL(40))", "", "DECIMAL(19)");
        testRuntimeCast("cast(NULL as DECIMAL(40))", "", "DECIMAL(18)");
        testRuntimeCast("cast(NULL as DECIMAL(40))", "", "DECIMAL(9)");
        testRuntimeCast("cast(NULL as DECIMAL(40))", "", "DECIMAL(4)");
        testRuntimeCast("cast(NULL as DECIMAL(40))", "", "DECIMAL(2)");
    }

    @Test
    public void testNarrowDecimal32() throws Exception {
        testRuntimeCast("cast(9999 as DECIMAL(9))", "9999", "DECIMAL(4)");
        testRuntimeCast("cast(-9999 as DECIMAL(9))", "-9999", "DECIMAL(4)");
        testRuntimeCast("cast(99 as DECIMAL(9))", "99", "DECIMAL(2)");
        testRuntimeCast("cast(-99 as DECIMAL(9))", "-99", "DECIMAL(2)");
        testRuntimeCastOverflow("cast(99999 as DECIMAL(9))", "DECIMAL(4)", "inconvertible value: 99999 [DECIMAL(9,0) -> DECIMAL(4,0)]");
        testRuntimeCastOverflow("cast(-99999 as DECIMAL(9))", "DECIMAL(4)", "inconvertible value: -99999 [DECIMAL(9,0) -> DECIMAL(4,0)]");
        testRuntimeCast("cast(NULL as DECIMAL(9))", "", "DECIMAL(4)");
        testRuntimeCast("cast(NULL as DECIMAL(9))", "", "DECIMAL(2)");
    }

    @Test
    public void testNarrowDecimal64() throws Exception {
        testRuntimeCast("cast(999999999 as DECIMAL(18))", "999999999", "DECIMAL(9)");
        testRuntimeCast("cast(-999999999 as DECIMAL(18))", "-999999999", "DECIMAL(9)");
        testRuntimeCast("cast(9999 as DECIMAL(18))", "9999", "DECIMAL(4)");
        testRuntimeCast("cast(-9999 as DECIMAL(18))", "-9999", "DECIMAL(4)");
        testRuntimeCast("cast(99 as DECIMAL(18))", "99", "DECIMAL(2)");
        testRuntimeCast("cast(-99 as DECIMAL(18))", "-99", "DECIMAL(2)");
        testRuntimeCastOverflow("cast(9999999999 as DECIMAL(18))", "DECIMAL(9)", "inconvertible value: 9999999999 [DECIMAL(18,0) -> DECIMAL(9,0)]");
        testRuntimeCastOverflow("cast(-9999999999 as DECIMAL(18))", "DECIMAL(9)", "inconvertible value: -9999999999 [DECIMAL(18,0) -> DECIMAL(9,0)]");
        testRuntimeCast("cast(NULL as DECIMAL(18))", "", "DECIMAL(9)");
        testRuntimeCast("cast(NULL as DECIMAL(18))", "", "DECIMAL(4)");
        testRuntimeCast("cast(NULL as DECIMAL(18))", "", "DECIMAL(2)");
    }

    @Test
    public void testNarrowDecimal8() throws Exception {
        testRuntimeCast("cast(9 as DECIMAL(2))", "9", "DECIMAL(1)");
        testRuntimeCast("cast(-9 as DECIMAL(2))", "-9", "DECIMAL(1)");
        testRuntimeCastOverflow("cast(99 as DECIMAL(2))", "DECIMAL(1)", "inconvertible value: 99 [DECIMAL(2,0) -> DECIMAL(1,0)]");
        testRuntimeCastOverflow("cast(-99 as DECIMAL(2))", "DECIMAL(1)", "inconvertible value: -99 [DECIMAL(2,0) -> DECIMAL(1,0)]");
        testRuntimeCast("cast(NULL as DECIMAL(2))", "", "DECIMAL(1)");
    }

    @Test
    public void testNullHandling() throws Exception {
        testNullCast("DECIMAL(2)", "DECIMAL(4,2)");
        testNullCast("DECIMAL(4)", "DECIMAL(6,3)");
        testNullCast("DECIMAL(9)", "DECIMAL(12,4)");
        testNullCast("DECIMAL(18)", "DECIMAL(20,5)");
        testNullCast("DECIMAL(19)", "DECIMAL(25,6)");
        testNullCast("DECIMAL(40)", "DECIMAL(50,7)");
    }

    @Test
    public void testRuntimeArithmeticOverflow() throws Exception {
        testRuntimeCastOverflow("cast(123456 as DECIMAL(6, 0))", "DECIMAL(76, 72)", "inconvertible value: 123456 [DECIMAL(6,0) -> DECIMAL(76,72)]");
    }

    @Test
    public void testScaledCasting() throws Exception {
        testRuntimeCast("99m", "99.00", "DECIMAL(4,2)");
        testRuntimeCast("99m", "99.0", "DECIMAL(3,1)");
        testRuntimeCast("cast(12.3 as DECIMAL(3,1))", "12.300", "DECIMAL(6,3)");
        testRuntimeCast("cast(12.34 as DECIMAL(4,2))", "12.340", "DECIMAL(7,3)");
        testRuntimeCast("cast(123.456 as DECIMAL(6,3))", "123.45600", "DECIMAL(8,5)");
        testRuntimeCast("cast(999999.999999 as DECIMAL(12,6))", "999999.999999000", "DECIMAL(15,9)");
        testRuntimeCast("cast(123 as DECIMAL(6,3))", "123.000", "DECIMAL(6,3)");
        testRuntimeCast("cast(123 as DECIMAL(6,3))", "123.00000", "DECIMAL(8,5)");
        testRuntimeCastOverflow("cast(99 as DECIMAL(2,0))", "DECIMAL(2,2)", "inconvertible value: 99 [DECIMAL(2,0) -> DECIMAL(2,2)]");
        testRuntimeCastOverflow("cast(99.9 as DECIMAL(3,1))", "DECIMAL(3,2)", "inconvertible value: 99.9 [DECIMAL(3,1) -> DECIMAL(3,2)]");
    }

    @Test
    public void testWidenDecimal128() throws Exception {
        testRuntimeCast("cast(9223372036854775807 as DECIMAL(19))", "9223372036854775807", "DECIMAL(40)");
        testRuntimeCast("cast(-9223372036854775807 as DECIMAL(19))", "-9223372036854775807", "DECIMAL(40)");
        testRuntimeCast("cast(-9223372036854775807 as DECIMAL(19))", "-9223372036854775807.00", "DECIMAL(40,2)");
        testRuntimeCast("cast(92233720368547758.07 as DECIMAL(20,2))", "92233720368547758.07", "DECIMAL(40,2)");
        testRuntimeCast("cast(12345678901234567890123456789 as DECIMAL(29))", "12345678901234567890123456789", "DECIMAL(40)");
        testRuntimeCast("cast(NULL as DECIMAL(19))", "", "DECIMAL(40)");
    }

    @Test
    public void testWidenDecimal16() throws Exception {
        testRuntimeCast("cast(9999 as DECIMAL(4))", "9999", "DECIMAL(9)");
        testRuntimeCast("cast(-9999 as DECIMAL(4))", "-9999", "DECIMAL(9)");
        testRuntimeCast("cast(9999 as DECIMAL(4))", "9999", "DECIMAL(18)");
        testRuntimeCast("cast(-9999 as DECIMAL(4))", "-9999", "DECIMAL(18)");
        testRuntimeCast("cast(9999 as DECIMAL(4))", "9999", "DECIMAL(19)");
        testRuntimeCast("cast(-9999 as DECIMAL(4))", "-9999", "DECIMAL(19)");
        testRuntimeCast("cast(9999 as DECIMAL(4))", "9999", "DECIMAL(40)");
        testRuntimeCast("cast(-9999 as DECIMAL(4))", "-9999", "DECIMAL(40)");
        testRuntimeCast("cast(NULL as DECIMAL(4))", "", "DECIMAL(9)");
        testRuntimeCast("cast(NULL as DECIMAL(4))", "", "DECIMAL(18)");
        testRuntimeCast("cast(NULL as DECIMAL(4))", "", "DECIMAL(19)");
        testRuntimeCast("cast(NULL as DECIMAL(4))", "", "DECIMAL(40)");
    }

    @Test
    public void testWidenDecimal256() throws Exception {
        testRuntimeCast("cast(9999999999999999999999999999999999999999 as DECIMAL(40))", "9999999999999999999999999999999999999999", "DECIMAL(42)");
        testRuntimeCast("cast(12345678901234567890123456789012345678.9012345678 as DECIMAL(48,10))", "12345678901234567890123456789012345678.90123456780", "DECIMAL(50,11)");
        testRuntimeCast("cast(12345678901234567890123456789012345678 as DECIMAL(38))", "12345678901234567890123456789012345678", "DECIMAL(38)");
        testRuntimeCast("cast(NULL as DECIMAL(40))", "", "DECIMAL(50)");
    }

    @Test
    public void testWidenDecimal32() throws Exception {
        testRuntimeCast("cast(999999999 as DECIMAL(9))", "999999999", "DECIMAL(18)");
        testRuntimeCast("cast(-999999999 as DECIMAL(9))", "-999999999", "DECIMAL(18)");
        testRuntimeCast("cast(999999999 as DECIMAL(9))", "999999999", "DECIMAL(19)");
        testRuntimeCast("cast(-999999999 as DECIMAL(9))", "-999999999", "DECIMAL(19)");
        testRuntimeCast("cast(999999999 as DECIMAL(9))", "999999999", "DECIMAL(40)");
        testRuntimeCast("cast(-999999999 as DECIMAL(9))", "-999999999", "DECIMAL(40)");
        testRuntimeCast("cast(NULL as DECIMAL(9))", "", "DECIMAL(18)");
        testRuntimeCast("cast(NULL as DECIMAL(9))", "", "DECIMAL(19)");
        testRuntimeCast("cast(NULL as DECIMAL(9))", "", "DECIMAL(40)");
    }

    @Test
    public void testWidenDecimal64() throws Exception {
        testRuntimeCast("cast(999999999999999999 as DECIMAL(18))", "999999999999999999", "DECIMAL(19)");
        testRuntimeCast("cast(-999999999999999999 as DECIMAL(18))", "-999999999999999999", "DECIMAL(19)");
        testRuntimeCast("cast(999999999999999999 as DECIMAL(18))", "999999999999999999", "DECIMAL(40)");
        testRuntimeCast("cast(-999999999999999999 as DECIMAL(18))", "-999999999999999999", "DECIMAL(40)");
        testRuntimeCast("cast(123456789012.345678 as DECIMAL(18,6))", "123456789012.345678", "DECIMAL(20,6)");
        testRuntimeCast("cast(NULL as DECIMAL(18))", "", "DECIMAL(19)");
        testRuntimeCast("cast(NULL as DECIMAL(18))", "", "DECIMAL(40)");
    }

    @Test
    public void testWidenDecimal8() throws Exception {
        testRuntimeCast("99m", "99", "DECIMAL(4)");
        testRuntimeCast("99m", "99", "DECIMAL(9)");
        testRuntimeCast("99m", "99", "DECIMAL(18)");
        testRuntimeCast("99m", "99", "DECIMAL(19)");
        testRuntimeCast("99m", "99", "DECIMAL(40)");
        testRuntimeCast("cast(NULL as DECIMAL(1))", "", "DECIMAL(2)");
        testRuntimeCast("cast(NULL as DECIMAL(2))", "", "DECIMAL(40)");
    }

    private Function parse(CharSequence cs) throws SqlException {
        return parseFunction(cs, metadata, functionParser);
    }

    private void testConstantCast(String inputDecimal, String expectedOutput, String targetType) throws Exception {
        assertSql(
                "cast\n" + expectedOutput + "\n",
                "SELECT cast(" + inputDecimal + " as " + targetType + ")"
        );
    }

    private void testNullCast(String sourceType, String targetType) throws Exception {
        assertSql(
                "cast\n\n",
                "WITH data AS (SELECT (cast(null as " + sourceType + ")) AS value) SELECT cast(value as " + targetType + ") FROM data"
        );
    }

    private void testRuntimeCast(String inputDecimal, String expectedOutput, String targetType) throws Exception {
        assertSql(
                "cast\n" + expectedOutput + "\n",
                "WITH data AS (SELECT (" + inputDecimal + ") AS value) SELECT cast(value as " + targetType + ") FROM data"
        );
    }

    private void testRuntimeCastOverflow(String inputDecimal, String targetType, String expectedErrorFragment) throws Exception {
        try {
            printSql(
                    "WITH data AS (SELECT (" + inputDecimal + ") AS value) SELECT cast(value as " + targetType + ") FROM data"
            );
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getMessage(), expectedErrorFragment);
        }
    }
}
