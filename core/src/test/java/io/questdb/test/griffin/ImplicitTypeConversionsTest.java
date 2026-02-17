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

package io.questdb.test.griffin;

import io.questdb.cairo.ImplicitCastException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test implicit narrowing conversions :
 * double -> float, long, int, short, byte
 * float -> long, int, short, byte
 * long -> int, short, byte
 * int -> short, byte
 * short -> byte
 */
public class ImplicitTypeConversionsTest extends AbstractCairoTest {

    @Test
    public void testInsertDoubleAsByte_Causes_Overflow_and_throws_exception() throws Exception {
        testInsertCausesException("double", "128", "byte");
    }

    @Test
    public void testInsertDoubleAsByte_Causes_Underflow_and_throws_exception() throws Exception {
        testInsertCausesException("double", "-129", "byte");
    }

    @Test
    public void testInsertDoubleAsByte_ReturnsMinValue() throws Exception {
        testInsert("double", "-128.0", "byte", "-128");
    }

    @Test
    public void testInsertDoubleAsFloat_CausesOverflow_And_ReturnsException() throws Exception {
        testInsertCausesException("double", "340282357000000000000000000000000000000.0", "float");
    }

    //double->float
    @Test
    public void testInsertDoubleAsFloat_CausesUnderflow_And_ReturnsException() throws Exception {
        testInsertCausesException("double", "-340282350000000000000000000000000000000.0", "float");
    }

    @Test
    public void testInsertDoubleAsFloat_Causes_Overflow_and_throws_exception() throws Exception {
        testInsertCausesException("double", "3.4028235E38", "float");
    }

    @Test
    public void testInsertDoubleAsFloat_Causes_Underflow_and_throws_exception() throws Exception {
        testInsertCausesException("double", "-3.4028236E38", "float");
    }

    @Test
    public void testInsertDoubleAsFloat_ReturnsApproximateValue() throws Exception {
        testInsert("double", "12345.6789", "float", "12345.679");
    }

    @Test
    public void testInsertDoubleAsFloat_ReturnsExactValue() throws Exception {
        testInsert("double", "123.4567", "float", "123.4567");
    }

    @Test
    public void testInsertDoubleAsFloat_ReturnsMaxValue() throws Exception {
        testInsert("double", "-3.4028234E38", "float", "-3.4028235E38");
    }

    @Test
    public void testInsertDoubleAsFloat_ReturnsMinValue() throws Exception {
        testInsert("double", "3.4028234E38", "float", "3.4028235E38");
    }

    @Test
    public void testInsertDoubleAsInt_Causes_Overflow_and_throws_exception() throws Exception {
        testInsertCausesException("double", "2147483648.0", "int");
    }

    @Test
    public void testInsertDoubleAsInt_Causes_Underflow_and_throws_exception() throws Exception {
        testInsertCausesException("double", "-2147483649.0", "int");
    }

    @Test
    public void testInsertDoubleAsInt_ReturnsActualMinValue() throws Exception {
        testInsert("double", "-2147483647.0", "int", "-2147483647");//see Numbers.append:127
    }

    @Test
    public void testInsertDoubleAsInt_ReturnsMaxValue() throws Exception {
        testInsert("double", "2147483647.0", "int", "2147483647");
    }

    @Test
    public void testInsertDoubleAsInt_ReturnsMinValue() throws Exception {
        testInsert("double", "-2147483648.0", "int", "null");//see Numbers.append:127
    }

    @Test
    public void testInsertDoubleAsLong_Causes_Overflow_and_throws_exception() throws Exception {
        testInsertCausesException("double", "9223372036854779908.0", "long");
    }

    @Test
    public void testInsertDoubleAsLong_Causes_Underflow_and_throws_exception() throws Exception {
        testInsertCausesException("double", "-9223372036855775809.0", "long");
    }

    //input can't be -9223372036854775808 due to  #1615
    @Test
    public void testInsertDoubleAsLong_ReturnsActualMinValue() throws Exception {
        testInsert("double", "-9223372036854775250.0", "long", "-9223372036854774784");
    }

    @Test
    public void testInsertDoubleAsLong_ReturnsMaxValue() throws Exception {
        testInsert("double", "9223372036854775807.0", "long", "9223372036854775807");
    }

    @Test
    public void testInsertDoubleAsLong_ReturnsMinValue() throws Exception {
        testInsertCausesException("double", "-9223372036854775300.0", "long");
    }

    @Test
    public void testInsertDoubleAsShort_Causes_Overflow_and_throws_exception() throws Exception {
        testInsertCausesException("double", "32768", "short");
    }

    @Test
    public void testInsertDoubleAsShort_Causes_Underflow_and_throws_exception() throws Exception {
        testInsertCausesException("double", "-32769", "short");
    }

    @Test
    public void testInsertDoubleAsShort_ReturnsMaxValue() throws Exception {
        testInsert("double", "32767.0", "short", "32767");
    }

    @Test
    public void testInsertDoubleAsShort_ReturnsMinValue() throws Exception {
        testInsert("double", "-32768.0", "short", "-32768");
    }

    @Test
    public void testInsertDoublesByte_ReturnsMaxValue() throws Exception {
        testInsert("double", "127.0", "byte", "127");
    }

    @Test
    public void testInsertFloatAsByte_Causes_Overflow_and_throws_exception() throws Exception {
        testInsertCausesException("float", "128", "byte");
    }

    @Test
    public void testInsertFloatAsByte_Causes_Underflow_and_throws_exception() throws Exception {
        testInsertCausesException("float", "-129", "byte");
    }

    @Test
    public void testInsertFloatAsByte_ReturnsMinValue() throws Exception {
        testInsert("float", "-128.0", "byte", "-128");
    }

    @Test
    public void testInsertFloatAsInt_Causes_Overflow_and_throws_exception() throws Exception {
        testInsert("float", "2147483648.0", "int", "2147483647");
    }

    @Test
    public void testInsertFloatAsInt_Causes_Underflow_and_throws_exception() throws Exception {
        testInsertCausesException("float", "-2147483848.0", "int");
    }

    @Test
    public void testInsertFloatAsInt_ReturnsActualMinValue() throws Exception {
        testInsert("float", "-2147483520.0", "int", "-2147483520");//see Numbers.append:127
    }

    @Test
    public void testInsertFloatAsInt_ReturnsMaxValue() throws Exception {
        testInsert("float", "2147483520.0", "int", "2147483520");
    }

    @Test
    public void testInsertFloatAsInt_ReturnsMinValue() throws Exception {
        testInsertCausesException("float", "-2147483648.0", "int");
    }

    @Test
    public void testInsertFloatAsLong_Causes_Overflow_and_throws_exception() throws Exception {
        testInsertCausesException("float", "9223373036854775808.0", "long");
    }

    @Test
    public void testInsertFloatAsLong_Causes_Underflow_and_throws_exception() throws Exception {
        testInsertCausesException("float", "-9223373036854775808.0", "long");
    }

    @Test
    public void testInsertFloatAsLong_ReturnsActualMinValue() throws Exception {
        testInsert("float", "-9223371036854775807.0", "long", "-9223370937343148032");
    }

    @Test
    public void testInsertFloatAsLong_ReturnsMaxValue() throws Exception {
        testInsert("float", "9223372036854775807.0", "long", "9223372036854775807");
    }

    //input can't be -9223372036854775808 due to  #1615
    @Test
    public void testInsertFloatAsLong_ReturnsMinValue() throws Exception {
        testInsertCausesException("float", "-9223372036854775807.0", "long");
    }

    @Test
    public void testInsertFloatAsShort_Causes_Overflow_and_throws_exception() throws Exception {
        testInsertCausesException("float", "32768", "short");
    }

    @Test
    public void testInsertFloatAsShort_Causes_Underflow_and_throws_exception() throws Exception {
        testInsertCausesException("float", "-32769", "short");
    }

    @Test
    public void testInsertFloatAsShort_ReturnsMaxValue() throws Exception {
        testInsert("float", "32767.0", "short", "32767");
    }

    @Test
    public void testInsertFloatAsShort_ReturnsMinValue() throws Exception {
        testInsert("float", "-32768.0", "short", "-32768");
    }

    @Test
    public void testInsertFloatsByte_ReturnsMaxValue() throws Exception {
        testInsert("float", "127.0", "byte", "127");
    }

    @Test
    public void testInsertIntAsByte_Causes_Overflow_and_throws_exception() throws Exception {
        testInsertCausesException("int", "128", "byte");
    }

    @Test
    public void testInsertIntAsByte_Causes_Underflow_and_throws_exception() throws Exception {
        testInsertCausesException("int", "-129", "byte");
    }

    @Test
    public void testInsertIntAsByte_ReturnsMinValue() throws Exception {
        testInsert("int", "-128", "byte");
    }

    @Test
    public void testInsertIntAsShort_Causes_Overflow_and_throws_exception() throws Exception {
        testInsertCausesException("int", "32768", "short");
    }

    @Test
    public void testInsertIntAsShort_Causes_Underflow_and_throws_exception() throws Exception {
        testInsertCausesException("int", "-32769", "short");
    }

    @Test
    public void testInsertIntAsShort_ReturnsMaxValue() throws Exception {
        testInsert("int", "32767", "short");
    }

    @Test
    public void testInsertIntAsShort_ReturnsMinValue() throws Exception {
        testInsert("int", "-32768", "short");
    }

    @Test
    public void testInsertIntsByte_ReturnsMaxValue() throws Exception {
        testInsert("int", "127", "byte");
    }

    @Test
    public void testInsertLongAsByte_Causes_Overflow_and_throws_exception() throws Exception {
        testInsertCausesException("long", "128", "byte");
    }

    @Test
    public void testInsertLongAsByte_Causes_Underflow_and_throws_exception() throws Exception {
        testInsertCausesException("long", "-129", "byte");
    }

    @Test
    public void testInsertLongAsByte_ReturnsMinValue() throws Exception {
        testInsert("long", "-128", "byte");
    }

    @Test
    public void testInsertLongAsInt_Causes_Overflow_and_throws_exception() throws Exception {
        testInsertCausesException("long", "2147483648", "int");
    }

    @Test
    public void testInsertLongAsInt_Causes_Underflow_and_throws_exception() throws Exception {
        testInsertCausesException("long", "-2147483649", "int");
    }

    @Test
    public void testInsertLongAsInt_ReturnsActualMinValue() throws Exception {
        testInsert("long", "-2147483647", "int", "-2147483647"); //see Numbers.append:127
    }

    @Test
    public void testInsertLongAsInt_ReturnsMaxValue() throws Exception {
        testInsert("long", "2147483647", "int");
    }

    @Test
    public void testInsertLongAsInt_ReturnsMinValue() throws Exception {
        testInsert("long", "-2147483648", "int", "null"); //see Numbers.append:127
    }

    @Test
    public void testInsertLongAsShort_Causes_Overflow_and_throws_exception() throws Exception {
        testInsertCausesException("long", "32768", "short");
    }

    @Test
    public void testInsertLongAsShort_Causes_Underflow_and_throws_exception() throws Exception {
        testInsertCausesException("long", "-32769", "short");
    }

    @Test
    public void testInsertLongAsShort_ReturnsMaxValue() throws Exception {
        testInsert("long", "32767", "short");
    }

    @Test
    public void testInsertLongAsShort_ReturnsMinValue() throws Exception {
        testInsert("long", "-32768", "short");
    }

    @Test
    public void testInsertLongsByte_ReturnsMaxValue() throws Exception {
        testInsert("long", "127", "byte");
    }

    @Test
    public void testInsertNonZeroDoubleAsFloat_ReturnsValueWithoutFraction() throws Exception {
        testInsert("double", "2.34567", "float", "2.34567");//formatting issue, number is stored properly
    }

    @Test
    public void testInsertShortAsByte_Causes_Overflow_and_throws_exception() throws Exception {
        testInsertCausesException("short", "128", "byte");
    }

    @Test
    public void testInsertShortAsByte_Causes_Underflow_and_throws_exception() throws Exception {
        testInsertCausesException("short", "-129", "byte");
    }

    @Test
    public void testInsertShortAsByte_ReturnsMaxValue() throws Exception {
        testInsert("short", "127", "byte");
    }

    @Test
    public void testInsertShortAsByte_ReturnsMinValue() throws Exception {
        testInsert("short", "-128", "byte");
    }

    @Test
    public void testInsertZeroDoubleAsFloat_ReturnsExactValue() throws Exception {
        testInsert("double", "0.0", "float", "0.0");
    }

    //double->int
    @Test
    public void testInsertZeroDoubleAsInt_ReturnsExactValue() throws Exception {
        testInsert("double", "0.0", "int", "0");
    }

    //double->long
    @Test
    public void testInsertZeroDoubleAsLong_ReturnsExactValue() throws Exception {
        testInsert("double", "0.0", "long", "0");
    }

    //double->short
    @Test
    public void testInsertZeroDoubleAsShort_ReturnsExactValue() throws Exception {
        testInsert("double", "0.0", "short", "0");
    }

    //float->int
    @Test
    public void testInsertZeroFloatAsInt_ReturnsExactValue() throws Exception {
        testInsert("float", "0.0", "int", "0");
    }

    //float->long
    @Test
    public void testInsertZeroFloatAsLong_ReturnsExactValue() throws Exception {
        testInsert("float", "0.0", "long", "0");
    }

    //float->short
    @Test
    public void testInsertZeroFloatAsShort_ReturnsExactValue() throws Exception {
        testInsert("float", "0.0", "short", "0");
    }

    //int->byte
    @Test
    public void testInsertZeroIntAsByte_ReturnsExactValue() throws Exception {
        testInsert("int", "0", "byte");
    }

    //int->short
    @Test
    public void testInsertZeroIntAsShort_ReturnsExactValue() throws Exception {
        testInsert("int", "0", "short");
    }

    //double->byte
    @Test
    public void testInsertZeroIntegerDoubleAsByte_ReturnsExactValue() throws Exception {
        testInsert("double", "0.0", "byte", "0");
    }

    //float->byte
    @Test
    public void testInsertZeroIntegerFloatAsByte_ReturnsExactValue() throws Exception {
        testInsert("float", "0.0", "byte", "0");
    }

    //long->byte
    @Test
    public void testInsertZeroLongAsByte_ReturnsExactValue() throws Exception {
        testInsert("long", "0", "byte");
    }

    //long->int
    @Test
    public void testInsertZeroLongAsInt_ReturnsExactValue() throws Exception {
        testInsert("long", "0", "int");
    }

    //long->short
    @Test
    public void testInsertZeroLongAsShort_ReturnsExactValue() throws Exception {
        testInsert("long", "0", "short");
    }

    @Test
    public void testInsertZeroNonDoubleDoubleAsInt_ReturnsValueWithoutFraction() throws Exception {
        testInsert("double", "2.34567", "int", "2");
    }

    @Test
    public void testInsertZeroNonDoubleDoubleAsLong_ReturnsValueWithoutFraction() throws Exception {
        testInsert("double", "2.34567", "short", "2");
    }

    @Test
    public void testInsertZeroNonFloatFloatAsInt_ReturnsValueWithoutFraction() throws Exception {
        testInsert("float", "2.34567", "int", "2");
    }

    @Test
    public void testInsertZeroNonFloatFloatAsLong_ReturnsValueWithoutFraction() throws Exception {
        testInsert("float", "7.891", "long", "7");
    }

    @Test
    public void testInsertZeroNonIntegerDoubleAsByte_ReturnsValueWithoutFraction() throws Exception {
        testInsert("double", "1.1234", "byte", "1");
    }

    @Test
    public void testInsertZeroNonIntegerDoubleAsShort_ReturnsValueWithoutFraction() throws Exception {
        testInsert("double", "5.678", "short", "5");
    }

    @Test
    public void testInsertZeroNonIntegerFloatAsByte_ReturnsValueWithoutFraction() throws Exception {
        testInsert("float", "1.1234", "byte", "1");
    }

    @Test
    public void testInsertZeroNonIntegerFloatAsShort_ReturnsValueWithoutFraction() throws Exception {
        testInsert("float", "5.678", "short", "5");
    }

    //short->byte
    @Test
    public void testInsertZeroShortAsByte_ReturnsExactValue() throws Exception {
        testInsert("short", "0", "byte");
    }

    private void testInsert(String valueType, String value, String targetColumnType, String expectedValue) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab(x " + targetColumnType + " );");
            execute("insert into tab values (cast(" + value + " as " + valueType + " ));");

            String expected = "x\n" + expectedValue + "\n";

            assertReader(expected, "tab");
        });
    }

    private void testInsert(String valueType, String value, String targetColumnType) throws Exception {
        testInsert(valueType, value, targetColumnType, value);
    }

    private void testInsertCausesException(String valueType, String value, String targetColumnType) throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("create table tab(x " + targetColumnType + " );");
                execute("insert into tab values (cast(" + value + " as " + valueType + " ));");
                Assert.fail("SqlException should be thrown!");
            } catch (ImplicitCastException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value");
            }
        });
    }
}
