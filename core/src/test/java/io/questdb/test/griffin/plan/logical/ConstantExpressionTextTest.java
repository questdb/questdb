/*+*****************************************************************************
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

package io.questdb.test.griffin.plan.logical;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the text of a bound constant in each of its renderings: the text of a text constant
 * ({@link ConstantExpression#getText()}), the characters of a text or CHAR constant
 * ({@link ConstantExpression#getChars()}), the text {@code =} compares with a SYMBOL by
 * ({@link ConstantExpression#getSymbolKeyText()}) and the SQL string literal that spells a text
 * ({@link ConstantExpression#putSqlLiteral}), over every constant type.
 */
public class ConstantExpressionTextTest {
    private static final String FOUR_BYTE = new String(Character.toChars(0x1D11E));
    private static final String MULTI_BYTE = "é中" + FOUR_BYTE;

    @Test
    public void testEveryConstantType() {
        // type, getText(), getChars(), getSymbolKeyText(); null stands for a null result
        assertText(new ConstantExpression().ofString("abc", 0), "abc", "abc", "abc");
        assertText(new ConstantExpression().ofString("", 0), "", "", "");
        assertText(new ConstantExpression().ofString("it's", 0), "it's", "it's", "it's");
        assertText(new ConstantExpression().ofString(MULTI_BYTE, 0), MULTI_BYTE, MULTI_BYTE, MULTI_BYTE);
        assertText(new ConstantExpression().ofString(null, 0), null, null, null);

        assertText(new ConstantExpression().ofVarchar(new Utf8String("abc"), 0), "abc", "abc", "abc");
        assertText(new ConstantExpression().ofVarchar(new Utf8String(""), 0), "", "", "");
        assertText(new ConstantExpression().ofVarchar(new Utf8String(MULTI_BYTE), 0), MULTI_BYTE, MULTI_BYTE, MULTI_BYTE);
        assertText(new ConstantExpression().ofVarchar(null, 0), null, null, null);

        assertText(new ConstantExpression().ofSymbol("sym", 0), "sym", "sym", "sym");
        assertText(new ConstantExpression().ofSymbol(null, 0), null, null, null);

        // a CHAR has characters but no text, and = compares it with a SYMBOL by its character code instead
        assertText(new ConstantExpression().ofChar('a', 0), null, "a", null);
        assertText(new ConstantExpression().ofChar('\'', 0), null, "'", null);
        assertText(new ConstantExpression().ofChar('é', 0), null, "é", null);
        // a CHAR NULL is the NULL character
        assertText(new ConstantExpression().ofChar((char) 0, 0), null, "\u0000", null);

        // an integer reads as its decimal text; BYTE and SHORT have no NULL
        assertText(new ConstantExpression().ofByte(Byte.MIN_VALUE, 0), null, null, "-128");
        assertText(new ConstantExpression().ofByte((byte) 0, 0), null, null, "0");
        assertText(new ConstantExpression().ofByte(Byte.MAX_VALUE, 0), null, null, "127");
        assertText(new ConstantExpression().ofShort(Short.MIN_VALUE, 0), null, null, "-32768");
        assertText(new ConstantExpression().ofShort(Short.MAX_VALUE, 0), null, null, "32767");
        assertText(new ConstantExpression().ofInt(Numbers.INT_NULL, 0), null, null, null);
        assertText(new ConstantExpression().ofInt(Numbers.INT_NULL + 1, 0), null, null, "-2147483647");
        assertText(new ConstantExpression().ofInt(-1, 0), null, null, "-1");
        assertText(new ConstantExpression().ofInt(0, 0), null, null, "0");
        assertText(new ConstantExpression().ofInt(Integer.MAX_VALUE, 0), null, null, "2147483647");
        assertText(new ConstantExpression().ofLong(Numbers.LONG_NULL, 0), null, null, null);
        assertText(new ConstantExpression().ofLong(Numbers.LONG_NULL + 1, 0), null, null, "-9223372036854775807");
        assertText(new ConstantExpression().ofLong(0, 0), null, null, "0");
        assertText(new ConstantExpression().ofLong(Long.MAX_VALUE, 0), null, null, "9223372036854775807");

        // no other type has text
        assertText(new ConstantExpression().ofNull(0), null, null, null);
        assertText(new ConstantExpression().ofBoolean(true, 0), null, null, null);
        assertText(new ConstantExpression().ofBoolean(false, 0), null, null, null);
        assertText(new ConstantExpression().ofDouble(1.5, 0), null, null, null);
        assertText(new ConstantExpression().ofDouble(Double.NaN, 0), null, null, null);
        assertText(new ConstantExpression().ofFloat(1.5f, 0).withLiteralText("1.5"), null, null, null);
        assertText(new ConstantExpression().ofTimestamp(0, ColumnType.TIMESTAMP_MICRO, 0), null, null, null);
        final ConstantExpression timestampText = new ConstantExpression().ofTimestamp(0, ColumnType.TIMESTAMP_NANO, 0);
        timestampText.withTimestampText("1970-01-01");
        assertText(timestampText, null, null, null);
        assertText(new ConstantExpression().ofTimestamp(Numbers.LONG_NULL, ColumnType.TIMESTAMP_MICRO, 0), null, null, null);
        assertText(new ConstantExpression().ofDate(0, 0), null, null, null);
        assertText(new ConstantExpression().ofIPv4(1, 0), null, null, null);
        assertText(new ConstantExpression().ofUuid(1, 2, 0), null, null, null);
        assertText(new ConstantExpression().ofLong128(1, 2, 0), null, null, null);
        assertText(new ConstantExpression().ofLong256(new Long256Impl(), 0), null, null, null);
        assertText(new ConstantExpression().ofGeoHash(1, ColumnType.getGeoHashTypeWithBits(5), 0), null, null, null);
        assertText(new ConstantExpression().ofDecimal(ColumnType.getDecimalType(18, 2), 0, 0, 0, 150, 0), null, null, null);
        assertText(new ConstantExpression().ofInterval(0, 1, ColumnType.INTERVAL_TIMESTAMP_MICRO, 0), null, null, null);
        assertText(new ConstantExpression().ofBinaryNull(0), null, null, null);
    }

    @Test
    public void testPutSqlLiteral() {
        assertSqlLiteral("''", "");
        assertSqlLiteral("'abc'", "abc");
        assertSqlLiteral("'it''s'", "it's");
        assertSqlLiteral("''''", "'");
        assertSqlLiteral("''''''", "''");
        assertSqlLiteral("'a''b''c'", "a'b'c");
        assertSqlLiteral("'\"'", "\"");
        assertSqlLiteral("'" + MULTI_BYTE + "'", MULTI_BYTE);
        // the sink keeps what it held before
        final StringSink sink = new StringSink();
        sink.put("x=");
        ConstantExpression.putSqlLiteral(sink, "y");
        Assert.assertEquals("x='y'", sink.toString());
    }

    @Test
    public void testTextOutlivesOtherConstants() {
        // Each constant formats into a sink of its own, so the text of one stays valid while another formats its own:
        // a key set borrows the text of every key constant without copying it.
        final ConstantExpression one = new ConstantExpression().ofInt(1, 0);
        final ConstantExpression two = new ConstantExpression().ofLong(-2, 0);
        final ConstantExpression a = new ConstantExpression().ofChar('a', 0);
        final ConstantExpression b = new ConstantExpression().ofChar('b', 0);
        final ConstantExpression nonAscii = new ConstantExpression().ofVarchar(new Utf8String(MULTI_BYTE), 0);
        final CharSequence oneText = one.getSymbolKeyText();
        final CharSequence aChars = a.getChars();
        final CharSequence nonAsciiText = nonAscii.getText();
        Assert.assertEquals("-2", two.getSymbolKeyText().toString());
        Assert.assertEquals("b", b.getChars().toString());
        Assert.assertEquals("1", oneText.toString());
        Assert.assertEquals("a", aChars.toString());
        Assert.assertEquals(MULTI_BYTE, nonAsciiText.toString());
        // asking again gives the same text
        Assert.assertEquals("1", one.getSymbolKeyText().toString());
        Assert.assertEquals("a", a.getChars().toString());
        Assert.assertEquals(MULTI_BYTE, nonAscii.getChars().toString());
        Assert.assertEquals(MULTI_BYTE, nonAscii.getSymbolKeyText().toString());
    }

    private static void assertEquals(String message, String expected, CharSequence actual) {
        Assert.assertEquals(message, expected, actual == null ? null : actual.toString());
    }

    private static void assertSqlLiteral(String expected, CharSequence text) {
        final StringSink sink = new StringSink();
        ConstantExpression.putSqlLiteral(sink, text);
        Assert.assertEquals(expected, sink.toString());
    }

    private static void assertText(ConstantExpression constant, String text, String chars, String symbolKeyText) {
        final String type = ColumnType.nameOf(constant.getDataType());
        assertEquals(type + " getText()", text, constant.getText());
        assertEquals(type + " getChars()", chars, constant.getChars());
        assertEquals(type + " getSymbolKeyText()", symbolKeyText, constant.getSymbolKeyText());
        if (chars != null) {
            final StringSink sink = new StringSink();
            ConstantExpression.putSqlLiteral(sink, constant.getChars());
            Assert.assertEquals(type + " SQL literal", '\'' + chars.replace("'", "''") + '\'', sink.toString());
        }
    }
}
