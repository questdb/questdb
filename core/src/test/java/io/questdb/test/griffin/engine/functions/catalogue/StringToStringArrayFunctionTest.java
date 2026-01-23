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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.catalogue.StringToStringArrayFunction;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class StringToStringArrayFunctionTest {

    @Test
    public void testEmptyArray() throws SqlException {
        assertArray("{}", array());
    }

    @Test
    public void testEmptyString() {
        assertFailure("", "array must start with '{'");
    }

    @Test
    public void testEscapeWithBackslash() throws SqlException {
        assertArray("{\\a\\b}", array("ab"));
        assertArray("{\"\\\\\"}", array("\\"));
        assertArray("{\\\\}", array("\\"));
        assertArray("{\\}}", array("}"));
        assertArray("{\\{}", array("{"));
        assertArray("{\\,}", array(","));
        assertArray("{\\\"}", array("\""));
        assertArray("{\\ }", array(" "));
        assertFailure("{\\}", "array must end with '}'");
    }

    @Test
    public void testEscapeWithDoubleQuote() throws SqlException {
        assertArray("{\"test 1\"}", array("test 1"));
        assertArray("{\" abc \" , xyz}", array(" abc ", "xyz"));
        assertArray("{\" \"}", array(" "));
        assertArray("{\",\"}", array(","));
        assertArray("{\"{}\"}", array("{}"));
        assertArray("{\"\"}", array(""));
        assertFailure("{\"}", "array must end with '}'");
        assertFailure("{\"aaa\"bbb}", "unexpected character after '\"'");
        assertFailure("{\"\"\"}", "unexpected character after '\"'");
        assertFailure("{abc\"}", "unexpected '\"' character");
    }

    @Test
    public void testGeoHashInterface() throws SqlException {
        StringToStringArrayFunction f = new StringToStringArrayFunction(5, "{abcd}");
        Assert.assertThrows(UnsupportedOperationException.class, () -> f.getGeoInt(null));
        Assert.assertThrows(UnsupportedOperationException.class, () -> f.getGeoLong(null));
        Assert.assertThrows(UnsupportedOperationException.class, () -> f.getGeoShort(null));
        Assert.assertThrows(UnsupportedOperationException.class, () -> f.getGeoByte(null));
    }

    @Test
    public void testGetStrEmpty() throws SqlException {
        StringToStringArrayFunction function = new StringToStringArrayFunction(42, "{}");
        TestUtils.assertEquals("{}", function.getStrA(null));
        TestUtils.assertEquals("{}", function.getStrB(null));
        Assert.assertEquals(2, function.getStrLen(null));
    }

    @Test
    public void testGetStrSimple() throws SqlException {
        StringToStringArrayFunction function = new StringToStringArrayFunction(42, "{ab, 3,true,1.26,test 1}");
        TestUtils.assertEquals("{ab,3,true,1.26,test 1}", function.getStrA(null));
        TestUtils.assertEquals("{ab,3,true,1.26,test 1}", function.getStrB(null));
        Assert.assertEquals(23, function.getStrLen(null));
    }

    @Test
    public void testNestedArray() {
        assertFailure("{{}}", "unexpected '{' character");
    }

    @Test
    public void testNonWhitespaceAfterClosingBracket() {
        assertFailure("{abc, a, bb}ccc", "unexpected character after '}'");
        assertFailure("{abc, a, bb}{ccc}", "unexpected character after '}'");
        assertFailure("{abc, a, bb},", "unexpected character after '}'");
        assertFailure("{abc, a, bb}}", "unexpected character after '}'");
    }

    @Test
    public void testNonWhitespaceBeforeOpeningBracket() {
        assertFailure("ccc{abc, a, bb}", "array must start with '{'");
        assertFailure(",{abc, a, bb}", "array must start with '{'");
        assertFailure("ccc,{abc, a, bb}", "array must start with '{'");
        assertFailure("}{abc, a, bb}", "array must start with '{'");
    }

    @Test
    public void testNull() {
        assertFailure(null, "NULL is not allowed");
    }

    @Test
    public void testSimple() throws SqlException {
        assertArray("{ab, 3,true,1.26,test 1}", array("ab", "3", "true", "1.26", "test 1"));
    }

    @Test
    public void testSkippingWhitespaces() throws SqlException {
        assertArray(" { test 1 } ", array("test 1"));
    }

    @Test
    public void testStringWithoutClosingBracket() {
        assertFailure("{ab,c,def", "array must end with '}'");
    }

    @Test
    public void testStringWithoutOpeningBracket() {
        assertFailure("ab,c,def}", "array must start with '{'");
        assertFailure(" ", "array must start with '{'");
    }

    @Test
    public void testUnexpectedComma() {
        assertFailure("{ab,}", "unexpected '}' character");
        assertFailure("{,}", "unexpected ',' character");
        assertFailure("{,ab}", "unexpected ',' character");
    }

    private static String[] array(String... items) {
        return items;
    }

    private static void assertArray(CharSequence expression, CharSequence[] expected) throws SqlException {
        StringToStringArrayFunction function = new StringToStringArrayFunction(5, expression);
        Assert.assertEquals(expected.length, function.extendedOps().getArrayLength());
        for (int i = 0, n = expected.length; i < n; i++) {
            Assert.assertEquals(expected[i], function.extendedOps().getStrA(null, i));
        }
    }

    private static void assertFailure(CharSequence expression, CharSequence expectedMsg) {
        try {
            new StringToStringArrayFunction(5, expression);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals(5, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), expectedMsg);
        }
    }
}
