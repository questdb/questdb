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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeTag;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FunctionFactoryDescriptorTest {

    private static final StringSink sink = new StringSink();

    @Test
    public void testGetArgTypeTagOfNonSignatureChars() {
        // 'y' is the one free ASCII letter; the brackets and the old '[' | 32 map key are not types
        for (char c : new char[]{'y', 'Y', '[', ']', '{', '(', ')', ',', ' ', '0', '\u00e0'}) {
            Assert.assertEquals("char " + c, -1, FunctionFactoryDescriptor.getArgTypeTag(c));
        }
    }

    @Test
    public void testSignatureCharsDifferFromUpperCaseInBit5Only() {
        // FunctionFactoryDescriptor reads the constant flag as (c & 32) != 0 and folds case as
        // c | 32, so every signature character must be a lower-case letter whose upper-case
        // form is the same code point with bit 5 cleared. This holds for the three non-ASCII
        // characters too (U+00F8/U+00D8, U+03B4/U+0394, U+03BE/U+039E).
        int count = 0;
        for (ColumnTypeTag tag : ColumnTypeTag.values()) {
            final char c = FunctionFactoryDescriptor.signatureChar(tag);
            if (c == FunctionFactoryDescriptor.NO_SIGNATURE_CHAR) {
                continue;
            }
            count++;
            final char upper = (char) (c & ~32);
            Assert.assertTrue(tag + ": not lower case: " + c, Character.isLowerCase(c));
            Assert.assertTrue(tag + ": bit 5 clear: " + c, (c & 32) != 0);
            Assert.assertEquals(tag + ": upper case is not bit 5: " + c, Character.toUpperCase(c), upper);
            Assert.assertTrue(tag + ": not upper case: " + upper, Character.isUpperCase(upper));
            Assert.assertEquals(tag + ": lower case is not bit 5: " + upper, c, Character.toLowerCase(upper));
            Assert.assertEquals(tag + ": lower", tag.code(), FunctionFactoryDescriptor.getArgTypeTag(c));
            Assert.assertEquals(tag + ": upper", tag.code(), FunctionFactoryDescriptor.getArgTypeTag(upper));
            Assert.assertNotNull(tag + ": no type name", FunctionFactoryDescriptor.signatureTypeName(tag));
        }
        Assert.assertEquals(28, count);
    }

    @Test
    public void testSignatureCharTable() {
        // Pins the character of every tag. A new tag appears here as a new row; a tag without a
        // character shows '-' and cannot be named by any factory signature.
        final StringSink table = new StringSink();
        for (ColumnTypeTag tag : ColumnTypeTag.values()) {
            final char c = FunctionFactoryDescriptor.signatureChar(tag);
            table.put(tag.name()).put(' ');
            if (c == FunctionFactoryDescriptor.NO_SIGNATURE_CHAR) {
                table.put('-');
            } else {
                table.put(c).put(' ').put(FunctionFactoryDescriptor.signatureTypeName(tag));
            }
            table.put('\n');
        }
        TestUtils.assertEquals(
                """
                        UNDEFINED -
                        BOOLEAN t boolean
                        BYTE b byte
                        SHORT e short
                        CHAR a char
                        INT i int
                        LONG l long
                        DATE m date
                        TIMESTAMP n timestamp
                        FLOAT f float
                        DOUBLE d double
                        STRING s string
                        SYMBOL k symbol
                        LONG256 h long256
                        GEOBYTE -
                        GEOSHORT -
                        GEOINT -
                        GEOLONG -
                        BINARY u binary
                        UUID z uuid
                        CURSOR c cursor
                        VAR_ARG v var_arg
                        RECORD r record
                        GEOHASH g geohash
                        LONG128 j long128
                        IPv4 x ipv4
                        VARCHAR \u00f8 varchar
                        ARRAY -
                        DECIMAL8 -
                        DECIMAL16 -
                        DECIMAL32 -
                        DECIMAL64 -
                        DECIMAL128 -
                        DECIMAL256 -
                        DECIMAL \u03be decimal
                        REGCLASS p reg_class
                        REGPROCEDURE q reg_procedure
                        ARRAY_STRING w array_string
                        PARAMETER -
                        INTERVAL \u03b4 interval
                        VARCHAR_SLICE -
                        NULL o null
                        UNKNOWN -
                        """,
                table
        );
    }

    @Test
    public void testSignatureCharsAreUnique() {
        final StringSink seen = new StringSink();
        for (ColumnTypeTag tag : ColumnTypeTag.values()) {
            final char c = FunctionFactoryDescriptor.signatureChar(tag);
            if (c != FunctionFactoryDescriptor.NO_SIGNATURE_CHAR) {
                Assert.assertEquals("character " + c + " taken twice, last by " + tag, -1, seen.indexOf(String.valueOf(c)));
                seen.put(c);
            }
        }
    }

    @Test
    public void testSignatureTypeNameOfTagWithoutCharThrows() {
        for (ColumnTypeTag tag : ColumnTypeTag.values()) {
            if (FunctionFactoryDescriptor.signatureChar(tag) == FunctionFactoryDescriptor.NO_SIGNATURE_CHAR) {
                try {
                    FunctionFactoryDescriptor.signatureTypeName(tag);
                    Assert.fail(tag.name());
                } catch (IllegalArgumentException e) {
                    TestUtils.assertContains(e.getMessage(), "tag has no signature character: " + tag);
                }
            }
        }
    }

    @Test
    public void testSignatureWithArrayAsAFirstArgument() throws SqlException {
        FunctionFactoryDescriptor descriptor = descriptorOf("=(S[]S)");

        Assert.assertEquals(2, descriptor.getSigArgCount());
        // S[]
        Assert.assertTrue(isArray(descriptor, 0));
        Assert.assertFalse(isConstant(descriptor, 0));
        assertType(descriptor, 0);
        // S
        Assert.assertFalse(isArray(descriptor, 1));
        Assert.assertFalse(isConstant(descriptor, 1));
        assertType(descriptor, 1);
    }

    @Test
    public void testSignatureWithArrayAsASecondArgument() throws SqlException {
        FunctionFactoryDescriptor descriptor = descriptorOf("=(SS[])");

        Assert.assertEquals(2, descriptor.getSigArgCount());
        // S
        Assert.assertFalse(isArray(descriptor, 0));
        Assert.assertFalse(isConstant(descriptor, 0));
        assertType(descriptor, 0);
        // S[]
        Assert.assertTrue(isArray(descriptor, 1));
        Assert.assertFalse(isConstant(descriptor, 1));
        assertType(descriptor, 1);
    }

    @Test
    public void testSignatureWithConstantArrayAsAFirstArgument() throws SqlException {
        FunctionFactoryDescriptor descriptor = descriptorOf("=(s[]S)");

        Assert.assertEquals(2, descriptor.getSigArgCount());
        // s[]
        Assert.assertTrue(isArray(descriptor, 0));
        Assert.assertTrue(isConstant(descriptor, 0));
        assertType(descriptor, 0);
        // S
        Assert.assertFalse(isArray(descriptor, 1));
        Assert.assertFalse(isConstant(descriptor, 1));
        assertType(descriptor, 1);
    }

    @Test
    public void testSignatureWithConstantArrayAsASecondArgument() throws SqlException {
        FunctionFactoryDescriptor descriptor = descriptorOf("=(Ss[])");

        Assert.assertEquals(2, descriptor.getSigArgCount());
        // S
        Assert.assertFalse(isArray(descriptor, 0));
        Assert.assertFalse(isConstant(descriptor, 0));
        assertType(descriptor, 0);
        // s[]
        Assert.assertTrue(isArray(descriptor, 1));
        Assert.assertTrue(isConstant(descriptor, 1));
        assertType(descriptor, 1);
    }

    @Test
    public void testTranslateBrokenSignatureWithArray() {
        assertFailTranslateSignature("=", "=(Ss]aV)", "offending: ']'");
        assertFailTranslateSignature("abs", "abs(S[)", "offending array: '['");
        assertFailTranslateSignature(">", "=([])", "offending array: '['");
        assertFailTranslateSignature("not", "=(s])", "offending: ']'");
    }

    @Test
    public void testTranslateSignatureWithArray() {
        sink.clear();
        FunctionFactoryDescriptor.translateSignature("=", "=(Ss[]aV)", sink);
        Assert.assertEquals("=(string, const string[], const char, var_arg)", sink.toString());
    }

    private static void assertFailTranslateSignature(CharSequence funcName, String signature, String expectedErrorMsg) {
        try {
            sink.clear();
            FunctionFactoryDescriptor.translateSignature(funcName, signature, sink);
            Assert.fail();
        } catch (IllegalArgumentException err) {
            TestUtils.assertEquals(expectedErrorMsg, err.getMessage());
        }
    }

    private static void assertType(FunctionFactoryDescriptor descriptor, int argIndex) {
        Assert.assertEquals(ColumnType.STRING, FunctionFactoryDescriptor.toTypeTag(descriptor.getArgTypeWithFlags(argIndex)));
    }

    private static FunctionFactoryDescriptor descriptorOf(String signature) throws SqlException {
        return new FunctionFactoryDescriptor(new FunctionFactory() {
            @Override
            public String getSignature() {
                return signature;
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                throw new UnsupportedOperationException();
            }
        });
    }

    private static boolean isArray(FunctionFactoryDescriptor descriptor, int argIndex) {
        return FunctionFactoryDescriptor.isArray(descriptor.getArgTypeWithFlags(argIndex));
    }

    private static boolean isConstant(FunctionFactoryDescriptor descriptor, int argIndex) {
        return FunctionFactoryDescriptor.isConstant(descriptor.getArgTypeWithFlags(argIndex));
    }
}
