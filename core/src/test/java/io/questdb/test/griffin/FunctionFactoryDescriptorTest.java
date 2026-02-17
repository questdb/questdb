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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
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
