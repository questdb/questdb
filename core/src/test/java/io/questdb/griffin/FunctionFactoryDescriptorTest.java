/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

public class FunctionFactoryDescriptorTest {

    @Test
    public void testSignatureWithArrayAsAFirstArgument() throws SqlException {
        FunctionFactoryDescriptor descriptor = descriptorOf("=(S[]S)");

        Assert.assertEquals(descriptor.getSigArgCount(), 2);
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

        Assert.assertEquals(descriptor.getSigArgCount(), 2);
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

        Assert.assertEquals(descriptor.getSigArgCount(), 2);
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

        Assert.assertEquals(descriptor.getSigArgCount(), 2);
        // S
        Assert.assertFalse(isArray(descriptor, 0));
        Assert.assertFalse(isConstant(descriptor, 0));
        assertType(descriptor, 0);
        // s[]
        Assert.assertTrue(isArray(descriptor, 1));
        Assert.assertTrue(isConstant(descriptor, 1));
        assertType(descriptor, 1);
    }

    private static boolean isArray(FunctionFactoryDescriptor descriptor, int argIndex) {
        return FunctionFactoryDescriptor.isArray(descriptor.getArgTypeMask(argIndex));
    }

    private static boolean isConstant(FunctionFactoryDescriptor descriptor, int argIndex) {
        return FunctionFactoryDescriptor.isConstant(descriptor.getArgTypeMask(argIndex));
    }

    private static void assertType(FunctionFactoryDescriptor descriptor, int argIndex) {
        Assert.assertEquals(ColumnType.STRING, FunctionFactoryDescriptor.toType(descriptor.getArgTypeMask(argIndex)));
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
}
