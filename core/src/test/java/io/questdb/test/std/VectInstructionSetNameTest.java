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

package io.questdb.test.std;

import io.questdb.std.Vect;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class VectInstructionSetNameTest {

    @Test
    public void testArmInstructionSetNames() throws Exception {
        Assert.assertEquals(" [NEON,1]", supportedInstructionSetName(1));
        Assert.assertEquals(" [SVE,11]", supportedInstructionSetName(11));
    }

    @Test
    public void testX86InstructionSetNames() throws Exception {
        Assert.assertEquals(" [Vanilla,0]", supportedInstructionSetName(0));
        Assert.assertEquals(" [SSE2,2]", supportedInstructionSetName(2));
        Assert.assertEquals(" [SSE4.1,5]", supportedInstructionSetName(5));
        Assert.assertEquals(" [AVX2,8]", supportedInstructionSetName(8));
        Assert.assertEquals(" [AVX512,10]", supportedInstructionSetName(10));
    }

    private static String supportedInstructionSetName(int inst)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final Method method = Vect.class.getDeclaredMethod("getSupportedInstructionSetName", int.class);
        method.setAccessible(true);
        return (String) method.invoke(null, inst);
    }
}
