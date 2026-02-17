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

package io.questdb.test.griffin.engine.functions.bind;

import io.questdb.griffin.engine.functions.bind.StrBindVariable;
import io.questdb.griffin.engine.functions.bind.VarcharBindVariable;
import io.questdb.std.str.Utf8String;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class StrBindVariableTest {
    @Test
    public void testNull() {
        StrBindVariable variable = new StrBindVariable();
        Assert.assertNull(variable.getStrA(null));
        Assert.assertNull(variable.getStrB(null));
        Assert.assertEquals(-1, variable.getStrLen(null));
    }

    @Test
    public void testNullVarchar() {
        VarcharBindVariable variable = new VarcharBindVariable();
        Assert.assertNull(variable.getVarcharA(null));
        Assert.assertNull(variable.getVarcharB(null));
    }

    @Test
    public void testSimple() {
        String expected = "xyz";
        StrBindVariable variable = new StrBindVariable();
        variable.setValue(expected);
        TestUtils.assertEquals(expected, variable.getStrA(null));
        TestUtils.assertEquals(expected, variable.getStrB(null));
        Assert.assertEquals(expected.length(), variable.getStrLen(null));
    }

    @Test
    public void testSimpleVarchar() {
        Utf8String expected = new Utf8String("йцукен");
        VarcharBindVariable variable = new VarcharBindVariable();
        variable.setValue(expected);
        TestUtils.assertEquals(expected, variable.getVarcharA(null));
        TestUtils.assertEquals(expected, variable.getVarcharB(null));
    }
}