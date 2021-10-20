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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class StrConstantTest {

    @Test
    public void testConstant() {
        StrConstant constant = new StrConstant("abc");
        Assert.assertTrue(constant.isConstant());
        TestUtils.assertEquals("abc", constant.getStr(null));
        TestUtils.assertEquals("abc", constant.getStrB(null));
        Assert.assertEquals(3, constant.getStrLen(null));

        CharSink sink = new StringSink();
        constant.getStr(null, sink);
        TestUtils.assertEquals("abc", (CharSequence) sink);
    }

    @Test
    public void testQuotedConstant() {
        StrConstant constant = new StrConstant("'abc'");
        Assert.assertTrue(constant.isConstant());
        TestUtils.assertEquals("abc", constant.getStr(null));
        TestUtils.assertEquals("abc", constant.getStrB(null));
        Assert.assertEquals(3, constant.getStrLen(null));

        CharSink sink = new StringSink();
        constant.getStr(null, sink);
        TestUtils.assertEquals("abc", (CharSequence) sink);
    }

}