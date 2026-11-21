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

package io.questdb.test.griffin.engine.functions.constants;

import io.questdb.cairo.TableUtils;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class StrConstantTest {

    @Test
    public void testConstant() {
        StrConstant constant = new StrConstant("abc");
        Assert.assertTrue(constant.isConstant());
        TestUtils.assertEquals("abc", constant.getStrA(null));
        TestUtils.assertEquals("abc", constant.getStrB(null));
        Assert.assertEquals(3, constant.getStrLen(null));
    }

    @Test
    public void testQuotedConstant() {
        StrConstant constant = new StrConstant("'abc'");
        Assert.assertTrue(constant.isConstant());
        TestUtils.assertEquals("abc", constant.getStrA(null));
        TestUtils.assertEquals("abc", constant.getStrB(null));
        Assert.assertEquals(3, constant.getStrLen(null));
    }

    @Test
    public void testVarchar() {
        assertVarchar("abc", new StrConstant("'abc'"));
        assertVarchar("", StrConstant.EMPTY);
        assertVarchar("Ä€ëšŝ", new StrConstant("Ä€ëšŝ"));
        assertVarchar("a\uD83D\uDE00b", StrConstant.fromValue("a\uD83D\uDE00b"));
        assertVarchar("x".repeat(10_000), StrConstant.newInstance("x".repeat(10_000)));
    }

    @Test
    public void testVarcharNull() {
        Assert.assertNull(StrConstant.NULL.getVarcharA(null));
        Assert.assertNull(StrConstant.NULL.getVarcharB(null));
        Assert.assertEquals(TableUtils.NULL_LEN, StrConstant.NULL.getVarcharSize(null));
        Assert.assertNull(new StrConstant(null).getVarcharA(null));
    }

    private static void assertVarchar(String expected, StrConstant constant) {
        final Utf8String utf8 = new Utf8String(expected);
        final Utf8Sequence a = constant.getVarcharA(null);
        Assert.assertTrue(Utf8s.equals(utf8, a));
        Assert.assertSame(a, constant.getVarcharA(null));
        Assert.assertSame(a, constant.getVarcharB(null));
        Assert.assertEquals(utf8.size(), constant.getVarcharSize(null));
    }
}
