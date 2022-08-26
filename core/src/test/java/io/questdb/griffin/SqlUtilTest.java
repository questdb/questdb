/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.ImplicitCastException;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SqlUtilTest {

    @Test
    public void testParseStrByte() {
        Assert.assertEquals(0, SqlUtil.parseByte(null));
        Assert.assertEquals(89, SqlUtil.parseByte("89"));
        Assert.assertEquals(-89, SqlUtil.parseByte("-89"));

        // overflow
        try {
            SqlUtil.parseByte("778");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: 778 [STRING -> BYTE] tuple: 0", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.parseByte("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: hello [STRING -> BYTE] tuple: 0", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrDouble() {
        //noinspection SimplifiableAssertion
        Assert.assertFalse(SqlUtil.parseDouble(null) == SqlUtil.parseDouble(null));
        Assert.assertEquals(9.901E62, SqlUtil.parseDouble("990.1e60"), 0.001);

        // overflow
        try {
            SqlUtil.parseDouble("1e450");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: 1e450 [STRING -> DOUBLE] tuple: 0", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.parseDouble("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: hello [STRING -> DOUBLE] tuple: 0", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrFloat() {
        //noinspection SimplifiableAssertion
        Assert.assertFalse(SqlUtil.parseFloat(null) == SqlUtil.parseFloat(null));
        Assert.assertEquals(990.1, SqlUtil.parseFloat("990.1"), 0.001);
        Assert.assertEquals(-899.23, SqlUtil.parseFloat("-899.23"), 0.001);

        // overflow
        Assert.assertTrue(Float.POSITIVE_INFINITY == SqlUtil.parseFloat("1e210"));

        // not a number
        try {
            SqlUtil.parseFloat("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: hello [STRING -> FLOAT] tuple: 0", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrInt() {
        Assert.assertEquals(Numbers.INT_NaN, SqlUtil.parseInt(null));
        Assert.assertEquals(22222123, SqlUtil.parseInt("22222123"));
        Assert.assertEquals(-2222232, SqlUtil.parseInt("-2222232"));

        // overflow
        try {
            SqlUtil.parseInt("77823232322323233");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: 77823232322323233 [STRING -> INT] tuple: 0", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.parseInt("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: hello [STRING -> INT] tuple: 0", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrLong() {
        Assert.assertEquals(Numbers.LONG_NaN, SqlUtil.parseLong(null));
        Assert.assertEquals(222221211212123L, SqlUtil.parseLong("222221211212123"));
        Assert.assertEquals(222221211212123L, SqlUtil.parseLong("222221211212123L"));
        Assert.assertEquals(-222221211212123L, SqlUtil.parseLong("-222221211212123"));
        Assert.assertEquals(-222221211212123L, SqlUtil.parseLong("-222221211212123L"));

        // overflow
        try {
            SqlUtil.parseLong("778232323223232389080898083");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: 778232323223232389080898083 [STRING -> LONG] tuple: 0", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.parseLong("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: hello [STRING -> LONG] tuple: 0", e.getFlyweightMessage());
        }
    }

    @Test
    public void testParseStrShort() {
        Assert.assertEquals(0, SqlUtil.parseShort(null));
        Assert.assertEquals(22222, SqlUtil.parseShort("22222"));
        Assert.assertEquals(-22222, SqlUtil.parseShort("-22222"));

        // overflow
        try {
            SqlUtil.parseShort("77823232323");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: 77823232323 [STRING -> SHORT] tuple: 0", e.getFlyweightMessage());
        }

        // not a number
        try {
            SqlUtil.parseShort("hello");
            Assert.fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertEquals("inconvertible value: hello [STRING -> SHORT] tuple: 0", e.getFlyweightMessage());
        }
    }
}