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

package io.questdb.griffin.engine.functions.str;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SplitPartFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new SplitPartFunctionFactory();
    }

    @Test
    public void testPositiveIndex() throws SqlException {
        assertQuery(
                "split_part\n" + "def\n",
                "select split_part('abc~@~def~@~ghi', '~@~', 2)",
                null,
                true,
                true);
        assertQuery(
                "split_part\n" + "def\n",
                "select split_part('abc,def,ghi,jkl', cast(',' as string), 2)",
                null,
                true,
                true);
    }

    @Test
    public void testNegativeIndex() throws SqlException {
        assertQuery(
                "split_part\n" + "def\n",
                "select split_part('abc~@~def~@~ghi', '~@~', -2)",
                null,
                true,
                true);
        assertQuery(
                "split_part\n" + "ghi\n",
                "select split_part('abc,def,ghi,jkl', cast(',' as string), -2)",
                null,
                true,
                true);
    }

    @Test
    public void testNullOrEmptyStr() throws SqlException {
        callCustomised(true, true, null, "~@~", 2).andAssert(null);
        callCustomised(true, true, "", ",", 2).andAssert("");
    }

    @Test
    public void testNullOrEmptyDelimiter() throws SqlException {
        callCustomised(true, true, "abc~@~def~@~ghi", null, 2).andAssert(null);
        callCustomised(true, true, "abc,def,ghi,jkl", "", 2).andAssert("");
    }

    @Test
    public void testNaNIndex() throws SqlException {
        callCustomised(true, true, "abc~@~def~@~ghi", "~@~", Numbers.INT_NaN).andAssert(null);
        callCustomised(true, true, "abc,def,ghi,jkl", ",", Numbers.INT_NaN).andAssert(null);
    }

    @Test
    public void testDynamicIndex() {
        try {
            call("abc~@~def~@~ghi", "~@~", 2);
            Assert.fail("Should fail for dynamic index param");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "index must be a constant or runtime-constant");
        }
    }

    @Test
    public void testZeroIndex() {
        try {
            callCustomised(true, true, "abc~@~def~@~ghi", "~@~", 0);
            Assert.fail("Should fail for 0 index");
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "field position must not be zero");
        }
    }
}
