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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.str.SplitPartCharFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SplitPartCharFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testDynamicIndex() throws Exception {
        assertMemoryLeak(() -> {
            try {
                call("abc,def,ghi,jkl", ',', 2);
                Assert.fail("Should fail for dynamic index param");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "index must be a constant or runtime-constant");
            }
        });
    }

    @Test
    public void testNaNIndex() throws Exception {
        assertMemoryLeak(() -> callCustomised(true, true, "abc,def,ghi,jkl", ',', Numbers.INT_NULL).andAssert(null));
    }

    @Test
    public void testNegativeIndex() throws Exception {
        assertQuery(
                "split_part\n" + "ghi\n",
                "select split_part('abc,def,ghi,jkl', ',', -2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testNullOrEmptyStr() throws Exception {
        assertMemoryLeak(() -> {
            callCustomised(true, true, null, ',', 2).andAssert(null);
            callCustomised(true, true, "", ',', 2).andAssert("");
        });
    }

    @Test
    public void testPositiveIndex() throws Exception {
        assertQuery(
                "split_part\n" + "def\n",
                "select split_part('abc,def,ghi,jkl', ',', 2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testSinkIsCleared() throws Exception {
        for (int i = 0; i < 10; i++) {
            assertQuery(
                    "split_part\n" +
                            "\n" +
                            "\n" +
                            "g\n" +
                            "j\n" +
                            "\n" +
                            "\n",
                    "select split_part(x, '.'::char, 3) from\n" +
                            "(select 'a.b' as x\n" +
                            "union select 'c.d' as x\n" +
                            "union select 'e.f.g' as x\n" +
                            "union select 'h.i.j' as x\n" +
                            "union select 'k.l' as x\n" +
                            "union select 'm.n' as x)", null, false, false
            );
        }
    }

    @Test
    public void testZeroDelimiter() throws Exception {
        assertMemoryLeak(() -> callCustomised(true, true, "abc,def,ghi,jkl", CharConstant.ZERO.getChar(null), 2).andAssert(null));
    }

    @Test
    public void testZeroIndex() throws Exception {
        assertMemoryLeak(() -> {
            try {
                callCustomised(true, true, "abc,def,ghi,jkl", ',', 0);
                Assert.fail("Should fail for 0 index");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "field position must not be zero");
            }
        });
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new SplitPartCharFunctionFactory();
    }
}
