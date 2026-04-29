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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.math.SubIntFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class SubIntFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testGetLongPropagatesNullAsLongNull() throws Exception {
        // Cross-type LONG = (INT - INT) compares c6.getLong() with the
        // subtract result widened to long. When either operand is INT_NULL
        // the result must widen to LONG_NULL, not the int value INT_NULL
        // (-2_147_483_648) accidentally widened to a regular long. Java
        // filter previously returned INT_NULL from SubIntFunc.getLong(),
        // which silently widened to long -2_147_483_648 and matched any
        // c6 column row holding that exact value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (c4 INT, c6 LONG)");
            execute("INSERT INTO x VALUES " +
                    "(NULL, NULL), " +
                    "(NULL, -2147483648), " +
                    "(5, 562940)");
            assertQueryNoLeakCheck(
                    "c4\tc6\nnull\tnull\n5\t562940\n",
                    "SELECT * FROM x WHERE c6 = (562945 - c4)",
                    null,
                    true,
                    false
            );
        });
    }

    @Test
    public void testLeftNan() throws Exception {
        assertQuery(
                """
                        column
                        null
                        """,
                "SELECT null - 5"
        );
    }

    @Test
    public void testNegative() throws Exception {
        assertQuery("column\n-7\n", "SELECT -3-4");
        assertQuery("column\n-7\n", "SELECT -3- 4");
        assertQuery("column\n-7\n", "SELECT -3 -4");
    }

    @Test
    public void testRightNan() throws Exception {
        assertQuery("column\nnull\n", "SELECT 123 - null");
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery("column\n2\n", "SELECT 10 - 8");
    }

    @Test
    public void testUnderflow() throws Exception {
        assertQuery("column\n-2147483650\n", "SELECT -2147483648 - 2");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new SubIntFunctionFactory();
    }
}
