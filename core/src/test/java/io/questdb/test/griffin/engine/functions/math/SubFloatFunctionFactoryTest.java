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
import io.questdb.griffin.engine.functions.math.SubFloatFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class SubFloatFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testIntMinusFloatResolvesAtFloatPrecision() throws Exception {
        // Before the SubFloatFunctionFactory was added, INT - FLOAT had no -(FF)
        // overload so it resolved to -(DD) and ran at f64 in the interpreter,
        // diverging from the JIT's f32 path. With the FF overload present, the
        // expression now resolves at f32 and matches the JIT.
        //
        // 16_777_217 is the smallest INT value that rounds when converted to FLOAT
        // (binary32 mantissa has 24 significand bits, so 2^24 + 1 rounds to 2^24).
        // At f32 the subtraction produces 16_777_216.0; at f64 it would have
        // produced 16_777_217.0. Casting back to INT exposes the rounding.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (i INT, f FLOAT)");
            execute("INSERT INTO t VALUES (16777217, 0)");
            assertQueryNoLeakCheck(
                    "cast\n16777216\n",
                    "SELECT (i - f)::INT FROM t",
                    null,
                    true,
                    true
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
                "SELECT null::FLOAT - 1::FLOAT"
        );
    }

    @Test
    public void testRightNan() throws Exception {
        assertQuery(
                """
                        column
                        null
                        """,
                "SELECT 1::FLOAT - null::FLOAT"
        );
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery("column\n2.5\n", "SELECT 5.5::FLOAT - 3.0::FLOAT");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new SubFloatFunctionFactory();
    }
}
