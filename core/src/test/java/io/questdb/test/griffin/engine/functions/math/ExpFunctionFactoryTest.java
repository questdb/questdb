/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ExpFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void testExpDouble() throws Exception {
        assertSql("exp\n" +
                        "7.38905609893065\n",
                "select exp(cast (2 as double))");
    }

    @Test
    public void testExpFloat() throws Exception {
        assertSql("exp\n" +
                        "7.38905609893065\n",
                "select exp(cast (2 as float))");
    }

    @Test
    public void testExpNegative() throws Exception {
        assertSql("exp\n" +
                        "0.1353352832366127\n",
                "select exp(-2)");
    }

    @Test
    public void testExpDoubleNaN() throws Exception {
        assertSql("exp\n" +
                        "NaN\n",
                "select exp(NaN)");
    }

    @Test
    public void testExpDoubleNull() throws Exception {
        assertSql("exp\n" +
                        "NaN\n",
                "select exp(null)");
    }
}
