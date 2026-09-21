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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ExpFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void testExpDouble() throws Exception {
        assertQuery("select exp(cast (2 as double))")
                .noLeakCheck()
                .expectSize()
                .returns("exp\n" +
                        "7.38905609893065\n");
    }

    @Test
    public void testExpDoubleNaN() throws Exception {
        assertQuery("select exp(NaN)")
                .noLeakCheck()
                .expectSize()
                .returns("exp\n" +
                        "null\n");
    }

    @Test
    public void testExpDoubleNull() throws Exception {
        assertQuery("select exp(null)")
                .noLeakCheck()
                .expectSize()
                .returns("exp\n" +
                        "null\n");
    }

    @Test
    public void testExpFloat() throws Exception {
        assertQuery("select exp(cast (2 as float))")
                .noLeakCheck()
                .expectSize()
                .returns("exp\n" +
                        "7.38905609893065\n");
    }

    @Test
    public void testExpNegative() throws Exception {
        assertQuery("select exp(-2)")
                .noLeakCheck()
                .expectSize()
                .returns("exp\n" +
                        "0.1353352832366127\n");
    }
}
