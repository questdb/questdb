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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SimulateCrashFunctionTest extends AbstractCairoTest {

    @Test
    public void testCrashDisabled() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    "simulate_crash\n" +
                            "false\n",
                    "select simulate_crash('C')"
            );

            assertSql(
                    "simulate_crash\n" +
                            "false\n",
                    "select simulate_crash('M')"
            );

            assertSql(
                    "simulate_crash\n" +
                            "false\n",
                    "select simulate_crash('E')"
            );

            assertSql(
                    "simulate_crash\n" +
                            "false\n",
                    "select simulate_crash('0')"
            );
        });
    }

    @Test
    public void testCrashEnabled() throws Exception {
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
        // select simulate_crash('C'), This is total crash, don't simulate it

        assertMemoryLeak(() -> {
            try {
                assertSql(
                        "simulate_crash\n" +
                                "false\n",
                        "select simulate_crash('M')"
                );
                Assert.fail();
            } catch (OutOfMemoryError ignore) {
            }

            try {
                assertSql(
                        "simulate_crash\n" +
                                "false\n",
                        "select simulate_crash('E')"
                );
                Assert.fail();
            } catch (CairoError ignore) {
            }

            try {
                assertSql(
                        "simulate_crash\n" +
                                "false\n",
                        "select simulate_crash('0')"
                );
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }
}
