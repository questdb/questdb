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

import io.questdb.griffin.SqlCompiler;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class DumpThreadStacksTest extends AbstractCairoTest {

    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertSql(
                        compiler,
                        sqlExecutionContext,
                        "select dump_thread_stacks",
                        sink,
                        "dump_thread_stacks\n" +
                                "true\n"
                );
            }
        });
        // this sleep to allow async logger to print out the values,
        // although we don't assert them it is less awkward than calling
        // the dump and see no output in the logs
        Os.sleep(500);
    }
}
