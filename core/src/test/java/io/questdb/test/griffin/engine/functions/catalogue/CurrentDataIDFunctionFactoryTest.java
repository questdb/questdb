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
import io.questdb.ServerMain;
import io.questdb.cairo.DataID;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CurrentDataIDFunctionFactoryTest extends AbstractBootstrapTest {

    @Test
    public void testCurrentDataID() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final StringSink sink = new StringSink();

            // Test the `current_data_id()` SQL function with a `.data_id` generated at start-up.
            try (
                    ServerMain serverMain = startWithEnvVariables();
                    SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(serverMain.getEngine())
            ) {
                Assert.assertTrue(serverMain.getEngine().getDataID().isInitialized());
                sink.put(serverMain.getEngine().getDataID().get());
                final String expected = sink.toString();
                TestUtils.assertSql(
                        serverMain.getEngine(),
                        executionContext,
                        "select current_data_id();",
                        sink,
                        "current_data_id\n" + expected + "\n"
                );
            }

            // Now that we have a full DB, we can remove the data id and make restart it as read-only.
            Assert.assertTrue(java.nio.file.Paths.get(root, "db", DataID.FILENAME).toFile().delete());
            try (
                    ServerMain serverMain = startWithEnvVariables(
                            PropertyKey.READ_ONLY_INSTANCE.getEnvVarName(), "true"
                    );
                    SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(serverMain.getEngine())
            ) {
                sink.clear();

                Assert.assertFalse(serverMain.getEngine().getDataID().isInitialized());
                TestUtils.assertSql(
                        serverMain.getEngine(),
                        executionContext,
                        "select current_data_id();",
                        sink,
                        "current_data_id\n\n"
                );
            }
        });
    }
}