/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.http;

import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SqlValidationTest extends AbstractCairoTest {
    private static TestHttpClient testHttpClient;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
        // this method could be called for multiple iterations within single test
        // we have some synthetic re-runs
        testHttpClient = Misc.free(testHttpClient);
        testHttpClient = new TestHttpClient();
        configuration = new DefaultCairoConfiguration(root);
    }

    @AfterClass
    public static void tearDownStatic() {
        testHttpClient = Misc.free(testHttpClient);
        AbstractCairoTest.tearDownStatic();
    }

    @Test
    public void testValidationOk() throws Exception {
        execute("create table xyz as (select rnd_int() a from long_sequence(1000))");
        Rnd rnd = TestUtils.generateRandom(LOG);
        getSimpleTester()
                .withForceRecvFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .withForceSendFragmentationChunkSize(Math.max(1, rnd.nextInt(1024)))
                .run((HttpQueryTestBuilder.HttpClientCode) (engine, sqlExecutionContext) -> testHttpClient.assertGet(
                        "/api/v1/sql/validate",
                        "{\"query\":\"select count() from xyz\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1}",
                        "select count() from xyz",
                        "localhost",
                        9001,
                        null,
                        null,
                        null
                ));
    }
}
