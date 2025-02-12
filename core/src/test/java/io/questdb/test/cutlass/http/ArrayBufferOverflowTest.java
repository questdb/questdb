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

import io.questdb.test.AbstractTest;
import org.junit.Test;

public class ArrayBufferOverflowTest extends AbstractTest {
    private static final TestHttpClient testHttpClient = new TestHttpClient();

    @Test
    public void testSimple() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            testHttpClient.assertGet(
                    "{\"query\":\"select rnd_double_array(6, 1, 0, 10, 10, 10, 10, 10, 10);\",\"columns\":[{\"name\":\"rnd_double_array\",\"type\":\"ARRAY\",\"dim\":6,\"elemType\":\"DOUBLE\"}],\"timestamp\":-1,\"dataset\":[[]],\"count\":1,\"error\":\"HTTP 400 (Bad request), response buffer is too small for the column value [columnName=rnd_double_array, columnIndex=0]\"}",
                    "select rnd_double_array(6, 1, 0, 10, 10, 10, 10, 10, 10);"
            );
        });
    }
}
