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

package io.questdb.test.griffin.engine.functions.json;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class JsonExtractTypedFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void testExtractTimestampNonExistingPlaces() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table json_test (text varchar)");
            insert("insert into json_test values ('{\"path\": [1,2,3]}')");
            insert("insert into json_test values ('{\"path\": [1,2]}')");
            insert("insert into json_test values ('{\"path\": []]}')");
            insert("insert into json_test values ('{\"path2\": \"4\"}')");
            insert("insert into json_test values ('{\"path\": \"1on1\"}')");
            assertSqlWithTypes(
                    "x\n" +
                            "1970-01-01T00:00:00.000003Z:TIMESTAMP\n" +
                            ":TIMESTAMP\n" +
                            ":TIMESTAMP\n" +
                            ":TIMESTAMP\n" +
                            ":TIMESTAMP\n",
                    "select json_extract(text, '.path[2]')::timestamp x from json_test order by 1 desc"
            );
        });
    }

    @Test
    public void testExtractTimestampBadJson() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table json_test (text varchar)");
            insert("insert into json_test values ('{\"path\": [1,2,3]}')");
            insert("insert into json_test values ('{\"\"path\": [1,2]}')");
            insert("insert into json_test values ('{\"path\": []]}')");
            insert("insert into json_test values ('{\"path2\": \"4\"}')");
            insert("insert into json_test values ('{\"path\": \"1on1\"}')");
            assertSqlWithTypes(
                    "x\n" +
                            "1970-01-01T00:00:00.000003Z:TIMESTAMP\n" +
                            ":TIMESTAMP\n" +
                            ":TIMESTAMP\n" +
                            ":TIMESTAMP\n" +
                            ":TIMESTAMP\n",
                    "select json_extract(text, '.path[2]')::timestamp x from json_test order by 1 desc"
            );
        });
    }

}
