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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class JsonExtractMemoizationTest extends AbstractCairoTest {
    @Test
    public void testJsonExtractMemoization() throws Exception {
        allowFunctionMemoization();

        execute("create table t (x varchar)");

        execute("insert into t values ('{\"byte\": 1,  \"ipv4\": \"127.0.0.1\", \"ts\": \"2000-01-01T00:00:00Z\", " +
                "\"uuid\": \"00000000-0000-0000-0000-000000000001\", \"date\": \"2000-01-01\", \"char\": \"a\", \"bool\": true, \"float\": 1.0, \"long256\": \"0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7\"}')");
        execute("insert into t values ('{\"byte\": 3,  \"ipv4\": \"127.0.0.2\", \"ts\": \"2000-01-01T00:00:01Z\", " +
                "\"uuid\": \"00000000-0000-0000-0000-000000000002\", \"date\": \"2000-01-02\", \"char\": \"b\", \"bool\": false, \"float\": 2.0, \"long256\": \"0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede8\"}')");
        execute("insert into t values ('{\"byte\": 5 , \"ipv4\": \"127.0.0.3\", \"ts\": \"2000-01-01T00:00:02Z\", " +
                "\"uuid\": \"00000000-0000-0000-0000-000000000003\", \"date\": \"2000-01-03\", \"char\": \"c\", \"bool\": false, \"float\": 3.0, \"long256\": \"0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede9\"}')");
        execute("insert into t values ('{\"byte\": 10, \"ipv4\": \"127.0.0.4\", \"ts\": \"2000-01-01T00:00:03Z\", " +
                "\"uuid\": \"00000000-0000-0000-0000-000000000004\", \"date\": \"2000-01-04\", \"char\": \"d\", \"bool\": true, \"float\": 4.0, \"long256\": \"0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdedea\"}')");

        // byte
        assertQuery("res\n" +
                        "1\n" +
                        "3\n" +
                        "5\n" +
                        "10\n",
                "select json_extract(x, '$.byte')::byte as res from t", true);

        // ipv4
        assertQuery("res\n" +
                        "127.0.0.1\n" +
                        "127.0.0.2\n" +
                        "127.0.0.3\n" +
                        "127.0.0.4\n",
                "select json_extract(x, '$.ipv4')::ipv4 as res from t",
                true);

        // timestamp
        assertQuery("res\n" +
                        "2000-01-01T00:00:00.000000Z\n" +
                        "2000-01-01T00:00:01.000000Z\n" +
                        "2000-01-01T00:00:02.000000Z\n" +
                        "2000-01-01T00:00:03.000000Z\n",
                "select json_extract(x, '$.ts')::timestamp as res from t",
                true);

        // uuid
        assertQuery("res\n" +
                        "00000000-0000-0000-0000-000000000001\n" +
                        "00000000-0000-0000-0000-000000000002\n" +
                        "00000000-0000-0000-0000-000000000003\n" +
                        "00000000-0000-0000-0000-000000000004\n",
                "select json_extract(x, '$.uuid')::uuid as res from t",
                true);

        // date
        assertQuery("res\n" +
                        "2000-01-01T00:00:00.000Z\n" +
                        "2000-01-02T00:00:00.000Z\n" +
                        "2000-01-03T00:00:00.000Z\n" +
                        "2000-01-04T00:00:00.000Z\n",
                "select json_extract(x, '$.date')::date as res from t",
                true);

        // char
        assertQuery("res\n" +
                        "a\n" +
                        "b\n" +
                        "c\n" +
                        "d\n",
                "select json_extract(x, '$.char')::char as res from t",
                true);

        // boolean
        assertQuery("res\n" +
                        "true\n" +
                        "false\n" +
                        "false\n" +
                        "true\n",
                "select json_extract(x, '$.bool')::boolean as res from t",
                true);

        // float
        assertQuery("res\n" +
                        "1.0\n" +
                        "2.0\n" +
                        "3.0\n" +
                        "4.0\n",
                "select json_extract(x, '$.float')::float as res from t",
                true);

        // long256
        assertQuery("res\n" +
                        "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7\n" +
                        "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede8\n" +
                        "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede9\n" +
                        "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdedea\n",
                "select json_extract(x, '$.long256')::long256 as res from t",
                true);
    }

    @Test
    public void testJsonExtractMemoizationByte() throws Exception {
        allowFunctionMemoization();

        execute("create table t (x varchar)");
        execute("insert into t values ('{\"a\": 1, \"b\": 2}')");
        execute("insert into t values ('{\"a\": 3, \"b\": 4}')");
        execute("insert into t values ('{\"a\": 5, \"b\": 7}')");
        execute("insert into t values ('{\"a\": 10, \"b\": 11}')");

        assertQuery("res\n" +
                        "1\n" +
                        "3\n" +
                        "5\n" +
                        "10\n",
                "select json_extract(x, '$.a')::byte as res from t", true);
        assertQuery("res\n" +
                        "2\n" +
                        "4\n" +
                        "7\n" +
                        "11\n",
                "select json_extract(x, '$.b')::byte as res from t", true);
    }
}
