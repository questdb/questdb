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
        assertQueryAndPlan("res\tcolumn\n" +
                        "1\t2\n" +
                        "3\t4\n" +
                        "5\t6\n" +
                        "10\t11\n",
                "VirtualRecord\n" +
                        "  functions: [memoize(json_extract()::byte),res+1]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n",
                "select json_extract(x, '$.byte')::byte as res, res + 1 from t",
                null,
                true,
                true);

        // ipv4
        assertQueryAndPlan("res\tcolumn\n" +
                        "127.0.0.1\t127.0.0.2\n" +
                        "127.0.0.2\t127.0.0.3\n" +
                        "127.0.0.3\t127.0.0.4\n" +
                        "127.0.0.4\t127.0.0.5\n",
                "VirtualRecord\n" +
                        "  functions: [memoize(json_extract()),res+1]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n",
                "select json_extract(x, '$.ipv4')::ipv4 as res, res + 1 from t",
                null,
                true,
                true);

        // timestamp
        assertQueryAndPlan("res\tcolumn\n" +
                        "2000-01-01T00:00:00.000000Z\t2000-01-01T00:00:00.000001Z\n" +
                        "2000-01-01T00:00:01.000000Z\t2000-01-01T00:00:01.000001Z\n" +
                        "2000-01-01T00:00:02.000000Z\t2000-01-01T00:00:02.000001Z\n" +
                        "2000-01-01T00:00:03.000000Z\t2000-01-01T00:00:03.000001Z\n",
                "VirtualRecord\n" +
                        "  functions: [memoize(json_extract()),res+1]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n",
                "select json_extract(x, '$.ts')::timestamp as res, res + 1 from t",
                null,
                true,
                true);

        // uuid
        assertQueryAndPlan("res\tres1\n" +
                        "00000000-0000-0000-0000-000000000001\t00000000-0000-0000-0000-000000000001\n" +
                        "00000000-0000-0000-0000-000000000002\t00000000-0000-0000-0000-000000000002\n" +
                        "00000000-0000-0000-0000-000000000003\t00000000-0000-0000-0000-000000000003\n" +
                        "00000000-0000-0000-0000-000000000004\t00000000-0000-0000-0000-000000000004\n",
                "VirtualRecord\n" +
                        "  functions: [memoize(json_extract()::uuid),res]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n",
                "select json_extract(x, '$.uuid')::uuid as res, res res1 from t",
                null,
                true,
                true);

        // date
        assertQueryAndPlan("res\tcolumn\n" +
                        "2000-01-01T00:00:00.000Z\t2000-01-01T00:00:00.000001Z\n" +
                        "2000-01-02T00:00:00.000Z\t2000-01-02T00:00:00.000001Z\n" +
                        "2000-01-03T00:00:00.000Z\t2000-01-03T00:00:00.000001Z\n" +
                        "2000-01-04T00:00:00.000Z\t2000-01-04T00:00:00.000001Z\n",
                "VirtualRecord\n" +
                        "  functions: [memoize(json_extract()),res+1]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n",
                "select json_extract(x, '$.date')::date as res, res + 1 from t",
                null,
                true,
                true);

        // char
        assertQueryAndPlan("res\tconcat\n" +
                        "a\tar\n" +
                        "b\tbr\n" +
                        "c\tcr\n" +
                        "d\tdr\n",
                "VirtualRecord\n" +
                        "  functions: [memoize(json_extract()::char),concat([res,'r'])]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n",
                "select json_extract(x, '$.char')::char as res, concat(res, 'r') from t",
                null,
                true,
                true);

        // boolean
        assertQueryAndPlan("res\tres1\n" +
                        "true\ttrue\n" +
                        "false\tfalse\n" +
                        "false\tfalse\n" +
                        "true\ttrue\n",
                "VirtualRecord\n" +
                        "  functions: [memoize(json_extract()),res]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n",
                "select json_extract(x, '$.bool')::boolean as res, res res1 from t",
                null,
                true,
                true);

        // float
        assertQueryAndPlan("res\tcolumn\n" +
                        "1.0\t2.0\n" +
                        "2.0\t3.0\n" +
                        "3.0\t4.0\n" +
                        "4.0\t5.0\n",
                "VirtualRecord\n" +
                        "  functions: [memoize(json_extract()),res+1]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n",
                "select json_extract(x, '$.float')::float as res, res + 1 from t",
                null,
                true,
                true);

        // long256
        assertQueryAndPlan("res\tres1\n" +
                        "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7\t0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7\n" +
                        "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede8\t0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede8\n" +
                        "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede9\t0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede9\n" +
                        "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdedea\t0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdedea\n",
                "VirtualRecord\n" +
                        "  functions: [memoize(json_extract()::long256),res]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n",
                "select json_extract(x, '$.long256')::long256 as res, res res1 from t",
                null,
                true,
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

        assertQueryAndPlan("res\tcolumn\n" +
                        "1\t2\n" +
                        "3\t4\n" +
                        "5\t6\n" +
                        "10\t11\n",
                "VirtualRecord\n" +
                        "  functions: [memoize(json_extract()::byte),res+1]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n",
                "select json_extract(x, '$.a')::byte as res, res + 1 from t",
                null,
                true,
                true);
        assertQueryAndPlan("res\tcolumn\n" +
                        "2\t3\n" +
                        "4\t5\n" +
                        "7\t8\n" +
                        "11\t12\n",
                "VirtualRecord\n" +
                        "  functions: [memoize(json_extract()::byte),res+1]\n" +
                        "    PageFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n",
                "select json_extract(x, '$.b')::byte as res, res + 1 from t",
                null,
                true,
                true);
    }
}
