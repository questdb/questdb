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
package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class OrderByWithIntervalFilterTest extends AbstractCairoTest {

    public static final String ORDER_BY_DESC = " order by ts desc";

    @Test
    public void testOrderByWithMaxTableTimestampBeyondLastInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE itest ( " +
                    "  id SYMBOL, " +
                    "  ts TIMESTAMP " +
                    ") timestamp (ts) PARTITION BY HOUR");
            execute("insert into itest " +
                    "select 'id-' || x, dateadd('m', x::int, '2023-04-06T00:00:00.000000Z') " +
                    "from long_sequence(200)");
        });

        String query = "select * " +
                "from itest " +
                "WHERE  ts in '2023-04-05T23:59:00.000000Z;60s;10m;10'";

        assertQuery("""
                        id\tts
                        id-9\t2023-04-06T00:09:00.000000Z
                        id-19\t2023-04-06T00:19:00.000000Z
                        id-29\t2023-04-06T00:29:00.000000Z
                        id-39\t2023-04-06T00:39:00.000000Z
                        id-49\t2023-04-06T00:49:00.000000Z
                        id-59\t2023-04-06T00:59:00.000000Z
                        id-69\t2023-04-06T01:09:00.000000Z
                        id-79\t2023-04-06T01:19:00.000000Z
                        id-89\t2023-04-06T01:29:00.000000Z
                        """,
                query,
                "ts",
                true,
                false
        );

        assertQuery("""
                        id\tts
                        id-89\t2023-04-06T01:29:00.000000Z
                        id-79\t2023-04-06T01:19:00.000000Z
                        id-69\t2023-04-06T01:09:00.000000Z
                        id-59\t2023-04-06T00:59:00.000000Z
                        id-49\t2023-04-06T00:49:00.000000Z
                        id-39\t2023-04-06T00:39:00.000000Z
                        id-29\t2023-04-06T00:29:00.000000Z
                        id-19\t2023-04-06T00:19:00.000000Z
                        id-9\t2023-04-06T00:09:00.000000Z
                        """,
                query + ORDER_BY_DESC,
                "ts###DESC",
                true,
                false
        );
    }

    @Test
    public void testOrderByWithMaxTableTimestampMatchingFirstInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE itest ( " +
                    "  id SYMBOL, " +
                    "  ts TIMESTAMP " +
                    ") timestamp (ts) PARTITION BY HOUR");
            execute("insert into itest " +
                    "select 'id-' || x, dateadd('m', x::int, '2023-04-06T00:00:00.000000Z') " +
                    "from long_sequence(90)");
        });

        String query = "select * " +
                "from itest " +
                "WHERE  ts in '2023-04-06T01:30:00.000000Z;60s;10m;10'";
        assertQuery("""
                        id\tts
                        id-90\t2023-04-06T01:30:00.000000Z
                        """,
                query, "ts", true, false);

        assertQuery("""
                        id\tts
                        id-90\t2023-04-06T01:30:00.000000Z
                        """,
                query + ORDER_BY_DESC, "ts###DESC", true, false);
    }

    @Test
    public void testOrderByWithMaxTableTimestampMatchingIntermediateInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE itest ( " +
                    "  id SYMBOL, " +
                    "  ts TIMESTAMP " +
                    ") timestamp (ts) PARTITION BY HOUR");
            execute("insert into itest " +
                    "select 'id-' || x, dateadd('m', x::int, '2023-04-06T00:00:00.000000Z') " +
                    "from long_sequence(90)");
        });

        String query = "select * " +
                "from itest " +
                "WHERE  ts in '2023-04-05T23:59:00.000000Z;60s;10m;15'";

        assertQuery("""
                        id\tts
                        id-9\t2023-04-06T00:09:00.000000Z
                        id-19\t2023-04-06T00:19:00.000000Z
                        id-29\t2023-04-06T00:29:00.000000Z
                        id-39\t2023-04-06T00:39:00.000000Z
                        id-49\t2023-04-06T00:49:00.000000Z
                        id-59\t2023-04-06T00:59:00.000000Z
                        id-69\t2023-04-06T01:09:00.000000Z
                        id-79\t2023-04-06T01:19:00.000000Z
                        id-89\t2023-04-06T01:29:00.000000Z
                        """,
                query, "ts", true, false);

        assertQuery("""
                        id\tts
                        id-89\t2023-04-06T01:29:00.000000Z
                        id-79\t2023-04-06T01:19:00.000000Z
                        id-69\t2023-04-06T01:09:00.000000Z
                        id-59\t2023-04-06T00:59:00.000000Z
                        id-49\t2023-04-06T00:49:00.000000Z
                        id-39\t2023-04-06T00:39:00.000000Z
                        id-29\t2023-04-06T00:29:00.000000Z
                        id-19\t2023-04-06T00:19:00.000000Z
                        id-9\t2023-04-06T00:09:00.000000Z
                        """,
                query + ORDER_BY_DESC, "ts###DESC", true, false);
    }

    @Test //end value of last interval
    public void testOrderByWithMaxTableTimestampMatchingLastInterval1() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE itest ( " +
                    "  id SYMBOL, " +
                    "  ts TIMESTAMP " +
                    ") timestamp (ts) PARTITION BY HOUR");
            execute("insert into itest " +
                    "select 'id-' || x, dateadd('m', x::int, '2023-04-06T00:00:00.000000Z') " +
                    "from long_sequence(90)");
        });

        String query = "select * " +
                "from itest " +
                "WHERE  ts in '2023-04-05T23:59:00.000000Z;60s;10m;10'";

        assertQuery("""
                        id\tts
                        id-9\t2023-04-06T00:09:00.000000Z
                        id-19\t2023-04-06T00:19:00.000000Z
                        id-29\t2023-04-06T00:29:00.000000Z
                        id-39\t2023-04-06T00:39:00.000000Z
                        id-49\t2023-04-06T00:49:00.000000Z
                        id-59\t2023-04-06T00:59:00.000000Z
                        id-69\t2023-04-06T01:09:00.000000Z
                        id-79\t2023-04-06T01:19:00.000000Z
                        id-89\t2023-04-06T01:29:00.000000Z
                        """,
                query, "ts", true, false);

        assertQuery("""
                        id\tts
                        id-89\t2023-04-06T01:29:00.000000Z
                        id-79\t2023-04-06T01:19:00.000000Z
                        id-69\t2023-04-06T01:09:00.000000Z
                        id-59\t2023-04-06T00:59:00.000000Z
                        id-49\t2023-04-06T00:49:00.000000Z
                        id-39\t2023-04-06T00:39:00.000000Z
                        id-29\t2023-04-06T00:29:00.000000Z
                        id-19\t2023-04-06T00:19:00.000000Z
                        id-9\t2023-04-06T00:09:00.000000Z
                        """,
                query + ORDER_BY_DESC, "ts###DESC", true, false);
    }

    @Test //start value of last interval
    public void testOrderByWithMaxTableTimestampMatchingLastInterval2() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE itest ( " +
                    "  id SYMBOL, " +
                    "  ts TIMESTAMP " +
                    ") timestamp (ts) PARTITION BY HOUR");
            execute("insert into itest " +
                    "select 'id-' || x, dateadd('m', x::int, '2023-04-06T00:00:00.000000Z') " +
                    "from long_sequence(90)");
        });

        String query = "select * " +
                "from itest " +
                "WHERE  ts in '2023-04-06T00:00:00.000000Z;60s;10m;10'";

        assertQuery("""
                        id\tts
                        id-10\t2023-04-06T00:10:00.000000Z
                        id-20\t2023-04-06T00:20:00.000000Z
                        id-30\t2023-04-06T00:30:00.000000Z
                        id-40\t2023-04-06T00:40:00.000000Z
                        id-50\t2023-04-06T00:50:00.000000Z
                        id-60\t2023-04-06T01:00:00.000000Z
                        id-70\t2023-04-06T01:10:00.000000Z
                        id-80\t2023-04-06T01:20:00.000000Z
                        id-90\t2023-04-06T01:30:00.000000Z
                        """,
                query, "ts", true, false);

        assertQuery("""
                        id\tts
                        id-90\t2023-04-06T01:30:00.000000Z
                        id-80\t2023-04-06T01:20:00.000000Z
                        id-70\t2023-04-06T01:10:00.000000Z
                        id-60\t2023-04-06T01:00:00.000000Z
                        id-50\t2023-04-06T00:50:00.000000Z
                        id-40\t2023-04-06T00:40:00.000000Z
                        id-30\t2023-04-06T00:30:00.000000Z
                        id-20\t2023-04-06T00:20:00.000000Z
                        id-10\t2023-04-06T00:10:00.000000Z
                        """,
                query + ORDER_BY_DESC, "ts###DESC", true, false);
    }

    @Test
    public void testOrderByWithMinTableTimestampBeyondLastInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE itest ( " +
                    "  id SYMBOL, " +
                    "  ts TIMESTAMP " +
                    ") timestamp (ts) PARTITION BY HOUR");
            execute("insert into itest " +
                    "select 'id-' || x, dateadd('m', x::int, '2023-04-06T01:30:00.000000Z') " +
                    "from long_sequence(90)");
        });

        String query = "select * " +
                "from itest " +
                "WHERE  ts in '2023-04-05T23:59:00.000000Z;60s;10m;10'";

        assertQuery("id\tts\n", query, "ts", true, false);
        assertQuery("id\tts\n", query + ORDER_BY_DESC, "ts###DESC", true, false);
    }

    @Test
    public void testOrderByWithMinTableTimestampMatchingLastInterval1() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE itest ( " +
                    "  id SYMBOL, " +
                    "  ts TIMESTAMP " +
                    ") timestamp (ts) PARTITION BY HOUR");
            execute("insert into itest " +
                    "select 'id-' || x, dateadd('m', x::int, '2023-04-06T01:29:00.000000Z') " +
                    "from long_sequence(90)");
        });

        String query = "select * " +
                "from itest " +
                "WHERE  ts in '2023-04-05T23:59:00.000000Z;60s;10m;10'";

        assertQuery("id\tts\n",
                query, "ts", true, false);

        assertQuery("id\tts\n",
                query + ORDER_BY_DESC, "ts###DESC", true, false);
    }

    @Test
    public void testOrderByWithMinTableTimestampMatchingLastInterval2() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE itest ( " +
                    "  id SYMBOL, " +
                    "  ts TIMESTAMP " +
                    ") timestamp (ts) PARTITION BY HOUR");
            execute("insert into itest " +
                    "select 'id-' || x, dateadd('m', x::int, '2023-04-06T01:28:00.000000Z') " +
                    "from long_sequence(90)");
        });

        String query = "select * " +
                "from itest " +
                "WHERE  ts in '2023-04-05T23:59:00.000000Z;60s;10m;10'";

        assertQuery("""
                        id\tts
                        id-1\t2023-04-06T01:29:00.000000Z
                        """,
                query, "ts", true, false);


        assertQuery("""
                        id\tts
                        id-1\t2023-04-06T01:29:00.000000Z
                        """,
                query + ORDER_BY_DESC, "ts###DESC", true, false);
    }


}
