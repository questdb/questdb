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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class QuoteIdentifierTest extends AbstractCairoTest {

    @Test
    public void testCreteTableWithQuotedNameAndColumns() throws Exception {
        assertQuery("quoted_table")
                .ddl("create table \"quoted_table\"(\"id\" long,\"name\" string)")
                .expectSize()
                .returns("id\tname\n");
    }

    @Test
    public void testQuotedDottedAliasInGroupBy() throws Exception {
        assertQuery("SELECT x % 2 AS \"a.b\", count() c FROM long_sequence(4) GROUP BY \"a.b\" ORDER BY 1")
                .expectSize()
                .returns("""
                        a.b\tc
                        0\t2
                        1\t2
                        """);
    }

    @Test
    public void testQuotedDottedAliasInOrderBy() throws Exception {
        assertQuery("SELECT x AS \"a.b\" FROM long_sequence(3) ORDER BY \"a.b\" DESC")
                .expectSize()
                .returns("""
                        a.b
                        3
                        2
                        1
                        """);
    }

    @Test
    public void testQuotedDottedAliasInSampleBy() throws Exception {
        assertQuery("""
                SELECT ts, x % 2 AS "a.b", count() c
                FROM (SELECT timestamp_sequence(0, 1_800_000_000) ts, x FROM long_sequence(4)) TIMESTAMP(ts)
                SAMPLE BY 1h
                ORDER BY ts, "a.b"
                """)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        ts\ta.b\tc
                        1970-01-01T00:00:00.000000Z\t0\t1
                        1970-01-01T00:00:00.000000Z\t1\t1
                        1970-01-01T01:00:00.000000Z\t0\t1
                        1970-01-01T01:00:00.000000Z\t1\t1
                        """);
    }

    @Test
    public void testQuotedDottedColumnAsJoinKey() throws Exception {
        assertQuery("""
                SELECT t1.x, t2.y
                FROM (SELECT x, x AS "a.b" FROM long_sequence(3)) t1
                JOIN (SELECT x y, x + 1 AS "t1.x" FROM long_sequence(3)) t2 ON "a.b" = "t1.x"
                ORDER BY t1.x""")
                .returns("""
                        x\ty
                        2\t1
                        3\t2
                        """);
    }
}
