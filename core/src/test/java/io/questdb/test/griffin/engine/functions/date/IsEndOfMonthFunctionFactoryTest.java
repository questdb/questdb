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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class IsEndOfMonthFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testNull() throws Exception {
        assertQuery("select is_end_of_month(null)")
                .expectSize()
                .returns("""
                        is_end_of_month
                        false
                        """);
    }

    @Test
    public void testTimestamp() throws Exception {
        assertQuery("select is_end_of_month('2024-02-29T00:00:00.000000Z'::timestamp)")
                .expectSize()
                .returns("""
                        is_end_of_month
                        true
                        """);

        assertQuery("select is_end_of_month('2024-02-28T00:00:00.000000Z'::timestamp)")
                .expectSize()
                .returns("""
                        is_end_of_month
                        false
                        """);

        assertQuery("select is_end_of_month('2023-02-28T00:00:00.000000Z'::timestamp)")
                .expectSize()
                .returns("""
                        is_end_of_month
                        true
                        """);

        assertQuery("select is_end_of_month('2026-04-30T00:00:00.000000Z'::timestamp)")
                .expectSize()
                .returns("""
                        is_end_of_month
                        true
                        """);

        assertQuery("select is_end_of_month('2026-04-29T00:00:00.000000Z'::timestamp)")
                .expectSize()
                .returns("""
                        is_end_of_month
                        false
                        """);

        assertQuery("select is_end_of_month('2026-01-31T00:00:00.000000Z'::timestamp)")
                .expectSize()
                .returns("""
                        is_end_of_month
                        true
                        """);

        assertQuery("select is_end_of_month('2025-12-31T00:00:00.000000Z'::timestamp)")
                .expectSize()
                .returns("""
                        is_end_of_month
                        true
                        """);
    }

    @Test
    public void testTimestampColumn() throws Exception {
        execute("create table x (ts timestamp)");
        execute("insert into x values ('2024-02-29T00:00:00.000000Z')");
        execute("insert into x values ('2024-02-28T00:00:00.000000Z')");
        execute("insert into x values (null)");
        execute("insert into x values ('2025-12-31T00:00:00.000000Z')");

        assertQuery("select ts, is_end_of_month(ts) from x")
                .expectSize()
                .returns("""
                        ts\tis_end_of_month
                        2024-02-29T00:00:00.000000Z\ttrue
                        2024-02-28T00:00:00.000000Z\tfalse
                        \tfalse
                        2025-12-31T00:00:00.000000Z\ttrue
                        """);
    }

    @Test
    public void testTimestampNs() throws Exception {
        assertQuery("select is_end_of_month('2024-02-29T00:00:00.000000123Z'::timestamp_ns)")
                .expectSize()
                .returns("""
                        is_end_of_month
                        true
                        """);

        assertQuery("select is_end_of_month('2024-02-28T00:00:00.000000123Z'::timestamp_ns)")
                .expectSize()
                .returns("""
                        is_end_of_month
                        false
                        """);
    }

    @Test
    public void testTimestampNsNull() throws Exception {
        assertQuery("select is_end_of_month(null::timestamp_ns)")
                .expectSize()
                .returns("""
                        is_end_of_month
                        false
                        """);
    }

    @Test
    public void testTimestampColumnWhereClause() throws Exception {
        execute("create table x (ts timestamp)");
        execute("insert into x values ('2024-02-29T00:00:00.000000Z')");
        execute("insert into x values ('2024-02-28T00:00:00.000000Z')");
        execute("insert into x values (null)");
        execute("insert into x values ('2025-12-31T00:00:00.000000Z')");

        assertQuery("select ts from x where is_end_of_month(ts)")
                .returns("""
                        ts
                        2024-02-29T00:00:00.000000Z
                        2025-12-31T00:00:00.000000Z
                        """);
    }
}