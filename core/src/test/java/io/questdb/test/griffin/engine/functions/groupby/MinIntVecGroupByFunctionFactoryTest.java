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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class MinIntVecGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAddColumn() throws Exception {
        // fix page frame size, because it affects AVG accuracy
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 10_000);
        assertQuery(
                """
                        avg
                        5261.376146789
                        """,
                "select round(avg(f),9) avg from tab",
                "create table tab as (select rnd_int(-55, 9009, 2) f from long_sequence(131))",
                null,
                "alter table tab add column b int",
                """
                        avg
                        5261.376146789
                        """,
                false,
                true,
                false
        );

        assertQuery(
                """
                        avg\tmin
                        14.792007\t93
                        """,
                "select round(avg(f),6) avg, min(b) min from tab",
                "insert into tab select rnd_int(2, 10, 2), rnd_int(93, 967, 4) from long_sequence(78057)",
                null,
                false,
                true
        );
    }

    @Test
    public void testAllNullThenOne() throws Exception {
        assertQuery("""
                        min
                        null
                        """,
                "select min(f) from tab",
                "create table tab as (select cast(null as int) f from long_sequence(33))",
                null,
                "insert into tab select 4567866 from long_sequence(1)",
                """
                        min
                        4567866
                        """,
                false,
                true,
                false
        );
    }

    @Test
    public void testMaxIntOrNullThenMaxInt() throws Exception {
        assertQuery
                ("""
                                min
                                null
                                """,
                        "select min(f) from tab",
                        "create table tab as (select cast(null as int) f from long_sequence(33))",
                        null,
                        "insert into tab select 2147483647 from long_sequence(1)",
                        """
                                min
                                2147483647
                                """,
                        false,
                        true,
                        false
                );
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery(
                """
                        min
                        761281
                        """,
                "select min(f) from tab",
                "create table tab as (select rnd_int(-78783, 123239980, 2) f from long_sequence(181))",
                null,
                false,
                true
        );
    }
}