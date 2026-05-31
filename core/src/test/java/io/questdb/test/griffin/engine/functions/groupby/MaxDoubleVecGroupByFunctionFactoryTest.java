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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class MaxDoubleVecGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAddColumn() throws Exception {
        // fix page frame size, because it affects AVG accuracy
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 10_000);
        assertQuery("select round(avg(f),9) avg from tab")
                .ddl("create table tab as (select rnd_double(2) f from long_sequence(131))")
                .mutateWith("alter table tab add column b double")
                .noRandomAccess()
                .expectSize()
                .returns("""
                avg
                0.511848387
                """, """
                avg
                0.511848387
                """);

        assertQuery("select round(avg(f),6) avg, max(b) max from tab")
                .ddl("insert into tab select rnd_double(2), rnd_double(2) from long_sequence(469)")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        avg\tmax
                        0.5008779999999999\t0.9997797234031688
                        """);
    }

    @Test
    public void testAllNullThenOne() throws Exception {
        assertQuery("select max(f) from tab")
                .ddl("create table tab as (select cast(null as double) f from long_sequence(33))")
                .mutateWith("insert into tab select 99092.008234 from long_sequence(1)")
                .noRandomAccess()
                .expectSize()
                .returns("""
                max
                null
                """, """
                max
                99092.008234
                """);
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery("select max(f) from tab")
                .ddl("create table tab as (select rnd_double(2) f from long_sequence(131))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        max
                        0.9884011094887449
                        """);
    }
}