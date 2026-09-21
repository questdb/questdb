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

public class MaxDateVecGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAddColumn() throws Exception {
        // fix page frame size, because it affects AVG accuracy
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 10_000);

        assertQuery("select round(avg(f),9) avg from tab")
                .ddl("create table tab as (select rnd_int(-55, 9009, 2) f from long_sequence(131))")
                .mutateWith("alter table tab add column b date")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        avg
                        5261.376146789
                        """, """
                        avg
                        5261.376146789
                        """);

        assertQuery("select round(avg(f),6) avg, max(b) max from tab")
                .ddl("insert into tab select rnd_int(2, 10, 2), rnd_long(16772, 88965, 4) from long_sequence(78057)")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        avg\tmax
                        14.792007\t1970-01-01T00:01:28.964Z
                        """);
    }

    @Test
    public void testAllNullThenOne() throws Exception {
        assertQuery("select max(f) from tab")
                .ddl("create table tab as (select cast(null as date) f from long_sequence(33))")
                .mutateWith("insert into tab select 99999999999995L from long_sequence(1)")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        max
                        
                        """, """
                        max
                        5138-11-16T09:46:39.995Z
                        """);
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery("select max(f) from tab")
                .ddl("create table tab as (select cast(rnd_long(-55, 9009, 2) as date) f from long_sequence(131))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        max
                        1970-01-01T00:00:08.826Z
                        """);
    }
}