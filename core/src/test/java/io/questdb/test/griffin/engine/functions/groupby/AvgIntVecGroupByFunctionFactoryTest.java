/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.test.AbstractGriffinTest;
import org.junit.Test;

public class AvgIntVecGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testAddColumn() throws Exception {

        // fix page frame size, because it affects AVG accuracy
        pageFrameMaxRows = 10_000;

        assertQuery13(
                "avg\n" +
                        "5261.376146789\n",
                "select round(avg(f),9) avg from tab",
                "create table tab as (select rnd_int(-55, 9009, 2) f from long_sequence(131))",
                null,
                "alter table tab add column b int",
                "avg\n" +
                        "5261.376146789\n",
                false,
                true
        );

        assertQuery(
                "avg\tavg2\n" +
                        "14.792006513\t528.729891188\n",
                "select round(avg(f),9) avg, round(avg(b),9) avg2 from tab",
                "insert into tab select rnd_int(2, 10, 2), rnd_int(93, 967, 4) from long_sequence(78057)",
                null,
                false,
                true
        );
    }

    @Test
    public void testAllNullThenOne() throws Exception {
        assertQuery13(
                "avg\n" +
                        "NaN\n",
                "select avg(f) from tab",
                "create table tab as (select cast(null as int) f from long_sequence(33))",
                null,
                "insert into tab select 123 from long_sequence(1)",
                "avg\n" +
                        "123.0\n",
                false,
                true
        );
    }

    @Test
    public void testSimple() throws Exception {
        // fix page frame size, because it affects AVG accuracy

        pageFrameMaxRows = 10_000;

        assertQuery(
                "avg\n" +
                        "5261.376146788991\n",
                "select avg(f) from tab",
                "create table tab as (select rnd_int(-55, 9009, 2) f from long_sequence(131))",
                null,
                false,
                true
        );
    }
}