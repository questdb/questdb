/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import org.junit.Before;
import org.junit.Test;

public class SumDoubleVecGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testAddColumn() throws Exception {
        Record[] expected = new Record[] {
                new Record() {
                    @Override
                    public double getDouble(int col) {
                        return 0.511848387;
                    }
                },
        };
        assertQuery(expected,
                "select round(avg(f),9) avg from tab",
                "create table tab as (select rnd_double(2) f from long_sequence(131))",
                null,
                "alter table tab add column b double",
                expected,
                false,
                true);

        Record[] expected2 = new Record[] {
                new Record() {
                    @Override
                    public double getDouble(int col) {
                        return col == 0 ? 0.504722 : 188.82913096423943;
                    }
                },
        };
        assertQuery(expected2,
                "select round(avg(f),6) avg, sum(b) sum from tab",
                "insert into tab select rnd_double(2), rnd_double(2) from long_sequence(469)",
                null,
                false,
                true);
    }

    @Test
    public void testAllNullThenOne() throws Exception {
        assertQuery(
                "sum\n" +
                        "NaN\n",
                "select sum(f) from tab",
                "create table tab as (select cast(null as double) f from long_sequence(33))",
                null,
                "insert into tab select 0.9822 from long_sequence(1)",
                "sum\n" +
                        "0.9822000000000001\n",
                false,
                true,
                true
        );
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery(
                "sum\n" +
                        "59.886261325258\n",
                "select round(sum(f), 12) sum from tab",
                "create table tab as (select rnd_double(2) f from long_sequence(131))",
                null,
                false,
                true,
                true
        );
    }
}