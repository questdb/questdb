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

package io.questdb.griffin.engine.functions;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class TouchTableFunctionTest extends AbstractGriffinTest {
    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testTouchUpdateTouchAgain() throws Exception {
        assertQuery("touch\n" +
                        "{\"data_pages\": 80, \"index_key_pages\":1043, \"index_values_pages\": 1043}\n",
                "select touch(select * from x)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_geohash(40) g," +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                null,
                "insert into x select * from (" +
                        " select" +
                        " rnd_geohash(40)," +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(100)" +
                        ") timestamp (t)",
                "touch\n" +
                        "{\"data_pages\": 84, \"index_key_pages\":1044, \"index_values_pages\": 1044}\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testTouchTableTimeInterval() throws Exception {
        assertQuery("touch\n" +
                        "{\"data_pages\": 4, \"index_key_pages\":1024, \"index_values_pages\": 1024}\n",
                "select touch(select * from x where k in '1970-01-22')",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_geohash(40) g," +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                null,
                null, null,
                true,
                true,
                true
        );
    }

    @Test
    public void testTouchTableNoTimestampColumnSelected() throws Exception {
        try {
            assertQuery("",
                    "select touch(select g,a,b from x where k in '1970-01-22')",
                    "create table x as " +
                            "(" +
                            "select" +
                            " rnd_geohash(40) g," +
                            " rnd_double(0)*100 a," +
                            " rnd_symbol(5,4,4,1) b," +
                            " timestamp_sequence(0, 100000000000) k" +
                            " from long_sequence(20)" +
                            "), index(b) timestamp(k) partition by DAY",
                    null,
                    null, null,
                    true,
                    true,
                    true
            );
        } catch (SqlException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "query is not support page frame cursor");
        }
    }

    @Test
    public void testTouchTableThrowOnComplexFilter() throws Exception {
        try {
            assertQuery("",
                    "select touch(select * from x where k in '1970-01-22' and a > 100.0)",
                    "create table x as " +
                            "(" +
                            "select" +
                            " rnd_geohash(40) g," +
                            " rnd_double(0)*100 a," +
                            " rnd_symbol(5,4,4,1) b," +
                            " timestamp_sequence(0, 100000000000) k" +
                            " from long_sequence(20)" +
                            "), index(b) timestamp(k) partition by DAY",
                    null,
                    null, null,
                    true,
                    true,
                    true
            );
        } catch (SqlException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "query is not support page frame cursor");
        }
    }

    @Test
    public void testTouchTableTimeRange() throws Exception {
            assertQuery("touch\n" +
                            "{\"data_pages\": 20, \"index_key_pages\":1028, \"index_values_pages\": 1028}\n",
                    "select touch(select * from x where k > '1970-01-18T00:00:00.000000Z')",
                    "create table x as " +
                            "(" +
                            "select" +
                            " rnd_geohash(40) g," +
                            " rnd_double(0)*100 a," +
                            " rnd_symbol(5,4,4,1) b," +
                            " timestamp_sequence(0, 100000000000) k" +
                            " from long_sequence(20)" +
                            "), index(b) timestamp(k) partition by DAY",
                    null,
                    null, null,
                    true,
                    true,
                    true
            );
    }

}
